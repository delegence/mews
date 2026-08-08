use std::{path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use async_trait::async_trait;
use mews_protocol::{MessageSource, SessionId, SourceKind};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::service::Mews;
use crate::service::StartedRun;

use super::HubRuntime;

struct HubPermissionHandler {
    root: std::path::PathBuf,
    session_id: crate::SessionId,
    run_id: crate::RunId,
    control: super::HubControl,
}

pub(crate) fn permission_handler(
    root: &Path,
    session_id: crate::SessionId,
    run_id: crate::RunId,
    control: super::HubControl,
) -> Arc<dyn mews_acp::AcpPermissionHandler> {
    Arc::new(HubPermissionHandler {
        root: root.to_path_buf(),
        session_id,
        run_id,
        control,
    })
}

#[async_trait]
impl mews_acp::AcpPermissionHandler for HubPermissionHandler {
    async fn request_permission(
        &self,
        request: &mews_acp::AcpPermissionRequest,
        cancellation: &mews_agent::CancellationToken,
    ) -> Result<mews_acp::AcpPermissionDecision> {
        let request_id = uuid::Uuid::now_v7().to_string();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.control
            .permission_waiters
            .lock()
            .await
            .insert(request_id.clone(), sender);
        let event = mews_protocol::PermissionRequest {
            id: request_id.clone(),
            tool_call: request.tool_call.clone(),
            options: request
                .options
                .iter()
                .map(|option| mews_protocol::PermissionOption {
                    id: option.option_id.clone(),
                    name: option.name.clone(),
                    kind: match option.kind {
                        mews_acp::AcpPermissionOptionKind::AllowOnce => "allow_once",
                        mews_acp::AcpPermissionOptionKind::AllowAlways => "allow_always",
                        mews_acp::AcpPermissionOptionKind::RejectOnce => "reject_once",
                        mews_acp::AcpPermissionOptionKind::RejectAlways => "reject_always",
                    }
                    .into(),
                })
                .collect(),
        };
        if let Err(error) = crate::service::Mews::open_connection(&self.root)?
            .append_permission_request(&self.session_id, &self.run_id, event)
        {
            self.control
                .permission_waiters
                .lock()
                .await
                .remove(&request_id);
            return Err(error);
        }
        self.control.event_notify.notify_waiters();
        let selected = tokio::select! {
            _ = cancellation.cancelled() => None,
            response = receiver => response.ok().flatten(),
        };
        self.control
            .permission_waiters
            .lock()
            .await
            .remove(&request_id);
        Ok(selected.map_or(
            mews_acp::AcpPermissionDecision::Cancelled,
            mews_acp::AcpPermissionDecision::Selected,
        ))
    }
}

pub(super) async fn poll_events(
    runtime: &HubRuntime,
    root: &Path,
    consumer_id: crate::ConsumerId,
    limit: u16,
    wait_ms: u32,
) -> Result<mews_protocol::EventBatch> {
    let deadline =
        tokio::time::Instant::now() + Duration::from_millis(u64::from(wait_ms.min(30_000)));
    loop {
        let notified = runtime.control.event_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let events =
            Mews::open_connection(root)?.client_events(&consumer_id, limit.clamp(1, 500))?;
        if events.advanced || tokio::time::Instant::now() >= deadline {
            return Ok(events);
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let _ = tokio::time::timeout(remaining, notified).await;
    }
}

pub(super) async fn start_turn(
    runtime: &HubRuntime,
    root: &Path,
    idempotency_key: String,
    session_id: SessionId,
    prompt: String,
    metadata: Value,
    source: Option<MessageSource>,
) -> Result<mews_protocol::Run> {
    let (session, installation, run, created) = {
        let mews = runtime.mews.lock().await;
        let (run, created) = mews.start_run_idempotent(&session_id, &idempotency_key)?;
        (
            mews.session(&session_id)?,
            mews.installation()?,
            run,
            created,
        )
    };
    if !created {
        return Ok(run);
    }
    runtime.control.event_notify.notify_waiters();

    let source = source.unwrap_or(MessageSource {
        kind: SourceKind::Client,
        id: "client".into(),
    });
    let root = root.to_path_buf();
    let remote_hosts = Arc::clone(&runtime.remote_hosts);
    let locks = Arc::clone(&runtime.control.session_locks);
    let run_task = run.clone();
    let tasks = Arc::clone(&runtime.control.run_tasks);
    let notify = Arc::clone(&runtime.control.event_notify);
    let permission_handler = permission_handler(
        &root,
        session.id.clone(),
        run.id.clone(),
        runtime.control.clone(),
    );
    let task = tokio::task::spawn_local(async move {
        let lock = {
            let mut locks = locks.lock().await;
            Arc::clone(
                locks
                    .entry(session.id.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(()))),
            )
        };
        let _guard = lock.lock().await;
        let result = async {
            let mut turn = Mews::open_connection(&root)?;
            if session.host_id == installation.hub_host_id {
                turn.send_from_started(
                    &session,
                    &prompt,
                    metadata,
                    source,
                    StartedRun {
                        id: run_task.id.clone(),
                        event_notify: Arc::clone(&notify),
                        permission_handler: Arc::clone(&permission_handler),
                    },
                )
                .await?;
            } else {
                let host = remote_hosts
                    .lock()
                    .await
                    .get(&session.host_id)
                    .cloned()
                    .with_context(|| format!("Session Host {} is offline", session.host_id))?;
                turn.send_on_from_started(
                    &session,
                    &prompt,
                    metadata,
                    host.as_ref(),
                    source,
                    StartedRun {
                        id: run_task.id.clone(),
                        event_notify: Arc::clone(&notify),
                        permission_handler: Arc::clone(&permission_handler),
                    },
                )
                .await?;
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = result {
            if let Ok(mews) = Mews::open_connection(&root) {
                let _ = mews.fail_run(&run_task.id, &format!("{error:#}"));
            }
        }
        tasks.lock().await.remove(&run_task.id);
        notify.notify_waiters();
    });
    runtime
        .control
        .run_tasks
        .lock()
        .await
        .insert(run.id.clone(), task.abort_handle());
    Ok(run)
}
