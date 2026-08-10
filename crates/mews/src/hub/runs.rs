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
        self.control.permission_waiters.lock().await.insert(
            request_id.clone(),
            super::PermissionWaiter {
                request_id: request_id.clone(),
                session_id: self.session_id.clone(),
                run_id: self.run_id.clone(),
                sender,
            },
        );
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
        let unresolved = self
            .control
            .permission_waiters
            .lock()
            .await
            .remove(&request_id)
            .is_some();
        // The resolver persists its outcome before waking this waiter. If the
        // waiter still existed, cancellation won the race and owns persistence.
        if selected.is_none() && unresolved {
            Mews::open_connection(&self.root)?.append_permission_resolution(
                &self.session_id,
                &self.run_id,
                &request_id,
                mews_protocol::PermissionOutcome::Cancelled,
            )?;
            self.control.event_notify.notify_waiters();
        }
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
        // Long-poll waiting must not hold the handoff gate. Only the actual
        // database read participates in the Hub movement fence.
        let operation_guard = runtime.control.handoff_gate.read().await;
        if runtime
            .control
            .moving
            .load(std::sync::atomic::Ordering::Acquire)
        {
            anyhow::bail!("Hub is moving; try again after handoff");
        }
        let events =
            Mews::open_connection(root)?.client_events(&consumer_id, limit.clamp(1, 500))?;
        drop(operation_guard);
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
    let mews = Mews::open_connection(root)?;
    let source = source.unwrap_or(MessageSource {
        kind: SourceKind::Client,
        id: "client".into(),
        channel_origin: None,
    });
    let (run, created) = mews.start_run_idempotent(
        &session_id,
        &idempotency_key,
        source.channel_origin.as_ref(),
    )?;
    let session = mews.session(&session_id)?;
    let installation = mews.installation()?;
    if !created {
        return Ok(run);
    }
    runtime.control.event_notify.notify_waiters();

    let root = root.to_path_buf();
    let remote_hosts = Arc::clone(&runtime.remote_hosts);
    let local_host = Arc::clone(&runtime.local_host);
    let locks = Arc::clone(&runtime.control.session_locks);
    let run_task = run.clone();
    let tasks = Arc::clone(&runtime.control.run_tasks);
    let notify = Arc::clone(&runtime.control.event_notify);
    let finished = Arc::new(tokio::sync::Notify::new());
    let run_finished = Arc::clone(&finished);
    let permission_handler = permission_handler(
        &root,
        session.id.clone(),
        run.id.clone(),
        runtime.control.clone(),
    );
    let cancellation = mews_agent::CancellationToken::new();
    let run_cancellation = cancellation.clone();
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
                turn.send_on_from_started(
                    &session,
                    &prompt,
                    metadata,
                    local_host.as_ref(),
                    source,
                    StartedRun {
                        id: run_task.id.clone(),
                        event_notify: Arc::clone(&notify),
                        permission_handler: Arc::clone(&permission_handler),
                        cancellation: run_cancellation.clone(),
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
                        cancellation: run_cancellation.clone(),
                    },
                )
                .await?;
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = result
            && !run_cancellation.is_cancelled()
            && let Ok(mews) = Mews::open_connection(&root)
        {
            let _ = mews.fail_run(&run_task.id, &format!("{error:#}"));
        }
        tasks.lock().await.remove(&run_task.id);
        notify.notify_waiters();
        run_finished.notify_waiters();
    });
    runtime.control.run_tasks.lock().await.insert(
        run.id.clone(),
        super::RunTask {
            cancellation,
            abort: task.abort_handle(),
            finished,
        },
    );
    Ok(run)
}

pub(crate) async fn cancel_run(control: &super::HubControl, root: &Path, run_id: &crate::RunId) {
    let task = control.run_tasks.lock().await.remove(run_id);
    if let Some(task) = task {
        let finished = task.finished.notified();
        tokio::pin!(finished);
        finished.as_mut().enable();
        task.cancellation.cancel();
        let waiters = {
            let mut waiters = control.permission_waiters.lock().await;
            let ids = waiters
                .iter()
                .filter(|(_, waiter)| waiter.run_id == *run_id)
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| waiters.remove(&id))
                .collect::<Vec<_>>()
        };
        for waiter in waiters {
            if let Ok(mews) = Mews::open_connection(root) {
                let _ = mews.append_permission_resolution(
                    &waiter.session_id,
                    &waiter.run_id,
                    &waiter.request_id,
                    mews_protocol::PermissionOutcome::Cancelled,
                );
            }
            let _ = waiter.sender.send(None);
        }
        // Let cancellation cross the active Host request boundary before a
        // bounded abort fallback. This is a synchronization guard, not a
        // timing assumption.
        let _ = tokio::time::timeout(Duration::from_secs(5), finished).await;
        task.abort.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn permission_token_cancellation_persists_its_resolution() {
        let root = tempfile::tempdir().unwrap();
        let mut mews = Mews::setup(root.path(), "laptop").unwrap();
        mews.create_agent("coder").unwrap();
        let session = mews.start_session("coder", root.path()).await.unwrap();
        let run = mews.start_run(&session.id).unwrap();
        let consumer = crate::ConsumerId::new();
        mews.subscribe_session(&consumer, &session.id, mews_protocol::ConsumerKind::Durable)
            .unwrap();
        let control = super::super::HubControl {
            moving: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            handoff_gate: Arc::new(tokio::sync::RwLock::new(())),
            session_locks: Arc::new(Mutex::new(std::collections::HashMap::new())),
            run_tasks: Arc::new(Mutex::new(std::collections::HashMap::new())),
            event_notify: Arc::new(tokio::sync::Notify::new()),
            permission_waiters: Arc::new(Mutex::new(std::collections::HashMap::new())),
        };
        let handler = permission_handler(
            root.path(),
            session.id.clone(),
            run.id.clone(),
            control.clone(),
        );
        let cancellation = mews_agent::CancellationToken::new();
        let pending = tokio::spawn({
            let cancellation = cancellation.clone();
            async move {
                handler
                    .request_permission(
                        &mews_acp::AcpPermissionRequest {
                            session_id: "native-session".into(),
                            tool_call: serde_json::json!({"toolCallId": "call-1"}),
                            options: vec![],
                            metadata: None,
                        },
                        &cancellation,
                    )
                    .await
            }
        });
        while control.permission_waiters.lock().await.is_empty() {
            tokio::task::yield_now().await;
        }

        cancellation.cancel();

        assert_eq!(
            pending.await.unwrap().unwrap(),
            mews_acp::AcpPermissionDecision::Cancelled
        );
        assert!(control.permission_waiters.lock().await.is_empty());
        let events = mews.client_events(&consumer, 100).unwrap().events;
        let request_id = events
            .iter()
            .find_map(|event| match &event.kind {
                mews_protocol::ClientEventKind::PermissionRequested { request, .. } => {
                    Some(request.id.clone())
                }
                _ => None,
            })
            .unwrap();
        assert!(events.iter().any(|event| matches!(
            &event.kind,
            mews_protocol::ClientEventKind::PermissionResolved {
                request_id: resolved,
                outcome: mews_protocol::PermissionOutcome::Cancelled,
                ..
            } if resolved == &request_id
        )));
    }
}
