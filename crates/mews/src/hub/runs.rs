use std::{path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use mews_protocol::{MessageSource, SessionId, SourceKind};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::service::Mews;
use crate::service::StartedRun;

use super::HubRuntime;

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

pub(crate) async fn cancel_run(control: &super::HubControl, _root: &Path, run_id: &crate::RunId) {
    let task = control.run_tasks.lock().await.remove(run_id);
    if let Some(task) = task {
        let finished = task.finished.notified();
        tokio::pin!(finished);
        finished.as_mut().enable();
        task.cancellation.cancel();
        // Let cancellation cross the active Host request boundary before a
        // bounded abort fallback. This is a synchronization guard, not a
        // timing assumption.
        let _ = tokio::time::timeout(Duration::from_secs(5), finished).await;
        task.abort.abort();
    }
}
