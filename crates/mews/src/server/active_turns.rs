use std::{path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use mews_protocol::{MessageSource, SessionId, SourceKind};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::app::Mews;
use crate::app::StartedTurn;

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
) -> Result<mews_protocol::Turn> {
    let mut mews = Mews::open_connection(root)?;
    let source = source.unwrap_or(MessageSource {
        kind: SourceKind::Client,
        id: "client".into(),
        channel_origin: None,
    });
    if source.id.is_empty() || source.id.len() > 256 {
        anyhow::bail!("message source ID must contain 1 to 256 bytes");
    }
    if !matches!(source.kind, SourceKind::Client | SourceKind::Channel) {
        anyhow::bail!("user messages may only be attributed to a client or channel");
    }
    if let Some((turn, _)) =
        mews.replay_turn_idempotent(&session_id, &idempotency_key, &prompt, &metadata, &source)?
    {
        return Ok(turn);
    }
    let session = mews.session(&session_id)?;
    let agent_slug = mews
        .agents()?
        .into_iter()
        .find(|agent| agent.id == session.agent_id)
        .context("Session Agent no longer exists")?
        .slug;
    let installation = mews.installation()?;
    if session.host_id == installation.hub_host_id {
        mews.commands(mews_store::CommandContext::system())
            .synchronize_agent_on(&agent_slug, runtime.local_host.as_ref())
            .await?;
    } else {
        let host = runtime
            .remote_hosts
            .lock()
            .await
            .get(&session.host_id)
            .cloned()
            .with_context(|| format!("Session Host {} is offline", session.host_id))?;
        mews.commands(mews_store::CommandContext::system())
            .synchronize_agent_on(&agent_slug, host.as_ref())
            .await?;
    }
    let (turn, _, created) = mews.accept_turn_idempotent(
        &session_id,
        &idempotency_key,
        prompt.clone(),
        prompt.clone(),
        metadata.clone(),
        source.clone(),
    )?;
    if !created {
        return Ok(turn);
    }
    runtime.control.event_notify.notify_waiters();

    let root = root.to_path_buf();
    let remote_hosts = Arc::clone(&runtime.remote_hosts);
    let local_host = Arc::clone(&runtime.local_host);
    let locks = Arc::clone(&runtime.control.session_locks);
    let turn_task = turn.clone();
    let tasks = Arc::clone(&runtime.control.turn_tasks);
    let task_registry = Arc::clone(&tasks);
    let notify = Arc::clone(&runtime.control.event_notify);
    let finished = Arc::new(tokio::sync::Notify::new());
    let turn_finished = Arc::clone(&finished);
    let cancellation = mews_agent::CancellationToken::new();
    let turn_cancellation = cancellation.clone();
    // Register while holding the map lock so a very fast task cannot remove
    // itself before its handle has been published.
    let mut registered_tasks = tasks.lock().await;
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
                    StartedTurn {
                        id: turn_task.id.clone(),
                        event_notify: Arc::clone(&notify),
                        cancellation: turn_cancellation.clone(),
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
                    StartedTurn {
                        id: turn_task.id.clone(),
                        event_notify: Arc::clone(&notify),
                        cancellation: turn_cancellation.clone(),
                    },
                )
                .await?;
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = result
            && !turn_cancellation.is_cancelled()
            && let Ok(mews) = Mews::open_connection(&root)
        {
            let _ = mews.fail_turn(&turn_task.id, &format!("{error:#}"));
        }
        task_registry.lock().await.remove(&turn_task.id);
        notify.notify_waiters();
        turn_finished.notify_waiters();
    });
    registered_tasks.insert(
        turn.id.clone(),
        super::TurnTask {
            cancellation,
            abort: task.abort_handle(),
            finished,
        },
    );
    drop(registered_tasks);
    Ok(turn)
}

pub(crate) async fn cancel_turn(
    control: &super::HubControl,
    _root: &Path,
    turn_id: &crate::TurnId,
) {
    let task = control.turn_tasks.lock().await.remove(turn_id);
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
