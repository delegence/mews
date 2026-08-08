use super::*;

pub(super) async fn resolve_remote_permission(
    host: &dyn crate::host::HostControl,
    handler: Option<&dyn mews_acp::AcpPermissionHandler>,
    session_id: &crate::SessionId,
    request: mews_protocol::PermissionRequest,
) -> Result<()> {
    let permission_id = request.id.clone();
    let selected = if let Some(handler) = handler {
        let options = request
            .options
            .into_iter()
            .map(|option| {
                let kind = match option.kind.as_str() {
                    "allow_once" => mews_acp::AcpPermissionOptionKind::AllowOnce,
                    "allow_always" => mews_acp::AcpPermissionOptionKind::AllowAlways,
                    "reject_once" => mews_acp::AcpPermissionOptionKind::RejectOnce,
                    "reject_always" => mews_acp::AcpPermissionOptionKind::RejectAlways,
                    other => {
                        bail!("remote Harness returned unknown permission option kind {other:?}")
                    }
                };
                Ok(mews_acp::AcpPermissionOption {
                    option_id: option.id,
                    name: option.name,
                    kind,
                    metadata: None,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        match handler
            .request_permission(
                &mews_acp::AcpPermissionRequest {
                    session_id: session_id.to_string(),
                    tool_call: request.tool_call,
                    options,
                    metadata: None,
                },
                &mews_agent::CancellationToken::new(),
            )
            .await?
        {
            mews_acp::AcpPermissionDecision::Selected(option_id) => Some(option_id),
            mews_acp::AcpPermissionDecision::Cancelled => None,
        }
    } else {
        None
    };
    host.resolve_acp_permission(permission_id, selected).await
}

pub(super) fn checked_acp_binding(
    binding: Option<mews_protocol::AcpSessionBinding>,
    session: &crate::Session,
    harness: &mews_protocol::HarnessDescriptor,
) -> Result<Option<mews_protocol::AcpSessionBinding>> {
    let Some(binding) = binding else {
        return Ok(None);
    };
    if binding.host_id != session.host_id || binding.harness != harness.name {
        bail!("ACP Session binding does not match this Session's Host and Harness");
    }
    // A changed Host definition can point at a different executable or launch
    // contract. Never resume an ACP session created under the old definition.
    if binding.harness_definition_hash != harness.definition_hash {
        return Ok(None);
    }
    Ok(Some(binding))
}

pub(super) fn persist_local_acp_event(
    store: &mews_store::Store,
    session: &crate::Session,
    run_id: &crate::RunId,
    harness: &mews_protocol::HarnessDescriptor,
    event: &mews_acp::AcpStreamEvent,
    notify: Option<&Arc<tokio::sync::Notify>>,
) -> Result<()> {
    match event {
        mews_acp::AcpStreamEvent::AssistantDelta { delta, message_id } => {
            store.append_client_event(
                &session.id,
                crate::ClientEventKind::AssistantDelta {
                    run_id: run_id.clone(),
                    delta: delta.clone(),
                    message_id: message_id.clone(),
                },
            )?;
        }
        mews_acp::AcpStreamEvent::ProviderState(data) => {
            store.append_message(
                &session.id,
                MessageRole::Assistant,
                MessageContent::ProviderState {
                    provider: "acp".into(),
                    model: "external".into(),
                    data: data.clone(),
                },
                Value::Null,
                MessageSource {
                    kind: SourceKind::Harness,
                    id: "default".into(),
                },
            )?;
        }
        mews_acp::AcpStreamEvent::ReasoningDelta { delta, message_id } => {
            store.append_client_event(
                &session.id,
                crate::ClientEventKind::ReasoningDelta {
                    run_id: run_id.clone(),
                    delta: delta.clone(),
                    message_id: message_id.clone(),
                },
            )?;
        }
        mews_acp::AcpStreamEvent::ToolActivity {
            call_id,
            title,
            kind,
            status,
            input,
        } => {
            store.append_client_event(
                &session.id,
                crate::ClientEventKind::ToolActivity {
                    run_id: run_id.clone(),
                    activity: mews_protocol::ToolActivity {
                        call_id: call_id.clone(),
                        title: title.clone(),
                        kind: kind.clone(),
                        status: status.clone(),
                        input: input.clone(),
                    },
                },
            )?;
        }
        mews_acp::AcpStreamEvent::SessionBound {
            session_id,
            replaced,
        } => {
            store.bind_acp_session(
                &session.id,
                &session.host_id,
                &harness.name,
                &harness.definition_hash,
                session_id,
                replaced.then_some("resource_not_found"),
            )?;
        }
    }
    if let Some(notify) = notify {
        notify.notify_waiters();
    }
    Ok(())
}

pub(super) async fn persist_remote_acp_binding(
    store: &mews_store::Store,
    host: &dyn crate::host::HostControl,
    session: &crate::Session,
    harness: &mews_protocol::HarnessDescriptor,
    acknowledgement_id: String,
    acp_session_id: String,
    replaced: bool,
) -> Result<()> {
    store.bind_acp_session(
        &session.id,
        &session.host_id,
        &harness.name,
        &harness.definition_hash,
        &acp_session_id,
        replaced.then_some("resource_not_found"),
    )?;
    host.acknowledge_acp_session_binding(acknowledgement_id)
        .await
}

pub(super) fn finish_acp_run(
    store: &mews_store::Store,
    session: &crate::Session,
    run: &crate::RunId,
    harness: &mews_protocol::HarnessDescriptor,
    outcome: Result<mews_acp::AcpSessionOutcome>,
    notify: Option<Arc<tokio::sync::Notify>>,
) -> Result<String> {
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(error) => {
            let error = format!("{error:#}");
            store.finish_run(run, RunStatus::Failed, Some(&error))?;
            if let Some(notify) = notify {
                notify.notify_waiters();
            }
            return Err(anyhow::anyhow!(error));
        }
    };
    eprintln!(
        "ACP timings: spawn={}ms initialize={}ms continuation={}ms",
        outcome.timings.spawn.as_millis(),
        outcome.timings.initialize.as_millis(),
        outcome.timings.continuation.as_millis()
    );
    if outcome.stop_reason == mews_acp::AcpStopReason::Cancelled {
        store.finish_run(run, RunStatus::Cancelled, None)?;
        if let Some(notify) = notify {
            notify.notify_waiters();
        }
        bail!("ACP Harness cancelled the Session prompt");
    }
    let binding = checked_acp_binding(store.acp_session_binding(&session.id)?, session, harness)?
        .context("ACP Harness completed without persisting its Session binding")?;
    if binding.acp_session_id != outcome.session_id {
        bail!("ACP Harness completed with a different Session than its durable binding");
    }
    if !outcome.answer.is_empty() {
        store.append_message(
            &session.id,
            MessageRole::Assistant,
            MessageContent::Text {
                text: outcome.answer.clone(),
            },
            Value::Null,
            MessageSource {
                kind: SourceKind::Harness,
                id: "default".into(),
            },
        )?;
    }
    store.finish_run(run, RunStatus::Completed, None)?;
    if let Some(notify) = notify {
        notify.notify_waiters();
    }
    Ok(outcome.answer)
}
