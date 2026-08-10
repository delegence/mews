use super::*;

const MAX_DURABLE_ACP_REASONING_BYTES: usize = 64 * 1024;

#[derive(Default)]
pub(super) struct AcpReasoningAggregate {
    items: std::collections::BTreeMap<Option<String>, String>,
    bytes: usize,
}

impl AcpReasoningAggregate {
    pub(super) fn push(&mut self, message_id: Option<String>, delta: &str) {
        let remaining = MAX_DURABLE_ACP_REASONING_BYTES.saturating_sub(self.bytes);
        if remaining == 0 {
            return;
        }
        let end = delta
            .char_indices()
            .map(|(index, _)| index)
            .take_while(|index| *index <= remaining)
            .last()
            .unwrap_or(0);
        let chunk = if delta.len() <= remaining {
            delta
        } else {
            &delta[..end]
        };
        self.items.entry(message_id).or_default().push_str(chunk);
        self.bytes += chunk.len();
    }

    pub(super) fn persist(
        self,
        store: &mews_store::Store,
        session: &crate::Session,
        run: &crate::RunId,
    ) -> Result<()> {
        for (index, (message_id, text)) in self.items.into_iter().enumerate() {
            store.append_reasoning(
                &session.id,
                run,
                text,
                mews_protocol::ReasoningVisibility::Visible,
                mews_protocol::ReasoningProvenance::Harness {
                    harness: "acp".into(),
                    message_id,
                },
                Some(format!("reasoning_completed:{run}:{index}")),
            )?;
        }
        Ok(())
    }
}

pub(super) fn checked_acp_binding(
    binding: Option<mews_protocol::AcpSessionBinding>,
    session: &crate::Session,
    harness: &mews_protocol::HarnessDescriptor,
) -> Result<mews_protocol::AcpBindingTransition> {
    let Some(binding) = binding else {
        return Ok(mews_protocol::AcpBindingTransition::New);
    };
    if binding.host_id != session.host_id || binding.harness != harness.name {
        bail!("ACP Session binding does not match this Session's Host and Harness");
    }
    if !binding.context_dispatched {
        return Ok(mews_protocol::AcpBindingTransition::Replace {
            reason: mews_protocol::AcpReplacementReason::ContextNotDispatched,
        });
    }
    // A changed Host definition can point at a different executable or launch
    // contract. Never resume an ACP session created under the old definition.
    if binding.harness_definition_hash != harness.definition_hash {
        return Ok(mews_protocol::AcpBindingTransition::Replace {
            reason: mews_protocol::AcpReplacementReason::HarnessDefinitionChanged,
        });
    }
    Ok(mews_protocol::AcpBindingTransition::Resume {
        acp_session_id: binding.acp_session_id,
    })
}

pub(super) fn persist_local_acp_event(
    store: &mews_store::Store,
    session: &crate::Session,
    run_id: &crate::RunId,
    _harness: &mews_protocol::HarnessDescriptor,
    event: &mews_acp::AcpStreamEvent,
    notify: Option<&Arc<tokio::sync::Notify>>,
    reasoning: &mut AcpReasoningAggregate,
) -> Result<()> {
    let acp_session_id = store
        .acp_session_binding(&session.id)?
        .map(|binding| binding.acp_session_id);
    match event {
        mews_acp::AcpStreamEvent::AssistantDelta {
            event_key,
            delta,
            message_id,
            raw,
        } => {
            store.append_acp_observation_with_client_event(
                &session.id,
                run_id.clone(),
                acp_session_id.clone(),
                Some(event_key.clone()),
                mews_protocol::AcpObservation::AssistantDelta {
                    delta: delta.clone(),
                    message_id: message_id.clone(),
                    raw: raw.clone(),
                },
                crate::ClientEventKind::AssistantDelta {
                    run_id: run_id.clone(),
                    delta: delta.clone(),
                    message_id: message_id.clone(),
                },
            )?;
        }
        mews_acp::AcpStreamEvent::ProviderState { event_key, data } => {
            store.append_acp_observation(
                &session.id,
                run_id.clone(),
                acp_session_id.clone(),
                Some(event_key.clone()),
                mews_protocol::AcpObservation::ProviderUpdate { data: data.clone() },
            )?;
        }
        mews_acp::AcpStreamEvent::ReasoningDelta {
            event_key,
            delta,
            message_id,
            raw,
        } => {
            let _ = (event_key, raw);
            reasoning.push(message_id.clone(), delta);
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
            event_key,
            call_id,
            title,
            kind,
            status,
            input,
        } => {
            let activity = mews_protocol::ToolActivity {
                call_id: call_id.clone(),
                title: title.clone(),
                kind: kind.clone(),
                status: status.clone(),
                input: input.clone(),
            };
            store.append_acp_observation_with_client_event(
                &session.id,
                run_id.clone(),
                acp_session_id.clone(),
                Some(event_key.clone()),
                mews_protocol::AcpObservation::ToolActivity {
                    activity: activity.clone(),
                },
                crate::ClientEventKind::ToolActivity {
                    run_id: run_id.clone(),
                    activity,
                },
            )?;
        }
        mews_acp::AcpStreamEvent::SessionBound {
            session_id,
            transition,
            ..
        } => {
            // Local callers persist bindings directly with their rendered context.
            let _ = (session_id, transition);
        }
        mews_acp::AcpStreamEvent::ContextDispatched { session_id, .. } => {
            store.mark_acp_context_dispatched_with_observation(
                &session.id,
                run_id.clone(),
                session_id,
            )?;
        }
        mews_acp::AcpStreamEvent::HookOutcome {
            event_key,
            hook,
            ok,
            detail,
            tool,
            call_id,
        } => {
            store.append_acp_observation(
                &session.id,
                run_id.clone(),
                acp_session_id.clone(),
                Some(event_key.clone()),
                mews_protocol::AcpObservation::HookOutcome {
                    hook: hook.clone(),
                    ok: *ok,
                    detail: detail.clone(),
                    tool: tool.clone(),
                    call_id: call_id.clone(),
                },
            )?;
        }
    }
    if let Some(notify) = notify {
        notify.notify_waiters();
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // remote acknowledgement contains one complete binding transition
pub(super) async fn persist_remote_acp_binding(
    store: &mews_store::Store,
    host: &dyn crate::host::HostControl,
    session: &crate::Session,
    run: &crate::RunId,
    harness: &mews_protocol::HarnessDescriptor,
    acknowledgement_id: String,
    acp_session_id: String,
    transition: mews_protocol::AcpBindingTransition,
    context: mews_protocol::AcpBindingContext,
) -> Result<()> {
    if context.text.len() > mews_protocol::MAX_ACP_CONTEXT_BYTES
        || context.hash != mews_protocol::AcpContextSnapshot::hash_rendered(&context.text)
    {
        bail!("remote Host returned an invalid ACP binding context");
    }
    let snapshot = mews_protocol::AcpContextSnapshot {
        version: context.version,
        agent_slug: String::new(),
        soul: String::new(),
        skills: Vec::new(),
    };
    store.bind_acp_session_with_observations(
        &session.id,
        &session.host_id,
        &harness.name,
        &harness.definition_hash,
        &acp_session_id,
        &transition,
        &snapshot,
        &context.text,
        context.channel,
        context.channel != mews_protocol::AcpInstructionChannel::FirstPrompt,
        run.clone(),
    )?;
    host.acknowledge_acp_session_binding(acknowledgement_id)
        .await
}

pub(super) async fn persist_remote_acp_dispatch(
    store: &mews_store::Store,
    host: &dyn crate::host::HostControl,
    session: &crate::Session,
    run: &crate::RunId,
    acknowledgement_id: String,
    acp_session_id: String,
) -> Result<()> {
    let binding = store
        .acp_session_binding(&session.id)?
        .context("ACP context dispatched without a binding")?;
    if binding.acp_session_id != acp_session_id {
        bail!("ACP context dispatched for an unexpected Session");
    }
    store.mark_acp_context_dispatched_with_observation(
        &session.id,
        run.clone(),
        &acp_session_id,
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
            if mews_acp::is_cancelled(&error) {
                store.finish_run(run, RunStatus::Cancelled, None)?;
                if let Some(notify) = notify {
                    notify.notify_waiters();
                }
                return Err(error);
            }
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
    let binding = store
        .acp_session_binding(&session.id)?
        .context("ACP Harness completed without persisting its Session binding")?;
    if binding.acp_session_id != outcome.session_id {
        bail!("ACP Harness completed with a different Session than its durable binding");
    }
    if !outcome.answer.is_empty() {
        store.append_assistant_response(
            &session.id,
            run,
            mews_protocol::AssistantResponse {
                provider: harness.name.clone(),
                model: harness
                    .models
                    .first()
                    .map_or_else(|| "default".into(), |model| model.id.clone()),
                api: "acp".into(),
                response_id: Some(outcome.session_id.clone()),
                blocks: vec![mews_protocol::AssistantResponseBlock::Text {
                    text: outcome.answer.clone(),
                }],
                usage: None,
                stop_reason: Some(format!("{:?}", outcome.stop_reason)),
            },
        )?;
    }
    store.finish_run_with_stop_reason(
        run,
        RunStatus::Completed,
        None,
        Some(&format!("{:?}", outcome.stop_reason)),
    )?;
    if let Some(notify) = notify {
        notify.notify_waiters();
    }
    Ok(outcome.answer)
}
