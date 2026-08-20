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
        turn: &crate::TurnId,
    ) -> Result<()> {
        for (index, (message_id, text)) in self.items.into_iter().enumerate() {
            store.append_reasoning(
                &session.id,
                turn,
                text,
                mews_protocol::ReasoningVisibility::Visible,
                mews_protocol::ReasoningProvenance::Harness {
                    harness: "acp".into(),
                    message_id,
                },
                Some(format!("reasoning_completed:{turn}:{index}")),
            )?;
        }
        Ok(())
    }
}

pub(super) fn checked_acp_binding(
    binding: Option<mews_protocol::AcpSessionBinding>,
    session: &crate::Session,
    harness: &mews_protocol::HarnessDescriptor,
    context: Option<&mews_protocol::AcpBindingContext>,
    previous_turn_harness: Option<&str>,
) -> Result<mews_protocol::AcpBindingTransition> {
    let Some(binding) = binding else {
        return Ok(mews_protocol::AcpBindingTransition::New);
    };
    if binding.host_id != session.host_id {
        bail!("ACP Session binding does not match this Session's Host");
    }
    if binding.harness != harness.name {
        return Ok(mews_protocol::AcpBindingTransition::Replace {
            reason: mews_protocol::AcpReplacementReason::HarnessChanged,
        });
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
    if let Some(context) = context
        && (binding.context_version != context.version
            || binding.context_hash != context.hash
            || binding.context_channel != context.channel)
    {
        return Ok(mews_protocol::AcpBindingTransition::Replace {
            reason: mews_protocol::AcpReplacementReason::InstructionContextChanged,
        });
    }
    if previous_turn_harness != Some(harness.name.as_str()) {
        return Ok(mews_protocol::AcpBindingTransition::Replace {
            reason: mews_protocol::AcpReplacementReason::HistoryDiverged,
        });
    }
    Ok(mews_protocol::AcpBindingTransition::Resume {
        acp_session_id: binding.acp_session_id,
    })
}

pub(super) fn persist_local_acp_event(
    store: &mews_store::Store,
    session: &crate::Session,
    turn_id: &crate::TurnId,
    _harness: &mews_protocol::HarnessDescriptor,
    event: &mews_acp::AcpStreamEvent,
    notify: Option<&Arc<tokio::sync::Notify>>,
    reasoning: &mut AcpReasoningAggregate,
) -> Result<()> {
    let acp_session_id = store
        .acp_session_binding(&session.id)?
        .map(|binding| binding.acp_session_id);
    match event {
        mews_acp::AcpStreamEvent::PromptDispatched { .. } => {
            // The caller owns the operation ID and records this effect boundary.
        }
        mews_acp::AcpStreamEvent::AssistantDelta {
            event_key: _,
            delta,
            message_id,
            raw: _,
        } => {
            super::turns::emit_runtime_signal(
                store,
                &session.id,
                turn_id,
                mews_protocol::RuntimeSignalPayload::AssistantDelta {
                    delta: delta.clone(),
                    message_id: message_id.clone(),
                },
            )?;
        }
        mews_acp::AcpStreamEvent::ProviderState { event_key, data } => {
            store.append_acp_observation(
                &session.id,
                turn_id.clone(),
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
            super::turns::emit_runtime_signal(
                store,
                &session.id,
                turn_id,
                mews_protocol::RuntimeSignalPayload::ReasoningDelta {
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
            store.append_acp_observation(
                &session.id,
                turn_id.clone(),
                acp_session_id.clone(),
                Some(event_key.clone()),
                mews_protocol::AcpObservation::ToolActivity {
                    activity: activity.clone(),
                },
            )?;
            super::turns::emit_runtime_signal(
                store,
                &session.id,
                turn_id,
                mews_protocol::RuntimeSignalPayload::ToolActivity { activity },
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
                turn_id.clone(),
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
                turn_id.clone(),
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
    turn: &crate::TurnId,
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
        system_instructions: String::new(),
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
        turn.clone(),
    )?;
    host.acknowledge_acp_session_binding(acknowledgement_id)
        .await
}

pub(super) async fn persist_remote_acp_dispatch(
    store: &mews_store::Store,
    host: &dyn crate::host::HostControl,
    session: &crate::Session,
    turn: &crate::TurnId,
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
        turn.clone(),
        &acp_session_id,
    )?;
    host.acknowledge_acp_session_binding(acknowledgement_id)
        .await
}

pub(super) fn finish_acp_turn(
    store: &mews_store::Store,
    session: &crate::Session,
    turn: &crate::TurnId,
    harness: &mews_protocol::HarnessDescriptor,
    model: Option<&str>,
    outcome: Result<mews_acp::AcpSessionOutcome>,
    notify: Option<Arc<tokio::sync::Notify>>,
) -> Result<String> {
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(error) => {
            if mews_acp::is_cancelled(&error) {
                store.finish_turn(turn, TurnStatus::Cancelled, None)?;
                if let Some(notify) = notify {
                    notify.notify_waiters();
                }
                return Err(error);
            }
            let error = format!("{error:#}");
            store.finish_turn(turn, TurnStatus::Failed, Some(&error))?;
            if let Some(notify) = notify {
                notify.notify_waiters();
            }
            return Err(anyhow::anyhow!(error));
        }
    };
    eprintln!(
        "ACP timings: queue={}ms spawn={}ms initialize={}ms continuation={}ms first_update={:?}ms first_token={:?}ms prompt={}ms total={}ms",
        outcome.timings.queue_ms,
        outcome.timings.spawn_ms,
        outcome.timings.initialize_ms,
        outcome.timings.continuation_ms,
        outcome.timings.prompt_to_first_update_ms,
        outcome.timings.prompt_to_first_token_ms,
        outcome.timings.prompt_ms,
        outcome.timings.total_ms,
    );
    if outcome.stop_reason == mews_acp::AcpStopReason::Cancelled {
        store.finish_turn(turn, TurnStatus::Cancelled, None)?;
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
            turn,
            mews_protocol::AssistantResponse {
                provider: harness.name.clone(),
                model: acp_response_model(model).into(),
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
    store.finish_turn_with_stop_reason(
        turn,
        TurnStatus::Completed,
        None,
        Some(&format!("{:?}", outcome.stop_reason)),
    )?;
    if let Some(notify) = notify {
        notify.notify_waiters();
    }
    Ok(outcome.answer)
}

fn acp_response_model(selected_model: Option<&str>) -> &str {
    selected_model.unwrap_or("default")
}

#[cfg(test)]
mod tests {
    use super::acp_response_model;

    #[test]
    fn acp_response_model_uses_the_selected_model() {
        assert_eq!(acp_response_model(Some("gpt-5.6-luna")), "gpt-5.6-luna");
        assert_eq!(acp_response_model(None), "default");
    }
}
