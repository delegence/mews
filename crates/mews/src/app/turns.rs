use super::*;

use super::{
    acp::{
        AcpReasoningAggregate, checked_acp_binding, finish_acp_turn, persist_local_acp_event,
        persist_remote_acp_binding, persist_remote_acp_dispatch,
    },
    sessions::expand_prompt,
};

struct SendRequest<'a> {
    session: &'a Session,
    prompt: &'a str,
    metadata: Value,
    source: MessageSource,
    turn_id: Option<crate::TurnId>,
    prompt_is_expanded: bool,
    event_notify: Option<Arc<tokio::sync::Notify>>,
    cancellation: mews_agent::CancellationToken,
}

pub(crate) struct StartedTurn {
    pub id: crate::TurnId,
    pub event_notify: Arc<tokio::sync::Notify>,
    pub cancellation: mews_agent::CancellationToken,
}

pub(super) fn emit_runtime_signal(
    store: &Store,
    session_id: &crate::SessionId,
    turn_id: &crate::TurnId,
    payload: mews_protocol::RuntimeSignalPayload,
) -> Result<()> {
    store.emit_runtime_signal(session_id, turn_id, payload)?;
    Ok(())
}

/// Closes setup-error gaps after atomic turn acceptance. Normal runtime paths
/// finish the Turn first, in which case this guard is a no-op.
struct TurnTerminalGuard<'a> {
    store: &'a Store,
    turn_id: crate::TurnId,
    cancellation: mews_agent::CancellationToken,
}

impl Drop for TurnTerminalGuard<'_> {
    fn drop(&mut self) {
        let Ok(turn) = self.store.turn(&self.turn_id) else {
            return;
        };
        if turn.completed_at.is_some() {
            return;
        }
        let cancelled = self.cancellation.is_cancelled();
        let _ = self.store.finish_turn(
            &self.turn_id,
            if cancelled {
                TurnStatus::Cancelled
            } else {
                TurnStatus::Failed
            },
            (!cancelled).then_some("Turn execution stopped before producing a terminal outcome"),
        );
    }
}

impl Mews {
    pub fn accept_turn_idempotent(
        &self,
        session_id: &crate::SessionId,
        key: &str,
        request_prompt: String,
        resolved_prompt: String,
        metadata: Value,
        source: MessageSource,
    ) -> Result<(crate::Turn, crate::Message, bool)> {
        Ok(self.store.accept_turn_idempotent(
            session_id,
            key,
            MessageContent::Text {
                text: request_prompt,
            },
            MessageContent::Text {
                text: resolved_prompt,
            },
            metadata,
            source,
        )?)
    }

    pub fn replay_turn_idempotent(
        &self,
        session_id: &crate::SessionId,
        key: &str,
        request_prompt: &str,
        metadata: &Value,
        source: &MessageSource,
    ) -> Result<Option<(crate::Turn, crate::Message)>> {
        Ok(self.store.replay_turn_idempotent(
            session_id,
            key,
            &MessageContent::Text {
                text: request_prompt.to_owned(),
            },
            metadata,
            source,
        )?)
    }

    pub fn turn(&self, turn_id: &crate::TurnId) -> Result<crate::Turn> {
        Ok(self.store.turn(turn_id)?)
    }

    pub fn record_turn_harness(
        &self,
        turn_id: &crate::TurnId,
        descriptor: &mews_protocol::HarnessDescriptor,
    ) -> Result<()> {
        Ok(self.store.record_turn_harness(
            turn_id,
            &descriptor.name,
            &descriptor.definition_hash,
            descriptor.executable_version.as_deref(),
        )?)
    }

    pub fn cancel_turn(&self, turn_id: &crate::TurnId) -> Result<()> {
        let turn = self.store.turn(turn_id)?;
        if turn.completed_at.is_some() {
            return Ok(());
        }
        self.store
            .finish_turn(turn_id, TurnStatus::Cancelled, Some("cancelled by client"))?;
        Ok(())
    }

    pub fn fail_turn(&self, turn_id: &crate::TurnId, error: &str) -> Result<()> {
        Ok(self
            .store
            .finish_turn(turn_id, TurnStatus::Failed, Some(error))?)
    }

    pub fn subscribe_session(
        &self,
        consumer: &crate::ConsumerId,
        session: &crate::SessionId,
        kind: mews_protocol::ConsumerKind,
    ) -> Result<()> {
        Ok(self.store.subscribe_session(consumer, session, kind)?)
    }

    pub fn delete_consumer(&self, consumer: &crate::ConsumerId) -> Result<()> {
        Ok(self.store.delete_consumer(consumer)?)
    }

    pub fn unsubscribe_session(
        &self,
        consumer: &crate::ConsumerId,
        session: &crate::SessionId,
    ) -> Result<()> {
        Ok(self.store.unsubscribe_session(consumer, session)?)
    }

    pub fn client_events(
        &self,
        consumer: &crate::ConsumerId,
        limit: u16,
    ) -> Result<crate::EventBatch> {
        Ok(self.store.client_events(consumer, limit)?)
    }

    pub fn acknowledge_events(&self, consumer: &crate::ConsumerId, checkpoint: u64) -> Result<()> {
        Ok(self.store.acknowledge_events(consumer, checkpoint)?)
    }

    pub async fn send(
        &mut self,
        session: &Session,
        prompt: &str,
        metadata: Value,
    ) -> Result<String> {
        self.send_from(
            session,
            prompt,
            metadata,
            MessageSource {
                kind: SourceKind::Client,
                id: "cli".into(),
                channel_origin: None,
            },
        )
        .await
    }

    pub async fn send_from(
        &mut self,
        session: &Session,
        prompt: &str,
        metadata: Value,
        source: MessageSource,
    ) -> Result<String> {
        self.send_locally(SendRequest {
            session,
            prompt,
            metadata,
            source,
            turn_id: None,
            prompt_is_expanded: false,
            event_notify: None,
            cancellation: mews_agent::CancellationToken::new(),
        })
        .await
    }

    async fn send_locally(&mut self, request: SendRequest<'_>) -> Result<String> {
        let installation = self.installation()?;
        let environment = mews_host::LocalEnvironment::new(
            Some(self.root.clone()),
            Arc::new(self.local_tools()?),
        );
        let harnesses = mews_host::HarnessCatalog::discover(Some(&self.root))?;
        self.execute_send(
            request,
            &environment,
            &installation.hub_host_id,
            Some(&harnesses),
            None,
        )
        .await
    }

    pub(super) fn local_tools(&self) -> Result<ToolRegistry> {
        let registry = ToolRegistry::with_agent_extensions(&self.root)?;
        tokio::spawn({
            let registry = registry.clone();
            let root = self.root.clone();
            async move { registry.watch_agent_extensions(root).await }
        });
        Ok(registry)
    }

    pub async fn send_on(
        &mut self,
        session: &Session,
        prompt: &str,
        metadata: Value,
        host: &dyn HostExecutor,
    ) -> Result<String> {
        self.send_on_from(
            session,
            prompt,
            metadata,
            host,
            MessageSource {
                kind: SourceKind::Client,
                id: "cli".into(),
                channel_origin: None,
            },
        )
        .await
    }

    pub async fn send_on_from(
        &mut self,
        session: &Session,
        prompt: &str,
        metadata: Value,
        host: &dyn HostExecutor,
        source: MessageSource,
    ) -> Result<String> {
        self.execute_send(
            SendRequest {
                session,
                prompt,
                metadata,
                source,
                turn_id: None,
                prompt_is_expanded: false,
                event_notify: None,
                cancellation: mews_agent::CancellationToken::new(),
            },
            host.agent_capabilities(),
            host.host_id(),
            None,
            Some(host),
        )
        .await
    }

    pub(crate) async fn send_on_from_started(
        &mut self,
        session: &Session,
        prompt: &str,
        metadata: Value,
        host: &dyn HostExecutor,
        source: MessageSource,
        turn: StartedTurn,
    ) -> Result<String> {
        self.execute_send(
            SendRequest {
                session,
                prompt,
                metadata,
                source,
                turn_id: Some(turn.id),
                prompt_is_expanded: true,
                event_notify: Some(turn.event_notify),
                cancellation: turn.cancellation,
            },
            host.agent_capabilities(),
            host.host_id(),
            None,
            Some(host),
        )
        .await
    }

    async fn execute_send(
        &mut self,
        request: SendRequest<'_>,
        environment: &dyn AgentCapabilities,
        environment_host_id: &crate::HostId,
        harnesses: Option<&mews_host::HarnessCatalog>,
        remote_host: Option<&dyn crate::host::HostControl>,
    ) -> Result<String> {
        let SendRequest {
            session,
            prompt,
            metadata,
            source,
            turn_id,
            prompt_is_expanded,
            event_notify,
            cancellation,
        } = request;
        if session.host_id != *environment_host_id {
            bail!("session belongs to a different Host");
        }
        let request_prompt = prompt.to_owned();
        let prompt = if prompt_is_expanded {
            prompt.to_owned()
        } else {
            expand_prompt(environment, &session.working_directory, prompt).await?
        };
        if source.id.is_empty() || source.id.len() > 256 {
            bail!("message source ID must contain 1 to 256 bytes");
        }
        if !matches!(source.kind, SourceKind::Client | SourceKind::Channel) {
            bail!("user messages may only be attributed to a client or channel");
        }
        if turn_id.is_none() {
            let model = self.session_model_config(session)?;
            if model.harness == mews_runtime::MEWS_HARNESS && model.model.is_none() {
                bail!(
                    "No model is configured for this Agent. Configure one with `mews providers login` or `mews providers set-key <provider>`, then select it with `mews providers models`."
                );
            }
        }
        let accepted_turn = match turn_id {
            Some(turn_id) => turn_id,
            None => {
                let command_id = format!("direct:{}", mews_protocol::RequestId::new());
                self.store
                    .accept_turn_idempotent(
                        &session.id,
                        &command_id,
                        MessageContent::Text {
                            text: request_prompt,
                        },
                        MessageContent::Text {
                            text: prompt.clone(),
                        },
                        metadata,
                        source,
                    )?
                    .0
                    .id
            }
        };
        let _terminal_guard = TurnTerminalGuard {
            store: &self.store,
            turn_id: accepted_turn.clone(),
            cancellation: cancellation.clone(),
        };
        let turn = self.store.turn(&accepted_turn)?;
        if turn.session_id != session.id {
            bail!("Turn belongs to a different Session");
        }
        let revision = self
            .store
            .agent_revision(&session.agent_id, turn.agent_revision)?;
        let config = crate::AgentConfig::parse(&revision.config_toml)?;
        // Native turns need a router model before the user message becomes
        // durable. External Harnesses validate their own options at dispatch.
        if config.harness == mews_runtime::MEWS_HARNESS
            && self
                .session_model_config_for_revision(session, &revision)?
                .model
                .is_none()
        {
            bail!(
                "No model is configured for this Agent. Configure one with `mews providers login` or `mews providers set-key <provider>`, then select it with `mews providers models`."
            );
        }
        let harness_descriptor = if let Some(host) = remote_host {
            host.harness_descriptor(&config.harness).with_context(|| {
                format!(
                    "Harness {:?} is not published by this bound Host",
                    config.harness
                )
            })?
        } else {
            harnesses
                .context("local Host Harness catalog is unavailable")?
                .descriptors()
                .into_iter()
                .find(|descriptor| descriptor.name == config.harness)
                .with_context(|| {
                    format!("Harness {:?} is not published by this Host", config.harness)
                })?
        };
        if !harness_descriptor.availability.ready() {
            bail!(
                "Harness {:?} is not ready on this bound Host: {}",
                config.harness,
                harness_descriptor
                    .availability
                    .detail
                    .clone()
                    .unwrap_or_else(|| "setup or refresh is required".into())
            );
        }
        if config.harness != mews_runtime::MEWS_HARNESS && !harness_descriptor.supports_continuation
        {
            bail!(
                "Harness {:?} cannot provide the persistent ACP Session required by MEWS",
                config.harness
            );
        }
        let acp = if config.harness == mews_runtime::MEWS_HARNESS || remote_host.is_some() {
            None
        } else {
            let catalog = harnesses.with_context(|| {
                format!(
                    "Harness {:?} is not configured on this bound Host",
                    config.harness
                )
            })?;
            let launch = catalog.launch(&self.root, &config.harness)?;
            let mut acp = mews_acp::AcpHarnessConfig::new(launch.command)?;
            acp.environment = launch.environment;
            Some((acp, launch.instruction_channel))
        };
        let system = revision.soul.clone();
        let agent_slug = self
            .store
            .agents()?
            .into_iter()
            .find(|agent| agent.id == session.agent_id)
            .context("Session Agent no longer exists")?
            .slug;
        let result = async {
            if config.harness != mews_runtime::MEWS_HARNESS && remote_host.is_none() {
            let turn = accepted_turn.clone();
            self.record_turn_harness(&turn, &harness_descriptor)?;
            let recovery_prompt = super::native::canonical_acp_prompt(&self.store, session, "")?;
            let (acp, launch_channel) = acp.context("local ACP Harness launch is unavailable")?;
            let skills = mews_host::resources::snapshot_agent_skills(&self.root, &agent_slug)?;
            let context = mews_protocol::AcpContextSnapshot {
                version: mews_protocol::ACP_CONTEXT_VERSION,
                agent_slug: agent_slug.clone(),
                soul: system.clone(),
                skills: skills
                    .iter()
                    .map(|skill| mews_protocol::AcpSkillInventoryItem {
                        name: skill.name.clone(),
                        description: skill.description.clone(),
                        hash: skill.hash.clone(),
                    })
                    .collect(),
            };
            // Replacements, including a failed Resume recovery, always use
            // this current Host snapshot. A successful Resume never sends it.
            let context_text = context.render().map_err(anyhow::Error::msg)?;
            let channel = launch_channel;
            let binding_context = mews_protocol::AcpBindingContext {
                version: context.version,
                hash: mews_protocol::AcpContextSnapshot::hash_rendered(&context_text),
                channel,
                text: context_text.clone(),
            };
            let previous_harness = self
                .store
                .previous_turn_harness(&session.id, &turn)?;
            let transition = checked_acp_binding(
                self.store.acp_session_binding(&session.id)?,
                session,
                &harness_descriptor,
                Some(&binding_context),
                previous_harness.as_deref(),
            )?;
            let mut reasoning = AcpReasoningAggregate::default();
            let operation_id = self.store.schedule_effect(
                &session.id,
                &turn,
                mews_protocol::EffectRequest::AcpPrompt {
                    host_id: session.host_id.clone(),
                    harness: harness_descriptor.name.clone(),
                },
            )?;
            // This commit authorizes the irreversible ACP prompt. If Hub stops
            // after this point, recovery must classify the outcome as uncertain.
            self.store
                .mark_effect_started(&session.id, &turn, &operation_id)?;
            let prompt_dispatched = true;
            let outcome = mews_acp::execute_acp_turn(mews_acp::AcpTurnRequest {
                config: acp,
                cwd: session.working_directory.clone(),
                harness_options: config.harness_options.clone(),
                session: mews_acp::AcpSessionRequest {
                    agent_id: session.agent_id.clone(),
                    agent_slug: agent_slug.clone(),
                    transition: transition.clone(),
                    prompt: prompt.clone(),
                    recovery_prompt,
                    context_text: context_text.clone(),
                    instruction_channel: channel,
                    skills: skills
                        .into_iter()
                        .map(|skill| mews_acp::AcpSkill {
                            name: skill.name,
                            description: skill.description,
                            hash: skill.hash,
                            content: skill.content,
                        })
                        .collect(),
                    hook_metadata: Some(mews_acp::AcpHookMetadata {
                        mews_session_id: session.id.to_string(),
                        turn_id: turn.to_string(),
                        harness: harness_descriptor.name.clone(),
                        context_hash: mews_protocol::AcpContextSnapshot::hash_rendered(
                            &context_text,
                        ),
                        context_channel: channel,
                        invoke_turn_start: true,
                    }),
                },
                environment,
                allowed_tools: &config.tools,
                cancellation: cancellation.clone(),
                events: &mut |event| {
                    if let mews_acp::AcpStreamEvent::SessionBound {
                        session_id,
                        transition,
                        ..
                    } = &event
                    {
                        self.store.bind_acp_session_with_observations(
                            &session.id,
                            &session.host_id,
                            &harness_descriptor.name,
                            &harness_descriptor.definition_hash,
                            session_id,
                            transition,
                            &context,
                            &context_text,
                            channel,
                            channel != mews_protocol::AcpInstructionChannel::FirstPrompt,
                            turn.clone(),
                        )?;
                    }
                    persist_local_acp_event(
                        &self.store,
                        session,
                        &turn,
                        &harness_descriptor,
                        &event,
                        event_notify.as_ref(),
                        &mut reasoning,
                    )
                },
            })
            .await;
            self.store.finish_effect(
                &session.id,
                &turn,
                &operation_id,
                acp_effect_outcome(&outcome, &cancellation, prompt_dispatched),
            )?;
            reasoning.persist(&self.store, session, &turn)?;
            return finish_acp_turn(
                &self.store,
                session,
                &turn,
                &harness_descriptor,
                outcome,
                event_notify,
            );
        }
        if config.harness != mews_runtime::MEWS_HARNESS
            && let Some(host) = remote_host
        {
            let turn = accepted_turn.clone();
            self.record_turn_harness(&turn, &harness_descriptor)?;
            let binding = checked_acp_binding(
                self.store.acp_session_binding(&session.id)?,
                session,
                &harness_descriptor,
                None,
                self.store
                    .previous_turn_harness(&session.id, &turn)?
                    .as_deref(),
            )?;
            let recovery_prompt = super::native::canonical_acp_prompt(&self.store, session, "")?;
            let resume_context = match &binding {
                mews_protocol::AcpBindingTransition::Resume { .. } => self
                    .store
                    .acp_session_binding(&session.id)?
                    .map(|binding| mews_protocol::AcpBindingContext {
                        version: binding.context_version,
                        hash: binding.context_hash,
                        channel: binding.context_channel,
                        text: binding.context_text,
                    }),
                mews_protocol::AcpBindingTransition::New
                | mews_protocol::AcpBindingTransition::Replace { .. } => None,
            };
            let mut reasoning = AcpReasoningAggregate::default();
            let operation_id = self.store.schedule_effect(
                &session.id,
                &turn,
                mews_protocol::EffectRequest::AcpPrompt {
                    host_id: session.host_id.clone(),
                    harness: harness_descriptor.name.clone(),
                },
            )?;
            // Commit dispatch authorization before the remote Host can send.
            self.store
                .mark_effect_started(&session.id, &turn, &operation_id)?;
            let prompt_dispatched = true;
            let mut on_event = |event: mews_protocol::AcpEvent| -> Result<()> {
                match event {
                    mews_protocol::AcpEvent::PromptDispatched { .. } => {
                        unreachable!("prompt dispatch events are handled by the operation owner")
                    }
                    mews_protocol::AcpEvent::AssistantDelta {
                        event_key: _,
                        delta,
                        message_id,
                        raw: _,
                    } => {
                        emit_runtime_signal(
                            &self.store,
                            &session.id,
                            &turn,
                            mews_protocol::RuntimeSignalPayload::AssistantDelta {
                                delta,
                                message_id,
                            },
                        )?;
                    }
                    mews_protocol::AcpEvent::ProviderState { event_key, data } => {
                        self.store.append_acp_observation(
                            &session.id,
                            turn.clone(),
                            self.store
                                .acp_session_binding(&session.id)?
                                .map(|binding| binding.acp_session_id),
                            Some(event_key),
                            mews_protocol::AcpObservation::ProviderUpdate { data },
                        )?;
                    }
                    mews_protocol::AcpEvent::ReasoningDelta {
                        event_key,
                        delta,
                        message_id,
                        raw,
                    } => {
                        let _ = (event_key, raw);
                        reasoning.push(message_id.clone(), &delta);
                        emit_runtime_signal(
                            &self.store,
                            &session.id,
                            &turn,
                            mews_protocol::RuntimeSignalPayload::ReasoningDelta {
                                delta,
                                message_id,
                            },
                        )?;
                    }
                    mews_protocol::AcpEvent::ToolActivity {
                        event_key,
                        activity,
                    } => {
                        self.store.append_acp_observation(
                            &session.id,
                            turn.clone(),
                            self.store
                                .acp_session_binding(&session.id)?
                                .map(|binding| binding.acp_session_id),
                            Some(event_key),
                            mews_protocol::AcpObservation::ToolActivity {
                                activity: activity.clone(),
                            },
                        )?;
                        emit_runtime_signal(
                            &self.store,
                            &session.id,
                            &turn,
                            mews_protocol::RuntimeSignalPayload::ToolActivity { activity },
                        )?;
                    }
                    mews_protocol::AcpEvent::HookOutcome {
                        event_key,
                        hook,
                        ok,
                        detail,
                        tool,
                        call_id,
                    } => {
                        self.store.append_acp_observation(
                            &session.id,
                            turn.clone(),
                            self.store
                                .acp_session_binding(&session.id)?
                                .map(|binding| binding.acp_session_id),
                            Some(event_key),
                            mews_protocol::AcpObservation::HookOutcome {
                                hook,
                                ok,
                                detail,
                                tool,
                                call_id,
                            },
                        )?;
                    }
                    mews_protocol::AcpEvent::ContextDispatched {
                        session_id: acp_session_id,
                        ..
                    } => {
                        let binding = self
                            .store
                            .acp_session_binding(&session.id)?
                            .context("ACP context dispatched without a binding")?;
                        if binding.acp_session_id != acp_session_id {
                            bail!("ACP context dispatched for an unexpected Session");
                        }
                        self.store.mark_acp_context_dispatched_with_observation(
                            &session.id,
                            turn.clone(),
                            &acp_session_id,
                        )?;
                    }
                    mews_protocol::AcpEvent::SessionBound { .. } => {
                        unreachable!("Session binding events are handled asynchronously")
                    }
                }
                if let Some(notify) = &event_notify {
                    notify.notify_waiters();
                }
                Ok(())
            };
            let (event_tx, mut event_rx) =
                tokio::sync::mpsc::channel(crate::host::ACP_EVENT_CHANNEL_CAPACITY);
            let outcome = host.execute_acp_turn(
                crate::host::RemoteAcpTurn {
                    harness: config.harness.clone(),
                    harness_options: config.harness_options.clone(),
                    tools: config.tools.clone(),
                    cwd: session.working_directory.clone(),
                    prompt: prompt.clone(),
                    recovery_prompt,
                    agent_id: session.agent_id.clone(),
                    agent_slug: agent_slug.clone(),
                    soul: system.clone(),
                    mews_session_id: session.id.to_string(),
                    turn_id: turn.to_string(),
                    transition: binding,
                    context: resume_context,
                },
                event_tx,
                &cancellation,
            );
            tokio::pin!(outcome);
            let outcome = loop {
                tokio::select! {
                    outcome = &mut outcome => break outcome,
                    event = event_rx.recv() => {
                        if let Some(event) = event {
                            match event {
                                mews_protocol::AcpEvent::PromptDispatched { .. } => {}
                                mews_protocol::AcpEvent::SessionBound { acknowledgement_id, session_id: acp_session_id, transition, context, .. } => {
                                    persist_remote_acp_binding(
                                        &self.store, host, session, &turn, &harness_descriptor,
                                        acknowledgement_id, acp_session_id, transition, context,
                                    ).await?;
                                }
                                mews_protocol::AcpEvent::ContextDispatched { acknowledgement_id, session_id: acp_session_id, .. } => {
                                    persist_remote_acp_dispatch(
                                        &self.store, host, session, &turn,
                                        acknowledgement_id, acp_session_id,
                                    ).await?;
                                }
                                event => on_event(event)?,
                            }
                        }
                    }
                }
            };
            while let Ok(event) = event_rx.try_recv() {
                match event {
                    mews_protocol::AcpEvent::PromptDispatched { .. } => {}
                    mews_protocol::AcpEvent::SessionBound {
                        acknowledgement_id,
                        session_id: acp_session_id,
                        transition,
                        context,
                        ..
                    } => {
                        persist_remote_acp_binding(
                            &self.store,
                            host,
                            session,
                            &turn,
                            &harness_descriptor,
                            acknowledgement_id,
                            acp_session_id,
                            transition,
                            context,
                        )
                        .await?;
                    }
                    mews_protocol::AcpEvent::ContextDispatched {
                        acknowledgement_id,
                        session_id: acp_session_id,
                        ..
                    } => {
                        persist_remote_acp_dispatch(
                            &self.store,
                            host,
                            session,
                            &turn,
                            acknowledgement_id,
                            acp_session_id,
                        )
                        .await?;
                    }
                    event => on_event(event)?,
                }
            }
            self.store.finish_effect(
                &session.id,
                &turn,
                &operation_id,
                acp_effect_outcome(&outcome, &cancellation, prompt_dispatched),
            )?;
            reasoning.persist(&self.store, session, &turn)?;
            match outcome {
                Ok(outcome) => {
                    return finish_acp_turn(
                        &self.store,
                        session,
                        &turn,
                        &harness_descriptor,
                        Ok(outcome),
                        event_notify,
                    );
                }
                Err(error) => {
                    let cancelled = cancellation.is_cancelled() || mews_acp::is_cancelled(&error);
                    let error = format!("{error:#}");
                    self.store.finish_turn(
                        &turn,
                        if cancelled {
                            TurnStatus::Cancelled
                        } else {
                            TurnStatus::Failed
                        },
                        (!cancelled).then_some(error.as_str()),
                    )?;
                    if let Some(notify) = event_notify {
                        notify.notify_waiters();
                    }
                    return Err(anyhow::anyhow!(error));
                }
            }
        }
        let provider = mews_router::RouterClient::new(&self.root);
            super::native::turn_started(
                &self.store,
                &provider,
                environment,
                session,
                &revision,
                super::native::StartedTurn {
                    id: accepted_turn.clone(),
                    event_notify,
                    harness: harness_descriptor,
                    cancellation: cancellation.clone(),
                },
            )
            .await
        }
        .await;
        if let Err(error) = &result {
            let cancelled = cancellation.is_cancelled()
                || mews_agent::is_turn_cancelled(error)
                || mews_acp::is_cancelled(error);
            let detail = format!("{error:#}");
            let _ = self.store.finish_turn(
                &accepted_turn,
                if cancelled {
                    TurnStatus::Cancelled
                } else {
                    TurnStatus::Failed
                },
                (!cancelled).then_some(detail.as_str()),
            );
        }
        result
    }
}

fn acp_effect_outcome(
    outcome: &Result<mews_acp::AcpSessionOutcome>,
    cancellation: &mews_agent::CancellationToken,
    prompt_dispatched: bool,
) -> mews_store::EffectOutcome {
    match outcome {
        Ok(outcome) => mews_store::EffectOutcome::Succeeded(Some(serde_json::json!({
            "session_id": outcome.session_id,
            "stop_reason": format!("{:?}", outcome.stop_reason),
        }))),
        Err(error) if mews_agent::effect_uncertainty(error).is_some() => {
            mews_store::EffectOutcome::Uncertain(format!("{error:#}"))
        }
        Err(error)
            if prompt_dispatched
                && (cancellation.is_cancelled() || mews_acp::is_cancelled(error)) =>
        {
            mews_store::EffectOutcome::Uncertain(
                "ACP prompt was cancelled after dispatch; remote effects may have occurred".into(),
            )
        }
        Err(error) if cancellation.is_cancelled() || mews_acp::is_cancelled(error) => {
            mews_store::EffectOutcome::Failed("ACP prompt was cancelled".into())
        }
        Err(error) => mews_store::EffectOutcome::Failed(format!("{error:#}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(mews: &mut Mews, root: &Path) -> Session {
        let agent = mews.create_agent("turn-test").unwrap();
        mews.store
            .create_session(&agent.id, &mews.installation().unwrap().hub_host_id, root)
            .unwrap()
    }

    fn source() -> MessageSource {
        MessageSource {
            kind: SourceKind::Client,
            id: "test".into(),
            channel_origin: None,
        }
    }

    #[test]
    fn turn_replay_does_not_duplicate_input_or_create_another_turn() {
        let root = tempfile::tempdir().unwrap();
        let mut mews = Mews::setup(root.path(), "laptop").unwrap();
        let session = session(&mut mews, root.path());

        let (first, first_message, created) = mews
            .accept_turn_idempotent(
                &session.id,
                "same-turn",
                "/template value".into(),
                "expanded version one".into(),
                serde_json::json!({"source":"test"}),
                source(),
            )
            .unwrap();
        assert!(created);
        assert!(matches!(
            first_message.content,
            MessageContent::Text { text } if text == "expanded version one"
        ));
        assert!(
            mews.replay_turn_idempotent(
                &session.id,
                "same-turn",
                "/template value",
                &serde_json::json!({"source":"test"}),
                &source(),
            )
            .unwrap()
            .is_some()
        );
        let (replayed, _, created) = mews
            .accept_turn_idempotent(
                &session.id,
                "same-turn",
                "/template value".into(),
                "expanded version two".into(),
                serde_json::json!({"source":"test"}),
                source(),
            )
            .unwrap();

        assert!(!created);
        assert_eq!(replayed.id, first.id);
        assert_eq!(mews.store.session_entries(&session.id).unwrap().len(), 1);
        let consumer = crate::ConsumerId::new();
        mews.subscribe_session(
            &consumer,
            &session.id,
            mews_protocol::ConsumerKind::Ephemeral,
        )
        .unwrap();
        emit_runtime_signal(
            &mews.store,
            &session.id,
            &first.id,
            mews_protocol::RuntimeSignalPayload::AssistantDelta {
                delta: "chunk".into(),
                message_id: None,
            },
        )
        .unwrap();
        let signals = mews.client_events(&consumer, 10).unwrap().events;
        assert!(matches!(
            signals.as_slice(),
            [event] if matches!(&event.kind, crate::ClientEventKind::AssistantDelta { delta, .. } if delta == "chunk")
        ));
        assert_eq!(mews.store.session_entries(&session.id).unwrap().len(), 1);
        assert!(
            mews.accept_turn_idempotent(
                &session.id,
                "same-turn",
                "different prompt".into(),
                "different prompt".into(),
                serde_json::json!({"source":"test"}),
                source(),
            )
            .is_err()
        );
    }

    #[test]
    fn late_failures_and_terminal_guard_do_not_overwrite_outcomes() {
        let root = tempfile::tempdir().unwrap();
        let mut mews = Mews::setup(root.path(), "laptop").unwrap();
        let first_session = session(&mut mews, root.path());
        let (cancelled, _, _) = mews
            .accept_turn_idempotent(
                &first_session.id,
                "cancelled-turn",
                "prompt".into(),
                "prompt".into(),
                Value::Null,
                source(),
            )
            .unwrap();
        mews.cancel_turn(&cancelled.id).unwrap();
        assert!(mews.fail_turn(&cancelled.id, "late failure").is_err());
        assert_eq!(
            mews.turn(&cancelled.id).unwrap().status,
            TurnStatus::Cancelled
        );

        let second_session = mews
            .store
            .create_session(&first_session.agent_id, &first_session.host_id, root.path())
            .unwrap();
        let (failed, _, _) = mews
            .accept_turn_idempotent(
                &second_session.id,
                "failed-turn",
                "prompt".into(),
                "prompt".into(),
                Value::Null,
                source(),
            )
            .unwrap();
        {
            let _guard = TurnTerminalGuard {
                store: &mews.store,
                turn_id: failed.id.clone(),
                cancellation: mews_agent::CancellationToken::new(),
            };
            mews.fail_turn(&failed.id, "provider returned the real failure")
                .unwrap();
        }
        let failed = mews.turn(&failed.id).unwrap();
        assert_eq!(failed.status, TurnStatus::Failed);
        assert_eq!(
            failed.error.as_deref(),
            Some("provider returned the real failure")
        );
    }

    #[test]
    fn ambiguous_acp_outcome_wins_over_local_cancellation_state() {
        let cancellation = mews_agent::CancellationToken::new();
        cancellation.cancel();
        let outcome =
            Err(mews_agent::EffectUncertain::new("Host disconnected after ACP dispatch").into());

        assert!(matches!(
            acp_effect_outcome(&outcome, &cancellation, true),
            mews_store::EffectOutcome::Uncertain(reason)
                if reason.contains("Host disconnected after ACP dispatch")
        ));
    }

    #[test]
    fn definitive_acp_errors_are_not_recorded_as_uncertain() {
        let outcome = Err(anyhow::anyhow!("adapter rejected the prompt"));

        assert!(matches!(
            acp_effect_outcome(&outcome, &mews_agent::CancellationToken::new(), true),
            mews_store::EffectOutcome::Failed(reason)
                if reason.contains("adapter rejected the prompt")
        ));
    }

    #[test]
    fn cancelled_acp_prompt_is_uncertain_only_after_dispatch() {
        let cancellation = mews_agent::CancellationToken::new();
        cancellation.cancel();
        let outcome = Err(anyhow::anyhow!("cancelled"));

        assert!(matches!(
            acp_effect_outcome(&outcome, &cancellation, true),
            mews_store::EffectOutcome::Uncertain(reason)
                if reason.contains("after dispatch")
        ));
        assert!(matches!(
            acp_effect_outcome(&outcome, &cancellation, false),
            mews_store::EffectOutcome::Failed(reason)
                if reason == "ACP prompt was cancelled"
        ));
    }
}
