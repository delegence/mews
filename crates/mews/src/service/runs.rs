use super::*;

use super::{
    acp_execution::{
        AcpReasoningAggregate, checked_acp_binding, finish_acp_run, persist_local_acp_event,
        persist_remote_acp_binding, persist_remote_acp_dispatch,
    },
    sessions::expand_prompt,
};

struct SendRequest<'a> {
    session: &'a Session,
    prompt: &'a str,
    metadata: Value,
    source: MessageSource,
    run_id: Option<crate::RunId>,
    event_notify: Option<Arc<tokio::sync::Notify>>,
    cancellation: mews_agent::CancellationToken,
}

pub(crate) struct StartedRun {
    pub id: crate::RunId,
    pub event_notify: Arc<tokio::sync::Notify>,
    pub cancellation: mews_agent::CancellationToken,
}

impl Mews {
    pub fn start_run(&self, session_id: &crate::SessionId) -> Result<crate::Run> {
        Ok(self.store.start_run(session_id)?)
    }

    pub fn start_run_idempotent(
        &self,
        session_id: &crate::SessionId,
        key: &str,
        channel_origin: Option<&crate::ChannelOrigin>,
    ) -> Result<(crate::Run, bool)> {
        Ok(self
            .store
            .start_run_idempotent(session_id, key, channel_origin)?)
    }

    pub fn run(&self, run_id: &crate::RunId) -> Result<crate::Run> {
        Ok(self.store.run(run_id)?)
    }

    pub fn record_run_harness(
        &self,
        run_id: &crate::RunId,
        descriptor: &mews_protocol::HarnessDescriptor,
    ) -> Result<()> {
        Ok(self.store.record_run_harness(
            run_id,
            &descriptor.name,
            &descriptor.definition_hash,
            descriptor.executable_version.as_deref(),
        )?)
    }

    pub fn cancel_run(&self, run_id: &crate::RunId) -> Result<()> {
        let run = self.store.run(run_id)?;
        if run.completed_at.is_some() {
            return Ok(());
        }
        self.store
            .finish_run(run_id, RunStatus::Cancelled, Some("cancelled by client"))?;
        Ok(())
    }

    pub fn fail_run(&self, run_id: &crate::RunId, error: &str) -> Result<()> {
        Ok(self
            .store
            .finish_run(run_id, RunStatus::Failed, Some(error))?)
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
            run_id: None,
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
        let registry = ToolRegistry::with_host_extensions(&self.root)?;
        tokio::spawn({
            let registry = registry.clone();
            let root = self.root.clone();
            async move { registry.watch_host_extensions(root).await }
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
                run_id: None,
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
        run: StartedRun,
    ) -> Result<String> {
        self.execute_send(
            SendRequest {
                session,
                prompt,
                metadata,
                source,
                run_id: Some(run.id),
                event_notify: Some(run.event_notify),
                cancellation: run.cancellation,
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
            run_id,
            event_notify,
            cancellation,
        } = request;
        if session.host_id != *environment_host_id {
            bail!("session belongs to a different Host");
        }
        let revision = self
            .store
            .agent_revision(&session.agent_id, session.agent_revision)?;
        let config = crate::AgentConfig::parse(&revision.config_toml)?;
        // Native runs need a router model before the user message becomes
        // durable. External Harnesses validate their own options at dispatch.
        if config.harness == mews_runtime::MEWS_HARNESS
            && self.session_model_config(session)?.model.is_none()
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
        let system = revision.soul;
        let agent_slug = self
            .store
            .agents()?
            .into_iter()
            .find(|agent| agent.id == session.agent_id)
            .context("Session Agent no longer exists")?
            .slug;
        let prompt = expand_prompt(environment, &session.working_directory, prompt).await?;
        if source.id.is_empty() || source.id.len() > 256 {
            bail!("message source ID must contain 1 to 256 bytes");
        }
        if !matches!(source.kind, SourceKind::Client | SourceKind::Channel) {
            bail!("user messages may only be attributed to a client or channel");
        }
        self.store.append_message(
            &session.id,
            MessageRole::User,
            MessageContent::Text {
                text: prompt.clone(),
            },
            metadata,
            source,
        )?;
        if config.harness != mews_runtime::MEWS_HARNESS && remote_host.is_none() {
            let run = match run_id {
                Some(id) => id,
                None => self.store.start_run(&session.id)?.id,
            };
            self.record_run_harness(&run, &harness_descriptor)?;
            let transition = checked_acp_binding(
                self.store.acp_session_binding(&session.id)?,
                session,
                &harness_descriptor,
            )?;
            let recovery_prompt = runtime_store::canonical_acp_prompt(&self.store, session, "")?;
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
            let mut reasoning = AcpReasoningAggregate::default();
            let outcome = mews_acp::run_acp_session_with_extensions_and_events(
                acp,
                session.working_directory.clone(),
                config.harness_options.clone(),
                mews_acp::AcpSessionRequest {
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
                        run_id: run.to_string(),
                        harness: harness_descriptor.name.clone(),
                        context_hash: mews_protocol::AcpContextSnapshot::hash_rendered(
                            &context_text,
                        ),
                        context_channel: channel,
                        invoke_run_start: true,
                    }),
                },
                environment,
                &config.tools,
                cancellation.clone(),
                &mut |event| {
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
                            run.clone(),
                        )?;
                    }
                    persist_local_acp_event(
                        &self.store,
                        session,
                        &run,
                        &harness_descriptor,
                        &event,
                        event_notify.as_ref(),
                        &mut reasoning,
                    )
                },
            )
            .await;
            reasoning.persist(&self.store, session, &run)?;
            return finish_acp_run(
                &self.store,
                session,
                &run,
                &harness_descriptor,
                outcome,
                event_notify,
            );
        }
        if config.harness != mews_runtime::MEWS_HARNESS
            && let Some(host) = remote_host
        {
            let run = match run_id {
                Some(id) => id,
                None => self.store.start_run(&session.id)?.id,
            };
            self.record_run_harness(&run, &harness_descriptor)?;
            let binding = checked_acp_binding(
                self.store.acp_session_binding(&session.id)?,
                session,
                &harness_descriptor,
            )?;
            let recovery_prompt = runtime_store::canonical_acp_prompt(&self.store, session, "")?;
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
            let mut on_event = |event: mews_protocol::AcpEvent| -> Result<()> {
                match event {
                    mews_protocol::AcpEvent::AssistantDelta {
                        event_key,
                        delta,
                        message_id,
                        raw,
                    } => {
                        self.store.append_acp_observation_with_client_event(
                            &session.id,
                            run.clone(),
                            self.store
                                .acp_session_binding(&session.id)?
                                .map(|binding| binding.acp_session_id),
                            Some(event_key),
                            mews_protocol::AcpObservation::AssistantDelta {
                                delta: delta.clone(),
                                message_id: message_id.clone(),
                                raw,
                            },
                            crate::ClientEventKind::AssistantDelta {
                                run_id: run.clone(),
                                delta,
                                message_id,
                            },
                        )?;
                    }
                    mews_protocol::AcpEvent::ProviderState { event_key, data } => {
                        self.store.append_acp_observation(
                            &session.id,
                            run.clone(),
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
                        self.store.append_client_event(
                            &session.id,
                            crate::ClientEventKind::ReasoningDelta {
                                run_id: run.clone(),
                                delta,
                                message_id,
                            },
                        )?;
                    }
                    mews_protocol::AcpEvent::ToolActivity {
                        event_key,
                        activity,
                    } => {
                        self.store.append_acp_observation_with_client_event(
                            &session.id,
                            run.clone(),
                            self.store
                                .acp_session_binding(&session.id)?
                                .map(|binding| binding.acp_session_id),
                            Some(event_key),
                            mews_protocol::AcpObservation::ToolActivity {
                                activity: activity.clone(),
                            },
                            crate::ClientEventKind::ToolActivity {
                                run_id: run.clone(),
                                activity,
                            },
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
                            run.clone(),
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
                            run.clone(),
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
            let outcome = host.run_acp(
                crate::host::RemoteAcpRun {
                    harness: config.harness.clone(),
                    harness_options: config.harness_options.clone(),
                    tools: config.tools.clone(),
                    cwd: session.working_directory.clone(),
                    prompt: prompt.clone(),
                    recovery_prompt,
                    agent_slug: agent_slug.clone(),
                    soul: system.clone(),
                    mews_session_id: session.id.to_string(),
                    run_id: run.to_string(),
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
                                mews_protocol::AcpEvent::SessionBound { acknowledgement_id, session_id: acp_session_id, transition, context, .. } => {
                                    persist_remote_acp_binding(
                                        &self.store, host, session, &run, &harness_descriptor,
                                        acknowledgement_id, acp_session_id, transition, context,
                                    ).await?;
                                }
                                mews_protocol::AcpEvent::ContextDispatched { acknowledgement_id, session_id: acp_session_id, .. } => {
                                    persist_remote_acp_dispatch(
                                        &self.store, host, session, &run,
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
                            &run,
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
                            &run,
                            acknowledgement_id,
                            acp_session_id,
                        )
                        .await?;
                    }
                    event => on_event(event)?,
                }
            }
            reasoning.persist(&self.store, session, &run)?;
            match outcome {
                Ok(outcome) => {
                    return finish_acp_run(
                        &self.store,
                        session,
                        &run,
                        &harness_descriptor,
                        Ok(outcome),
                        event_notify,
                    );
                }
                Err(error) => {
                    let cancelled = cancellation.is_cancelled() || mews_acp::is_cancelled(&error);
                    let error = format!("{error:#}");
                    self.store.finish_run(
                        &run,
                        if cancelled {
                            RunStatus::Cancelled
                        } else {
                            RunStatus::Failed
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
        match run_id {
            Some(run_id) => {
                runtime_store::run_started(
                    &self.store,
                    &provider,
                    environment,
                    session,
                    system,
                    runtime_store::StartedRun {
                        id: run_id,
                        event_notify: event_notify
                            .context("started Run requires an event notifier")?,
                        harness: harness_descriptor.clone(),
                        cancellation: cancellation.clone(),
                    },
                )
                .await
            }
            None => {
                runtime_store::run(
                    &self.store,
                    &provider,
                    environment,
                    session,
                    system,
                    harness_descriptor,
                )
                .await
            }
        }
    }
}
