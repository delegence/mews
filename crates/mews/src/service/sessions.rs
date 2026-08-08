use super::*;

struct SendRequest<'a> {
    session: &'a Session,
    prompt: &'a str,
    metadata: Value,
    source: MessageSource,
    run_id: Option<crate::RunId>,
    event_notify: Option<Arc<tokio::sync::Notify>>,
    permission_handler: Option<Arc<dyn mews_acp::AcpPermissionHandler>>,
}

async fn resolve_remote_permission(
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

pub(crate) struct StartedRun {
    pub id: crate::RunId,
    pub event_notify: Arc<tokio::sync::Notify>,
    pub permission_handler: Arc<dyn mews_acp::AcpPermissionHandler>,
}

impl Mews {
    pub fn session_model_config(
        &self,
        session: &crate::Session,
    ) -> Result<crate::SessionModelConfig> {
        let revision = self
            .store
            .agent_revision(&session.agent_id, session.agent_revision)?;
        let config = crate::AgentConfig::parse(&revision.config_toml)?;
        if config.harness != mews_runtime::MEWS_HARNESS {
            return Ok(crate::SessionModelConfig {
                model: config.harness_options.get("model").cloned(),
                reasoning: config
                    .harness_options
                    .get("reasoning")
                    .or_else(|| config.harness_options.get("reasoning_effort"))
                    .and_then(|value| match value.as_str() {
                        "none" => Some(crate::ReasoningEffort::None),
                        "auto" => Some(crate::ReasoningEffort::Auto),
                        "minimal" => Some(crate::ReasoningEffort::Minimal),
                        "low" => Some(crate::ReasoningEffort::Low),
                        "medium" => Some(crate::ReasoningEffort::Medium),
                        "high" => Some(crate::ReasoningEffort::High),
                        "xhigh" => Some(crate::ReasoningEffort::XHigh),
                        "max" => Some(crate::ReasoningEffort::Max),
                        _ => None,
                    }),
            });
        }
        let options = mews_runtime::MewsHarnessOptions::from_agent(&config)?;
        let defaults = self.store.provider_defaults()?;
        let reasoning = options.reasoning.or(if options.model.is_none() {
            defaults.reasoning
        } else {
            None
        });
        Ok(crate::SessionModelConfig {
            model: session
                .model_override
                .clone()
                .or(options.model)
                .or(defaults.model),
            reasoning,
        })
    }

    pub async fn ask(
        &mut self,
        slug: &str,
        cwd: &Path,
        prompt: &str,
        metadata: Value,
    ) -> Result<(Session, String)> {
        let session = self.start_session(slug, cwd).await?;
        let answer = self.send(&session, prompt, metadata).await?;
        Ok((session, answer))
    }

    pub async fn start_session(&mut self, slug: &str, cwd: &Path) -> Result<Session> {
        let installation = self.installation()?;
        let registry = self.local_tools()?;
        let host = ConnectedHost::in_process(installation.hub_host_id.clone(), registry).await?;
        self.start_session_on(slug, cwd, &host).await
    }

    pub async fn start_session_on(
        &mut self,
        slug: &str,
        cwd: &Path,
        host: &dyn HostExecutor,
    ) -> Result<Session> {
        let agent = self.synchronize_agent(slug)?;
        self.store.host(host.host_id())?;
        let mut agent = agent;
        let current = self
            .store
            .agent_revision(&agent.id, agent.current_revision)?;
        let observed_replica = host.agent_replica(slug).await?;
        if let Some(replica) = &observed_replica {
            if replica.revision > current.revision {
                bail!("Host agent replica is newer than Hub");
            }
            let base = self.store.agent_revision(&agent.id, replica.revision)?;
            let edited = replica.soul.trim_end() != base.soul.trim_end()
                || replica.config_toml != base.config_toml;
            if replica.revision < current.revision && edited {
                bail!("Host agent replica conflicts with the newer Hub revision");
            }
            if replica.revision == current.revision && edited {
                crate::AgentConfig::parse(&replica.config_toml)?
                    .validate()
                    .map_err(anyhow::Error::msg)?;
                self.store.update_agent(
                    &agent.id,
                    current.revision,
                    replica.soul.trim_end(),
                    &replica.config_toml,
                    host.host_id(),
                )?;
                agent = self.store.agent_by_slug(slug)?;
            }
        }
        let revision = self
            .store
            .agent_revision(&agent.id, agent.current_revision)?;
        host.synchronize_agent(&agent, &revision, observed_replica.as_ref())
            .await?;
        let cwd = host.attest_directory(cwd).await?;
        Ok(self.store.create_session(&agent.id, host.host_id(), &cwd)?)
    }

    pub fn session(&self, id: &crate::SessionId) -> Result<Session> {
        Ok(self.store.session(id)?)
    }

    pub fn set_session_model(&self, id: &crate::SessionId, model: Option<&str>) -> Result<Session> {
        Ok(self.store.set_session_model(id, model)?)
    }

    pub fn start_run(&self, session_id: &crate::SessionId) -> Result<crate::Run> {
        Ok(self.store.start_run(session_id)?)
    }

    pub fn start_run_idempotent(
        &self,
        session_id: &crate::SessionId,
        key: &str,
    ) -> Result<(crate::Run, bool)> {
        Ok(self.store.start_run_idempotent(session_id, key)?)
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
        if run.status == RunStatus::Cancelled {
            return Ok(());
        }
        if run.completed_at.is_some() {
            bail!("Run already finished with status {:?}", run.status);
        }
        Ok(self
            .store
            .finish_run(run_id, RunStatus::Cancelled, Some("cancelled by client"))?)
    }

    pub fn fail_run(&self, run_id: &crate::RunId, error: &str) -> Result<()> {
        Ok(self
            .store
            .finish_run(run_id, RunStatus::Failed, Some(error))?)
    }

    pub fn append_permission_request(
        &self,
        session_id: &crate::SessionId,
        run_id: &crate::RunId,
        request: mews_protocol::PermissionRequest,
    ) -> Result<()> {
        Ok(self.store.append_client_event(
            session_id,
            crate::ClientEventKind::PermissionRequested {
                run_id: run_id.clone(),
                request,
            },
        )?)
    }

    pub fn subscribe_session(
        &self,
        consumer: &crate::ConsumerId,
        session: &crate::SessionId,
    ) -> Result<()> {
        Ok(self.store.subscribe_session(consumer, session)?)
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
            permission_handler: None,
        })
        .await
    }

    pub(crate) async fn send_from_started(
        &mut self,
        session: &Session,
        prompt: &str,
        metadata: Value,
        source: MessageSource,
        run: StartedRun,
    ) -> Result<String> {
        self.send_locally(SendRequest {
            session,
            prompt,
            metadata,
            source,
            run_id: Some(run.id),
            event_notify: Some(run.event_notify),
            permission_handler: Some(run.permission_handler),
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

    fn local_tools(&self) -> Result<ToolRegistry> {
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
                permission_handler: None,
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
                permission_handler: Some(run.permission_handler),
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
            permission_handler,
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
            Some(acp)
        };
        let system = revision.soul;
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
            let binding = checked_acp_binding(
                self.store.acp_session_binding(&session.id)?,
                session,
                &harness_descriptor,
            )?;
            let recovery_prompt =
                runtime_store::canonical_acp_prompt(&self.store, session, &system)?;
            let mut acp = acp.context("local ACP Harness launch is unavailable")?;
            if let Some(handler) = permission_handler {
                acp.permission_handler = handler;
            }
            let outcome = mews_acp::run_acp_session_with_extensions_and_events(
                acp,
                session.working_directory.clone(),
                config.harness_options.clone(),
                mews_acp::AcpSessionRequest {
                    prompt: mews_runtime::initial_session_prompt(&system, &prompt),
                    recovery_prompt,
                    session_id: binding
                        .as_ref()
                        .map(|binding| binding.acp_session_id.clone()),
                },
                environment,
                &config.tools,
                &mut |event| {
                    persist_local_acp_event(
                        &self.store,
                        session,
                        &run,
                        &harness_descriptor,
                        &event,
                        event_notify.as_ref(),
                    )
                },
            )
            .await;
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
            let recovery_prompt =
                runtime_store::canonical_acp_prompt(&self.store, session, &system)?;
            let on_event = |event: mews_protocol::AcpEvent| -> Result<()> {
                match event {
                    mews_protocol::AcpEvent::AssistantDelta { delta, message_id } => {
                        self.store.append_client_event(
                            &session.id,
                            crate::ClientEventKind::AssistantDelta { delta, message_id },
                        )?;
                    }
                    mews_protocol::AcpEvent::ProviderState { data } => {
                        self.store.append_message(
                            &session.id,
                            MessageRole::Assistant,
                            MessageContent::ProviderState {
                                provider: "acp".into(),
                                model: "external".into(),
                                data,
                            },
                            Value::Null,
                            MessageSource {
                                kind: SourceKind::Harness,
                                id: "default".into(),
                            },
                        )?;
                    }
                    mews_protocol::AcpEvent::ReasoningDelta { delta, message_id } => {
                        self.store.append_client_event(
                            &session.id,
                            crate::ClientEventKind::ReasoningDelta {
                                run_id: run.clone(),
                                delta,
                                message_id,
                            },
                        )?;
                    }
                    mews_protocol::AcpEvent::ToolActivity { activity } => {
                        self.store.append_client_event(
                            &session.id,
                            crate::ClientEventKind::ToolActivity {
                                run_id: run.clone(),
                                activity,
                            },
                        )?;
                    }
                    mews_protocol::AcpEvent::SessionBound { .. } => {
                        unreachable!("Session binding events are handled asynchronously")
                    }
                    mews_protocol::AcpEvent::PermissionRequested { .. } => {
                        unreachable!("permission events are handled asynchronously")
                    }
                }
                if let Some(notify) = &event_notify {
                    notify.notify_waiters();
                }
                Ok(())
            };
            let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
            let outcome = host.run_acp(
                crate::host::RemoteAcpRun {
                    harness: config.harness.clone(),
                    harness_options: config.harness_options.clone(),
                    tools: config.tools.clone(),
                    cwd: session.working_directory.clone(),
                    prompt: mews_runtime::initial_session_prompt(&system, &prompt),
                    recovery_prompt,
                    session_id: binding
                        .as_ref()
                        .map(|binding| binding.acp_session_id.clone()),
                },
                event_tx,
            );
            tokio::pin!(outcome);
            let outcome = loop {
                tokio::select! {
                    outcome = &mut outcome => break outcome,
                    event = event_rx.recv() => {
                        if let Some(event) = event {
                            match event {
                                mews_protocol::AcpEvent::PermissionRequested { request } => {
                                    resolve_remote_permission(
                                        host,
                                        permission_handler.as_deref(),
                                        &session.id,
                                        request,
                                    ).await?;
                                }
                                mews_protocol::AcpEvent::SessionBound { acknowledgement_id, session_id: acp_session_id, replaced } => {
                                    persist_remote_acp_binding(
                                        &self.store, host, session, &harness_descriptor,
                                        acknowledgement_id, acp_session_id, replaced,
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
                    mews_protocol::AcpEvent::PermissionRequested { request } => {
                        resolve_remote_permission(
                            host,
                            permission_handler.as_deref(),
                            &session.id,
                            request,
                        )
                        .await?;
                    }
                    mews_protocol::AcpEvent::SessionBound {
                        acknowledgement_id,
                        session_id: acp_session_id,
                        replaced,
                    } => {
                        persist_remote_acp_binding(
                            &self.store,
                            host,
                            session,
                            &harness_descriptor,
                            acknowledgement_id,
                            acp_session_id,
                            replaced,
                        )
                        .await?;
                    }
                    event => on_event(event)?,
                }
            }
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
                    let error = format!("{error:#}");
                    self.store
                        .finish_run(&run, RunStatus::Failed, Some(&error))?;
                    if let Some(notify) = event_notify {
                        notify.notify_waiters();
                    }
                    return Err(anyhow::anyhow!(error));
                }
            }
        }
        let provider = mews_router::RouterClient::new(&self.root);
        let outcome = match run_id {
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
        };
        outcome
    }
}

fn checked_acp_binding(
    binding: Option<mews_protocol::AcpSessionBinding>,
    session: &crate::Session,
    harness: &mews_protocol::HarnessDescriptor,
) -> Result<Option<mews_protocol::AcpSessionBinding>> {
    if let Some(binding) = &binding
        && (binding.host_id != session.host_id || binding.harness != harness.name)
    {
        bail!("ACP Session binding does not match this Session's Host and Harness");
    }
    Ok(binding)
}

fn persist_local_acp_event(
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

async fn persist_remote_acp_binding(
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

fn finish_acp_run(
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

pub(super) async fn expand_prompt(
    host: &dyn AgentCapabilities,
    cwd: &Path,
    prompt: &str,
) -> Result<String> {
    let Some(command) = prompt.strip_prefix('/') else {
        return Ok(prompt.to_owned());
    };
    let (name, arguments) = command
        .split_once(char::is_whitespace)
        .unwrap_or((command, ""));
    let Some(template) = host.read_prompt(cwd, name).await? else {
        return Ok(prompt.to_owned());
    };
    let body = template
        .strip_prefix("---\n")
        .and_then(|rest| rest.split_once("\n---"))
        .map_or(template.as_str(), |(_, body)| {
            body.trim_start_matches(['\r', '\n'])
        });
    Ok(substitute_prompt_arguments(
        body,
        &parse_prompt_arguments(arguments),
    ))
}

pub(super) fn parse_prompt_arguments(input: &str) -> Vec<String> {
    let mut arguments = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    for character in input.chars() {
        match (quote, character) {
            (Some(open), close) if open == close => quote = None,
            (Some(_), character) => current.push(character),
            (None, '"' | '\'') => quote = Some(character),
            (None, character) if character.is_whitespace() => {
                if !current.is_empty() {
                    arguments.push(std::mem::take(&mut current));
                }
            }
            (None, character) => current.push(character),
        }
    }
    if !current.is_empty() {
        arguments.push(current);
    }
    arguments
}

pub(super) fn substitute_prompt_arguments(template: &str, arguments: &[String]) -> String {
    let pattern = regex::Regex::new(
        r"\$\{(\d+|ARGUMENTS|@):-([^}]*)\}|\$\{@:(\d+)(?::(\d+))?\}|\$(ARGUMENTS|@|\d+)",
    )
    .expect("prompt placeholder regex is valid");
    let all = arguments.join(" ");
    pattern
        .replace_all(template, |captures: &regex::Captures<'_>| {
            if let Some(target) = captures.get(1) {
                let value = match target.as_str() {
                    "@" | "ARGUMENTS" => all.as_str(),
                    index => index
                        .parse::<usize>()
                        .ok()
                        .and_then(|index| arguments.get(index.saturating_sub(1)))
                        .map(String::as_str)
                        .unwrap_or(""),
                };
                return if value.is_empty() {
                    captures
                        .get(2)
                        .map_or("", |value| value.as_str())
                        .to_owned()
                } else {
                    value.to_owned()
                };
            }
            if let Some(start) = captures.get(3) {
                let start = start
                    .as_str()
                    .parse::<usize>()
                    .unwrap_or(1)
                    .saturating_sub(1);
                let values = match captures
                    .get(4)
                    .and_then(|length| length.as_str().parse::<usize>().ok())
                {
                    Some(length) => {
                        &arguments
                            [start.min(arguments.len())..(start + length).min(arguments.len())]
                    }
                    None => &arguments[start.min(arguments.len())..],
                };
                return values.join(" ");
            }
            match captures
                .get(5)
                .map(|value| value.as_str())
                .unwrap_or_default()
            {
                "@" | "ARGUMENTS" => all.clone(),
                index => index
                    .parse::<usize>()
                    .ok()
                    .and_then(|index| arguments.get(index.saturating_sub(1)))
                    .cloned()
                    .unwrap_or_default(),
            }
        })
        .into_owned()
}
