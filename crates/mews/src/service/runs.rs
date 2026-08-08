use super::*;

use super::{
    acp_execution::{
        checked_acp_binding, finish_acp_run, persist_local_acp_event, persist_remote_acp_binding,
        resolve_remote_permission,
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
    permission_handler: Option<Arc<dyn mews_acp::AcpPermissionHandler>>,
    cancellation: mews_agent::CancellationToken,
}

pub(crate) struct StartedRun {
    pub id: crate::RunId,
    pub event_notify: Arc<tokio::sync::Notify>,
    pub permission_handler: Arc<dyn mews_acp::AcpPermissionHandler>,
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

    pub fn append_permission_resolution(
        &self,
        session_id: &crate::SessionId,
        run_id: &crate::RunId,
        request_id: &str,
        outcome: mews_protocol::PermissionOutcome,
    ) -> Result<()> {
        Ok(self.store.append_client_event(
            session_id,
            crate::ClientEventKind::PermissionResolved {
                run_id: run_id.clone(),
                request_id: request_id.to_owned(),
                outcome,
            },
        )?)
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
                permission_handler: Some(run.permission_handler),
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
            permission_handler,
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
                cancellation.clone(),
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
                            crate::ClientEventKind::AssistantDelta {
                                run_id: run.clone(),
                                delta,
                                message_id,
                            },
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
            let (event_tx, mut event_rx) =
                tokio::sync::mpsc::channel(crate::host::ACP_EVENT_CHANNEL_CAPACITY);
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
                &cancellation,
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
