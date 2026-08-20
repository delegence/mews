use super::*;

impl Mews {
    pub fn session_history(&self, session_id: &crate::SessionId) -> Result<Vec<crate::Message>> {
        self.store.active_messages(session_id).map_err(Into::into)
    }

    pub fn session_history_page(
        &self,
        session_id: &crate::SessionId,
        after: Option<u64>,
        limit: u16,
    ) -> Result<crate::SessionHistoryPage> {
        let (messages, next) = self.store.active_messages_page(session_id, after, limit)?;
        Ok(crate::SessionHistoryPage { messages, next })
    }

    pub fn session_entries(
        &self,
        session_id: &crate::SessionId,
    ) -> Result<Vec<crate::SessionEntry>> {
        self.store.session_entries(session_id).map_err(Into::into)
    }

    pub fn session_entries_page(
        &self,
        session_id: &crate::SessionId,
        after: Option<u64>,
        limit: u16,
    ) -> Result<crate::SessionEntriesPage> {
        let (entries, next) = self.store.session_entries_page(session_id, after, limit)?;
        Ok(crate::SessionEntriesPage { entries, next })
    }

    pub fn session_model_config(
        &self,
        session: &crate::Session,
    ) -> Result<crate::SessionModelConfig> {
        let agent = self
            .store
            .agents()?
            .into_iter()
            .find(|agent| agent.id == session.agent_id)
            .context("Session Agent no longer exists")?;
        let revision = self
            .store
            .agent_revision(&session.agent_id, agent.current_revision)?;
        self.session_model_config_for_revision(session, &revision)
    }

    pub(crate) fn session_model_config_for_revision(
        &self,
        session: &crate::Session,
        revision: &crate::AgentRevision,
    ) -> Result<crate::SessionModelConfig> {
        let config = crate::AgentConfig::parse(&revision.config_toml)?;
        if config.harness != mews_runtime::MEWS_HARNESS {
            return Ok(crate::SessionModelConfig {
                harness: config.harness,
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
            harness: config.harness,
            model: session
                .model_override
                .clone()
                .or(options.model)
                .or(defaults.model),
            reasoning,
        })
    }
}

impl MewsCommands<'_> {
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
        let agent = self.synchronize_agent_on(slug, host).await?;
        let cwd = host.attest_directory(cwd).await?;
        Ok(self
            .mews
            .store
            .create_session(&agent.id, host.host_id(), &cwd)?)
    }

    /// Reconcile the Hub and selected Host replicas before a Turn snapshots
    /// the Agent revision it will execute.
    pub async fn synchronize_agent_on(
        &mut self,
        slug: &str,
        host: &dyn HostExecutor,
    ) -> Result<Agent> {
        let agent = self.synchronize_agent(slug)?;
        self.mews.store.host(host.host_id())?;
        let mut agent = agent;
        let current = self
            .store
            .agent_revision(&agent.id, agent.current_revision)?;
        let observed_replica = host.agent_replica(slug).await?;
        if let Some(replica) = &observed_replica {
            if replica.revision > current.revision {
                bail!("Host agent replica is newer than Hub");
            }
            let base = self
                .mews
                .store
                .agent_revision(&agent.id, replica.revision)?;
            let edited = replica.soul.trim_end() != base.soul.trim_end()
                || replica.config_toml != base.config_toml;
            if replica.revision < current.revision && edited {
                bail!("Host agent replica conflicts with the newer Hub revision");
            }
            if replica.revision == current.revision && edited {
                crate::AgentConfig::parse(&replica.config_toml)?
                    .validate()
                    .map_err(anyhow::Error::msg)?;
                self.mews.store.update_agent(
                    &self.context,
                    &agent.id,
                    current.revision,
                    replica.soul.trim_end(),
                    &replica.config_toml,
                    host.host_id(),
                )?;
                agent = self.mews.store.agent_by_slug(slug)?;
            }
        }
        let revision = self
            .store
            .agent_revision(&agent.id, agent.current_revision)?;
        host.synchronize_agent(&agent, &revision, observed_replica.as_ref(), None)
            .await?;
        Ok(agent)
    }
}

impl Mews {
    pub fn session(&self, id: &crate::SessionId) -> Result<Session> {
        Ok(self.store.session(id)?)
    }

    pub fn set_session_model(&self, id: &crate::SessionId, model: Option<&str>) -> Result<Session> {
        let session = self.store.session(id)?;
        let agent = self
            .store
            .agents()?
            .into_iter()
            .find(|agent| agent.id == session.agent_id)
            .context("Session Agent no longer exists")?;
        let revision = self
            .store
            .agent_revision(&agent.id, agent.current_revision)?;
        let config = crate::AgentConfig::parse(&revision.config_toml)?;
        if config.harness != mews_runtime::MEWS_HARNESS {
            bail!("Session model overrides are only supported by the native mews Harness");
        }
        Ok(self.store.set_session_model(id, model)?)
    }
}
