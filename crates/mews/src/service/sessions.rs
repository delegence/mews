use super::*;

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
        host.synchronize_agent(&agent, &revision, observed_replica.as_ref(), None)
            .await?;
        let cwd = host.attest_directory(cwd).await?;
        Ok(self.store.create_session(&agent.id, host.host_id(), &cwd)?)
    }

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
            .agent_revision(&agent.id, session.agent_revision)?;
        let config = crate::AgentConfig::parse(&revision.config_toml)?;
        if config.harness != mews_runtime::MEWS_HARNESS {
            bail!("Session model overrides are only supported by the native mews Harness");
        }
        Ok(self.store.set_session_model(id, model)?)
    }
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
