use std::collections::BTreeMap;

use super::*;

impl Mews {
    pub fn create_agent(&mut self, slug: &str) -> Result<Agent> {
        self.create_agent_with_harness(slug, mews_runtime::MEWS_HARNESS, BTreeMap::new())
    }

    pub fn create_agent_with_harness(
        &mut self,
        slug: &str,
        harness: &str,
        mut harness_options: BTreeMap<String, String>,
    ) -> Result<Agent> {
        let installation = self.installation()?;
        if harness == mews_runtime::MEWS_HARNESS {
            let defaults = self.store.provider_defaults()?;
            if let Some(model) = defaults.model {
                harness_options.entry("model".into()).or_insert(model);
            }
            if let Some(reasoning) = defaults.reasoning {
                harness_options
                    .entry("reasoning".into())
                    .or_insert_with(|| reasoning_name(reasoning).into());
            }
        }
        let config = toml::to_string(&crate::AgentConfig {
            harness: harness.into(),
            harness_options,
            tools: vec!["*".into()],
            tool_execution: Default::default(),
        })?;
        let directory = self.root.join("agents").join(slug);
        if directory.exists() {
            bail!("agent directory already exists: {}", directory.display())
        }
        fs::create_dir_all(&directory)?;
        fs::write(directory.join("SOUL.md"), format!("{DEFAULT_SOUL}\n"))?;
        fs::write(directory.join("agent.toml"), &config)?;
        match self
            .store
            .create_agent(slug, DEFAULT_SOUL, &config, &installation.hub_host_id)
        {
            Ok((agent, revision)) => {
                fs::write(directory.join(REVISION_FILE), revision.revision.to_string())?;
                Ok(agent)
            }
            Err(error) => {
                let _ = fs::remove_dir_all(&directory);
                Err(error.into())
            }
        }
    }

    pub fn synchronize_agent(&mut self, slug: &str) -> Result<Agent> {
        let agent = self.store.agent_by_slug(slug)?;
        let current = self
            .store
            .agent_revision(&agent.id, agent.current_revision)?;
        let directory = self.root.join("agents").join(slug);
        fs::create_dir_all(&directory)?;
        let soul_path = directory.join("SOUL.md");
        let config_path = directory.join("agent.toml");
        let revision_path = directory.join(REVISION_FILE);
        if !soul_path.exists() || !config_path.exists() {
            let surviving_edit = if soul_path.exists() {
                fs::read_to_string(&soul_path)?.trim_end() != current.soul.trim_end()
            } else if config_path.exists() {
                fs::read_to_string(&config_path)? != current.config_toml
            } else {
                false
            };
            if surviving_edit {
                bail!(
                    "agent definition is incomplete and contains local edits; restore the missing file before synchronizing"
                );
            }
            materialize(&directory, &current)?;
            return Ok(agent);
        }
        let soul = fs::read_to_string(&soul_path)?;
        let config = fs::read_to_string(&config_path)?;
        let replica_revision = fs::read_to_string(&revision_path)
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok());
        let Some(replica_revision) = replica_revision else {
            if soul.trim_end() == current.soul.trim_end() && config == current.config_toml {
                fs::write(&revision_path, current.revision.to_string())?;
                return Ok(agent);
            }
            fs::write(directory.join("SOUL.conflict-unknown.md"), &soul)?;
            fs::write(directory.join("agent.conflict-unknown.toml"), &config)?;
            bail!(
                "agent replica revision is missing or invalid; local edits were preserved as conflict files"
            );
        };
        if replica_revision < current.revision {
            let base = self.store.agent_revision(&agent.id, replica_revision)?;
            if soul.trim_end() != base.soul.trim_end() || config != base.config_toml {
                fs::write(
                    directory.join(format!("SOUL.conflict-r{replica_revision}.md")),
                    &soul,
                )?;
                fs::write(
                    directory.join(format!("agent.conflict-r{replica_revision}.toml")),
                    &config,
                )?;
                materialize(&directory, &current)?;
                bail!(
                    "agent definition conflicts with Hub revision {}; local edits were preserved as conflict files",
                    current.revision
                );
            }
            materialize(&directory, &current)?;
            return Ok(agent);
        }
        if replica_revision > current.revision {
            bail!(
                "local agent revision {replica_revision} is newer than Hub revision {}",
                current.revision
            );
        }
        if soul.trim_end() != current.soul.trim_end() || config != current.config_toml {
            let installation = self.installation()?;
            let revision = self.store.update_agent(
                &agent.id,
                current.revision,
                soul.trim_end(),
                &config,
                &installation.hub_host_id,
            )?;
            fs::write(revision_path, revision.revision.to_string())?;
            return Ok(self.store.agent_by_slug(slug)?);
        }
        Ok(agent)
    }
}

fn reasoning_name(reasoning: crate::ReasoningEffort) -> &'static str {
    match reasoning {
        crate::ReasoningEffort::None => "none",
        crate::ReasoningEffort::Auto => "auto",
        crate::ReasoningEffort::Minimal => "minimal",
        crate::ReasoningEffort::Low => "low",
        crate::ReasoningEffort::Medium => "medium",
        crate::ReasoningEffort::High => "high",
        crate::ReasoningEffort::XHigh => "xhigh",
        crate::ReasoningEffort::Max => "max",
    }
}

fn materialize(directory: &Path, revision: &crate::AgentRevision) -> Result<()> {
    let parent = directory
        .parent()
        .context("agent directory has no parent")?;
    let name = directory
        .file_name()
        .and_then(|name| name.to_str())
        .context("agent directory name is not UTF-8")?;
    let staged = parent.join(format!(".{name}.staged-{}", uuid::Uuid::now_v7()));
    let previous = parent.join(format!(".{name}.previous-{}", uuid::Uuid::now_v7()));
    fs::create_dir(&staged)?;
    fs::write(staged.join("SOUL.md"), &revision.soul)?;
    fs::write(staged.join("agent.toml"), &revision.config_toml)?;
    fs::write(staged.join(REVISION_FILE), revision.revision.to_string())?;
    for file in ["SOUL.md", "agent.toml", REVISION_FILE] {
        fs::OpenOptions::new()
            .write(true)
            .open(staged.join(file))?
            .sync_all()?;
    }
    fs::File::open(&staged)?.sync_all()?;
    if directory.exists() {
        fs::rename(directory, &previous)?;
    }
    if let Err(error) = fs::rename(&staged, directory) {
        if previous.exists() {
            let _ = fs::rename(&previous, directory);
        }
        return Err(error.into());
    }
    fs::File::open(parent)?.sync_all()?;
    Ok(())
}
