use std::collections::BTreeMap;

use super::*;

impl MewsCommands<'_> {
    /// Replays a committed rename before callers try to read the old slug.
    /// A transport retry can arrive after the database commit, the local file
    /// move, or both, so recovery must precede normal rename preflight.
    pub(crate) fn replay_agent_rename(
        &mut self,
        slug: &str,
        new_slug: &str,
    ) -> Result<Option<Agent>> {
        let source = self.root.join("agents").join(slug);
        let destination = self.root.join("agents").join(new_slug);
        if !source.exists() && destination.exists() {
            return Ok(Some(self.mews.store.rename_agent(
                &self.context,
                slug,
                new_slug,
            )?));
        }
        if source.exists() && !destination.exists() && self.mews.store.agent_by_slug(slug).is_err()
        {
            let agent = self
                .mews
                .store
                .rename_agent(&self.context, slug, new_slug)?;
            fs::rename(&source, &destination)?;
            fs::File::open(self.root.join("agents"))?.sync_all()?;
            return Ok(Some(agent));
        }
        Ok(None)
    }

    pub fn rename_agent(&mut self, slug: &str, new_slug: &str) -> Result<Agent> {
        let source = self.root.join("agents").join(slug);
        let destination = self.root.join("agents").join(new_slug);
        if let Some(agent) = self.replay_agent_rename(slug, new_slug)? {
            return Ok(agent);
        }
        self.synchronize_agent(slug)?;
        if destination.exists() {
            bail!("agent directory already exists: {}", destination.display());
        }
        let agent = self
            .mews
            .store
            .rename_agent(&self.context, slug, new_slug)?;
        if let Err(error) = fs::rename(&source, &destination) {
            let rollback = self
                .store
                .rollback_agent_rename(&self.context, new_slug, slug);
            rollback.context("agent directory rename failed and database rollback also failed")?;
            return Err(error.into());
        }
        fs::File::open(self.root.join("agents"))?.sync_all()?;
        Ok(agent)
    }

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
            let defaults = self.mews.store.provider_defaults()?;
            if let Some(model) = defaults.model {
                harness_options.entry("model".into()).or_insert(model);
            }
            if let Some(reasoning) = defaults.reasoning {
                if reasoning == crate::ReasoningEffort::Auto {
                    bail!(
                        "reasoning auto is not supported by the native mews Harness; use Provider default instead"
                    );
                }
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
            if let Ok(agent) = self.mews.store.agent_by_slug(slug) {
                let revision = self
                    .store
                    .agent_revision(&agent.id, agent.current_revision)?;
                if revision.soul == DEFAULT_SOUL && revision.config_toml == config {
                    fs::write(directory.join(REVISION_FILE), revision.revision.to_string())?;
                    fs::write(directory.join(AGENT_ID_FILE), agent.id.as_str())?;
                    return Ok(agent);
                }
            }
            bail!("agent directory already exists: {}", directory.display())
        }
        fs::create_dir_all(&directory)?;
        fs::write(directory.join("SOUL.md"), format!("{DEFAULT_SOUL}\n"))?;
        fs::write(directory.join("agent.toml"), &config)?;
        match self.mews.store.create_agent(
            &self.context,
            slug,
            DEFAULT_SOUL,
            &config,
            &installation.hub_host_id,
        ) {
            Ok((agent, revision)) => {
                fs::write(directory.join(REVISION_FILE), revision.revision.to_string())?;
                fs::write(directory.join(AGENT_ID_FILE), agent.id.as_str())?;
                Ok(agent)
            }
            Err(error) => {
                let _ = fs::remove_dir_all(&directory);
                Err(error.into())
            }
        }
    }

    pub fn synchronize_agent(&mut self, slug: &str) -> Result<Agent> {
        let agent = self.mews.store.agent_by_slug(slug)?;
        let current = self
            .store
            .agent_revision(&agent.id, agent.current_revision)?;
        let directory = self.root.join("agents").join(slug);
        fs::create_dir_all(&directory)?;
        fs::write(directory.join(AGENT_ID_FILE), agent.id.as_str())?;
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
            let base = self
                .mews
                .store
                .agent_revision(&agent.id, replica_revision)?;
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
            let revision = self.mews.store.update_agent(
                &self.context,
                &agent.id,
                current.revision,
                soul.trim_end(),
                &config,
                &installation.hub_host_id,
            )?;
            fs::write(revision_path, revision.revision.to_string())?;
            return Ok(self.mews.store.agent_by_slug(slug)?);
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
    fs::write(staged.join(AGENT_ID_FILE), revision.agent_id.as_str())?;
    for file in ["SOUL.md", "agent.toml", REVISION_FILE, AGENT_ID_FILE] {
        fs::OpenOptions::new()
            .write(true)
            .open(staged.join(file))?
            .sync_all()?;
    }
    fs::File::open(&staged)?.sync_all()?;
    if directory.exists() {
        fs::rename(directory, &previous)?;
        if let Err(error) = move_local_resources(&previous, &staged) {
            let _ = fs::rename(&previous, directory);
            return Err(error);
        }
    }
    if let Err(error) = fs::rename(&staged, directory) {
        if previous.exists() {
            let _ = move_local_resources(&staged, &previous);
            let _ = fs::rename(&previous, directory);
        }
        return Err(error.into());
    }
    fs::File::open(parent)?.sync_all()?;
    retain_previous_directories(parent, name)?;
    Ok(())
}

/// Skills and extensions belong to the Host-local Agent replica, not its
/// synchronized revision, so an atomic revision swap must carry them forward.
fn move_local_resources(from: &Path, to: &Path) -> Result<()> {
    let mut moved = Vec::new();
    for name in ["skills", "extensions"] {
        let source = from.join(name);
        if !source.exists() {
            continue;
        }
        if let Err(error) = fs::rename(&source, to.join(name)) {
            for moved_name in moved.into_iter().rev() {
                let _ = fs::rename(to.join(moved_name), from.join(moved_name));
            }
            return Err(error.into());
        }
        moved.push(name);
    }
    Ok(())
}

fn retain_previous_directories(parent: &Path, name: &str) -> Result<()> {
    let prefix = format!(".{name}.previous-");
    let mut previous = fs::read_dir(parent)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with(&prefix))
        .collect::<Vec<_>>();
    previous.sort_by_key(|entry| entry.file_name());
    for stale in previous.into_iter().rev().skip(1) {
        fs::remove_dir_all(stale.path())?;
    }
    Ok(())
}
