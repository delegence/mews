use super::*;

impl MewsCommands<'_> {
    pub fn set_relay_url(&self, relay_url: &str) -> Result<()> {
        self.mews.set_relay_url(&self.context, relay_url)
    }

    pub fn archive_agent(&self, slug: &str) -> Result<()> {
        self.mews.archive_agent(&self.context, slug)
    }

    pub fn remove_host(&self, id: &crate::HostId) -> Result<()> {
        self.mews.remove_host(&self.context, id)
    }
}

impl Mews {
    pub fn commands(&mut self, context: CommandContext) -> MewsCommands<'_> {
        MewsCommands {
            mews: self,
            context,
        }
    }

    fn recover_preparing_hub_move(
        root: &Path,
        store: &mut Store,
        context: &CommandContext,
    ) -> Result<()> {
        let phase_path = root.join("hub-move.phase");
        if !phase_path.exists() || fs::read_to_string(&phase_path)?.trim() != "preparing" {
            return Ok(());
        }
        let installation = store
            .installation()?
            .context("Hub move journal exists without an installation")?;
        let local_key = HostIdentity::load(&root.join("secrets/host.key"))?.public_key();
        let local = store
            .hosts()?
            .into_iter()
            .find(|host| host.public_key == local_key)
            .context("local Host identity is absent from Hub database")?;
        if installation.hub_host_id != local.id {
            store.move_hub(context, &installation.hub_host_id, &local.id)?;
        }
        let _ = fs::remove_file(root.join("hub.json"));
        let _ = fs::remove_file(root.join("hub-move-recovery.json"));
        fs::remove_file(phase_path)?;
        fs::File::open(root)?.sync_all()?;
        Ok(())
    }

    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        let mut store = Store::open_hub(root.join(DATABASE_FILE), root.join("hub.lock"))?;
        let command_context = CommandContext::system();
        Self::recover_preparing_hub_move(&root, &mut store, &command_context)?;
        NoiseIdentity::load(&root.join("secrets/hub-noise.key"))?;
        validate_installation_authority(&root, &store)?;
        validate_hub_assignment(&root, &store)?;
        store.recover_interrupted_work()?;
        Ok(Self { root, store })
    }

    pub(crate) fn open_connection(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        let store = Store::open_existing(root.join(DATABASE_FILE))?;
        Ok(Self { root, store })
    }

    pub(crate) fn open_handoff(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        let store = Store::open_hub(root.join(DATABASE_FILE), root.join("hub.lock"))?;
        NoiseIdentity::load(&root.join("secrets/hub-noise.key"))?;
        validate_installation_authority(&root, &store)?;
        Ok(Self { root, store })
    }

    pub fn setup(root: impl Into<PathBuf>, host_name: &str) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(root.join("agents"))?;
        crate::paths::ensure_directories(&root)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
            fs::set_permissions(root.join("agents"), fs::Permissions::from_mode(0o700))?;
        }
        let identity = HostIdentity::load_or_create(&root.join("secrets/host.key"))?;
        let noise_identity = NoiseIdentity::load_or_create(&root.join("secrets/host-noise.key"))?;
        NoiseIdentity::load_or_create(&root.join("secrets/hub-noise.key"))?;
        let installation_identity =
            HostIdentity::load_or_create(&root.join("secrets/installation.key"))?;
        let mut mews = Self::open(root)?;
        mews.store.initialize(
            &CommandContext::system(),
            host_name,
            &identity.public_key(),
            &noise_identity.public_key(),
            &installation_identity.public_key(),
        )?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for name in [
                DATABASE_FILE,
                "hub.lock",
                "secrets/host.key",
                "secrets/host-noise.key",
                "secrets/hub-noise.key",
                "secrets/installation.key",
            ] {
                let path = mews.root.join(name);
                if path.exists() {
                    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
                }
            }
        }
        Ok(mews)
    }

    pub fn installation(&self) -> Result<crate::Installation> {
        self.store
            .installation()?
            .context("MEWS is not set up; run `mews setup`")
    }

    pub fn set_relay_url(&self, context: &CommandContext, relay_url: &str) -> Result<()> {
        self.store.set_relay_url(context, relay_url)?;
        Ok(())
    }

    pub fn agents(&self) -> Result<Vec<Agent>> {
        Ok(self.store.agents()?)
    }

    pub(crate) fn agent_revision(&self, agent: &Agent) -> Result<crate::AgentRevision> {
        Ok(self
            .store
            .agent_revision(&agent.id, agent.current_revision)?)
    }
    pub fn sessions(&self) -> Result<Vec<Session>> {
        Ok(self.store.sessions()?)
    }
    pub fn hosts(&self) -> Result<Vec<crate::Host>> {
        Ok(self.store.hosts()?)
    }

    pub fn archive_agent(&self, context: &CommandContext, slug: &str) -> Result<()> {
        self.store.archive_agent(context, slug)?;
        Ok(())
    }

    pub fn remove_host(&self, context: &CommandContext, id: &crate::HostId) -> Result<()> {
        self.store.revoke_host(context, id)?;
        Ok(())
    }

    pub(crate) fn ensure_host(&self, id: &crate::HostId) -> Result<()> {
        self.store.host(id)?;
        Ok(())
    }
}

fn validate_installation_authority(root: &Path, store: &Store) -> Result<()> {
    let Some(installation) = store.installation()? else {
        return Ok(());
    };
    if installation.public_key.is_empty() {
        bail!("installation authority is missing; create fresh MEWS state or repair it explicitly");
    }
    let identity = HostIdentity::load(&root.join("secrets/installation.key"))?;
    if identity.public_key() != installation.public_key {
        bail!("secrets/installation.key does not match the authority recorded by Hub");
    }
    Ok(())
}

fn validate_hub_assignment(root: &Path, store: &Store) -> Result<()> {
    let Some(installation) = store.installation()? else {
        return Ok(());
    };
    let local = HostIdentity::load(&root.join("secrets/host.key"))?;
    let assigned = store.host(&installation.hub_host_id)?;
    if assigned.public_key != local.public_key() {
        bail!(
            "this Host is not the Hub for generation {}",
            installation.generation
        );
    }
    Ok(())
}
