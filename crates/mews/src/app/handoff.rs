use super::*;
use sha2::Digest;
use std::io::Read;

impl Mews {
    pub fn begin_hub_move(&mut self, target: &crate::HostId) -> Result<HubSnapshot> {
        if self.store.active_run_count()? != 0 {
            bail!("cannot move Hub while Runs are active");
        }
        let previous = self.installation()?.hub_host_id;
        if previous == *target {
            bail!("target Host already runs Hub");
        }
        let previous_relay = self.store.host(&previous)?.relay_url;
        let target_relay = self
            .store
            .host(target)?
            .relay_url
            .context("target Host has no relay URL")?;
        self.store.set_installation_relay_url(&target_relay)?;
        self.store.move_hub(&previous, target)?;
        let moved = self.installation()?;
        let snapshot_path = self.root.join("hub-move.snapshot");
        let result = (|| -> Result<HubSnapshot> {
            if snapshot_path.exists() {
                fs::remove_file(&snapshot_path)?;
            }
            self.store.backup_to(&snapshot_path)?;
            let database_size = fs::metadata(&snapshot_path)?.len();
            let mut database = fs::File::open(&snapshot_path)?;
            let mut hasher = sha2::Sha256::new();
            let mut chunk = [0_u8; 96 * 1024];
            loop {
                let read = database.read(&mut chunk)?;
                if read == 0 {
                    break;
                }
                sha2::Digest::update(&mut hasher, &chunk[..read]);
            }
            Ok(HubSnapshot {
                move_nonce: uuid::Uuid::now_v7().to_string(),
                installation_id: moved.id,
                generation: moved.generation,
                database_path: snapshot_path,
                database_size,
                database_sha256: format!("{:x}", hasher.finalize()),
                installation_key: fs::read(self.root.join("secrets/installation.key"))?,
                hub_noise_key: fs::read(self.root.join("secrets/hub-noise.key"))?,
                credentials: fs::read(self.root.join("auth.json")).unwrap_or_default(),
                previous_hub: previous.clone(),
                target_hub: target.clone(),
            })
        })();
        if result.is_err() {
            let _ = self.store.move_hub(target, &previous);
            if let Some(previous_relay) = previous_relay {
                let _ = self.store.set_installation_relay_url(&previous_relay);
            }
        }
        result
    }

    pub fn rollback_hub_move(&mut self, snapshot: &HubSnapshot) -> Result<()> {
        self.store
            .move_hub(&snapshot.target_hub, &snapshot.previous_hub)?;
        if let Some(relay_url) = self.store.host(&snapshot.previous_hub)?.relay_url {
            self.store.set_installation_relay_url(&relay_url)?;
        }
        Ok(())
    }

    pub fn demoted_host_state(
        &self,
        target_id: &crate::HostId,
    ) -> Result<crate::enrollment::join::JoinedHostState> {
        let installation = self.installation()?;
        let old_host = self.store.host(&installation.hub_host_id)?;
        let target = self.store.host(target_id)?;
        let relay_url = target.relay_url.context("target Host has no relay URL")?;
        let authority = HostIdentity::load(&self.root.join("secrets/installation.key"))?;
        let expires_at = chrono::Utc::now() + chrono::Duration::days(36_500);
        let hub_noise_public_key =
            NoiseIdentity::load(&self.root.join("secrets/hub-noise.key"))?.public_key();
        let relay_admission = self.relay_admission_for_host(&old_host)?;
        let hub_relay_admission = RelayAdmission::create(
            &authority,
            installation.id.clone(),
            RelayPeerId::new(format!("hub-host:{}", old_host.id))?,
            authority.public_key(),
            expires_at,
        );
        Ok(crate::enrollment::join::JoinedHostState {
            installation_id: installation.id,
            installation_public_key: authority.public_key(),
            hub_noise_public_key,
            relay_urls: vec![relay_url.clone()],
            accepted: crate::enrollment::join::EnrollmentAccepted {
                host: old_host,
                relay_urls: vec![relay_url],
                relay_admission,
                hub_relay_admission,
            },
        })
    }

    pub fn create_invitation(
        &self,
        relay_url: Option<&str>,
    ) -> Result<crate::enrollment::JoinOffer> {
        let installation = self.installation()?;
        let relay_url = relay_url
            .map(str::to_owned)
            .or(installation.relay_url.clone())
            .context("no relay is configured; pass `--relay <URL>`")?;
        let expires_at = chrono::Utc::now() + chrono::Duration::minutes(15);
        let (invitation_id, secret) = self.store.create_invitation(expires_at)?;
        let identity = HostIdentity::load(&self.root.join("secrets/installation.key"))?;
        let hub_noise = NoiseIdentity::load(&self.root.join("secrets/hub-noise.key"))?;
        self.store.set_relay_url(&relay_url)?;
        crate::enrollment::JoinOffer::create(
            installation.id,
            invitation_id,
            installation.hub_host_id,
            &identity,
            crate::enrollment::JoinOfferConnection {
                relay_url,
                hub_noise_public_key: hub_noise.public_key(),
                secret,
                expires_at,
            },
        )
    }

    pub fn remote_host_acceptances(
        &self,
    ) -> Result<Vec<(Vec<String>, crate::enrollment::join::EnrollmentAccepted)>> {
        let installation = self.installation()?;
        let authority = HostIdentity::load(&self.root.join("secrets/installation.key"))?;
        self.hosts()?
            .into_iter()
            .filter(|host| host.id != installation.hub_host_id)
            .map(|host| {
                let relay_urls = self.relay_candidates()?;
                let relay_admission = self.relay_admission_for_host(&host)?;
                let hub_relay_admission = RelayAdmission::create(
                    &authority,
                    installation.id.clone(),
                    RelayPeerId::new(format!("hub-host:{}", host.id))?,
                    authority.public_key(),
                    chrono::Utc::now() + chrono::Duration::days(36_500),
                );
                Ok((
                    relay_urls.clone(),
                    crate::enrollment::join::EnrollmentAccepted {
                        host,
                        relay_urls,
                        relay_admission,
                        hub_relay_admission,
                    },
                ))
            })
            .collect()
    }

    pub(crate) fn remote_host_acceptance(
        &self,
        host_id: &crate::HostId,
    ) -> Result<(Vec<String>, crate::enrollment::join::EnrollmentAccepted)> {
        let installation = self.installation()?;
        let authority = HostIdentity::load(&self.root.join("secrets/installation.key"))?;
        let host = self.store.host(host_id)?;
        let relay_urls = self.relay_candidates()?;
        Ok((
            relay_urls.clone(),
            crate::enrollment::join::EnrollmentAccepted {
                relay_urls,
                relay_admission: self.relay_admission_for_host(&host)?,
                hub_relay_admission: RelayAdmission::create(
                    &authority,
                    installation.id,
                    RelayPeerId::new(format!("hub-host:{}", host.id))?,
                    authority.public_key(),
                    chrono::Utc::now() + chrono::Duration::days(36_500),
                ),
                host,
            },
        ))
    }

    pub fn enroll_host(
        &mut self,
        offer: &crate::enrollment::JoinOffer,
        request: &crate::enrollment::JoinRequest,
    ) -> Result<crate::Host> {
        offer.verify()?;
        request.verify(offer)?;
        let installation = self.installation()?;
        if offer.installation_id != installation.id
            || offer.installation_public_key != installation.public_key
        {
            bail!("invitation does not belong to this installation");
        }
        Ok(self.store.consume_invitation(
            &offer.invitation_id,
            &offer.secret,
            &request.host_name,
            &request.host_public_key,
            &request.host_noise_public_key,
            &request.relay_url,
        )?)
    }

    pub fn relay_candidates(&self) -> Result<Vec<String>> {
        let installation = self.installation()?;
        let mut urls = Vec::new();
        if let Some(url) = installation.relay_url {
            urls.push(url);
        }
        for host in self.hosts()? {
            if let Some(url) = host.relay_url
                && !urls.contains(&url)
            {
                urls.push(url);
            }
        }
        Ok(urls)
    }

    pub fn relay_admission_for_host(&self, host: &crate::Host) -> Result<RelayAdmission> {
        let installation = self.installation()?;
        let authority = HostIdentity::load(&self.root.join("secrets/installation.key"))?;
        Ok(RelayAdmission::create(
            &authority,
            installation.id,
            RelayPeerId::new(format!("host:{}", host.id))?,
            host.public_key.clone(),
            chrono::Utc::now() + chrono::Duration::days(36_500),
        ))
    }
}
