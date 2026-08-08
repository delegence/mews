use super::*;

impl Store {
    pub fn initialize(
        &mut self,
        host_name: &str,
        host_public_key: &str,
        host_noise_public_key: &str,
        installation_public_key: &str,
    ) -> Result<Installation, StoreError> {
        validate_name("Host name", host_name)?;
        if let Some(existing) = self.installation()? {
            return Ok(existing);
        }
        let now = Utc::now();
        let host = Host {
            id: HostId::new(),
            name: host_name.to_owned(),
            public_key: host_public_key.to_owned(),
            noise_public_key: host_noise_public_key.to_owned(),
            relay_url: None,
            created_at: now,
        };
        let installation = Installation {
            id: InstallationId::new(),
            public_key: installation_public_key.to_owned(),
            relay_url: None,
            hub_host_id: host.id.clone(),
            generation: 1,
            created_at: now,
        };
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "INSERT INTO hosts (id, name, public_key, noise_public_key, relay_url, created_at) VALUES (?1, ?2, ?3, ?4, NULL, ?5)",
            params![host.id.as_str(), host.name, host.public_key, host.noise_public_key, timestamp(now)],
        )?;
        transaction.execute(
            "INSERT INTO installation (singleton, id, public_key, relay_url, hub_host_id, generation, created_at)
             VALUES (1, ?1, ?2, NULL, ?3, ?4, ?5)",
            params![
                installation.id.as_str(),
                installation.public_key,
                host.id.as_str(),
                installation.generation,
                timestamp(now)
            ],
        )?;
        transaction.commit()?;
        Ok(installation)
    }

    pub fn installation(&self) -> Result<Option<Installation>, StoreError> {
        self.connection
            .query_row(
                "SELECT id, public_key, relay_url, hub_host_id, generation, created_at FROM installation WHERE singleton = 1",
                [],
                |row| {
                    Ok(Installation {
                        id: parse_id(row.get::<_, String>(0)?)?,
                        public_key: row.get(1)?,
                        relay_url: row.get(2)?,
                        hub_host_id: parse_id(row.get::<_, String>(3)?)?,
                        generation: row.get(4)?,
                        created_at: parse_time(row.get::<_, String>(5)?)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn set_relay_url(&self, relay_url: &str) -> Result<(), StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        transaction.execute(
            "UPDATE hosts SET relay_url = ?1 WHERE id = (SELECT hub_host_id FROM installation WHERE singleton = 1)",
            [relay_url],
        )?;
        transaction.execute(
            "UPDATE installation SET relay_url = ?1 WHERE singleton = 1",
            [relay_url],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn set_installation_relay_url(&self, relay_url: &str) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE installation SET relay_url = ?1 WHERE singleton = 1",
            [relay_url],
        )?;
        Ok(())
    }

    pub fn provider_defaults(&self) -> Result<crate::ProviderDefaults, StoreError> {
        let model = self.setting("default_model")?;
        let reasoning = self
            .setting("default_reasoning")?
            .map(|value| serde_json::from_str(&value))
            .transpose()
            .map_err(|error| StoreError::InvalidData(error.to_string()))?;
        Ok(crate::ProviderDefaults { model, reasoning })
    }

    pub fn set_default_model(&self, model: &str) -> Result<(), StoreError> {
        self.set_setting("default_model", model)?;
        self.connection
            .execute("DELETE FROM settings WHERE key = 'default_reasoning'", [])?;
        Ok(())
    }

    pub fn set_default_reasoning(
        &self,
        reasoning: Option<crate::ReasoningEffort>,
    ) -> Result<(), StoreError> {
        match reasoning {
            Some(value) => self.set_setting(
                "default_reasoning",
                &serde_json::to_string(&value)
                    .map_err(|e| StoreError::InvalidData(e.to_string()))?,
            ),
            None => {
                self.connection
                    .execute("DELETE FROM settings WHERE key = 'default_reasoning'", [])?;
                Ok(())
            }
        }
    }

    fn setting(&self, key: &str) -> Result<Option<String>, StoreError> {
        self.connection
            .query_row("SELECT value FROM settings WHERE key = ?1", [key], |row| {
                row.get(0)
            })
            .optional()
            .map_err(Into::into)
    }

    fn set_setting(&self, key: &str, value: &str) -> Result<(), StoreError> {
        self.connection.execute("INSERT INTO settings (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value", params![key, value])?;
        Ok(())
    }

    pub fn active_run_count(&self) -> Result<u64, StoreError> {
        Ok(self.connection.query_row(
            "SELECT COUNT(*) FROM runs WHERE completed_at IS NULL",
            [],
            |row| row.get(0),
        )?)
    }

    pub fn move_hub(
        &mut self,
        expected: &HostId,
        target: &HostId,
    ) -> Result<Installation, StoreError> {
        self.host(target)?;
        let changed = self.connection.execute(
            "UPDATE installation SET hub_host_id = ?2, generation = generation + 1
             WHERE singleton = 1 AND hub_host_id = ?1",
            params![expected.as_str(), target.as_str()],
        )?;
        if changed != 1 {
            return Err(StoreError::InvalidData(
                "Hub generation changed concurrently".into(),
            ));
        }
        self.installation()?
            .ok_or_else(|| StoreError::InvalidData("installation is missing".into()))
    }

    pub fn backup_to(&self, path: &Path) -> Result<(), StoreError> {
        let mut destination = Connection::open(path)?;
        let backup = rusqlite::backup::Backup::new(&self.connection, &mut destination)?;
        backup.run_to_completion(64, Duration::from_millis(10), None)?;
        Ok(())
    }
}
