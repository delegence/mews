use super::*;
use mews_protocol::{EventActor, JournalEvent, JournalSubjectType};

impl Store {
    pub fn initialize(
        &mut self,
        context: &CommandContext,
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
        let mut append = JournalAppend {
            command_id: context.operation_id("initialize"),
            request_hash: command_request_hash(&serde_json::json!({
                "host_name": host_name,
                "host_public_key": host_public_key,
                "host_noise_public_key": host_noise_public_key,
                "installation_public_key": installation_public_key,
            }))?,
            result: serde_json::to_value(&installation)
                .map_err(|error| StoreError::InvalidData(error.to_string()))?,
            subjects: vec![
                JournalSubjectAppend {
                    subject_type: JournalSubjectType::Host,
                    subject_id: host.id.to_string(),
                    entries: vec![NewJournalEntry::new(
                        EventActor::system(),
                        JournalEvent::HostEnrolled { host: host.clone() },
                    )],
                },
                JournalSubjectAppend {
                    subject_type: JournalSubjectType::Installation,
                    subject_id: installation.id.to_string(),
                    entries: vec![NewJournalEntry::new(
                        EventActor::system(),
                        JournalEvent::InstallationCreated {
                            installation: installation.clone(),
                        },
                    )],
                },
            ],
        };
        context.decorate(&mut append);
        self.append_journal_entries_with(&append, |transaction, events| {
            super::hosts::apply_host_events(transaction, events)?;
            apply_installation_events(transaction, events)
        })?;
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

    pub fn set_relay_url(
        &self,
        context: &CommandContext,
        relay_url: &str,
    ) -> Result<(), StoreError> {
        let installation = required_installation(self)?;
        append_installation_event(
            self,
            context,
            "set_relay_url",
            serde_json::json!({ "relay_url": relay_url }),
            JournalEvent::RelayChanged {
                relay_url: Some(relay_url.to_owned()),
                host_id: Some(installation.hub_host_id.clone()),
            },
        )
    }

    pub fn set_installation_relay_url(
        &self,
        context: &CommandContext,
        relay_url: &str,
    ) -> Result<(), StoreError> {
        append_installation_event(
            self,
            context,
            "set_installation_relay_url",
            serde_json::json!({ "relay_url": relay_url }),
            JournalEvent::RelayChanged {
                relay_url: Some(relay_url.to_owned()),
                host_id: None,
            },
        )
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

    pub fn set_default_model(
        &self,
        context: &CommandContext,
        model: &str,
    ) -> Result<(), StoreError> {
        let request_hash = command_request_hash(&serde_json::json!({ "model": model }))?;
        self.transact_command(
            context,
            "set_default_model",
            request_hash,
            |transaction| {
                let installation = required_installation_in(transaction)?;
                let reasoning = setting_in(transaction, "default_reasoning")?
                    .map(|value| serde_json::from_str(&value))
                    .transpose()
                    .map_err(|error| StoreError::InvalidData(error.to_string()))?;
                Ok((
                    (),
                    vec![JournalSubjectAppend {
                        subject_type: JournalSubjectType::Installation,
                        subject_id: installation.id.to_string(),
                        entries: vec![NewJournalEntry::new(
                            EventActor::system(),
                            JournalEvent::ProviderDefaultsChanged {
                                defaults: ProviderDefaults {
                                    model: Some(model.to_owned()),
                                    reasoning,
                                },
                            },
                        )],
                    }],
                ))
            },
            apply_installation_events,
        )?;
        Ok(())
    }

    pub fn set_default_reasoning(
        &self,
        context: &CommandContext,
        reasoning: Option<crate::ReasoningEffort>,
    ) -> Result<(), StoreError> {
        let request_hash = command_request_hash(&serde_json::json!({ "reasoning": reasoning }))?;
        self.transact_command(
            context,
            "set_default_reasoning",
            request_hash,
            |transaction| {
                let installation = required_installation_in(transaction)?;
                let model = setting_in(transaction, "default_model")?;
                Ok((
                    (),
                    vec![JournalSubjectAppend {
                        subject_type: JournalSubjectType::Installation,
                        subject_id: installation.id.to_string(),
                        entries: vec![NewJournalEntry::new(
                            EventActor::system(),
                            JournalEvent::ProviderDefaultsChanged {
                                defaults: ProviderDefaults { model, reasoning },
                            },
                        )],
                    }],
                ))
            },
            apply_installation_events,
        )?;
        Ok(())
    }

    fn setting(&self, key: &str) -> Result<Option<String>, StoreError> {
        self.connection
            .query_row("SELECT value FROM settings WHERE key = ?1", [key], |row| {
                row.get(0)
            })
            .optional()
            .map_err(Into::into)
    }

    pub fn active_turn_count(&self) -> Result<u64, StoreError> {
        Ok(self.connection.query_row(
            "SELECT COUNT(*) FROM turns WHERE completed_at IS NULL",
            [],
            |row| row.get(0),
        )?)
    }

    pub fn move_hub(
        &mut self,
        context: &CommandContext,
        expected: &HostId,
        target: &HostId,
    ) -> Result<Installation, StoreError> {
        let request_hash =
            command_request_hash(&serde_json::json!({ "expected": expected, "target": target }))?;
        let (installation, _) = self.transact_command(
            context,
            "move_hub",
            request_hash,
            |transaction| {
                let target_exists: bool = transaction.query_row(
                    "SELECT EXISTS(SELECT 1 FROM hosts WHERE id = ?1 AND revoked = 0)",
                    [target.as_str()],
                    |row| row.get(0),
                )?;
                if !target_exists {
                    return Err(StoreError::NotFound {
                        kind: "Host",
                        id: target.to_string(),
                    });
                }
                let mut installation = required_installation_in(transaction)?;
                if installation.hub_host_id != *expected {
                    return Err(StoreError::InvalidData(
                        "Hub generation changed concurrently".into(),
                    ));
                }
                installation.hub_host_id = target.clone();
                installation.generation += 1;
                let subjects = vec![JournalSubjectAppend {
                    subject_type: JournalSubjectType::Installation,
                    subject_id: installation.id.to_string(),
                    entries: vec![NewJournalEntry::new(
                        EventActor::system(),
                        JournalEvent::HubChanged {
                            host_id: target.clone(),
                            generation: installation.generation,
                        },
                    )],
                }];
                Ok((installation, subjects))
            },
            apply_installation_events,
        )?;
        Ok(installation)
    }

    pub fn backup_to(&self, path: &Path) -> Result<(), StoreError> {
        let mut destination = Connection::open(path)?;
        let backup = rusqlite::backup::Backup::new(&self.connection, &mut destination)?;
        backup.run_to_completion(64, Duration::from_millis(10), None)?;
        Ok(())
    }
}

fn required_installation(store: &Store) -> Result<Installation, StoreError> {
    store
        .installation()?
        .ok_or_else(|| StoreError::InvalidData("installation is missing".into()))
}

pub(crate) fn required_installation_in(
    connection: &rusqlite::Connection,
) -> Result<Installation, StoreError> {
    connection
        .query_row(
            "SELECT id, public_key, relay_url, hub_host_id, generation, created_at
             FROM installation WHERE singleton = 1",
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
        .optional()?
        .ok_or_else(|| StoreError::InvalidData("installation is missing".into()))
}

fn setting_in(connection: &rusqlite::Connection, key: &str) -> Result<Option<String>, StoreError> {
    connection
        .query_row("SELECT value FROM settings WHERE key = ?1", [key], |row| {
            row.get(0)
        })
        .optional()
        .map_err(Into::into)
}

fn append_installation_event(
    store: &Store,
    context: &CommandContext,
    command: &str,
    request: Value,
    payload: JournalEvent,
) -> Result<(), StoreError> {
    let installation = required_installation(store)?;
    let mut append = JournalAppend {
        command_id: context.operation_id(command),
        request_hash: command_request_hash(&request)?,
        result: Value::Null,
        subjects: vec![JournalSubjectAppend {
            subject_type: JournalSubjectType::Installation,
            subject_id: installation.id.to_string(),
            entries: vec![NewJournalEntry::new(EventActor::system(), payload)],
        }],
    };
    context.decorate(&mut append);
    store.append_journal_entries_with(&append, apply_installation_events)?;
    Ok(())
}

pub(crate) fn apply_installation_events(
    transaction: &rusqlite::Transaction<'_>,
    events: &[mews_protocol::JournalEntry],
) -> Result<(), StoreError> {
    for event in events {
        match &event.payload {
            JournalEvent::InstallationCreated { installation } => {
                transaction.execute(
                    "INSERT INTO installation
                     (singleton, id, public_key, relay_url, hub_host_id, generation, created_at)
                     VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        installation.id.as_str(),
                        installation.public_key,
                        installation.relay_url,
                        installation.hub_host_id.as_str(),
                        installation.generation,
                        timestamp(installation.created_at)
                    ],
                )?;
            }
            JournalEvent::RelayChanged { relay_url, host_id } => {
                transaction.execute(
                    "UPDATE installation SET relay_url = ?1 WHERE singleton = 1",
                    [relay_url],
                )?;
                if let Some(host_id) = host_id {
                    transaction.execute(
                        "UPDATE hosts SET relay_url = ?1 WHERE id = ?2",
                        params![relay_url, host_id.as_str()],
                    )?;
                }
            }
            JournalEvent::ProviderDefaultsChanged { defaults } => {
                match &defaults.model {
                    Some(model) => {
                        transaction.execute(
                            "INSERT INTO settings (key, value) VALUES ('default_model', ?1)
                             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                            [model],
                        )?;
                    }
                    None => {
                        transaction
                            .execute("DELETE FROM settings WHERE key = 'default_model'", [])?;
                    }
                }
                match defaults.reasoning {
                    Some(reasoning) => {
                        let value = serde_json::to_string(&reasoning)
                            .map_err(|error| StoreError::InvalidData(error.to_string()))?;
                        transaction.execute(
                            "INSERT INTO settings (key, value) VALUES ('default_reasoning', ?1)
                             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                            [value],
                        )?;
                    }
                    None => {
                        transaction
                            .execute("DELETE FROM settings WHERE key = 'default_reasoning'", [])?;
                    }
                }
            }
            JournalEvent::HubChanged {
                host_id,
                generation,
            } => {
                transaction.execute(
                    "UPDATE installation SET hub_host_id = ?1, generation = ?2
                     WHERE singleton = 1",
                    params![host_id.as_str(), generation],
                )?;
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod transactional_tests {
    use super::*;

    #[test]
    fn concurrent_hub_moves_share_one_owner_and_generation_fence() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("state.db");
        let mut setup = Store::open(&database).unwrap();
        let original = setup
            .initialize(
                &CommandContext::system(),
                "laptop",
                "host-key",
                "noise-key",
                "installation-key",
            )
            .unwrap();
        let enroll = |store: &mut Store, name: &str| {
            let (invitation_id, secret) = store
                .create_invitation(
                    &CommandContext::system(),
                    Utc::now() + chrono::Duration::minutes(5),
                )
                .unwrap();
            store
                .consume_invitation(
                    &CommandContext::system(),
                    &invitation_id,
                    &secret,
                    name,
                    &format!("{name}-key"),
                    &format!("{name}-noise"),
                    "wss://relay.example",
                )
                .unwrap()
        };
        let first_target = enroll(&mut setup, "desktop");
        let second_target = enroll(&mut setup, "server");
        drop(setup);

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let first_store = Store::open(&database).unwrap();
        let second_store = Store::open(&database).unwrap();
        let move_in_thread = |mut store: Store, target: HostId| {
            let barrier = barrier.clone();
            let expected = original.hub_host_id.clone();
            std::thread::spawn(move || {
                barrier.wait();
                (
                    store
                        .move_hub(&CommandContext::system(), &expected, &target)
                        .is_ok(),
                    target,
                )
            })
        };
        let first = move_in_thread(first_store, first_target.id.clone());
        let second = move_in_thread(second_store, second_target.id.clone());
        let first = first.join().unwrap();
        let second = second.join().unwrap();

        assert_ne!(first.0, second.0);
        let winner = if first.0 { first.1 } else { second.1 };
        let store = Store::open(&database).unwrap();
        let installation = store.installation().unwrap().unwrap();
        assert_eq!(installation.hub_host_id, winner);
        assert_eq!(installation.generation, 2);
        assert_eq!(
            store
                .journal_entries_after(0, 1_000)
                .unwrap()
                .iter()
                .filter(|event| matches!(event.payload, JournalEvent::HubChanged { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn changing_model_preserves_existing_reasoning_default() {
        let mut store = Store::open_in_memory().unwrap();
        store
            .initialize(
                &CommandContext::system(),
                "laptop",
                "host-key",
                "noise-key",
                "installation-key",
            )
            .unwrap();
        store
            .set_default_reasoning(&CommandContext::system(), Some(ReasoningEffort::High))
            .unwrap();
        store
            .set_default_model(&CommandContext::system(), "test/new-model")
            .unwrap();

        assert_eq!(
            store.provider_defaults().unwrap(),
            ProviderDefaults {
                model: Some("test/new-model".into()),
                reasoning: Some(ReasoningEffort::High),
            }
        );
    }

    #[test]
    fn concurrent_provider_default_changes_merge_under_the_write_transaction() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("state.db");
        let mut setup = Store::open(&database).unwrap();
        setup
            .initialize(
                &CommandContext::system(),
                "laptop",
                "host-key",
                "noise-key",
                "installation-key",
            )
            .unwrap();
        drop(setup);

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let model = {
            let barrier = barrier.clone();
            let store = Store::open(&database).unwrap();
            std::thread::spawn(move || {
                barrier.wait();
                store
                    .set_default_model(&CommandContext::system(), "test/model")
                    .unwrap();
            })
        };
        let reasoning = {
            let store = Store::open(&database).unwrap();
            std::thread::spawn(move || {
                barrier.wait();
                store
                    .set_default_reasoning(&CommandContext::system(), Some(ReasoningEffort::High))
                    .unwrap();
            })
        };
        model.join().unwrap();
        reasoning.join().unwrap();

        assert_eq!(
            Store::open(&database).unwrap().provider_defaults().unwrap(),
            ProviderDefaults {
                model: Some("test/model".into()),
                reasoning: Some(ReasoningEffort::High),
            }
        );
    }
}
