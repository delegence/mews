use super::*;
use mews_protocol::{EventActor, JournalEvent, JournalSubjectType, RequestId};

impl Store {
    pub fn hosts(&self) -> Result<Vec<Host>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, name, public_key, noise_public_key, relay_url, created_at FROM hosts WHERE revoked = 0 ORDER BY name",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(Host {
                id: parse_id(row.get::<_, String>(0)?)?,
                name: row.get(1)?,
                public_key: row.get(2)?,
                noise_public_key: row.get(3)?,
                relay_url: row.get(4)?,
                created_at: parse_time(row.get::<_, String>(5)?)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn host(&self, id: &HostId) -> Result<Host, StoreError> {
        self.connection
            .query_row(
                "SELECT name, public_key, noise_public_key, relay_url, created_at FROM hosts WHERE id = ?1 AND revoked = 0",
                [id.as_str()],
                |row| {
                    Ok(Host {
                        id: id.clone(),
                        name: row.get(0)?,
                        public_key: row.get(1)?,
                        noise_public_key: row.get(2)?,
                        relay_url: row.get(3)?,
                        created_at: parse_time(row.get::<_, String>(4)?)?,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound {
                kind: "Host",
                id: id.to_string(),
            })
    }

    pub fn revoke_host(&self, context: &CommandContext, id: &HostId) -> Result<(), StoreError> {
        let request_hash = command_request_hash(&serde_json::json!({ "host_id": id }))?;
        self.transact_command(
            context,
            "revoke_host",
            request_hash,
            |transaction| {
                let installation = super::installation::required_installation_in(transaction)?;
                if installation.hub_host_id == *id {
                    return Err(StoreError::InvalidData(
                        "cannot remove the Host currently running Hub".into(),
                    ));
                }
                let exists: bool = transaction.query_row(
                    "SELECT EXISTS(SELECT 1 FROM hosts WHERE id = ?1 AND revoked = 0)",
                    [id.as_str()],
                    |row| row.get(0),
                )?;
                if !exists {
                    return Err(StoreError::NotFound {
                        kind: "Host",
                        id: id.to_string(),
                    });
                }
                Ok((
                    (),
                    vec![JournalSubjectAppend {
                        subject_type: JournalSubjectType::Host,
                        subject_id: id.to_string(),
                        entries: vec![NewJournalEntry::new(
                            EventActor::system(),
                            JournalEvent::HostRevoked {
                                host_id: id.clone(),
                            },
                        )],
                    }],
                ))
            },
            apply_host_events,
        )?;
        Ok(())
    }

    pub fn create_invitation(
        &self,
        context: &CommandContext,
        expires_at: DateTime<Utc>,
    ) -> Result<(InvitationId, String), StoreError> {
        // Invitation creation is intentionally not retryable: its one-time secret is
        // returned to the caller and is never persisted in recoverable plaintext.
        let command_id = format!(
            "{}:{}",
            context.operation_id("create_invitation"),
            RequestId::new()
        );
        let request_hash = command_request_hash(&serde_json::json!({
            "expires_at": expires_at,
        }))?;
        if expires_at <= Utc::now() {
            return Err(StoreError::InvalidData(
                "invitation expiry must be in the future".into(),
            ));
        }
        let id = InvitationId::new();
        let mut secret_bytes = [0; 32];
        OsRng.fill_bytes(&mut secret_bytes);
        let secret = URL_SAFE_NO_PAD.encode(secret_bytes);
        let verifier = secret_hash(&secret);
        let installation = required_installation(self)?;
        let mut append = JournalAppend {
            command_id,
            request_hash,
            result: serde_json::to_value(&id)
                .map_err(|error| StoreError::InvalidData(error.to_string()))?,
            subjects: vec![JournalSubjectAppend {
                subject_type: JournalSubjectType::Installation,
                subject_id: installation.id.to_string(),
                entries: vec![NewJournalEntry::new(
                    EventActor::system(),
                    JournalEvent::HostInvitationCreated {
                        invitation_id: id.clone(),
                        expires_at,
                        secret_hash: verifier,
                    },
                )],
            }],
        };
        context.decorate(&mut append);
        self.append_journal_entries_with(&append, apply_host_events)?;
        Ok((id, secret))
    }

    #[allow(clippy::too_many_arguments)] // one authenticated enrollment command boundary
    pub fn consume_invitation(
        &mut self,
        context: &CommandContext,
        invitation_id: &InvitationId,
        secret: &str,
        name: &str,
        public_key: &str,
        noise_public_key: &str,
        relay_url: &str,
    ) -> Result<Host, StoreError> {
        validate_name("Host name", name)?;
        let request_hash = command_request_hash(&serde_json::json!({
            "invitation_id": invitation_id,
            "secret_hash": secret_hash(secret),
            "name": name,
            "public_key": public_key,
            "noise_public_key": noise_public_key,
            "relay_url": relay_url,
        }))?;
        let (host, _) = self.transact_command(
            context,
            "consume_invitation",
            request_hash,
            |transaction| {
                let invitation = transaction
                    .query_row(
                        "SELECT secret_hash, expires_at, consumed_at FROM invitations WHERE id = ?1",
                        [invitation_id.as_str()],
                        |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                parse_time(row.get(1)?)?,
                                row.get::<_, Option<String>>(2)?,
                            ))
                        },
                    )
                    .optional()?
                    .ok_or_else(|| StoreError::NotFound {
                        kind: "invitation",
                        id: invitation_id.to_string(),
                    })?;
                if invitation.2.is_some()
                    || invitation.1 <= Utc::now()
                    || invitation.0 != secret_hash(secret)
                {
                    return Err(StoreError::InvalidData(
                        "invitation is invalid, expired, or already used".into(),
                    ));
                }
                let host = Host {
                    id: HostId::new(),
                    name: name.to_owned(),
                    public_key: public_key.to_owned(),
                    noise_public_key: noise_public_key.to_owned(),
                    relay_url: Some(relay_url.to_owned()),
                    created_at: Utc::now(),
                };
                let installation = super::installation::required_installation_in(transaction)?;
                let subjects = vec![
                    JournalSubjectAppend {
                        subject_type: JournalSubjectType::Installation,
                        subject_id: installation.id.to_string(),
                        entries: vec![NewJournalEntry::new(
                            EventActor::system(),
                            JournalEvent::HostInvitationConsumed {
                                invitation_id: invitation_id.clone(),
                                host_id: host.id.clone(),
                            },
                        )],
                    },
                    JournalSubjectAppend {
                        subject_type: JournalSubjectType::Host,
                        subject_id: host.id.to_string(),
                        entries: vec![NewJournalEntry::new(
                            EventActor::system(),
                            JournalEvent::HostEnrolled { host: host.clone() },
                        )],
                    },
                ];
                Ok((host, subjects))
            },
            apply_host_events,
        )?;
        Ok(host)
    }
}

fn required_installation(store: &Store) -> Result<Installation, StoreError> {
    store
        .installation()?
        .ok_or_else(|| StoreError::InvalidData("installation is missing".into()))
}

pub(crate) fn apply_host_events(
    transaction: &rusqlite::Transaction<'_>,
    events: &[mews_protocol::JournalEntry],
) -> Result<(), StoreError> {
    for event in events {
        match &event.payload {
            JournalEvent::HostEnrolled { host } => {
                transaction.execute(
                    "INSERT INTO hosts
                     (id, name, public_key, noise_public_key, relay_url, revoked, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6)",
                    params![
                        host.id.as_str(),
                        host.name,
                        host.public_key,
                        host.noise_public_key,
                        host.relay_url,
                        timestamp(host.created_at)
                    ],
                )?;
            }
            JournalEvent::HostRevoked { host_id } => {
                let changed = transaction.execute(
                    "UPDATE hosts SET revoked = 1
                     WHERE id = ?1 AND revoked = 0
                       AND id != (SELECT hub_host_id FROM installation WHERE singleton = 1)",
                    [host_id.as_str()],
                )?;
                if changed == 0 {
                    return Err(StoreError::NotFound {
                        kind: "Host",
                        id: host_id.to_string(),
                    });
                }
            }
            JournalEvent::HostInvitationCreated {
                invitation_id,
                expires_at,
                secret_hash,
            } => {
                transaction.execute(
                    "INSERT INTO invitations (id, secret_hash, expires_at, consumed_at)
                     VALUES (?1, ?2, ?3, NULL)",
                    params![invitation_id.as_str(), secret_hash, timestamp(*expires_at)],
                )?;
            }
            JournalEvent::HostInvitationConsumed { invitation_id, .. } => {
                let consumed = transaction.execute(
                    "UPDATE invitations SET consumed_at = ?2
                     WHERE id = ?1 AND consumed_at IS NULL",
                    params![invitation_id.as_str(), timestamp(event.recorded_at)],
                )?;
                if consumed != 1 {
                    return Err(StoreError::InvalidData(
                        "invitation was concurrently consumed".into(),
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}
