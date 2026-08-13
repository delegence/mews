use super::*;
use mews_protocol::{EventActor, EventActorKind, JournalEvent, JournalSubjectType};

impl Store {
    pub fn create_agent(
        &mut self,
        context: &CommandContext,
        slug: &str,
        soul: &str,
        config_toml: &str,
        author_host_id: &HostId,
    ) -> Result<(Agent, AgentRevision), StoreError> {
        validate_slug(slug)?;
        validate_definition(soul, config_toml)?;
        let request_hash = command_request_hash(&serde_json::json!({
            "slug": slug,
            "soul": soul,
            "config_toml": config_toml,
            "author_host_id": author_host_id,
        }))?;
        let (result, _) = self.transact_command(
            context,
            "create_agent",
            request_hash,
            |_| {
                let now = Utc::now();
                let agent = Agent {
                    id: AgentId::new(),
                    slug: slug.to_owned(),
                    current_revision: 1,
                    archived: false,
                    created_at: now,
                };
                let revision = revision(&agent.id, 1, soul, config_toml, author_host_id, now);
                let subjects = vec![JournalSubjectAppend {
                    subject_type: JournalSubjectType::Agent,
                    subject_id: agent.id.to_string(),
                    entries: vec![NewJournalEntry::new(
                        host_actor(author_host_id),
                        JournalEvent::AgentCreated {
                            agent: agent.clone(),
                            initial_revision: revision.clone(),
                        },
                    )],
                }];
                Ok(((agent, revision), subjects))
            },
            apply_agent_events,
        )?;
        Ok(result)
    }

    pub fn agents(&self) -> Result<Vec<Agent>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, slug, current_revision, archived, created_at
             FROM agents WHERE archived = 0 ORDER BY slug",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(Agent {
                id: parse_id(row.get::<_, String>(0)?)?,
                slug: row.get(1)?,
                current_revision: row.get(2)?,
                archived: row.get(3)?,
                created_at: parse_time(row.get::<_, String>(4)?)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn rename_agent(
        &self,
        context: &CommandContext,
        slug: &str,
        new_slug: &str,
    ) -> Result<Agent, StoreError> {
        self.rename_agent_with_command(context, "rename_agent", slug, new_slug)
    }

    /// Compensates a committed rename when the corresponding filesystem move fails.
    pub fn rollback_agent_rename(
        &self,
        context: &CommandContext,
        slug: &str,
        new_slug: &str,
    ) -> Result<Agent, StoreError> {
        self.rename_agent_with_command(context, "rollback_agent_rename", slug, new_slug)
    }

    fn rename_agent_with_command(
        &self,
        context: &CommandContext,
        command: &str,
        slug: &str,
        new_slug: &str,
    ) -> Result<Agent, StoreError> {
        validate_slug(new_slug)?;
        let request_hash = command_request_hash(&serde_json::json!({
            "slug": slug,
            "new_slug": new_slug,
        }))?;
        let (agent, _) = self.transact_command(
            context,
            command,
            request_hash,
            |transaction| {
                let mut agent = select_agent_by_slug(transaction, slug)?;
                agent.slug = new_slug.to_owned();
                let subjects = vec![JournalSubjectAppend {
                    subject_type: JournalSubjectType::Agent,
                    subject_id: agent.id.to_string(),
                    entries: vec![NewJournalEntry::new(
                        EventActor::system(),
                        JournalEvent::AgentRenamed {
                            slug: new_slug.to_owned(),
                        },
                    )],
                }];
                Ok((agent, subjects))
            },
            apply_agent_events,
        )?;
        Ok(agent)
    }

    pub fn archive_agent(&self, context: &CommandContext, slug: &str) -> Result<(), StoreError> {
        let request_hash = command_request_hash(&serde_json::json!({ "slug": slug }))?;
        self.transact_command(
            context,
            "archive_agent",
            request_hash,
            |transaction| {
                let agent = select_agent_by_slug(transaction, slug)?;
                Ok((
                    (),
                    vec![JournalSubjectAppend {
                        subject_type: JournalSubjectType::Agent,
                        subject_id: agent.id.to_string(),
                        entries: vec![NewJournalEntry::new(
                            EventActor::system(),
                            JournalEvent::AgentArchived,
                        )],
                    }],
                ))
            },
            apply_agent_events,
        )?;
        Ok(())
    }

    pub fn agent_by_slug(&self, slug: &str) -> Result<Agent, StoreError> {
        select_agent_by_slug(&self.connection, slug)
    }

    pub fn agent_revision(
        &self,
        agent_id: &AgentId,
        revision: u64,
    ) -> Result<AgentRevision, StoreError> {
        self.connection
            .query_row(
                "SELECT soul, config_toml, content_hash, author_host_id, created_at
                 FROM agent_revisions WHERE agent_id = ?1 AND revision = ?2",
                params![agent_id.as_str(), revision],
                |row| {
                    Ok(AgentRevision {
                        agent_id: agent_id.clone(),
                        revision,
                        soul: row.get(0)?,
                        config_toml: row.get(1)?,
                        content_hash: row.get(2)?,
                        author_host_id: parse_id(row.get::<_, String>(3)?)?,
                        created_at: parse_time(row.get::<_, String>(4)?)?,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound {
                kind: "agent revision",
                id: format!("{agent_id}@{revision}"),
            })
    }

    pub fn update_agent(
        &mut self,
        context: &CommandContext,
        agent_id: &AgentId,
        expected_revision: u64,
        soul: &str,
        config_toml: &str,
        author_host_id: &HostId,
    ) -> Result<AgentRevision, StoreError> {
        validate_definition(soul, config_toml)?;
        let request_hash = command_request_hash(&serde_json::json!({
            "agent_id": agent_id,
            "expected_revision": expected_revision,
            "soul": soul,
            "config_toml": config_toml,
            "author_host_id": author_host_id,
        }))?;
        let (revision, _) = self.transact_command(
            context,
            "update_agent",
            request_hash,
            |transaction| {
                let current: Option<u64> = transaction
                    .query_row(
                        "SELECT current_revision FROM agents WHERE id = ?1 AND archived = 0",
                        [agent_id.as_str()],
                        |row| row.get(0),
                    )
                    .optional()?;
                let current = current.ok_or_else(|| StoreError::NotFound {
                    kind: "agent",
                    id: agent_id.to_string(),
                })?;
                if current != expected_revision {
                    return Err(StoreError::RevisionConflict {
                        expected: expected_revision,
                        current,
                    });
                }
                let revision = revision(
                    agent_id,
                    current + 1,
                    soul,
                    config_toml,
                    author_host_id,
                    Utc::now(),
                );
                let subjects = vec![JournalSubjectAppend {
                    subject_type: JournalSubjectType::Agent,
                    subject_id: agent_id.to_string(),
                    entries: vec![NewJournalEntry::new(
                        host_actor(author_host_id),
                        JournalEvent::AgentRevisionCreated {
                            revision: revision.clone(),
                        },
                    )],
                }];
                Ok((revision, subjects))
            },
            apply_agent_events,
        )?;
        Ok(revision)
    }
}

fn select_agent_by_slug(
    connection: &rusqlite::Connection,
    slug: &str,
) -> Result<Agent, StoreError> {
    connection
        .query_row(
            "SELECT id, slug, current_revision, archived, created_at
             FROM agents WHERE slug = ?1 AND archived = 0",
            [slug],
            |row| {
                Ok(Agent {
                    id: parse_id(row.get::<_, String>(0)?)?,
                    slug: row.get(1)?,
                    current_revision: row.get(2)?,
                    archived: row.get(3)?,
                    created_at: parse_time(row.get::<_, String>(4)?)?,
                })
            },
        )
        .optional()?
        .ok_or_else(|| StoreError::NotFound {
            kind: "agent",
            id: slug.to_owned(),
        })
}

fn host_actor(host_id: &HostId) -> EventActor {
    EventActor {
        kind: EventActorKind::Host,
        id: Some(host_id.to_string()),
    }
}

pub(crate) fn apply_agent_events(
    transaction: &rusqlite::Transaction<'_>,
    events: &[mews_protocol::JournalEntry],
) -> Result<(), StoreError> {
    for event in events {
        match &event.payload {
            JournalEvent::AgentCreated {
                agent,
                initial_revision,
            } => {
                let inserted = transaction.execute(
                    "INSERT OR IGNORE INTO agents (id, slug, current_revision, archived, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        agent.id.as_str(),
                        agent.slug,
                        agent.current_revision,
                        agent.archived,
                        timestamp(agent.created_at)
                    ],
                )?;
                if inserted == 0 {
                    return Err(StoreError::DuplicateAgent(agent.slug.clone()));
                }
                insert_revision(transaction, initial_revision)?;
            }
            JournalEvent::AgentRevisionCreated { revision } => {
                insert_revision(transaction, revision)?;
                let changed = transaction.execute(
                    "UPDATE agents SET current_revision = ?2
                     WHERE id = ?1 AND current_revision = ?3 AND archived = 0",
                    params![
                        revision.agent_id.as_str(),
                        revision.revision,
                        revision.revision - 1
                    ],
                )?;
                if changed != 1 {
                    let current = transaction
                        .query_row(
                            "SELECT current_revision FROM agents WHERE id = ?1",
                            [revision.agent_id.as_str()],
                            |row| row.get(0),
                        )
                        .optional()?
                        .ok_or_else(|| StoreError::NotFound {
                            kind: "agent",
                            id: revision.agent_id.to_string(),
                        })?;
                    return Err(StoreError::RevisionConflict {
                        expected: revision.revision - 1,
                        current,
                    });
                }
            }
            JournalEvent::AgentRenamed { slug } => {
                let changed = transaction.execute(
                    "UPDATE OR IGNORE agents SET slug = ?2 WHERE id = ?1 AND archived = 0",
                    params![event.subject.id, slug],
                )?;
                if changed == 0 {
                    return Err(StoreError::DuplicateAgent(slug.clone()));
                }
            }
            JournalEvent::AgentArchived => {
                let changed = transaction.execute(
                    "UPDATE agents SET archived = 1 WHERE id = ?1 AND archived = 0",
                    [&event.subject.id],
                )?;
                if changed == 0 {
                    return Err(StoreError::NotFound {
                        kind: "agent",
                        id: event.subject.id.clone(),
                    });
                }
            }
            _ => {}
        }
    }
    Ok(())
}
