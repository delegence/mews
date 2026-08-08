use super::*;

impl Store {
    pub fn create_agent(
        &mut self,
        slug: &str,
        soul: &str,
        config_toml: &str,
        author_host_id: &HostId,
    ) -> Result<(Agent, AgentRevision), StoreError> {
        validate_slug(slug)?;
        validate_definition(soul, config_toml)?;
        let now = Utc::now();
        let agent = Agent {
            id: AgentId::new(),
            slug: slug.to_owned(),
            current_revision: 1,
            archived: false,
            created_at: now,
        };
        let revision = revision(&agent.id, 1, soul, config_toml, author_host_id, now);
        let transaction = self.connection.transaction()?;
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO agents (id, slug, current_revision, archived, created_at)
             VALUES (?1, ?2, 1, 0, ?3)",
            params![agent.id.as_str(), agent.slug, timestamp(now)],
        )?;
        if inserted == 0 {
            return Err(StoreError::DuplicateAgent(slug.to_owned()));
        }
        insert_revision(&transaction, &revision)?;
        transaction.commit()?;
        Ok((agent, revision))
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

    pub fn rename_agent(&self, slug: &str, new_slug: &str) -> Result<Agent, StoreError> {
        validate_slug(new_slug)?;
        let agent = self.agent_by_slug(slug)?;
        let changed = self.connection.execute(
            "UPDATE OR IGNORE agents SET slug = ?2 WHERE id = ?1 AND archived = 0",
            params![agent.id.as_str(), new_slug],
        )?;
        if changed == 0 {
            return Err(StoreError::DuplicateAgent(new_slug.to_owned()));
        }
        self.agent_by_slug(new_slug)
    }

    pub fn archive_agent(&self, slug: &str) -> Result<(), StoreError> {
        let changed = self.connection.execute(
            "UPDATE agents SET archived = 1 WHERE slug = ?1 AND archived = 0",
            [slug],
        )?;
        if changed == 0 {
            return Err(StoreError::NotFound {
                kind: "agent",
                id: slug.to_owned(),
            });
        }
        Ok(())
    }

    pub fn agent_by_slug(&self, slug: &str) -> Result<Agent, StoreError> {
        self.connection
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
        agent_id: &AgentId,
        expected_revision: u64,
        soul: &str,
        config_toml: &str,
        author_host_id: &HostId,
    ) -> Result<AgentRevision, StoreError> {
        validate_definition(soul, config_toml)?;
        let transaction = self.connection.transaction()?;
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
        let next = current + 1;
        let revision = revision(
            agent_id,
            next,
            soul,
            config_toml,
            author_host_id,
            Utc::now(),
        );
        insert_revision(&transaction, &revision)?;
        transaction.execute(
            "UPDATE agents SET current_revision = ?2 WHERE id = ?1",
            params![agent_id.as_str(), next],
        )?;
        transaction.commit()?;
        Ok(revision)
    }
}
