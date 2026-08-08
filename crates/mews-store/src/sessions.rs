use super::*;

impl Store {
    pub fn acp_session_binding(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<AcpSessionBinding>, StoreError> {
        self.connection
            .query_row(
                "SELECT host_id, harness, harness_definition_hash, acp_session_id,
                        created_at, replaced_at
                 FROM acp_session_bindings WHERE session_id = ?1",
                [session_id.as_str()],
                |row| {
                    Ok(AcpSessionBinding {
                        session_id: session_id.clone(),
                        host_id: parse_id(row.get::<_, String>(0)?)?,
                        harness: row.get(1)?,
                        harness_definition_hash: row.get(2)?,
                        acp_session_id: row.get(3)?,
                        created_at: parse_time(row.get::<_, String>(4)?)?,
                        replaced_at: row
                            .get::<_, Option<String>>(5)?
                            .map(parse_time)
                            .transpose()?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn bind_acp_session(
        &self,
        session_id: &SessionId,
        host_id: &HostId,
        harness: &str,
        definition_hash: &str,
        acp_session_id: &str,
        replacement_reason: Option<&str>,
    ) -> Result<AcpSessionBinding, StoreError> {
        if harness.is_empty() || definition_hash.is_empty() || acp_session_id.is_empty() {
            return Err(StoreError::InvalidData(
                "invalid ACP Session binding".into(),
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        let existing: Option<(String, String, String, String)> = transaction
            .query_row(
                "SELECT host_id, harness, harness_definition_hash, acp_session_id
                 FROM acp_session_bindings WHERE session_id = ?1",
                [session_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let now = Utc::now();
        match existing {
            None if replacement_reason.is_none() => {
                transaction.execute(
                    "INSERT INTO acp_session_bindings
                     (session_id, host_id, harness, harness_definition_hash, acp_session_id, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![session_id.as_str(), host_id.as_str(), harness, definition_hash, acp_session_id, timestamp(now)],
                )?;
            }
            Some((bound_host, bound_harness, _bound_hash, _old_id))
                if replacement_reason.is_some()
                    && bound_host == host_id.as_str()
                    && bound_harness == harness =>
            {
                transaction.execute(
                    "UPDATE acp_session_bindings
                     SET acp_session_id = ?2, harness_definition_hash = ?3, replaced_at = ?4
                     WHERE session_id = ?1",
                    params![
                        session_id.as_str(),
                        acp_session_id,
                        definition_hash,
                        timestamp(now)
                    ],
                )?;
            }
            Some((bound_host, bound_harness, bound_hash, old_id))
                if replacement_reason.is_none()
                    && bound_host == host_id.as_str()
                    && bound_harness == harness
                    && bound_hash == definition_hash
                    && old_id == acp_session_id => {}
            _ => {
                return Err(StoreError::InvalidData(
                    "ACP Session binding conflicts with its existing Host or Harness".into(),
                ));
            }
        }
        transaction.commit()?;
        self.acp_session_binding(session_id)?
            .ok_or_else(|| StoreError::InvalidData("ACP Session binding was not persisted".into()))
    }

    pub fn session(&self, session_id: &SessionId) -> Result<Session, StoreError> {
        self.connection
            .query_row(
                "SELECT agent_id, agent_revision, host_id, working_directory, model_override, created_at
                 FROM sessions WHERE id = ?1",
                [session_id.as_str()],
                |row| {
                    Ok(Session {
                        id: session_id.clone(),
                        agent_id: parse_id(row.get::<_, String>(0)?)?,
                        agent_revision: row.get(1)?,
                        host_id: parse_id(row.get::<_, String>(2)?)?,
                        working_directory: row.get::<_, String>(3)?.into(),
                        model_override: row.get(4)?,
                        created_at: parse_time(row.get::<_, String>(5)?)?,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound {
                kind: "session",
                id: session_id.to_string(),
            })
    }

    pub fn sessions(&self) -> Result<Vec<Session>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, agent_id, agent_revision, host_id, working_directory, model_override, created_at
             FROM sessions ORDER BY created_at DESC",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(Session {
                id: parse_id(row.get::<_, String>(0)?)?,
                agent_id: parse_id(row.get::<_, String>(1)?)?,
                agent_revision: row.get(2)?,
                host_id: parse_id(row.get::<_, String>(3)?)?,
                working_directory: row.get::<_, String>(4)?.into(),
                model_override: row.get(5)?,
                created_at: parse_time(row.get::<_, String>(6)?)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn create_session(
        &mut self,
        agent_id: &AgentId,
        host_id: &HostId,
        working_directory: &Path,
    ) -> Result<Session, StoreError> {
        if !working_directory.is_absolute() {
            return Err(StoreError::InvalidData(
                "working directory must be absolute".into(),
            ));
        }
        if working_directory
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        {
            return Err(StoreError::InvalidData(
                "working directory must be Host-canonicalized".into(),
            ));
        }
        let transaction = self.connection.transaction()?;
        let revision: Option<u64> = transaction
            .query_row(
                "SELECT current_revision FROM agents WHERE id = ?1 AND archived = 0",
                [agent_id.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        let revision = revision.ok_or_else(|| StoreError::NotFound {
            kind: "agent",
            id: agent_id.to_string(),
        })?;
        let session = Session {
            id: SessionId::new(),
            agent_id: agent_id.clone(),
            agent_revision: revision,
            host_id: host_id.clone(),
            working_directory: working_directory.to_path_buf(),
            model_override: None,
            created_at: Utc::now(),
        };
        transaction.execute(
            "INSERT INTO sessions
             (id, agent_id, agent_revision, host_id, working_directory, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                session.id.as_str(),
                session.agent_id.as_str(),
                session.agent_revision,
                session.host_id.as_str(),
                session.working_directory.to_string_lossy(),
                timestamp(session.created_at)
            ],
        )?;
        transaction.commit()?;
        Ok(session)
    }

    pub fn set_session_model(
        &self,
        session_id: &SessionId,
        model: Option<&str>,
    ) -> Result<Session, StoreError> {
        if model.is_some_and(|value| value.trim().is_empty() || value.len() > 200) {
            return Err(StoreError::InvalidData(
                "invalid Session model override".into(),
            ));
        }
        let changed = self.connection.execute(
            "UPDATE sessions SET model_override = ?2 WHERE id = ?1",
            params![session_id.as_str(), model],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound {
                kind: "session",
                id: session_id.to_string(),
            });
        }
        self.session(session_id)
    }

    pub fn append_message(
        &self,
        session_id: &SessionId,
        role: MessageRole,
        content: MessageContent,
        metadata: Value,
        source: MessageSource,
    ) -> Result<Message, StoreError> {
        let metadata_bytes = serde_json::to_vec(&metadata)
            .map_err(|error| StoreError::InvalidData(error.to_string()))?;
        if metadata_bytes.len() > 64 * 1024 {
            return Err(StoreError::InvalidData(
                "message metadata exceeds 64 KiB".into(),
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        let exists: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM sessions WHERE id = ?1)",
            [session_id.as_str()],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(StoreError::NotFound {
                kind: "session",
                id: session_id.to_string(),
            });
        }
        let sequence: u64 = transaction.query_row(
            "SELECT COALESCE(MAX(sequence) + 1, 1) FROM messages WHERE session_id = ?1",
            [session_id.as_str()],
            |row| row.get(0),
        )?;
        let message = Message {
            id: MessageId::new(),
            session_id: session_id.clone(),
            sequence,
            role,
            content,
            metadata,
            source,
            created_at: Utc::now(),
        };
        transaction.execute(
            "INSERT INTO messages
             (id, session_id, sequence, role_json, content_json, metadata_json, source_json, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                message.id.as_str(),
                message.session_id.as_str(),
                message.sequence,
                json(&message.role)?,
                json(&message.content)?,
                json(&message.metadata)?,
                json(&message.source)?,
                timestamp(message.created_at)
            ],
        )?;
        if message.role == MessageRole::Assistant
            && matches!(message.content, MessageContent::Text { .. })
        {
            transaction.execute(
                "INSERT INTO client_events (id, session_id, kind_json, transient, created_at)
                 VALUES (?1, ?2, ?3, 0, ?4)",
                params![
                    EventId::new().as_str(),
                    session_id.as_str(),
                    json(&ClientEventKind::AssistantMessage {
                        message: message.clone()
                    })?,
                    timestamp(Utc::now())
                ],
            )?;
        }
        if matches!(
            message.content,
            MessageContent::ToolCall { .. } | MessageContent::ToolResult { .. }
        ) {
            let run_id: Option<String> = transaction
                .query_row(
                    "SELECT id FROM runs WHERE session_id = ?1 AND completed_at IS NULL",
                    [session_id.as_str()],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(run_id) = run_id {
                let run_id = parse_id(run_id)?;
                let kind = if matches!(message.content, MessageContent::ToolCall { .. }) {
                    ClientEventKind::ToolStarted {
                        run_id,
                        message: message.clone(),
                    }
                } else {
                    ClientEventKind::ToolCompleted {
                        run_id,
                        message: message.clone(),
                    }
                };
                transaction.execute(
                    "INSERT INTO client_events (id, session_id, kind_json, transient, created_at) VALUES (?1, ?2, ?3, 0, ?4)",
                    params![EventId::new().as_str(), session_id.as_str(), json(&kind)?, timestamp(Utc::now())],
                )?;
            }
        }
        transaction.commit()?;
        Ok(message)
    }

    pub fn messages(&self, session_id: &SessionId) -> Result<Vec<Message>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, sequence, role_json, content_json, metadata_json, source_json, created_at
             FROM messages WHERE session_id = ?1 ORDER BY sequence",
        )?;
        let rows = statement.query_map([session_id.as_str()], |row| {
            Ok(Message {
                id: parse_id(row.get::<_, String>(0)?)?,
                session_id: session_id.clone(),
                sequence: row.get(1)?,
                role: parse_json(row.get::<_, String>(2)?)?,
                content: parse_json(row.get::<_, String>(3)?)?,
                metadata: parse_json(row.get::<_, String>(4)?)?,
                source: parse_json(row.get::<_, String>(5)?)?,
                created_at: parse_time(row.get::<_, String>(6)?)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }
}
