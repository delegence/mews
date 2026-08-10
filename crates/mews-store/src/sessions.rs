use super::*;

impl Store {
    pub fn acp_session_binding(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<AcpSessionBinding>, StoreError> {
        self.connection
            .query_row(
                "SELECT host_id, harness, harness_definition_hash, acp_session_id,
                        context_version, context_hash, context_channel, context_text, context_dispatched,
                        created_at, replaced_at, last_replacement_reason
                 FROM acp_session_bindings WHERE session_id = ?1",
                [session_id.as_str()],
                |row| {
                    Ok(AcpSessionBinding {
                        session_id: session_id.clone(),
                        host_id: parse_id(row.get::<_, String>(0)?)?,
                        harness: row.get(1)?,
                        harness_definition_hash: row.get(2)?,
                        acp_session_id: row.get(3)?,
                        context_version: row.get(4)?,
                        context_hash: row.get(5)?,
                        context_channel: parse_json(format!("\"{}\"", row.get::<_, String>(6)?))?,
                        context_text: row.get(7)?,
                        context_dispatched: row.get::<_, i64>(8)? != 0,
                        created_at: parse_time(row.get::<_, String>(9)?)?,
                        replaced_at: row
                            .get::<_, Option<String>>(10)?
                            .map(parse_time)
                            .transpose()?,
                        last_replacement_reason: row
                            .get::<_, Option<String>>(11)?
                            .map(|value| parse_json(format!("\"{value}\"")))
                            .transpose()?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)] // persistence boundary validates one complete binding transition
    pub fn bind_acp_session(
        &self,
        session_id: &SessionId,
        host_id: &HostId,
        harness: &str,
        definition_hash: &str,
        acp_session_id: &str,
        transition: &AcpBindingTransition,
        context: &mews_protocol::AcpContextSnapshot,
        context_text: &str,
        channel: AcpInstructionChannel,
        context_dispatched: bool,
    ) -> Result<AcpSessionBinding, StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        write_acp_binding(
            &transaction,
            session_id,
            host_id,
            harness,
            definition_hash,
            acp_session_id,
            transition,
            context,
            context_text,
            channel,
            context_dispatched,
        )?;
        transaction.commit()?;
        self.acp_session_binding(session_id)?
            .ok_or_else(|| StoreError::InvalidData("ACP Session binding was not persisted".into()))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn bind_acp_session_with_observations(
        &self,
        session_id: &SessionId,
        host_id: &HostId,
        harness: &str,
        definition_hash: &str,
        acp_session_id: &str,
        transition: &AcpBindingTransition,
        context: &mews_protocol::AcpContextSnapshot,
        context_text: &str,
        channel: AcpInstructionChannel,
        context_dispatched: bool,
        run_id: RunId,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        write_acp_binding(
            &transaction,
            session_id,
            host_id,
            harness,
            definition_hash,
            acp_session_id,
            transition,
            context,
            context_text,
            channel,
            context_dispatched,
        )?;
        append_acp_observation_transaction(
            &transaction,
            session_id,
            run_id.clone(),
            Some(acp_session_id.to_owned()),
            Some(format!("binding:{run_id}")),
            AcpObservation::BindingChanged {
                transition: transition.clone(),
            },
        )?;
        if context_dispatched {
            append_acp_observation_transaction(
                &transaction,
                session_id,
                run_id,
                Some(acp_session_id.to_owned()),
                Some(format!("context_dispatched:{acp_session_id}")),
                AcpObservation::ContextDispatched {
                    version: context.version,
                    hash: AcpContextSnapshot::hash_rendered(context_text),
                    channel,
                    text: context_text.to_owned(),
                },
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// The dispatch flag and its audit record describe one irreversible
    /// boundary: a FirstPrompt context is about to cross into the provider.
    pub fn mark_acp_context_dispatched_with_observation(
        &self,
        session_id: &SessionId,
        run_id: RunId,
        acp_session_id: &str,
    ) -> Result<(), StoreError> {
        let binding =
            self.acp_session_binding(session_id)?
                .ok_or_else(|| StoreError::NotFound {
                    kind: "ACP Session binding",
                    id: session_id.to_string(),
                })?;
        let transaction = self.connection.unchecked_transaction()?;
        let changed = transaction.execute(
            "UPDATE acp_session_bindings SET context_dispatched = 1
             WHERE session_id = ?1 AND acp_session_id = ?2",
            params![session_id.as_str(), acp_session_id],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound {
                kind: "ACP Session binding",
                id: session_id.to_string(),
            });
        }
        let leaf: Option<MessageId> = transaction.query_row(
            "SELECT leaf_entry_id FROM sessions WHERE id = ?1",
            [session_id.as_str()],
            |row| row.get::<_, Option<String>>(0)?.map(parse_id).transpose(),
        )?;
        let sequence: u64 = transaction.query_row(
            "SELECT COALESCE(MAX(sequence) + 1, 1) FROM session_entries WHERE session_id = ?1",
            [session_id.as_str()],
            |row| row.get(0),
        )?;
        let observation = AcpObservation::ContextDispatched {
            version: binding.context_version,
            hash: binding.context_hash,
            channel: binding.context_channel,
            text: binding.context_text,
        };
        let payload =
            harness_observation_payload(run_id, Some(acp_session_id.to_owned()), observation)?;
        validate_session_item(&payload)?;
        transaction.execute(
            "INSERT OR IGNORE INTO session_entries (id, session_id, sequence, parent_id, kind, contextual, observation_key, payload_json, created_at)
             VALUES (?1, ?2, ?3, ?4, 'harness_observation', 0, ?5, ?6, ?7)",
            params![MessageId::new().as_str(), session_id.as_str(), sequence, leaf.as_ref().map(MessageId::as_str), format!("context_dispatched:{acp_session_id}"), json(&payload)?, timestamp(Utc::now())],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn session(&self, session_id: &SessionId) -> Result<Session, StoreError> {
        self.connection
            .query_row(
                "SELECT agent_id, agent_revision, host_id, working_directory, model_override, leaf_entry_id, created_at
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
                        leaf_entry_id: row
                            .get::<_, Option<String>>(5)?
                            .map(parse_id)
                            .transpose()?,
                        created_at: parse_time(row.get::<_, String>(6)?)?,
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
            "SELECT id, agent_id, agent_revision, host_id, working_directory, model_override, leaf_entry_id, created_at
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
                leaf_entry_id: row.get::<_, Option<String>>(6)?.map(parse_id).transpose()?,
                created_at: parse_time(row.get::<_, String>(7)?)?,
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
            leaf_entry_id: None,
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
        let expected_leaf = self.session(session_id)?.leaf_entry_id;
        self.append_message_checked(
            session_id,
            expected_leaf.as_ref(),
            role,
            content,
            metadata,
            source,
        )
    }

    pub fn append_assistant_response(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
        response: AssistantResponse,
    ) -> Result<SessionEntry, StoreError> {
        let text = response
            .blocks
            .iter()
            .filter_map(|block| match block {
                mews_protocol::AssistantResponseBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();
        let transaction = self.connection.unchecked_transaction()?;
        let entry = append_contextual_entry_transaction(
            &transaction,
            session_id,
            SessionEntryPayload::AssistantResponse {
                run_id: run_id.clone(),
                response,
            },
            "assistant_response",
        )?;
        if !text.is_empty() {
            let event = ClientEventKind::AssistantMessage {
                run_id: run_id.clone(),
                message: Message {
                    id: entry.id.clone(),
                    session_id: session_id.clone(),
                    sequence: entry.sequence,
                    role: MessageRole::Assistant,
                    content: MessageContent::Text { text },
                    metadata: Value::Null,
                    source: MessageSource {
                        kind: SourceKind::Harness,
                        id: "default".into(),
                        channel_origin: None,
                    },
                    created_at: entry.created_at,
                },
            };
            let event_id = EventId::new();
            let origin = crate::events::channel_origin_json(&transaction, &event)?;
            crate::events::validate_client_event(
                session_id,
                &event_id,
                &event,
                origin.as_deref(),
                entry.created_at,
            )?;
            transaction.execute(
                "INSERT INTO client_events (id, session_id, entry_id, kind_json, channel_origin_json, transient, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6)",
                params![
                    event_id.as_str(),
                    session_id.as_str(),
                    entry.id.as_str(),
                    json(&event)?,
                    origin,
                    timestamp(entry.created_at)
                ],
            )?;
        }
        transaction.commit()?;
        self.prune_client_events()?;
        Ok(entry)
    }

    pub fn append_tool_result(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
        result: ToolResult,
    ) -> Result<SessionEntry, StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        let entry = append_contextual_entry_transaction(
            &transaction,
            session_id,
            SessionEntryPayload::ToolResult {
                run_id: run_id.clone(),
                result: result.clone(),
            },
            "tool_result",
        )?;
        let message = Message {
            id: entry.id.clone(),
            session_id: session_id.clone(),
            sequence: entry.sequence,
            role: MessageRole::Tool,
            content: MessageContent::ToolResult {
                call_id: result.call_id,
                tool: result.tool,
                result: result.result,
                is_error: result.is_error,
            },
            metadata: Value::Null,
            source: MessageSource {
                kind: SourceKind::Host,
                id: "default".into(),
                channel_origin: None,
            },
            created_at: entry.created_at,
        };
        let event = ClientEventKind::ToolCompleted {
            run_id: run_id.clone(),
            message,
        };
        transaction.execute(
            "INSERT INTO client_events (id, session_id, entry_id, kind_json, channel_origin_json, transient, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6)",
            params![
                EventId::new().as_str(),
                session_id.as_str(),
                entry.id.as_str(),
                json(&event)?,
                crate::events::channel_origin_json(&transaction, &event)?,
                timestamp(entry.created_at)
            ],
        )?;
        transaction.commit()?;
        Ok(entry)
    }

    pub fn append_tool_started(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
        call: ToolCall,
    ) -> Result<SessionEntry, StoreError> {
        let message_content = MessageContent::ToolCall {
            call_id: call.call_id.clone(),
            tool: call.tool.clone(),
            arguments: call.arguments.clone(),
            thought_signature: call.thought_signature.clone(),
        };
        let transaction = self.connection.unchecked_transaction()?;
        let entry = append_observational_entry_transaction(
            &transaction,
            session_id,
            SessionEntryPayload::ToolStarted {
                run_id: run_id.clone(),
                call,
            },
            "tool_started",
            None,
        )?
        .ok_or_else(|| StoreError::InvalidData("tool start was not persisted".into()))?;
        let message = Message {
            id: entry.id.clone(),
            session_id: session_id.clone(),
            sequence: entry.sequence,
            role: MessageRole::Assistant,
            content: message_content,
            metadata: Value::Null,
            source: MessageSource {
                kind: SourceKind::Harness,
                id: "default".into(),
                channel_origin: None,
            },
            created_at: entry.created_at,
        };
        let event = ClientEventKind::ToolStarted {
            run_id: run_id.clone(),
            message,
        };
        transaction.execute(
            "INSERT INTO client_events (id, session_id, entry_id, kind_json, channel_origin_json, transient, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6)",
            params![
                EventId::new().as_str(),
                session_id.as_str(),
                entry.id.as_str(),
                json(&event)?,
                crate::events::channel_origin_json(&transaction, &event)?,
                timestamp(entry.created_at)
            ],
        )?;
        transaction.commit()?;
        Ok(entry)
    }

    pub fn append_reasoning(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
        text: String,
        visibility: ReasoningVisibility,
        provenance: ReasoningProvenance,
        idempotency_key: Option<String>,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        append_observational_entry_transaction(
            &transaction,
            session_id,
            SessionEntryPayload::Reasoning {
                run_id: run_id.clone(),
                text,
                visibility,
                provenance,
            },
            "reasoning",
            idempotency_key.as_deref(),
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn append_harness_observation(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
        harness_session_id: Option<String>,
        kind: impl Into<String>,
        data: Value,
        idempotency_key: Option<String>,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        append_observational_entry_transaction(
            &transaction,
            session_id,
            SessionEntryPayload::HarnessObservation {
                run_id: run_id.clone(),
                harness_session_id,
                kind: kind.into(),
                data,
            },
            "harness_observation",
            idempotency_key.as_deref(),
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn append_context_compaction(
        &self,
        session_id: &SessionId,
        summary: String,
        first_kept_entry_id: MessageId,
        tokens_before: u64,
    ) -> Result<SessionEntry, StoreError> {
        if summary.trim().is_empty() {
            return Err(StoreError::InvalidData(
                "compaction summary must not be empty".into(),
            ));
        }
        if !self
            .active_entries(session_id)?
            .iter()
            .any(|entry| entry.id == first_kept_entry_id)
        {
            return Err(StoreError::InvalidData(
                "compaction boundary must be in the active branch".into(),
            ));
        }
        self.append_contextual_entry(
            session_id,
            SessionEntryPayload::ContextCompaction {
                summary,
                first_kept_entry_id,
                tokens_before,
            },
            "context_compaction",
        )
    }

    fn append_contextual_entry(
        &self,
        session_id: &SessionId,
        payload: SessionEntryPayload,
        kind: &str,
    ) -> Result<SessionEntry, StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        let entry = append_contextual_entry_transaction(&transaction, session_id, payload, kind)?;
        transaction.commit()?;
        Ok(entry)
    }

    /// ACP observations are timeline side entries: they share the current
    /// contextual leaf as an anchor but never advance it.
    pub fn append_acp_observation(
        &self,
        session_id: &SessionId,
        run_id: RunId,
        acp_session_id: Option<String>,
        event_key: Option<mews_protocol::AcpEventKey>,
        observation: AcpObservation,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        let leaf: Option<MessageId> = transaction
            .query_row(
                "SELECT leaf_entry_id FROM sessions WHERE id = ?1",
                [session_id.as_str()],
                |row| row.get::<_, Option<String>>(0)?.map(parse_id).transpose(),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound {
                kind: "session",
                id: session_id.to_string(),
            })?;
        let sequence: u64 = transaction.query_row(
            "SELECT COALESCE(MAX(sequence) + 1, 1) FROM session_entries WHERE session_id = ?1",
            [session_id.as_str()],
            |row| row.get(0),
        )?;
        let entry = SessionEntry {
            id: MessageId::new(),
            session_id: session_id.clone(),
            sequence,
            parent_id: leaf,
            payload: harness_observation_payload(run_id, acp_session_id, observation)?,
            created_at: Utc::now(),
        };
        validate_session_item(&entry.payload)?;
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO session_entries (id, session_id, sequence, parent_id, kind, contextual, observation_key, payload_json, created_at)
             VALUES (?1, ?2, ?3, ?4, 'harness_observation', 0, ?5, ?6, ?7)",
            params![entry.id.as_str(), entry.session_id.as_str(), entry.sequence,
                entry.parent_id.as_ref().map(MessageId::as_str), event_key, json(&entry.payload)?, timestamp(entry.created_at)],
        )?;
        transaction.commit()?;
        if inserted == 0 {
            return Ok(());
        }
        Ok(())
    }

    /// Contextual entries form a parent chain; only a successful checked append
    /// advances the active leaf.
    pub fn append_message_checked(
        &self,
        session_id: &SessionId,
        expected_leaf: Option<&MessageId>,
        role: MessageRole,
        content: MessageContent,
        metadata: Value,
        source: MessageSource,
    ) -> Result<Message, StoreError> {
        if role != MessageRole::User {
            return Err(StoreError::InvalidData(
                "append_message only accepts user messages; use a typed transcript append".into(),
            ));
        }
        let metadata_bytes = serde_json::to_vec(&metadata)
            .map_err(|error| StoreError::InvalidData(error.to_string()))?;
        if metadata_bytes.len() > 64 * 1024 {
            return Err(StoreError::InvalidData(
                "message metadata exceeds 64 KiB".into(),
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        let current_leaf: Option<MessageId> = transaction
            .query_row(
                "SELECT leaf_entry_id FROM sessions WHERE id = ?1",
                [session_id.as_str()],
                |row| row.get::<_, Option<String>>(0)?.map(parse_id).transpose(),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound {
                kind: "session",
                id: session_id.to_string(),
            })?;
        if current_leaf.as_ref() != expected_leaf {
            return Err(StoreError::LeafConflict {
                expected: expected_leaf.cloned(),
                current: current_leaf,
            });
        }
        let sequence: u64 = transaction.query_row(
            "SELECT COALESCE(MAX(sequence) + 1, 1) FROM session_entries WHERE session_id = ?1",
            [session_id.as_str()],
            |row| row.get(0),
        )?;
        let entry = SessionEntry {
            id: MessageId::new(),
            session_id: session_id.clone(),
            sequence,
            parent_id: current_leaf.clone(),
            payload: SessionEntryPayload::UserMessage {
                content: content.clone(),
                metadata: metadata.clone(),
                source: source.clone(),
            },
            created_at: Utc::now(),
        };
        validate_session_item(&entry.payload)?;
        let message = message_from_entry(entry.clone())?;
        transaction.execute(
            "INSERT INTO session_entries
             (id, session_id, sequence, parent_id, kind, contextual, payload_json, created_at)
             VALUES (?1, ?2, ?3, ?4, 'user_message', 1, ?5, ?6)",
            params![
                entry.id.as_str(),
                entry.session_id.as_str(),
                entry.sequence,
                entry.parent_id.as_ref().map(MessageId::as_str),
                json(&entry.payload)?,
                timestamp(entry.created_at)
            ],
        )?;
        let advanced = transaction.execute(
            "UPDATE sessions SET leaf_entry_id = ?2
             WHERE id = ?1 AND leaf_entry_id IS ?3",
            params![
                session_id.as_str(),
                entry.id.as_str(),
                current_leaf.as_ref().map(MessageId::as_str),
            ],
        )?;
        if advanced != 1 {
            return Err(StoreError::LeafConflict {
                expected: expected_leaf.cloned(),
                current: current_leaf,
            });
        }
        transaction.commit()?;
        Ok(message)
    }

    pub fn set_session_leaf_checked(
        &self,
        session_id: &SessionId,
        expected_leaf: Option<&MessageId>,
        new_leaf: Option<&MessageId>,
    ) -> Result<Session, StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        let current_leaf: Option<MessageId> = transaction
            .query_row(
                "SELECT leaf_entry_id FROM sessions WHERE id = ?1",
                [session_id.as_str()],
                |row| row.get::<_, Option<String>>(0)?.map(parse_id).transpose(),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound {
                kind: "session",
                id: session_id.to_string(),
            })?;
        if current_leaf.as_ref() != expected_leaf {
            return Err(StoreError::LeafConflict {
                expected: expected_leaf.cloned(),
                current: current_leaf,
            });
        }
        if let Some(new_leaf) = new_leaf {
            let is_contextual_message: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM session_entries WHERE id = ?1 AND session_id = ?2 AND contextual = 1)",
                params![new_leaf.as_str(), session_id.as_str()],
                |row| row.get(0),
            )?;
            if !is_contextual_message {
                return Err(StoreError::InvalidData(
                    "Session leaf must be a contextual entry in its Session".into(),
                ));
            }
        }
        let changed = transaction.execute(
            "UPDATE sessions SET leaf_entry_id = ?2 WHERE id = ?1 AND leaf_entry_id IS ?3",
            params![
                session_id.as_str(),
                new_leaf.map(MessageId::as_str),
                current_leaf.as_ref().map(MessageId::as_str),
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::LeafConflict {
                expected: expected_leaf.cloned(),
                current: current_leaf,
            });
        }
        transaction.commit()?;
        self.session(session_id)
    }

    pub fn messages(&self, session_id: &SessionId) -> Result<Vec<Message>, StoreError> {
        self.active_messages(session_id)
    }

    pub fn active_messages(&self, session_id: &SessionId) -> Result<Vec<Message>, StoreError> {
        let entries = self.active_entries(session_id)?;
        Ok(entries_to_messages(&entries))
    }

    pub fn active_entries(&self, session_id: &SessionId) -> Result<Vec<SessionEntry>, StoreError> {
        let mut statement = self.connection.prepare(
            "WITH RECURSIVE active_entries AS (
                 SELECT id, session_id, sequence, parent_id, kind, payload_json, created_at
                 FROM session_entries
                 WHERE id = (SELECT leaf_entry_id FROM sessions WHERE id = ?1)
                 UNION ALL
                 SELECT parent.id, parent.session_id, parent.sequence, parent.parent_id,
                        parent.kind, parent.payload_json, parent.created_at
                 FROM session_entries AS parent
                 JOIN active_entries AS child
                   ON parent.id = child.parent_id AND parent.session_id = child.session_id
             )
             SELECT id, sequence, parent_id, payload_json, created_at
             FROM active_entries ORDER BY sequence",
        )?;
        let rows = statement.query_map([session_id.as_str()], |row| {
            Ok(SessionEntry {
                id: parse_id(row.get::<_, String>(0)?)?,
                session_id: session_id.clone(),
                sequence: row.get(1)?,
                parent_id: row.get::<_, Option<String>>(2)?.map(parse_id).transpose()?,
                payload: parse_json(row.get::<_, String>(3)?)?,
                created_at: parse_time(row.get::<_, String>(4)?)?,
            })
        })?;
        let entries = rows.collect::<Result<Vec<_>, _>>()?;
        Ok(apply_latest_compaction(entries))
    }

    pub fn session_entries(&self, session_id: &SessionId) -> Result<Vec<SessionEntry>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, sequence, parent_id, payload_json, created_at
             FROM session_entries WHERE session_id = ?1 ORDER BY sequence",
        )?;
        let rows = statement.query_map([session_id.as_str()], |row| {
            Ok(SessionEntry {
                id: parse_id(row.get::<_, String>(0)?)?,
                session_id: session_id.clone(),
                sequence: row.get(1)?,
                parent_id: row.get::<_, Option<String>>(2)?.map(parse_id).transpose()?,
                payload: parse_json(row.get::<_, String>(3)?)?,
                created_at: parse_time(row.get::<_, String>(4)?)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn session_entries_page(
        &self,
        session_id: &SessionId,
        after: Option<u64>,
        limit: u16,
    ) -> Result<(Vec<SessionEntry>, Option<u64>), StoreError> {
        let entries = self.session_entries(session_id)?;
        page_entries(entries, after, limit)
    }

    pub fn active_messages_page(
        &self,
        session_id: &SessionId,
        after: Option<u64>,
        limit: u16,
    ) -> Result<(Vec<Message>, Option<u64>), StoreError> {
        let messages = self.active_messages(session_id)?;
        page_entries(messages, after, limit)
    }
}

trait HasSequence {
    fn sequence(&self) -> u64;
}
impl HasSequence for Message {
    fn sequence(&self) -> u64 {
        self.sequence
    }
}
impl HasSequence for SessionEntry {
    fn sequence(&self) -> u64 {
        self.sequence
    }
}

fn page_entries<T: HasSequence + serde::Serialize>(
    entries: Vec<T>,
    after: Option<u64>,
    limit: u16,
) -> Result<(Vec<T>, Option<u64>), StoreError> {
    let limit = usize::from(limit.clamp(1, 500));
    let mut entries = entries
        .into_iter()
        .filter(|entry| after.is_none_or(|after| entry.sequence() > after));
    let mut payload_bytes: usize = 0;
    let mut page: Vec<T> = Vec::new();
    while page.len() < limit {
        let Some(entry) = entries.next() else { break };
        let entry_bytes = serde_json::to_vec(&entry)
            .map_err(|error| StoreError::InvalidData(error.to_string()))?
            .len();
        if entry_bytes > mews_protocol::MAX_SESSION_PAGE_PAYLOAD_BYTES {
            return Err(StoreError::InvalidData(
                "stored session item exceeds the page payload limit".into(),
            ));
        }
        if !page.is_empty()
            && payload_bytes.saturating_add(entry_bytes)
                > mews_protocol::MAX_SESSION_PAGE_PAYLOAD_BYTES
        {
            // Retain the item for the next cursor rather than dropping it.
            let next = Some(page.last().expect("nonempty page").sequence());
            return Ok((page, next));
        }
        payload_bytes += entry_bytes;
        page.push(entry);
    }
    let next = entries
        .next()
        .map(|_| page.last().expect("nonempty page").sequence());
    Ok((page, next))
}

pub(super) fn validate_session_item(payload: &SessionEntryPayload) -> Result<(), StoreError> {
    let bytes =
        serde_json::to_vec(payload).map_err(|error| StoreError::InvalidData(error.to_string()))?;
    if bytes.len() > mews_protocol::MAX_SESSION_ITEM_BYTES {
        return Err(StoreError::InvalidData(
            "session item exceeds the page-safe size limit".into(),
        ));
    }
    Ok(())
}

pub(super) fn session_entry_kind(payload: &SessionEntryPayload) -> &'static str {
    match payload {
        SessionEntryPayload::UserMessage { .. } => "user_message",
        SessionEntryPayload::RunStarted { .. } => "run_started",
        SessionEntryPayload::AssistantResponse { .. } => "assistant_response",
        SessionEntryPayload::ToolStarted { .. } => "tool_started",
        SessionEntryPayload::ToolResult { .. } => "tool_result",
        SessionEntryPayload::Reasoning { .. } => "reasoning",
        SessionEntryPayload::RunCompleted { .. } => "run_completed",
        SessionEntryPayload::RunFailed { .. } => "run_failed",
        SessionEntryPayload::RunCancelled { .. } => "run_cancelled",
        SessionEntryPayload::ContextCompaction { .. } => "context_compaction",
        SessionEntryPayload::HarnessObservation { .. } => "harness_observation",
    }
}

fn message_from_entry(entry: SessionEntry) -> Result<Message, StoreError> {
    let (role, content, metadata, source) = match entry.payload {
        SessionEntryPayload::UserMessage {
            content,
            metadata,
            source,
        } => (MessageRole::User, content, metadata, source),
        SessionEntryPayload::AssistantResponse { response, .. } => {
            let text = response
                .blocks
                .into_iter()
                .filter_map(|block| match block {
                    mews_protocol::AssistantResponseBlock::Text { text } => Some(text),
                    _ => None,
                })
                .collect::<String>();
            (
                MessageRole::Assistant,
                MessageContent::Text { text },
                Value::Null,
                MessageSource {
                    kind: SourceKind::Harness,
                    id: "default".into(),
                    channel_origin: None,
                },
            )
        }
        SessionEntryPayload::ToolResult { result, .. } => (
            MessageRole::Tool,
            MessageContent::ToolResult {
                call_id: result.call_id,
                tool: result.tool,
                result: result.result,
                is_error: result.is_error,
            },
            Value::Null,
            MessageSource {
                kind: SourceKind::Host,
                id: "default".into(),
                channel_origin: None,
            },
        ),
        _ => {
            return Err(StoreError::InvalidData(
                "entry is not a contextual message".into(),
            ));
        }
    };
    Ok(Message {
        id: entry.id,
        session_id: entry.session_id,
        sequence: entry.sequence,
        role,
        content,
        metadata,
        source,
        created_at: entry.created_at,
    })
}

fn entries_to_messages(entries: &[SessionEntry]) -> Vec<Message> {
    let session_id = entries.first().map(|entry| entry.session_id.clone());
    mews_protocol::portable_history(entries)
        .into_iter()
        .enumerate()
        .filter_map(|(index, item)| {
            Some(Message {
                id: entries
                    .get(index)
                    .map_or_else(MessageId::new, |entry| entry.id.clone()),
                session_id: session_id.clone()?,
                sequence: u64::try_from(index + 1).unwrap_or(u64::MAX),
                role: item.role,
                content: item.content,
                metadata: Value::Null,
                source: MessageSource {
                    kind: SourceKind::Client,
                    id: "cli".into(),
                    channel_origin: None,
                },
                created_at: entries
                    .get(index)
                    .map_or_else(Utc::now, |entry| entry.created_at),
            })
        })
        .collect()
}

fn apply_latest_compaction(entries: Vec<SessionEntry>) -> Vec<SessionEntry> {
    let Some((compaction_index, first_kept_id)) =
        entries
            .iter()
            .enumerate()
            .rev()
            .find_map(|(index, entry)| match &entry.payload {
                SessionEntryPayload::ContextCompaction {
                    first_kept_entry_id,
                    ..
                } => Some((index, first_kept_entry_id)),
                _ => None,
            })
    else {
        return entries;
    };
    let Some(first_kept_index) = entries.iter().position(|entry| &entry.id == first_kept_id) else {
        return entries;
    };
    let mut active = Vec::with_capacity(entries.len() - first_kept_index + 1);
    active.push(entries[compaction_index].clone());
    active.extend(entries[first_kept_index..compaction_index].iter().cloned());
    active.extend(entries[compaction_index + 1..].iter().cloned());
    active
}

fn append_contextual_entry_transaction(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
    payload: SessionEntryPayload,
    kind: &str,
) -> Result<SessionEntry, StoreError> {
    validate_session_item(&payload)?;
    let leaf: Option<MessageId> = transaction
        .query_row(
            "SELECT leaf_entry_id FROM sessions WHERE id=?1",
            [session_id.as_str()],
            |row| row.get::<_, Option<String>>(0)?.map(parse_id).transpose(),
        )
        .optional()?
        .ok_or_else(|| StoreError::NotFound {
            kind: "session",
            id: session_id.to_string(),
        })?;
    let sequence = transaction.query_row(
        "SELECT COALESCE(MAX(sequence) + 1, 1) FROM session_entries WHERE session_id=?1",
        [session_id.as_str()],
        |row| row.get(0),
    )?;
    let entry = SessionEntry {
        id: MessageId::new(),
        session_id: session_id.clone(),
        sequence,
        parent_id: leaf.clone(),
        payload,
        created_at: Utc::now(),
    };
    transaction.execute(
        "INSERT INTO session_entries (id,session_id,sequence,parent_id,kind,contextual,payload_json,created_at) VALUES (?1,?2,?3,?4,?5,1,?6,?7)",
        params![entry.id.as_str(), session_id.as_str(), sequence, leaf.as_ref().map(MessageId::as_str), kind, json(&entry.payload)?, timestamp(entry.created_at)],
    )?;
    transaction.execute(
        "UPDATE sessions SET leaf_entry_id=?2 WHERE id=?1",
        params![session_id.as_str(), entry.id.as_str()],
    )?;
    Ok(entry)
}

fn append_observational_entry_transaction(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
    payload: SessionEntryPayload,
    kind: &str,
    idempotency_key: Option<&str>,
) -> Result<Option<SessionEntry>, StoreError> {
    validate_session_item(&payload)?;
    let leaf: Option<MessageId> = transaction
        .query_row(
            "SELECT leaf_entry_id FROM sessions WHERE id = ?1",
            [session_id.as_str()],
            |row| row.get::<_, Option<String>>(0)?.map(parse_id).transpose(),
        )
        .optional()?
        .ok_or_else(|| StoreError::NotFound {
            kind: "session",
            id: session_id.to_string(),
        })?;
    let sequence: u64 = transaction.query_row(
        "SELECT COALESCE(MAX(sequence) + 1, 1) FROM session_entries WHERE session_id = ?1",
        [session_id.as_str()],
        |row| row.get(0),
    )?;
    let entry = SessionEntry {
        id: MessageId::new(),
        session_id: session_id.clone(),
        sequence,
        parent_id: leaf,
        payload,
        created_at: Utc::now(),
    };
    let inserted = transaction.execute(
        "INSERT OR IGNORE INTO session_entries
         (id, session_id, sequence, parent_id, kind, contextual, observation_key, payload_json, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7, ?8)",
        params![entry.id.as_str(), session_id.as_str(), sequence,
            entry.parent_id.as_ref().map(MessageId::as_str), kind, idempotency_key,
            json(&entry.payload)?, timestamp(entry.created_at)],
    )?;
    Ok((inserted == 1).then_some(entry))
}

#[allow(clippy::too_many_arguments)]
fn write_acp_binding(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
    host_id: &HostId,
    harness: &str,
    definition_hash: &str,
    acp_session_id: &str,
    transition: &AcpBindingTransition,
    context: &mews_protocol::AcpContextSnapshot,
    context_text: &str,
    channel: AcpInstructionChannel,
    context_dispatched: bool,
) -> Result<(), StoreError> {
    if harness.is_empty()
        || definition_hash.is_empty()
        || acp_session_id.is_empty()
        || acp_session_id.len() > mews_protocol::MAX_ACP_SESSION_ID_BYTES
        || context_text.len() > mews_protocol::MAX_ACP_CONTEXT_BYTES
    {
        return Err(StoreError::InvalidData(
            "invalid ACP Session binding".into(),
        ));
    }
    let channel_json = serde_json::to_string(&channel)
        .map_err(|error| StoreError::InvalidData(error.to_string()))?;
    let channel = channel_json.trim_matches('"');
    let existing: Option<(String, String, String, String)> = transaction.query_row(
        "SELECT host_id, harness, harness_definition_hash, acp_session_id FROM acp_session_bindings WHERE session_id = ?1", [session_id.as_str()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    ).optional()?;
    let now = Utc::now();
    match existing {
        None if matches!(transition, AcpBindingTransition::New) => {
            transaction.execute("INSERT INTO acp_session_bindings (session_id, host_id, harness, harness_definition_hash, acp_session_id, context_version, context_hash, context_channel, context_text, context_dispatched, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)", params![session_id.as_str(), host_id.as_str(), harness, definition_hash, acp_session_id, context.version, AcpContextSnapshot::hash_rendered(context_text), channel, context_text, context_dispatched, timestamp(now)])?;
        }
        Some((bound_host, bound_harness, _, _))
            if matches!(transition, AcpBindingTransition::Replace { .. })
                && bound_host == host_id.as_str()
                && bound_harness == harness =>
        {
            let AcpBindingTransition::Replace { reason } = transition else {
                unreachable!()
            };
            let reason = serde_json::to_string(reason)
                .map_err(|error| StoreError::InvalidData(error.to_string()))?;
            transaction.execute("UPDATE acp_session_bindings SET acp_session_id=?2, harness_definition_hash=?3, context_version=?4, context_hash=?5, context_channel=?6, context_text=?7, context_dispatched=?8, replaced_at=?9, last_replacement_reason=?10 WHERE session_id=?1", params![session_id.as_str(), acp_session_id, definition_hash, context.version, AcpContextSnapshot::hash_rendered(context_text), channel, context_text, context_dispatched, timestamp(now), reason.trim_matches('"')])?;
        }
        Some((bound_host, bound_harness, bound_hash, old_id))
            if matches!(transition, AcpBindingTransition::New)
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
    Ok(())
}

fn append_acp_observation_transaction(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
    run_id: RunId,
    acp_session_id: Option<String>,
    event_key: Option<mews_protocol::AcpEventKey>,
    observation: AcpObservation,
) -> Result<(), StoreError> {
    let leaf: Option<MessageId> = transaction.query_row(
        "SELECT leaf_entry_id FROM sessions WHERE id=?1",
        [session_id.as_str()],
        |row| row.get::<_, Option<String>>(0)?.map(parse_id).transpose(),
    )?;
    let sequence: u64 = transaction.query_row(
        "SELECT COALESCE(MAX(sequence)+1, 1) FROM session_entries WHERE session_id=?1",
        [session_id.as_str()],
        |row| row.get(0),
    )?;
    let payload = harness_observation_payload(run_id, acp_session_id, observation)?;
    validate_session_item(&payload)?;
    transaction.execute("INSERT OR IGNORE INTO session_entries (id,session_id,sequence,parent_id,kind,contextual,observation_key,payload_json,created_at) VALUES (?1,?2,?3,?4,'harness_observation',0,?5,?6,?7)", params![MessageId::new().as_str(), session_id.as_str(), sequence, leaf.as_ref().map(MessageId::as_str), event_key, json(&payload)?, timestamp(Utc::now())])?;
    Ok(())
}

pub(super) fn harness_observation_payload(
    run_id: RunId,
    harness_session_id: Option<String>,
    observation: AcpObservation,
) -> Result<SessionEntryPayload, StoreError> {
    match observation {
        AcpObservation::CompletedReasoning {
            text,
            message_id,
            visibility,
        } => Ok(SessionEntryPayload::Reasoning {
            run_id,
            text,
            visibility,
            provenance: ReasoningProvenance::Harness {
                harness: "acp".into(),
                message_id,
            },
        }),
        AcpObservation::ToolActivity { activity }
            if matches!(activity.status.as_deref(), Some("started" | "in_progress")) =>
        {
            Ok(SessionEntryPayload::ToolStarted {
                run_id,
                call: ToolCall {
                    call_id: activity.call_id,
                    tool: activity.title,
                    arguments: activity.input,
                    thought_signature: None,
                },
            })
        }
        observation => {
            let value = serde_json::to_value(observation)
                .map_err(|error| StoreError::InvalidData(error.to_string()))?;
            let kind = value
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("acp")
                .to_owned();
            Ok(SessionEntryPayload::HarnessObservation {
                run_id,
                harness_session_id,
                kind,
                data: value,
            })
        }
    }
}
