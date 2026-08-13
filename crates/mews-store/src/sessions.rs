use super::*;

#[derive(Clone, Debug, PartialEq)]
pub enum EffectOutcome {
    Succeeded(Option<Value>),
    Failed(String),
    Uncertain(String),
}

impl Store {
    pub fn schedule_effect(
        &self,
        session_id: &SessionId,
        turn_id: &TurnId,
        effect: mews_protocol::EffectRequest,
    ) -> Result<mews_protocol::OperationId, StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        validate_active_turn(&transaction, session_id, turn_id)?;
        let operation_id = schedule_effect_in(&transaction, session_id, turn_id, effect)?;
        transaction.commit()?;
        Ok(operation_id)
    }

    pub fn mark_effect_started(
        &self,
        session_id: &SessionId,
        turn_id: &TurnId,
        operation_id: &mews_protocol::OperationId,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        validate_active_turn(&transaction, session_id, turn_id)?;
        mark_effect_started_in(&transaction, session_id, turn_id, operation_id)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn start_effect(
        &self,
        session_id: &SessionId,
        turn_id: &TurnId,
        effect: mews_protocol::EffectRequest,
    ) -> Result<mews_protocol::OperationId, StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        validate_active_turn(&transaction, session_id, turn_id)?;
        let operation_id = schedule_effect_in(&transaction, session_id, turn_id, effect)?;
        mark_effect_started_in(&transaction, session_id, turn_id, &operation_id)?;
        transaction.commit()?;
        Ok(operation_id)
    }

    pub fn finish_effect(
        &self,
        session_id: &SessionId,
        turn_id: &TurnId,
        operation_id: &mews_protocol::OperationId,
        outcome: EffectOutcome,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        validate_active_turn(&transaction, session_id, turn_id)?;
        finish_effect_in(&transaction, session_id, turn_id, operation_id, outcome)?;
        transaction.commit()?;
        Ok(())
    }

    /// Accepts the user's input and creates its Turn as one idempotent command.
    /// A key reused with different content, metadata, or source is rejected.
    pub fn replay_turn_idempotent(
        &self,
        session_id: &SessionId,
        key: &str,
        request_content: &MessageContent,
        metadata: &Value,
        source: &MessageSource,
    ) -> Result<Option<(Turn, Message)>, StoreError> {
        validate_turn_key(key)?;
        let command_id = turn_command_id(session_id, key);
        let Some(receipt) = self.command_receipt(&command_id)? else {
            return Ok(None);
        };
        let hash = turn_request_hash(session_id, request_content, metadata, source)?;
        if receipt.request_hash != hash {
            return Err(StoreError::CommandConflict { command_id });
        }
        let (turn, message) = load_turn_result(self, session_id, &receipt.result)?;
        Ok(Some((turn, message)))
    }

    pub fn accept_turn_idempotent(
        &self,
        session_id: &SessionId,
        key: &str,
        request_content: MessageContent,
        resolved_content: MessageContent,
        metadata: Value,
        source: MessageSource,
    ) -> Result<(Turn, Message, bool), StoreError> {
        validate_turn_key(key)?;
        validate_message_input(&request_content, &metadata)?;
        validate_message_input(&resolved_content, &metadata)?;
        validate_turn_source(&source)?;
        // Reserve the write slot before reading state so the accepted Turn uses
        // one authoritative Session/Agent snapshot.
        let transaction = rusqlite::Transaction::new_unchecked(
            &self.connection,
            rusqlite::TransactionBehavior::Immediate,
        )?;
        let agent_revision: u64 = transaction
            .query_row(
                "SELECT agents.current_revision
                 FROM sessions JOIN agents ON agents.id = sessions.agent_id
                 WHERE sessions.id = ?1",
                [session_id.as_str()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound {
                kind: "session",
                id: session_id.to_string(),
            })?;
        let turn_id = TurnId::new();
        let entry_id = MessageId::new();
        let payload = mews_protocol::JournalEvent::TurnAccepted {
            turn_id: turn_id.clone(),
            agent_revision,
            entry_id: entry_id.clone(),
            content: resolved_content,
            metadata: metadata.clone(),
            source: source.clone(),
        };
        let append = crate::JournalAppend {
            command_id: turn_command_id(session_id, key),
            request_hash: turn_request_hash(session_id, &request_content, &metadata, &source)?,
            result: serde_json::json!({
                "turn_id": turn_id,
                "entry_id": entry_id,
            }),
            subjects: vec![crate::JournalSubjectAppend {
                subject_type: mews_protocol::JournalSubjectType::Session,
                subject_id: session_id.to_string(),
                entries: vec![crate::NewJournalEntry {
                    id: EventId::new(),
                    actor: mews_protocol::EventActor::from_source(&source),
                    correlation_id: Some(key.to_owned()),
                    payload,
                }],
            }],
        };
        let outcome = crate::events::append_journal_entries_in(
            &transaction,
            &append,
            |transaction, events| apply_session_journal_entry(transaction, &events[0]),
        )?;
        transaction.commit()?;
        let created = !outcome.was_replayed();
        let (turn, message) = load_turn_result(self, session_id, &outcome.receipt().result)?;
        Ok((turn, message, created))
    }

    pub fn acp_session_binding(
        &self,
        session_id: &SessionId,
    ) -> Result<Option<AcpSessionBinding>, StoreError> {
        select_acp_binding(&self.connection, session_id)
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
        let binding = decide_acp_binding(
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
        let event = record_session_event(
            &transaction,
            session_id,
            mews_protocol::EventActor {
                kind: mews_protocol::EventActorKind::Harness,
                id: Some(harness.to_owned()),
            },
            mews_protocol::JournalEvent::AcpBindingChanged { binding },
        )?;
        apply_session_journal_entry(&transaction, &event)?;
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
        turn_id: TurnId,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        let binding_key = format!("binding:{turn_id}");
        if observation_exists(&transaction, session_id, Some(&binding_key))? {
            transaction.commit()?;
            return Ok(());
        }
        let binding = decide_acp_binding(
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
        let event = record_session_event(
            &transaction,
            session_id,
            mews_protocol::EventActor {
                kind: mews_protocol::EventActorKind::Harness,
                id: Some(harness.to_owned()),
            },
            mews_protocol::JournalEvent::AcpBindingChanged { binding },
        )?;
        apply_session_journal_entry(&transaction, &event)?;
        record_acp_observation_transaction(
            &transaction,
            session_id,
            turn_id.clone(),
            Some(acp_session_id.to_owned()),
            Some(binding_key),
            AcpObservation::BindingChanged {
                transition: transition.clone(),
            },
        )?;
        if context_dispatched {
            record_acp_observation_transaction(
                &transaction,
                session_id,
                turn_id,
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
        turn_id: TurnId,
        acp_session_id: &str,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        let observation_key = format!("context_dispatched:{acp_session_id}");
        if observation_exists(&transaction, session_id, Some(&observation_key))? {
            transaction.commit()?;
            return Ok(());
        }
        let binding =
            select_acp_binding(&transaction, session_id)?.ok_or_else(|| StoreError::NotFound {
                kind: "ACP Session binding",
                id: session_id.to_string(),
            })?;
        if binding.acp_session_id != acp_session_id {
            return Err(StoreError::NotFound {
                kind: "ACP Session binding",
                id: session_id.to_string(),
            });
        }
        let dispatched = record_session_event(
            &transaction,
            session_id,
            mews_protocol::EventActor {
                kind: mews_protocol::EventActorKind::Harness,
                id: Some(binding.harness.clone()),
            },
            mews_protocol::JournalEvent::AcpContextDispatched {
                host_id: binding.host_id.clone(),
                harness: binding.harness.clone(),
                context_version: binding.context_version,
                context_hash: binding.context_hash.clone(),
                channel: binding.context_channel,
            },
        )?;
        apply_session_journal_entry(&transaction, &dispatched)?;
        let observation = AcpObservation::ContextDispatched {
            version: binding.context_version,
            hash: binding.context_hash,
            channel: binding.context_channel,
            text: binding.context_text,
        };
        record_acp_observation_transaction(
            &transaction,
            session_id,
            turn_id,
            Some(acp_session_id.to_owned()),
            Some(observation_key),
            observation,
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn session(&self, session_id: &SessionId) -> Result<Session, StoreError> {
        self.connection
            .query_row(
                "SELECT agent_id, host_id, working_directory, model_override, leaf_entry_id, created_at
                 FROM sessions WHERE id = ?1",
                [session_id.as_str()],
                |row| {
                    Ok(Session {
                        id: session_id.clone(),
                        agent_id: parse_id(row.get::<_, String>(0)?)?,
                        host_id: parse_id(row.get::<_, String>(1)?)?,
                        working_directory: row.get::<_, String>(2)?.into(),
                        model_override: row.get(3)?,
                        leaf_entry_id: row
                            .get::<_, Option<String>>(4)?
                            .map(parse_id)
                            .transpose()?,
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
            "SELECT id, agent_id, host_id, working_directory, model_override, leaf_entry_id, created_at
             FROM sessions ORDER BY created_at DESC",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(Session {
                id: parse_id(row.get::<_, String>(0)?)?,
                agent_id: parse_id(row.get::<_, String>(1)?)?,
                host_id: parse_id(row.get::<_, String>(2)?)?,
                working_directory: row.get::<_, String>(3)?.into(),
                model_override: row.get(4)?,
                leaf_entry_id: row.get::<_, Option<String>>(5)?.map(parse_id).transpose()?,
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
        let exists: Option<bool> = transaction
            .query_row(
                "SELECT 1 FROM agents WHERE id = ?1 AND archived = 0",
                [agent_id.as_str()],
                |_| Ok(true),
            )
            .optional()?;
        exists.ok_or_else(|| StoreError::NotFound {
            kind: "agent",
            id: agent_id.to_string(),
        })?;
        let session = Session {
            id: SessionId::new(),
            agent_id: agent_id.clone(),
            host_id: host_id.clone(),
            working_directory: working_directory.to_path_buf(),
            model_override: None,
            leaf_entry_id: None,
            created_at: Utc::now(),
        };
        let event = crate::events::record_journal_entry(
            &transaction,
            mews_protocol::JournalSubjectType::Session,
            session.id.as_str(),
            crate::NewJournalEntry {
                id: EventId::new(),
                actor: mews_protocol::EventActor::system(),
                correlation_id: None,
                payload: mews_protocol::JournalEvent::SessionCreated {
                    session: session.clone(),
                },
            },
        )?;
        apply_session_journal_entry(&transaction, &event)?;
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
        if self
            .connection
            .query_row(
                "SELECT 1 FROM sessions WHERE id = ?1",
                [session_id.as_str()],
                |_| Ok(()),
            )
            .optional()?
            .is_none()
        {
            return Err(StoreError::NotFound {
                kind: "session",
                id: session_id.to_string(),
            });
        }
        let transaction = self.connection.unchecked_transaction()?;
        let event = crate::events::record_journal_entry(
            &transaction,
            mews_protocol::JournalSubjectType::Session,
            session_id.as_str(),
            crate::NewJournalEntry {
                id: EventId::new(),
                actor: mews_protocol::EventActor::system(),
                correlation_id: None,
                payload: mews_protocol::JournalEvent::SessionModelChanged {
                    model: model.map(str::to_owned),
                },
            },
        )?;
        apply_session_journal_entry(&transaction, &event)?;
        transaction.commit()?;
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
        turn_id: &TurnId,
        response: AssistantResponse,
    ) -> Result<SessionEntry, StoreError> {
        let transaction = rusqlite::Transaction::new_unchecked(
            &self.connection,
            rusqlite::TransactionBehavior::Immediate,
        )?;
        validate_active_turn(&transaction, session_id, turn_id)?;
        let entry_id = MessageId::new();
        let event = record_session_event(
            &transaction,
            session_id,
            turn_actor(
                &transaction,
                session_id,
                turn_id,
                mews_protocol::EventActorKind::Harness,
            )?,
            mews_protocol::JournalEvent::AssistantResponseRecorded {
                turn_id: turn_id.clone(),
                entry_id: entry_id.clone(),
                response,
            },
        )?;
        apply_session_journal_entry(&transaction, &event)?;
        let entry = select_session_entry(&transaction, session_id, &entry_id)?;
        transaction.commit()?;
        self.prune_client_events()?;
        Ok(entry)
    }

    pub fn append_tool_result(
        &self,
        session_id: &SessionId,
        turn_id: &TurnId,
        result: ToolResult,
    ) -> Result<SessionEntry, StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        validate_active_turn(&transaction, session_id, turn_id)?;
        let effect_status: Option<String> = transaction
            .query_row(
                "SELECT status FROM effects
                 WHERE session_id = ?1 AND turn_id = ?2 AND call_id = ?3",
                params![session_id.as_str(), turn_id.as_str(), result.call_id],
                |row| row.get(0),
            )
            .optional()?;
        if matches!(effect_status.as_deref(), Some("scheduled" | "started")) {
            return Err(StoreError::InvalidData(format!(
                "tool result {} cannot be recorded before its execution completes",
                result.call_id
            )));
        }
        let entry_id = MessageId::new();
        let event = record_session_event(
            &transaction,
            session_id,
            turn_actor(
                &transaction,
                session_id,
                turn_id,
                mews_protocol::EventActorKind::Host,
            )?,
            mews_protocol::JournalEvent::ToolResultRecorded {
                turn_id: turn_id.clone(),
                entry_id: entry_id.clone(),
                result: result.clone(),
            },
        )?;
        apply_session_journal_entry(&transaction, &event)?;
        let entry = select_session_entry(&transaction, session_id, &entry_id)?;
        transaction.commit()?;
        Ok(entry)
    }

    pub fn complete_tool_execution(
        &self,
        session_id: &SessionId,
        turn_id: &TurnId,
        result: ToolResult,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        validate_active_turn(&transaction, session_id, turn_id)?;
        let operation_id: mews_protocol::OperationId = transaction
            .query_row(
                "SELECT operation_id FROM effects
                 WHERE session_id = ?1 AND turn_id = ?2 AND call_id = ?3
                   AND status = 'started'",
                params![session_id.as_str(), turn_id.as_str(), result.call_id],
                |row| parse_id(row.get::<_, String>(0)?),
            )
            .optional()?
            .ok_or_else(|| {
                StoreError::InvalidData(format!(
                    "started tool effect {} is missing from Turn {turn_id}",
                    result.call_id
                ))
            })?;
        let event = record_session_event(
            &transaction,
            session_id,
            turn_actor(
                &transaction,
                session_id,
                turn_id,
                mews_protocol::EventActorKind::Host,
            )?,
            mews_protocol::JournalEvent::ToolExecutionCompleted {
                operation_id,
                turn_id: turn_id.clone(),
                result,
            },
        )?;
        apply_session_journal_entry(&transaction, &event)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn append_tool_requested(
        &self,
        session_id: &SessionId,
        turn_id: &TurnId,
        call: ToolCall,
    ) -> Result<SessionEntry, StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        validate_active_turn(&transaction, session_id, turn_id)?;
        let entry_id = MessageId::new();
        let event = record_session_event(
            &transaction,
            session_id,
            turn_actor(
                &transaction,
                session_id,
                turn_id,
                mews_protocol::EventActorKind::Harness,
            )?,
            mews_protocol::JournalEvent::ToolCallRequested {
                turn_id: turn_id.clone(),
                entry_id: entry_id.clone(),
                call: call.clone(),
            },
        )?;
        apply_session_journal_entry(&transaction, &event)?;
        let entry = select_session_entry(&transaction, session_id, &entry_id)?;
        transaction.commit()?;
        Ok(entry)
    }

    pub fn start_tool_effect(
        &self,
        session_id: &SessionId,
        turn_id: &TurnId,
        call: ToolCall,
    ) -> Result<mews_protocol::OperationId, StoreError> {
        self.start_effect(
            session_id,
            turn_id,
            mews_protocol::EffectRequest::ToolCall { call },
        )
    }

    pub fn append_reasoning(
        &self,
        session_id: &SessionId,
        turn_id: &TurnId,
        text: String,
        visibility: ReasoningVisibility,
        provenance: ReasoningProvenance,
        idempotency_key: Option<String>,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        validate_active_turn(&transaction, session_id, turn_id)?;
        if observation_exists(&transaction, session_id, idempotency_key.as_deref())? {
            transaction.commit()?;
            return Ok(());
        }
        let entry_id = MessageId::new();
        let event = record_session_event_correlated(
            &transaction,
            session_id,
            turn_actor(
                &transaction,
                session_id,
                turn_id,
                mews_protocol::EventActorKind::Harness,
            )?,
            mews_protocol::JournalEvent::ReasoningRecorded {
                turn_id: turn_id.clone(),
                entry_id,
                text,
                visibility,
                provenance,
            },
            idempotency_key,
        )?;
        apply_session_journal_entry(&transaction, &event)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn append_harness_observation(
        &self,
        session_id: &SessionId,
        turn_id: &TurnId,
        harness_session_id: Option<String>,
        kind: impl Into<String>,
        data: Value,
        idempotency_key: Option<String>,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        validate_active_turn(&transaction, session_id, turn_id)?;
        if observation_exists(&transaction, session_id, idempotency_key.as_deref())? {
            transaction.commit()?;
            return Ok(());
        }
        let entry_id = MessageId::new();
        let event = record_session_event_correlated(
            &transaction,
            session_id,
            turn_actor(
                &transaction,
                session_id,
                turn_id,
                mews_protocol::EventActorKind::Harness,
            )?,
            mews_protocol::JournalEvent::HarnessObservationRecorded {
                turn_id: turn_id.clone(),
                entry_id,
                harness_session_id,
                kind: kind.into(),
                data,
            },
            idempotency_key,
        )?;
        apply_session_journal_entry(&transaction, &event)?;
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
        let transaction = self.connection.unchecked_transaction()?;
        let entry_id = MessageId::new();
        let event = record_session_event(
            &transaction,
            session_id,
            mews_protocol::EventActor::system(),
            mews_protocol::JournalEvent::ContextCompacted {
                entry_id: entry_id.clone(),
                summary,
                first_kept_entry_id,
                tokens_before,
            },
        )?;
        apply_session_journal_entry(&transaction, &event)?;
        let entry = select_session_entry(&transaction, session_id, &entry_id)?;
        transaction.commit()?;
        Ok(entry)
    }

    /// ACP observations are timeline side entries: they share the current
    /// contextual leaf as an anchor but never advance it.
    pub fn append_acp_observation(
        &self,
        session_id: &SessionId,
        turn_id: TurnId,
        acp_session_id: Option<String>,
        event_key: Option<mews_protocol::AcpEventKey>,
        observation: AcpObservation,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        record_acp_observation_transaction(
            &transaction,
            session_id,
            turn_id,
            acp_session_id,
            event_key,
            observation,
        )?;
        transaction.commit()?;
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
        validate_message_input(&content, &metadata)?;
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
        let entry_id = MessageId::new();
        let event = crate::events::record_journal_entry(
            &transaction,
            mews_protocol::JournalSubjectType::Session,
            session_id.as_str(),
            crate::NewJournalEntry {
                id: EventId::new(),
                actor: mews_protocol::EventActor::from_source(&source),
                correlation_id: None,
                payload: mews_protocol::JournalEvent::UserMessageAppended {
                    entry_id: entry_id.clone(),
                    content,
                    metadata,
                    source,
                },
            },
        )?;
        apply_session_journal_entry(&transaction, &event)?;
        let entry = select_session_entry(&transaction, session_id, &entry_id)?;
        let message = message_from_entry(entry)?;
        transaction.commit()?;
        Ok(message)
    }

    pub fn set_session_leaf_checked(
        &self,
        session_id: &SessionId,
        expected_leaf: Option<&MessageId>,
        new_leaf: Option<&MessageId>,
    ) -> Result<Session, StoreError> {
        let transaction = rusqlite::Transaction::new_unchecked(
            &self.connection,
            rusqlite::TransactionBehavior::Immediate,
        )?;
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
        let event = crate::events::record_journal_entry(
            &transaction,
            mews_protocol::JournalSubjectType::Session,
            session_id.as_str(),
            crate::NewJournalEntry {
                id: EventId::new(),
                actor: mews_protocol::EventActor::system(),
                correlation_id: None,
                payload: mews_protocol::JournalEvent::SessionLeafChanged {
                    leaf_entry_id: new_leaf.cloned(),
                },
            },
        )?;
        apply_session_journal_entry(&transaction, &event)?;
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

fn validate_message_input(content: &MessageContent, metadata: &Value) -> Result<(), StoreError> {
    let metadata_bytes =
        serde_json::to_vec(metadata).map_err(|error| StoreError::InvalidData(error.to_string()))?;
    if metadata_bytes.len() > 64 * 1024 {
        return Err(StoreError::InvalidData(
            "message metadata exceeds 64 KiB".into(),
        ));
    }
    if matches!(content, MessageContent::Text { text } if text.trim().is_empty()) {
        return Err(StoreError::InvalidData(
            "message text cannot be empty".into(),
        ));
    }
    Ok(())
}

fn validate_turn_key(key: &str) -> Result<(), StoreError> {
    if key.is_empty() || key.len() > 200 {
        return Err(StoreError::InvalidData(
            "invalid turn idempotency key".into(),
        ));
    }
    Ok(())
}

fn validate_turn_source(source: &MessageSource) -> Result<(), StoreError> {
    if !matches!(source.kind, SourceKind::Client | SourceKind::Channel)
        || source.id.is_empty()
        || source.id.len() > 256
    {
        return Err(StoreError::InvalidData(
            "turn source must be a Client or Channel with a valid ID".into(),
        ));
    }
    Ok(())
}

fn turn_command_id(session_id: &SessionId, key: &str) -> String {
    format!("turn:{session_id}:{key}")
}

fn turn_request_hash(
    session_id: &SessionId,
    request_content: &MessageContent,
    metadata: &Value,
    source: &MessageSource,
) -> Result<String, StoreError> {
    crate::command_request_hash(&serde_json::json!({
        "session_id": session_id,
        "content": request_content,
        "metadata": metadata,
        "source": source,
    }))
}

fn load_turn_result(
    store: &Store,
    session_id: &SessionId,
    result: &Value,
) -> Result<(Turn, Message), StoreError> {
    let turn_id: TurnId = parse_id(
        result
            .get("turn_id")
            .and_then(Value::as_str)
            .ok_or_else(|| StoreError::InvalidData("turn receipt is missing Turn ID".into()))?
            .to_owned(),
    )?;
    let entry_id: MessageId = parse_id(
        result
            .get("entry_id")
            .and_then(Value::as_str)
            .ok_or_else(|| StoreError::InvalidData("turn receipt is missing entry ID".into()))?
            .to_owned(),
    )?;
    Ok((
        store.turn(&turn_id)?,
        message_from_entry(select_session_entry(
            &store.connection,
            session_id,
            &entry_id,
        )?)?,
    ))
}

fn enum_json_string(value: &impl serde::Serialize) -> Result<String, StoreError> {
    let value =
        serde_json::to_string(value).map_err(|error| StoreError::InvalidData(error.to_string()))?;
    Ok(value.trim_matches('"').to_owned())
}

fn select_acp_binding(
    connection: &rusqlite::Connection,
    session_id: &SessionId,
) -> Result<Option<AcpSessionBinding>, StoreError> {
    connection
        .query_row(
            "SELECT host_id, harness, harness_definition_hash, acp_session_id,
                    context_version, context_hash, context_channel, context_text,
                    context_dispatched, created_at, replaced_at, last_replacement_reason
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
        .map_err(StoreError::from)
}

fn select_session_entry(
    connection: &rusqlite::Connection,
    session_id: &SessionId,
    entry_id: &MessageId,
) -> Result<SessionEntry, StoreError> {
    connection
        .query_row(
            "SELECT sequence, parent_id, payload_json, created_at
             FROM session_entries WHERE session_id = ?1 AND id = ?2",
            params![session_id.as_str(), entry_id.as_str()],
            |row| {
                Ok(SessionEntry {
                    id: entry_id.clone(),
                    session_id: session_id.clone(),
                    sequence: row.get(0)?,
                    parent_id: row.get::<_, Option<String>>(1)?.map(parse_id).transpose()?,
                    payload: parse_json(row.get(2)?)?,
                    created_at: parse_time(row.get(3)?)?,
                })
            },
        )
        .optional()?
        .ok_or_else(|| StoreError::NotFound {
            kind: "Session entry",
            id: entry_id.to_string(),
        })
}

pub(crate) fn apply_session_journal_entry(
    transaction: &rusqlite::Transaction<'_>,
    event: &mews_protocol::JournalEntry,
) -> Result<(), StoreError> {
    let session_id = crate::events::journal_session_id(event)?;
    match &event.payload {
        mews_protocol::JournalEvent::SessionCreated { session } => {
            transaction.execute(
                "INSERT INTO sessions
                 (id, agent_id, host_id, working_directory, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    session.id.as_str(),
                    session.agent_id.as_str(),
                    session.host_id.as_str(),
                    session.working_directory.to_string_lossy(),
                    timestamp(session.created_at)
                ],
            )?;
        }
        mews_protocol::JournalEvent::SessionModelChanged { model } => {
            let changed = transaction.execute(
                "UPDATE sessions SET model_override = ?2 WHERE id = ?1",
                params![session_id.as_str(), model],
            )?;
            if changed != 1 {
                return Err(StoreError::NotFound {
                    kind: "session",
                    id: session_id.to_string(),
                });
            }
        }
        mews_protocol::JournalEvent::UserMessageAppended {
            entry_id,
            content,
            metadata,
            source,
        } => {
            append_session_entry(
                transaction,
                event,
                entry_id,
                SessionEntryPayload::UserMessage {
                    content: content.clone(),
                    metadata: metadata.clone(),
                    source: source.clone(),
                },
                "user_message",
                true,
            )?;
        }
        mews_protocol::JournalEvent::SessionLeafChanged { leaf_entry_id } => {
            let changed = transaction.execute(
                "UPDATE sessions SET leaf_entry_id = ?2 WHERE id = ?1",
                params![
                    session_id.as_str(),
                    leaf_entry_id.as_ref().map(MessageId::as_str)
                ],
            )?;
            if changed != 1 {
                return Err(StoreError::NotFound {
                    kind: "session",
                    id: session_id.to_string(),
                });
            }
        }
        mews_protocol::JournalEvent::TurnAccepted {
            turn_id,
            agent_revision,
            entry_id,
            content,
            metadata,
            source,
        } => {
            if let Some(active_turn) = transaction
                .query_row(
                    "SELECT id FROM turns WHERE session_id = ?1 AND completed_at IS NULL",
                    [session_id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
            {
                return Err(StoreError::ActiveTurnConflict {
                    session_id: session_id.to_string(),
                    turn_id: active_turn,
                });
            }
            let parent_id = current_session_leaf(transaction, &session_id)?;
            let sequence: u64 = transaction.query_row(
                "SELECT COALESCE(MAX(sequence) + 1, 1) FROM session_entries WHERE session_id = ?1",
                [session_id.as_str()],
                |row| row.get(0),
            )?;
            let payload = SessionEntryPayload::UserMessage {
                content: content.clone(),
                metadata: metadata.clone(),
                source: source.clone(),
            };
            validate_session_item(&payload)?;
            transaction.execute(
                "INSERT INTO session_entries
                 (id, session_id, sequence, parent_id, kind, contextual, payload_json, created_at)
                 VALUES (?1, ?2, ?3, ?4, 'user_message', 1, ?5, ?6)",
                params![
                    entry_id.as_str(),
                    session_id.as_str(),
                    sequence,
                    parent_id.as_ref().map(MessageId::as_str),
                    json(&payload)?,
                    timestamp(event.recorded_at)
                ],
            )?;
            transaction.execute(
                "UPDATE sessions SET leaf_entry_id = ?2 WHERE id = ?1",
                params![session_id.as_str(), entry_id.as_str()],
            )?;
            let revision_exists: bool = transaction.query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM sessions
                     JOIN agent_revisions ON agent_revisions.agent_id = sessions.agent_id
                     WHERE sessions.id = ?1 AND agent_revisions.revision = ?2
                 )",
                params![session_id.as_str(), agent_revision],
                |row| row.get(0),
            )?;
            if !revision_exists {
                return Err(StoreError::InvalidData(
                    "Turn Agent revision does not belong to its Session Agent".into(),
                ));
            }
            transaction.execute(
                "INSERT INTO turns
                 (id, session_id, agent_revision, idempotency_key, channel_origin_json, status_json, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    turn_id.as_str(),
                    session_id.as_str(),
                    agent_revision,
                    event.correlation_id,
                    source.channel_origin.as_ref().map(json).transpose()?,
                    json(&TurnStatus::Running)?,
                    timestamp(event.recorded_at)
                ],
            )?;
        }
        mews_protocol::JournalEvent::TurnStarted { turn_id, harness } => {
            validate_active_turn(transaction, &session_id, turn_id)?;
            transaction.execute(
                "UPDATE turns SET harness = ?2, harness_definition_hash = ?3,
                 harness_version = ?4 WHERE id = ?1",
                params![
                    turn_id.as_str(),
                    harness.name,
                    harness.definition_hash,
                    harness.version
                ],
            )?;
            let entry_id = message_id_for_event(&event.id)?;
            append_session_entry(
                transaction,
                event,
                &entry_id,
                SessionEntryPayload::TurnStarted {
                    turn_id: turn_id.clone(),
                    harness: harness.clone(),
                },
                "turn_started",
                false,
            )?;
            crate::delivery::append_durable_client_event(
                transaction,
                event,
                Some(&entry_id),
                &ClientEventKind::TurnStarted {
                    turn_id: turn_id.clone(),
                },
            )?;
        }
        mews_protocol::JournalEvent::AssistantResponseRecorded {
            turn_id,
            entry_id,
            response,
        } => {
            let entry = append_session_entry(
                transaction,
                event,
                entry_id,
                SessionEntryPayload::AssistantResponse {
                    turn_id: turn_id.clone(),
                    response: response.clone(),
                },
                "assistant_response",
                true,
            )?;
            let text = response
                .blocks
                .iter()
                .filter_map(|block| match block {
                    mews_protocol::AssistantResponseBlock::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<String>();
            if !text.is_empty() {
                crate::delivery::append_durable_client_event(
                    transaction,
                    event,
                    Some(entry_id),
                    &ClientEventKind::AssistantMessage {
                        turn_id: turn_id.clone(),
                        message: Message {
                            id: entry.id,
                            session_id: session_id.clone(),
                            sequence: entry.sequence,
                            role: MessageRole::Assistant,
                            content: MessageContent::Text { text },
                            metadata: Value::Null,
                            source: message_source_for_event(event),
                            created_at: entry.created_at,
                        },
                    },
                )?;
            }
        }
        mews_protocol::JournalEvent::ToolCallRequested {
            turn_id,
            entry_id,
            call,
        } => {
            let entry = append_session_entry(
                transaction,
                event,
                entry_id,
                SessionEntryPayload::ToolStarted {
                    turn_id: turn_id.clone(),
                    call: call.clone(),
                },
                "tool_started",
                false,
            )?;
            crate::delivery::append_durable_client_event(
                transaction,
                event,
                Some(entry_id),
                &ClientEventKind::ToolStarted {
                    turn_id: turn_id.clone(),
                    message: Message {
                        id: entry.id,
                        session_id: session_id.clone(),
                        sequence: entry.sequence,
                        role: MessageRole::Assistant,
                        content: MessageContent::ToolCall {
                            call_id: call.call_id.clone(),
                            tool: call.tool.clone(),
                            arguments: call.arguments.clone(),
                            thought_signature: call.thought_signature.clone(),
                        },
                        metadata: Value::Null,
                        source: message_source_for_event(event),
                        created_at: entry.created_at,
                    },
                },
            )?;
        }
        mews_protocol::JournalEvent::ToolResultRecorded {
            turn_id,
            entry_id,
            result,
        } => {
            let entry = append_session_entry(
                transaction,
                event,
                entry_id,
                SessionEntryPayload::ToolResult {
                    turn_id: turn_id.clone(),
                    result: result.clone(),
                },
                "tool_result",
                true,
            )?;
            crate::delivery::append_durable_client_event(
                transaction,
                event,
                Some(entry_id),
                &ClientEventKind::ToolCompleted {
                    turn_id: turn_id.clone(),
                    message: Message {
                        id: entry.id,
                        session_id: session_id.clone(),
                        sequence: entry.sequence,
                        role: MessageRole::Tool,
                        content: MessageContent::ToolResult {
                            call_id: result.call_id.clone(),
                            tool: result.tool.clone(),
                            result: result.result.clone(),
                            is_error: result.is_error,
                            uncertain: result.uncertain,
                        },
                        metadata: Value::Null,
                        source: message_source_for_event(event),
                        created_at: entry.created_at,
                    },
                },
            )?;
        }
        mews_protocol::JournalEvent::ToolExecutionCompleted {
            operation_id,
            turn_id,
            result,
        } => {
            let status = if result.uncertain {
                "uncertain"
            } else if result.is_error {
                "failed"
            } else {
                "succeeded"
            };
            set_effect_terminal(
                transaction,
                event,
                operation_id,
                turn_id,
                status,
                Some(result),
            )?;
        }
        mews_protocol::JournalEvent::ReasoningRecorded {
            turn_id,
            entry_id,
            text,
            visibility,
            provenance,
        } => {
            append_session_entry(
                transaction,
                event,
                entry_id,
                SessionEntryPayload::Reasoning {
                    turn_id: turn_id.clone(),
                    text: text.clone(),
                    visibility: *visibility,
                    provenance: provenance.clone(),
                },
                "reasoning",
                false,
            )?;
        }
        mews_protocol::JournalEvent::HarnessObservationRecorded {
            turn_id,
            entry_id,
            harness_session_id,
            kind,
            data,
        } => {
            append_session_entry(
                transaction,
                event,
                entry_id,
                SessionEntryPayload::HarnessObservation {
                    turn_id: turn_id.clone(),
                    harness_session_id: harness_session_id.clone(),
                    kind: kind.clone(),
                    data: data.clone(),
                },
                "harness_observation",
                false,
            )?;
        }
        mews_protocol::JournalEvent::ContextCompacted {
            entry_id,
            summary,
            first_kept_entry_id,
            tokens_before,
        } => {
            append_session_entry(
                transaction,
                event,
                entry_id,
                SessionEntryPayload::ContextCompaction {
                    summary: summary.clone(),
                    first_kept_entry_id: first_kept_entry_id.clone(),
                    tokens_before: *tokens_before,
                },
                "context_compaction",
                true,
            )?;
        }
        mews_protocol::JournalEvent::EffectScheduled {
            operation_id,
            turn_id,
            effect,
        } => {
            let (call_id, tool) = match effect {
                mews_protocol::EffectRequest::ToolCall { call } => {
                    (Some(call.call_id.as_str()), Some(call.tool.as_str()))
                }
                _ => (None, None),
            };
            transaction.execute(
                "INSERT INTO effects
                 (operation_id, session_id, turn_id, call_id, tool, request_json,
                  status, scheduled_journal_entry_id, scheduled_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'scheduled', ?7, ?8)",
                params![
                    operation_id.as_str(),
                    session_id.as_str(),
                    turn_id.as_str(),
                    call_id,
                    tool,
                    json(effect)?,
                    event.id.as_str(),
                    timestamp(event.recorded_at)
                ],
            )?;
        }
        mews_protocol::JournalEvent::EffectStarted { operation_id, .. } => {
            let changed = transaction.execute(
                "UPDATE effects SET status = 'started', started_at = ?2
                 WHERE operation_id = ?1 AND status = 'scheduled'",
                params![operation_id.as_str(), timestamp(event.recorded_at)],
            )?;
            if changed != 1 {
                return Err(StoreError::InvalidData(format!(
                    "effect {operation_id} was not scheduled before it started"
                )));
            }
        }
        mews_protocol::JournalEvent::EffectSucceeded {
            operation_id,
            turn_id,
            ..
        } => {
            set_effect_terminal(transaction, event, operation_id, turn_id, "succeeded", None)?;
        }
        mews_protocol::JournalEvent::EffectFailed {
            operation_id,
            turn_id,
            ..
        } => {
            set_effect_terminal(transaction, event, operation_id, turn_id, "failed", None)?;
        }
        mews_protocol::JournalEvent::EffectUncertain {
            operation_id,
            turn_id,
            ..
        } => {
            set_effect_terminal(transaction, event, operation_id, turn_id, "uncertain", None)?;
        }
        mews_protocol::JournalEvent::AcpBindingChanged { binding } => {
            transaction.execute(
                "INSERT INTO acp_session_bindings
                 (session_id, host_id, harness, harness_definition_hash, acp_session_id,
                  context_version, context_hash, context_channel, context_text,
                  context_dispatched, created_at, replaced_at, last_replacement_reason)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                 ON CONFLICT(session_id) DO UPDATE SET
                    host_id=excluded.host_id, harness=excluded.harness,
                    harness_definition_hash=excluded.harness_definition_hash,
                    acp_session_id=excluded.acp_session_id,
                    context_version=excluded.context_version, context_hash=excluded.context_hash,
                    context_channel=excluded.context_channel, context_text=excluded.context_text,
                    context_dispatched=excluded.context_dispatched,
                    replaced_at=excluded.replaced_at,
                    last_replacement_reason=excluded.last_replacement_reason",
                params![
                    binding.session_id.as_str(),
                    binding.host_id.as_str(),
                    binding.harness,
                    binding.harness_definition_hash,
                    binding.acp_session_id,
                    binding.context_version,
                    binding.context_hash,
                    enum_json_string(&binding.context_channel)?,
                    binding.context_text,
                    binding.context_dispatched,
                    timestamp(binding.created_at),
                    binding.replaced_at.map(timestamp),
                    binding
                        .last_replacement_reason
                        .as_ref()
                        .map(enum_json_string)
                        .transpose()?
                ],
            )?;
        }
        mews_protocol::JournalEvent::AcpContextDispatched {
            host_id,
            harness,
            context_version,
            context_hash,
            channel,
        } => {
            let changed = transaction.execute(
                "UPDATE acp_session_bindings SET context_dispatched = 1
                 WHERE session_id = ?1 AND host_id = ?2 AND harness = ?3
                   AND context_version = ?4 AND context_hash = ?5 AND context_channel = ?6",
                params![
                    session_id.as_str(),
                    host_id.as_str(),
                    harness,
                    context_version,
                    context_hash,
                    enum_json_string(channel)?
                ],
            )?;
            if changed != 1 {
                return Err(StoreError::InvalidData(
                    "ACP context dispatch does not match its binding".into(),
                ));
            }
        }
        mews_protocol::JournalEvent::TurnCompleted {
            turn_id,
            stop_reason,
        } => project_turn_terminal(
            transaction,
            event,
            turn_id,
            TurnStatus::Completed,
            None,
            SessionEntryPayload::TurnCompleted {
                turn_id: turn_id.clone(),
                stop_reason: stop_reason.clone(),
            },
            ClientEventKind::TurnCompleted {
                turn_id: turn_id.clone(),
            },
        )?,
        mews_protocol::JournalEvent::TurnFailed { turn_id, error } => project_turn_terminal(
            transaction,
            event,
            turn_id,
            TurnStatus::Failed,
            Some(error),
            SessionEntryPayload::TurnFailed {
                turn_id: turn_id.clone(),
                error: error.clone(),
            },
            ClientEventKind::TurnFailed {
                turn_id: turn_id.clone(),
                error: error.clone(),
            },
        )?,
        mews_protocol::JournalEvent::TurnCancelled { turn_id } => project_turn_terminal(
            transaction,
            event,
            turn_id,
            TurnStatus::Cancelled,
            None,
            SessionEntryPayload::TurnCancelled {
                turn_id: turn_id.clone(),
            },
            ClientEventKind::TurnCancelled {
                turn_id: turn_id.clone(),
            },
        )?,
        mews_protocol::JournalEvent::TurnInterrupted { turn_id, reason } => project_turn_terminal(
            transaction,
            event,
            turn_id,
            TurnStatus::Failed,
            Some(reason),
            SessionEntryPayload::TurnFailed {
                turn_id: turn_id.clone(),
                error: reason.clone(),
            },
            ClientEventKind::TurnFailed {
                turn_id: turn_id.clone(),
                error: reason.clone(),
            },
        )?,
        _ => {}
    }
    Ok(())
}

fn message_id_for_event(event_id: &EventId) -> Result<MessageId, StoreError> {
    let uuid = event_id
        .as_str()
        .strip_prefix("evt_")
        .ok_or_else(|| StoreError::InvalidData("invalid event ID".into()))?;
    Ok(parse_id(format!("msg_{uuid}"))?)
}

fn message_source_for_event(event: &mews_protocol::JournalEntry) -> MessageSource {
    let kind = match event.actor.kind {
        mews_protocol::EventActorKind::Client => SourceKind::Client,
        mews_protocol::EventActorKind::Channel => SourceKind::Channel,
        mews_protocol::EventActorKind::Harness => SourceKind::Harness,
        mews_protocol::EventActorKind::Host | mews_protocol::EventActorKind::System => {
            SourceKind::Host
        }
    };
    MessageSource {
        kind,
        id: event.actor.id.clone().unwrap_or_else(|| "system".into()),
        channel_origin: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn project_turn_terminal(
    transaction: &rusqlite::Transaction<'_>,
    event: &mews_protocol::JournalEntry,
    turn_id: &TurnId,
    status: TurnStatus,
    error: Option<&String>,
    payload: SessionEntryPayload,
    client_kind: ClientEventKind,
) -> Result<(), StoreError> {
    let changed = transaction.execute(
        "UPDATE turns SET status_json = ?2, error = ?3, completed_at = ?4
         WHERE id = ?1 AND completed_at IS NULL",
        params![
            turn_id.as_str(),
            json(&status)?,
            error,
            timestamp(event.recorded_at)
        ],
    )?;
    if changed != 1 {
        return Err(StoreError::InvalidData(format!(
            "Turn {turn_id} is missing or already terminal"
        )));
    }
    let entry_id = message_id_for_event(&event.id)?;
    append_session_entry(
        transaction,
        event,
        &entry_id,
        payload,
        match status {
            TurnStatus::Completed => "turn_completed",
            TurnStatus::Failed => "turn_failed",
            TurnStatus::Cancelled => "turn_cancelled",
            TurnStatus::Running => unreachable!(),
        },
        false,
    )?;
    crate::delivery::append_durable_client_event(transaction, event, Some(&entry_id), &client_kind)
}

fn set_effect_terminal(
    transaction: &rusqlite::Transaction<'_>,
    event: &mews_protocol::JournalEntry,
    operation_id: &mews_protocol::OperationId,
    turn_id: &TurnId,
    status: &str,
    raw_result: Option<&ToolResult>,
) -> Result<(), StoreError> {
    let session_id = crate::events::journal_session_id(event)?;
    let changed = transaction.execute(
        "UPDATE effects
         SET status = ?4, terminal_journal_entry_id = ?5, completed_at = ?6,
             raw_result_json = ?7
         WHERE operation_id = ?1 AND session_id = ?2 AND turn_id = ?3
           AND status IN ('scheduled', 'started')",
        params![
            operation_id.as_str(),
            session_id.as_str(),
            turn_id.as_str(),
            status,
            event.id.as_str(),
            timestamp(event.recorded_at),
            raw_result.map(json).transpose()?
        ],
    )?;
    if changed != 1 {
        return Err(StoreError::InvalidData(format!(
            "effect {operation_id} is missing or already terminal"
        )));
    }
    Ok(())
}

pub(crate) fn validate_active_turn(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
    turn_id: &TurnId,
) -> Result<(), StoreError> {
    let active: bool = transaction.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM turns
             WHERE id = ?1 AND session_id = ?2 AND completed_at IS NULL
         )",
        params![turn_id.as_str(), session_id.as_str()],
        |row| row.get(0),
    )?;
    if !active {
        return Err(StoreError::InvalidData(format!(
            "Turn {turn_id} is not active in Session {session_id}"
        )));
    }
    Ok(())
}

fn turn_actor(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
    turn_id: &TurnId,
    kind: mews_protocol::EventActorKind,
) -> Result<mews_protocol::EventActor, StoreError> {
    let id = match kind {
        mews_protocol::EventActorKind::Host => transaction.query_row(
            "SELECT host_id FROM sessions WHERE id = ?1",
            [session_id.as_str()],
            |row| row.get(0),
        )?,
        mews_protocol::EventActorKind::Harness => transaction
            .query_row(
                "SELECT harness FROM turns WHERE id = ?1 AND session_id = ?2",
                params![turn_id.as_str(), session_id.as_str()],
                |row| row.get::<_, Option<String>>(0),
            )?
            .unwrap_or_else(|| "mews".into()),
        _ => return Err(StoreError::InvalidData("invalid Turn actor kind".into())),
    };
    Ok(mews_protocol::EventActor { kind, id: Some(id) })
}

fn observation_exists(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
    key: Option<&str>,
) -> Result<bool, StoreError> {
    let Some(key) = key else {
        return Ok(false);
    };
    transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM session_entries
                 WHERE session_id = ?1 AND observation_key = ?2
             )",
            params![session_id.as_str(), key],
            |row| row.get(0),
        )
        .map_err(StoreError::from)
}

pub(crate) fn record_session_event(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
    actor: mews_protocol::EventActor,
    payload: mews_protocol::JournalEvent,
) -> Result<mews_protocol::JournalEntry, StoreError> {
    record_session_event_correlated(transaction, session_id, actor, payload, None)
}

fn record_session_event_correlated(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
    actor: mews_protocol::EventActor,
    payload: mews_protocol::JournalEvent,
    correlation_id: Option<String>,
) -> Result<mews_protocol::JournalEntry, StoreError> {
    crate::events::record_journal_entry(
        transaction,
        mews_protocol::JournalSubjectType::Session,
        session_id.as_str(),
        crate::NewJournalEntry {
            id: EventId::new(),
            actor,
            correlation_id,
            payload,
        },
    )
}

fn schedule_effect_in(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
    turn_id: &TurnId,
    effect: mews_protocol::EffectRequest,
) -> Result<mews_protocol::OperationId, StoreError> {
    let operation_id = mews_protocol::OperationId::new();
    let actor = turn_actor(
        transaction,
        session_id,
        turn_id,
        mews_protocol::EventActorKind::Harness,
    )?;
    let scheduled = record_session_event(
        transaction,
        session_id,
        actor.clone(),
        mews_protocol::JournalEvent::EffectScheduled {
            operation_id: operation_id.clone(),
            turn_id: turn_id.clone(),
            effect,
        },
    )?;
    apply_session_journal_entry(transaction, &scheduled)?;
    Ok(operation_id)
}

fn mark_effect_started_in(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
    turn_id: &TurnId,
    operation_id: &mews_protocol::OperationId,
) -> Result<(), StoreError> {
    let owned: bool = transaction.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM effects
             WHERE operation_id = ?1 AND session_id = ?2 AND turn_id = ?3
               AND status = 'scheduled'
         )",
        params![operation_id.as_str(), session_id.as_str(), turn_id.as_str()],
        |row| row.get(0),
    )?;
    if !owned {
        return Err(StoreError::InvalidData(format!(
            "scheduled effect {operation_id} does not belong to active Turn {turn_id} in Session {session_id}"
        )));
    }
    let actor = turn_actor(
        transaction,
        session_id,
        turn_id,
        mews_protocol::EventActorKind::Harness,
    )?;
    let started = record_session_event(
        transaction,
        session_id,
        actor,
        mews_protocol::JournalEvent::EffectStarted {
            operation_id: operation_id.clone(),
            turn_id: turn_id.clone(),
        },
    )?;
    apply_session_journal_entry(transaction, &started)?;
    Ok(())
}

fn finish_effect_in(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
    turn_id: &TurnId,
    operation_id: &mews_protocol::OperationId,
    outcome: EffectOutcome,
) -> Result<(), StoreError> {
    let owned: bool = transaction.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM effects
             WHERE operation_id = ?1 AND session_id = ?2 AND turn_id = ?3
               AND status = 'started'
         )",
        params![operation_id.as_str(), session_id.as_str(), turn_id.as_str()],
        |row| row.get(0),
    )?;
    if !owned {
        return Err(StoreError::InvalidData(format!(
            "started effect {operation_id} does not belong to active Turn {turn_id} in Session {session_id}"
        )));
    }
    let payload = match outcome {
        EffectOutcome::Succeeded(result) => mews_protocol::JournalEvent::EffectSucceeded {
            operation_id: operation_id.clone(),
            turn_id: turn_id.clone(),
            result,
        },
        EffectOutcome::Failed(error) => mews_protocol::JournalEvent::EffectFailed {
            operation_id: operation_id.clone(),
            turn_id: turn_id.clone(),
            error,
        },
        EffectOutcome::Uncertain(reason) => mews_protocol::JournalEvent::EffectUncertain {
            operation_id: operation_id.clone(),
            turn_id: turn_id.clone(),
            reason,
        },
    };
    let terminal = record_session_event(
        transaction,
        session_id,
        turn_actor(
            transaction,
            session_id,
            turn_id,
            mews_protocol::EventActorKind::Host,
        )?,
        payload,
    )?;
    apply_session_journal_entry(transaction, &terminal)?;
    Ok(())
}

fn append_session_entry(
    transaction: &rusqlite::Transaction<'_>,
    event: &mews_protocol::JournalEntry,
    entry_id: &MessageId,
    payload: SessionEntryPayload,
    kind: &str,
    contextual: bool,
) -> Result<SessionEntry, StoreError> {
    let session_id = crate::events::journal_session_id(event)?;
    validate_session_item(&payload)?;
    let parent_id = current_session_leaf(transaction, &session_id)?;
    let sequence: u64 = transaction.query_row(
        "SELECT COALESCE(MAX(sequence) + 1, 1) FROM session_entries WHERE session_id = ?1",
        [session_id.as_str()],
        |row| row.get(0),
    )?;
    // ACP item keys deduplicate observations. General correlation metadata is
    // provenance and must not impose entry uniqueness for the whole Turn.
    let observation_key = matches!(
        payload,
        SessionEntryPayload::Reasoning { .. } | SessionEntryPayload::HarnessObservation { .. }
    )
    .then_some(event.correlation_id.as_deref())
    .flatten();
    transaction.execute(
        "INSERT INTO session_entries
         (id, session_id, sequence, parent_id, kind, contextual, observation_key,
          payload_json, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            entry_id.as_str(),
            session_id.as_str(),
            sequence,
            parent_id.as_ref().map(MessageId::as_str),
            kind,
            contextual,
            observation_key,
            json(&payload)?,
            timestamp(event.recorded_at)
        ],
    )?;
    if contextual {
        transaction.execute(
            "UPDATE sessions SET leaf_entry_id = ?2 WHERE id = ?1",
            params![session_id.as_str(), entry_id.as_str()],
        )?;
    }
    Ok(SessionEntry {
        id: entry_id.clone(),
        session_id: session_id.clone(),
        sequence,
        parent_id,
        payload,
        created_at: event.recorded_at,
    })
}

fn current_session_leaf(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
) -> Result<Option<MessageId>, StoreError> {
    transaction
        .query_row(
            "SELECT leaf_entry_id FROM sessions WHERE id = ?1",
            [session_id.as_str()],
            |row| row.get::<_, Option<String>>(0)?.map(parse_id).transpose(),
        )
        .map_err(StoreError::from)
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
                uncertain: result.uncertain,
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

#[allow(clippy::too_many_arguments)]
fn decide_acp_binding(
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
) -> Result<AcpSessionBinding, StoreError> {
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
    let existing = select_acp_binding(transaction, session_id)?;
    let now = Utc::now();
    match existing {
        None if matches!(transition, AcpBindingTransition::New) => Ok(AcpSessionBinding {
            session_id: session_id.clone(),
            host_id: host_id.clone(),
            harness: harness.to_owned(),
            harness_definition_hash: definition_hash.to_owned(),
            acp_session_id: acp_session_id.to_owned(),
            context_version: context.version,
            context_hash: AcpContextSnapshot::hash_rendered(context_text),
            context_channel: channel,
            context_text: context_text.to_owned(),
            context_dispatched,
            created_at: now,
            replaced_at: None,
            last_replacement_reason: None,
        }),
        Some(existing)
            if matches!(transition, AcpBindingTransition::Replace { .. })
                && existing.host_id == *host_id =>
        {
            let AcpBindingTransition::Replace { reason } = transition else {
                unreachable!()
            };
            Ok(AcpSessionBinding {
                acp_session_id: acp_session_id.to_owned(),
                harness: harness.to_owned(),
                harness_definition_hash: definition_hash.to_owned(),
                context_version: context.version,
                context_hash: AcpContextSnapshot::hash_rendered(context_text),
                context_channel: channel,
                context_text: context_text.to_owned(),
                context_dispatched,
                replaced_at: Some(now),
                last_replacement_reason: Some(*reason),
                ..existing
            })
        }
        Some(existing)
            if matches!(transition, AcpBindingTransition::New)
                && existing.host_id == *host_id
                && existing.harness == harness
                && existing.harness_definition_hash == definition_hash
                && existing.acp_session_id == acp_session_id =>
        {
            Ok(existing)
        }
        _ => Err(StoreError::InvalidData(
            "ACP Session binding conflicts with its existing Host or Harness".into(),
        )),
    }
}

pub(crate) fn record_acp_observation_transaction(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
    turn_id: TurnId,
    acp_session_id: Option<String>,
    event_key: Option<mews_protocol::AcpEventKey>,
    observation: AcpObservation,
) -> Result<(), StoreError> {
    if observation_exists(transaction, session_id, event_key.as_deref())? {
        return Ok(());
    }
    let entry_id = MessageId::new();
    let payload = match observation {
        AcpObservation::CompletedReasoning {
            text,
            message_id,
            visibility,
        } => mews_protocol::JournalEvent::ReasoningRecorded {
            turn_id,
            entry_id,
            text,
            visibility,
            provenance: ReasoningProvenance::Harness {
                harness: "acp".into(),
                message_id,
            },
        },
        observation => {
            let data = serde_json::to_value(observation)
                .map_err(|error| StoreError::InvalidData(error.to_string()))?;
            let kind = data
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("acp")
                .to_owned();
            mews_protocol::JournalEvent::HarnessObservationRecorded {
                turn_id,
                entry_id,
                harness_session_id: acp_session_id,
                kind,
                data,
            }
        }
    };
    let event = record_session_event_correlated(
        transaction,
        session_id,
        mews_protocol::EventActor {
            kind: mews_protocol::EventActorKind::Harness,
            id: Some("acp".into()),
        },
        payload,
        event_key,
    )?;
    apply_session_journal_entry(transaction, &event)?;
    Ok(())
}
