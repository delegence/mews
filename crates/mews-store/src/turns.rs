use super::*;

impl Store {
    #[cfg(test)]
    pub fn start_turn(&self, session_id: &SessionId) -> Result<Turn, StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        let (turn, _) = insert_turn(&transaction, session_id, None, None)?;
        transaction.commit()?;
        Ok(turn)
    }

    #[cfg(test)]
    pub fn start_turn_idempotent(
        &self,
        session_id: &SessionId,
        key: &str,
        channel_origin: Option<&mews_protocol::ChannelOrigin>,
    ) -> Result<(Turn, bool), StoreError> {
        if key.is_empty() || key.len() > 200 {
            return Err(StoreError::InvalidData(
                "invalid turn idempotency key".into(),
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        let (turn, created) = insert_turn(&transaction, session_id, Some(key), channel_origin)?;
        transaction.commit()?;
        Ok((turn, created))
    }

    pub fn finish_turn(
        &self,
        turn_id: &TurnId,
        status: TurnStatus,
        error: Option<&str>,
    ) -> Result<(), StoreError> {
        self.finish_turn_with_stop_reason(turn_id, status, error, None)
    }

    pub fn finish_turn_with_stop_reason(
        &self,
        turn_id: &TurnId,
        status: TurnStatus,
        error: Option<&str>,
        stop_reason: Option<&str>,
    ) -> Result<(), StoreError> {
        if status == TurnStatus::Running {
            return Err(StoreError::InvalidData(
                "a finished Turn cannot remain running".into(),
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        let session_id: Option<String> = transaction
            .query_row(
                "SELECT session_id FROM turns WHERE id = ?1 AND completed_at IS NULL",
                [turn_id.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(session_id) = session_id else {
            let existing = self.turn(turn_id)?;
            if existing.status == status {
                return Ok(());
            }
            return Err(StoreError::InvalidData(format!(
                "Turn already finished with status {:?}",
                existing.status
            )));
        };
        let session_id: SessionId = parse_id(session_id)?;
        let has_open_effects: bool = transaction.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM effects
                 WHERE turn_id = ?1 AND status IN ('scheduled', 'started')
             )",
            [turn_id.as_str()],
            |row| row.get(0),
        )?;
        if status == TurnStatus::Completed && has_open_effects {
            return Err(StoreError::InvalidData(
                "a Turn cannot complete while it has open effects".into(),
            ));
        }
        if has_open_effects {
            close_open_effects(&transaction, &session_id, turn_id)?;
        }
        close_orphan_tool_calls(&transaction, &session_id, turn_id)?;
        let payload = match status {
            TurnStatus::Completed => mews_protocol::JournalEvent::TurnCompleted {
                turn_id: turn_id.clone(),
                stop_reason: stop_reason.map(str::to_owned),
            },
            TurnStatus::Failed => mews_protocol::JournalEvent::TurnFailed {
                turn_id: turn_id.clone(),
                error: error.unwrap_or("Turn failed").to_owned(),
            },
            TurnStatus::Cancelled => mews_protocol::JournalEvent::TurnCancelled {
                turn_id: turn_id.clone(),
            },
            TurnStatus::Running => unreachable!(),
        };
        let event = crate::sessions::record_session_event(
            &transaction,
            &session_id,
            mews_protocol::EventActor::system(),
            payload,
        )?;
        crate::sessions::apply_session_journal_entry(&transaction, &event)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn record_turn_harness(
        &self,
        turn_id: &TurnId,
        harness: &str,
        definition_hash: &str,
        version: Option<&str>,
    ) -> Result<(), StoreError> {
        if harness.is_empty() || definition_hash.is_empty() {
            return Err(StoreError::InvalidData(
                "Turn Harness name and definition hash must not be empty".into(),
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        let (session_id, existing): (SessionId, Option<(String, String, Option<String>)>) =
            transaction.query_row(
                "SELECT session_id, harness, harness_definition_hash, harness_version
                 FROM turns WHERE id = ?1 AND completed_at IS NULL",
                [turn_id.as_str()],
                |row| {
                    Ok((
                        parse_id(row.get::<_, String>(0)?)?,
                        row.get::<_, Option<String>>(1)?
                            .map(|name| Ok::<_, rusqlite::Error>((name, row.get(2)?, row.get(3)?)))
                            .transpose()?,
                    ))
                },
            )?;
        if let Some((existing_name, existing_hash, existing_version)) = existing {
            if existing_name == harness
                && existing_hash == definition_hash
                && existing_version.as_deref() == version
            {
                transaction.commit()?;
                return Ok(());
            }
            return Err(StoreError::InvalidData(
                "Turn Harness provenance is already different".into(),
            ));
        }
        let event = crate::sessions::record_session_event(
            &transaction,
            &session_id,
            mews_protocol::EventActor {
                kind: mews_protocol::EventActorKind::Harness,
                id: Some(harness.to_owned()),
            },
            mews_protocol::JournalEvent::TurnStarted {
                turn_id: turn_id.clone(),
                harness: HarnessProvenance {
                    name: harness.to_owned(),
                    definition_hash: definition_hash.to_owned(),
                    version: version.map(str::to_owned),
                },
            },
        )?;
        crate::sessions::apply_session_journal_entry(&transaction, &event)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn turn(&self, turn_id: &TurnId) -> Result<Turn, StoreError> {
        select_turn(&self.connection, turn_id)
    }

    pub fn previous_turn_harness(
        &self,
        session_id: &SessionId,
        turn_id: &TurnId,
    ) -> Result<Option<String>, StoreError> {
        self.connection
            .query_row(
                "SELECT harness FROM turns
                 WHERE session_id = ?1
                   AND rowid < (SELECT rowid FROM turns WHERE id = ?2)
                 ORDER BY rowid DESC LIMIT 1",
                params![session_id.as_str(), turn_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }
}

pub(crate) fn close_open_effects(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
    turn_id: &TurnId,
) -> Result<(), StoreError> {
    let effects = {
        let mut statement = transaction.prepare(
            "SELECT operation_id, status, call_id, tool FROM effects
             WHERE turn_id = ?1 AND status IN ('scheduled', 'started')
             ORDER BY scheduled_at",
        )?;
        statement
            .query_map([turn_id.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (operation_id, status, call_id, tool) in effects {
        let operation_id: mews_protocol::OperationId = parse_id(operation_id)?;
        let started = status == "started";
        let payload = if started {
            mews_protocol::JournalEvent::EffectUncertain {
                operation_id,
                turn_id: turn_id.clone(),
                reason: "Turn terminated before the effect reported an outcome".into(),
            }
        } else {
            mews_protocol::JournalEvent::EffectFailed {
                operation_id,
                turn_id: turn_id.clone(),
                error: "Turn terminated before the effect started".into(),
            }
        };
        let event = crate::sessions::record_session_event(
            transaction,
            session_id,
            mews_protocol::EventActor::system(),
            payload,
        )?;
        crate::sessions::apply_session_journal_entry(transaction, &event)?;
        if let (Some(call_id), Some(tool)) = (call_id, tool) {
            record_terminated_tool_result(
                transaction,
                session_id,
                turn_id,
                call_id,
                tool,
                started,
            )?;
        }
    }
    Ok(())
}

pub(crate) fn close_orphan_tool_calls(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
    turn_id: &TurnId,
) -> Result<(), StoreError> {
    let entries = {
        let mut statement = transaction.prepare(
            "SELECT payload_json FROM session_entries
             WHERE session_id = ?1 ORDER BY sequence",
        )?;
        statement
            .query_map([session_id.as_str()], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut requested = Vec::new();
    let mut completed = std::collections::HashSet::new();
    for entry in entries {
        match parse_json::<SessionEntryPayload>(entry)? {
            SessionEntryPayload::ToolStarted {
                turn_id: entry_turn,
                call,
            } if entry_turn == *turn_id => requested.push(call),
            SessionEntryPayload::ToolResult {
                turn_id: entry_turn,
                result,
            } if entry_turn == *turn_id => {
                completed.insert(result.call_id);
            }
            _ => {}
        }
    }
    let raw_completions = {
        let mut statement = transaction.prepare(
            "SELECT raw_result_json FROM effects
             WHERE session_id = ?1 AND turn_id = ?2 AND raw_result_json IS NOT NULL",
        )?;
        let payloads = statement
            .query_map(params![session_id.as_str(), turn_id.as_str()], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut completions = std::collections::HashMap::new();
        for payload in payloads {
            let result: ToolResult = parse_json(payload)?;
            completions.insert(result.call_id.clone(), result);
        }
        completions
    };
    for call in requested {
        if !completed.contains(&call.call_id) {
            if let Some(raw) = raw_completions.get(&call.call_id) {
                record_interrupted_tool_processing(transaction, session_id, turn_id, raw)?;
            } else {
                record_terminated_tool_result(
                    transaction,
                    session_id,
                    turn_id,
                    call.call_id,
                    call.tool,
                    false,
                )?;
            }
        }
    }
    Ok(())
}

fn record_interrupted_tool_processing(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
    turn_id: &TurnId,
    raw: &ToolResult,
) -> Result<(), StoreError> {
    let event = crate::sessions::record_session_event(
        transaction,
        session_id,
        mews_protocol::EventActor::system(),
        mews_protocol::JournalEvent::ToolResultRecorded {
            turn_id: turn_id.clone(),
            entry_id: MessageId::new(),
            result: ToolResult {
                call_id: raw.call_id.clone(),
                tool: raw.tool.clone(),
                result: Value::String(
                    "tool execution completed, but result processing was interrupted before hooks finished"
                        .into(),
                ),
                is_error: true,
                uncertain: true,
            },
        },
    )?;
    crate::sessions::apply_session_journal_entry(transaction, &event)
}

fn record_terminated_tool_result(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
    turn_id: &TurnId,
    call_id: String,
    tool: String,
    uncertain: bool,
) -> Result<(), StoreError> {
    let detail = if uncertain {
        "outcome unknown because the Turn ended during tool execution"
    } else {
        "tool execution did not start before the Turn ended"
    };
    let event = crate::sessions::record_session_event(
        transaction,
        session_id,
        mews_protocol::EventActor::system(),
        mews_protocol::JournalEvent::ToolResultRecorded {
            turn_id: turn_id.clone(),
            entry_id: MessageId::new(),
            result: ToolResult {
                call_id,
                tool,
                result: Value::String(detail.into()),
                is_error: true,
                uncertain,
            },
        },
    )?;
    crate::sessions::apply_session_journal_entry(transaction, &event)
}

#[cfg(test)]
fn insert_turn(
    connection: &rusqlite::Connection,
    session_id: &SessionId,
    idempotency_key: Option<&str>,
    channel_origin: Option<&mews_protocol::ChannelOrigin>,
) -> Result<(Turn, bool), StoreError> {
    let agent_revision = connection.query_row(
        "SELECT agents.current_revision
         FROM sessions JOIN agents ON agents.id = sessions.agent_id
         WHERE sessions.id = ?1",
        [session_id.as_str()],
        |row| row.get(0),
    )?;
    let turn = Turn {
        id: TurnId::new(),
        session_id: session_id.clone(),
        agent_revision,
        harness: None,
        harness_definition_hash: None,
        harness_version: None,
        status: TurnStatus::Running,
        error: None,
        created_at: Utc::now(),
        completed_at: None,
    };
    let created = connection.execute(
        "INSERT INTO turns (id, session_id, agent_revision, idempotency_key, harness, harness_definition_hash,
                           harness_version, channel_origin_json, status_json, error, created_at, completed_at)
         VALUES (?1, ?2, ?3, ?4, NULL, NULL, NULL, ?5, ?6, NULL, ?7, NULL)
         ON CONFLICT(session_id, idempotency_key) DO NOTHING",
        params![
            turn.id.as_str(),
            turn.session_id.as_str(),
            turn.agent_revision,
            idempotency_key,
            channel_origin.map(json).transpose()?,
            json(&turn.status)?,
            timestamp(turn.created_at)
        ],
    )? == 1;
    if created {
        return Ok((turn, true));
    }
    let turn_id: String = connection.query_row(
        "SELECT id FROM turns WHERE session_id = ?1 AND idempotency_key = ?2",
        params![session_id.as_str(), idempotency_key],
        |row| row.get(0),
    )?;
    Ok((select_turn(connection, &parse_id(turn_id)?)?, false))
}

fn select_turn(connection: &rusqlite::Connection, turn_id: &TurnId) -> Result<Turn, StoreError> {
    connection
        .query_row(
            "SELECT session_id, agent_revision, harness, harness_definition_hash, harness_version,
                    status_json, error, created_at, completed_at
             FROM turns WHERE id = ?1",
            [turn_id.as_str()],
            |row| {
                Ok(Turn {
                    id: turn_id.clone(),
                    session_id: parse_id(row.get::<_, String>(0)?)?,
                    agent_revision: row.get(1)?,
                    harness: row.get(2)?,
                    harness_definition_hash: row.get(3)?,
                    harness_version: row.get(4)?,
                    status: parse_json(row.get::<_, String>(5)?)?,
                    error: row.get(6)?,
                    created_at: parse_time(row.get::<_, String>(7)?)?,
                    completed_at: row
                        .get::<_, Option<String>>(8)?
                        .map(parse_time)
                        .transpose()?,
                })
            },
        )
        .optional()?
        .ok_or_else(|| StoreError::NotFound {
            kind: "turn",
            id: turn_id.to_string(),
        })
}
