use super::*;

impl Store {
    pub fn start_run(&self, session_id: &SessionId) -> Result<Run, StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        let (run, _) = insert_run(&transaction, session_id, None, None)?;
        transaction.commit()?;
        Ok(run)
    }

    pub fn start_run_idempotent(
        &self,
        session_id: &SessionId,
        key: &str,
        channel_origin: Option<&mews_protocol::ChannelOrigin>,
    ) -> Result<(Run, bool), StoreError> {
        if key.is_empty() || key.len() > 200 {
            return Err(StoreError::InvalidData(
                "invalid turn idempotency key".into(),
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        let (run, created) = insert_run(&transaction, session_id, Some(key), channel_origin)?;
        transaction.commit()?;
        Ok((run, created))
    }

    pub fn finish_run(
        &self,
        run_id: &RunId,
        status: RunStatus,
        error: Option<&str>,
    ) -> Result<(), StoreError> {
        self.finish_run_with_stop_reason(run_id, status, error, None)
    }

    pub fn finish_run_with_stop_reason(
        &self,
        run_id: &RunId,
        status: RunStatus,
        error: Option<&str>,
        stop_reason: Option<&str>,
    ) -> Result<(), StoreError> {
        if status == RunStatus::Running {
            return Err(StoreError::InvalidData(
                "a finished Run cannot remain running".into(),
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        let session_id: Option<String> = transaction
            .query_row(
                "SELECT session_id FROM runs WHERE id = ?1 AND completed_at IS NULL",
                [run_id.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(session_id) = session_id else {
            let existing = self.run(run_id)?;
            if existing.status == status {
                return Ok(());
            }
            return Err(StoreError::InvalidData(format!(
                "Run already finished with status {:?}",
                existing.status
            )));
        };
        let session_id: SessionId = parse_id(session_id)?;
        let kind = match status {
            RunStatus::Completed => ClientEventKind::RunCompleted {
                run_id: run_id.clone(),
            },
            RunStatus::Failed => ClientEventKind::RunFailed {
                run_id: run_id.clone(),
                error: error.unwrap_or("Run failed").to_owned(),
            },
            RunStatus::Cancelled => ClientEventKind::RunCancelled {
                run_id: run_id.clone(),
            },
            RunStatus::Running => unreachable!(),
        };
        let event_id = EventId::new();
        let now = Utc::now();
        let origin = crate::events::channel_origin_json(&transaction, &kind)?;
        crate::events::validate_client_event(
            &session_id,
            &event_id,
            &kind,
            origin.as_deref(),
            now,
        )?;
        let changed = transaction.execute(
            "UPDATE runs SET status_json = ?2, error = ?3, completed_at = ?4
             WHERE id = ?1 AND completed_at IS NULL",
            params![run_id.as_str(), json(&status)?, error, timestamp(now)],
        )?;
        debug_assert_eq!(changed, 1);
        let payload = match status {
            RunStatus::Completed => SessionEntryPayload::RunCompleted {
                run_id: run_id.clone(),
                stop_reason: stop_reason.map(str::to_owned),
            },
            RunStatus::Failed => SessionEntryPayload::RunFailed {
                run_id: run_id.clone(),
                error: error.unwrap_or("Run failed").to_owned(),
            },
            RunStatus::Cancelled => SessionEntryPayload::RunCancelled {
                run_id: run_id.clone(),
            },
            RunStatus::Running => unreachable!(),
        };
        let entry_id = insert_run_entry(
            &transaction,
            &session_id,
            &payload,
            terminal_kind(status),
            now,
        )?;
        transaction.execute(
            "INSERT INTO client_events (id, session_id, entry_id, kind_json, channel_origin_json, transient, created_at) VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6)",
            params![event_id.as_str(), session_id.as_str(), entry_id.as_str(), json(&kind)?, origin, timestamp(now)],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn record_run_harness(
        &self,
        run_id: &RunId,
        harness: &str,
        definition_hash: &str,
        version: Option<&str>,
    ) -> Result<(), StoreError> {
        if harness.is_empty() || definition_hash.is_empty() {
            return Err(StoreError::InvalidData(
                "Run Harness name and definition hash must not be empty".into(),
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        let (session_id, existing): (SessionId, Option<(String, String, Option<String>)>) =
            transaction.query_row(
                "SELECT session_id, harness, harness_definition_hash, harness_version
                 FROM runs WHERE id = ?1 AND completed_at IS NULL",
                [run_id.as_str()],
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
                "Run Harness provenance is already different".into(),
            ));
        }
        let changed = transaction.execute(
            "UPDATE runs
             SET harness = ?2, harness_definition_hash = ?3, harness_version = ?4
             WHERE id = ?1 AND completed_at IS NULL
               AND (harness IS NULL OR (harness = ?2 AND harness_definition_hash = ?3 AND harness_version IS ?4))",
            params![run_id.as_str(), harness, definition_hash, version],
        )?;
        if changed != 1 {
            return Err(StoreError::InvalidData(
                "Run Harness provenance is missing, finished, or already differs".into(),
            ));
        }
        let now = Utc::now();
        let entry_id = insert_run_entry(
            &transaction,
            &session_id,
            &SessionEntryPayload::RunStarted {
                run_id: run_id.clone(),
                harness: HarnessProvenance {
                    name: harness.to_owned(),
                    definition_hash: definition_hash.to_owned(),
                    version: version.map(str::to_owned),
                },
            },
            "run_started",
            now,
        )?;
        let event = ClientEventKind::RunStarted {
            run_id: run_id.clone(),
        };
        transaction.execute(
            "INSERT INTO client_events (id, session_id, entry_id, kind_json, channel_origin_json, transient, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6)",
            params![
                EventId::new().as_str(),
                session_id.as_str(),
                entry_id.as_str(),
                json(&event)?,
                crate::events::channel_origin_json(&transaction, &event)?,
                timestamp(now)
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn run(&self, run_id: &RunId) -> Result<Run, StoreError> {
        select_run(&self.connection, run_id)
    }
}

fn insert_run(
    connection: &rusqlite::Connection,
    session_id: &SessionId,
    idempotency_key: Option<&str>,
    channel_origin: Option<&mews_protocol::ChannelOrigin>,
) -> Result<(Run, bool), StoreError> {
    let run = Run {
        id: RunId::new(),
        session_id: session_id.clone(),
        harness: None,
        harness_definition_hash: None,
        harness_version: None,
        status: RunStatus::Running,
        error: None,
        created_at: Utc::now(),
        completed_at: None,
    };
    let created = connection.execute(
        "INSERT INTO runs (id, session_id, idempotency_key, harness, harness_definition_hash,
                           harness_version, channel_origin_json, status_json, error, created_at, completed_at)
         VALUES (?1, ?2, ?3, NULL, NULL, NULL, ?4, ?5, NULL, ?6, NULL)
         ON CONFLICT(session_id, idempotency_key) DO NOTHING",
        params![
            run.id.as_str(),
            run.session_id.as_str(),
            idempotency_key,
            channel_origin.map(json).transpose()?,
            json(&run.status)?,
            timestamp(run.created_at)
        ],
    )? == 1;
    if created {
        return Ok((run, true));
    }
    let run_id: String = connection.query_row(
        "SELECT id FROM runs WHERE session_id = ?1 AND idempotency_key = ?2",
        params![session_id.as_str(), idempotency_key],
        |row| row.get(0),
    )?;
    Ok((select_run(connection, &parse_id(run_id)?)?, false))
}

fn select_run(connection: &rusqlite::Connection, run_id: &RunId) -> Result<Run, StoreError> {
    connection
        .query_row(
            "SELECT session_id, harness, harness_definition_hash, harness_version,
                    status_json, error, created_at, completed_at
             FROM runs WHERE id = ?1",
            [run_id.as_str()],
            |row| {
                Ok(Run {
                    id: run_id.clone(),
                    session_id: parse_id(row.get::<_, String>(0)?)?,
                    harness: row.get(1)?,
                    harness_definition_hash: row.get(2)?,
                    harness_version: row.get(3)?,
                    status: parse_json(row.get::<_, String>(4)?)?,
                    error: row.get(5)?,
                    created_at: parse_time(row.get::<_, String>(6)?)?,
                    completed_at: row
                        .get::<_, Option<String>>(7)?
                        .map(parse_time)
                        .transpose()?,
                })
            },
        )
        .optional()?
        .ok_or_else(|| StoreError::NotFound {
            kind: "run",
            id: run_id.to_string(),
        })
}

fn terminal_kind(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Completed => "run_completed",
        RunStatus::Failed => "run_failed",
        RunStatus::Cancelled => "run_cancelled",
        RunStatus::Running => unreachable!(),
    }
}

fn insert_run_entry(
    transaction: &rusqlite::Transaction<'_>,
    session_id: &SessionId,
    payload: &SessionEntryPayload,
    kind: &str,
    created_at: chrono::DateTime<Utc>,
) -> Result<MessageId, StoreError> {
    crate::sessions::validate_session_item(payload)?;
    let leaf: Option<String> = transaction.query_row(
        "SELECT leaf_entry_id FROM sessions WHERE id = ?1",
        [session_id.as_str()],
        |row| row.get(0),
    )?;
    let sequence: u64 = transaction.query_row(
        "SELECT COALESCE(MAX(sequence) + 1, 1) FROM session_entries WHERE session_id = ?1",
        [session_id.as_str()],
        |row| row.get(0),
    )?;
    let entry_id = MessageId::new();
    transaction.execute(
        "INSERT INTO session_entries
         (id, session_id, sequence, parent_id, kind, contextual, payload_json, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7)",
        params![
            entry_id.as_str(),
            session_id.as_str(),
            sequence,
            leaf,
            kind,
            json(payload)?,
            timestamp(created_at)
        ],
    )?;
    Ok(entry_id)
}
