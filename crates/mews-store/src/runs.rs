use super::*;

impl Store {
    pub fn start_run(&self, session_id: &SessionId) -> Result<Run, StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        let run = insert_run(&transaction, session_id)?;
        transaction.commit()?;
        Ok(run)
    }

    pub fn start_run_idempotent(
        &self,
        session_id: &SessionId,
        key: &str,
    ) -> Result<(Run, bool), StoreError> {
        if key.is_empty() || key.len() > 200 {
            return Err(StoreError::InvalidData(
                "invalid turn idempotency key".into(),
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        if let Some(run_id) = transaction
            .query_row(
                "SELECT run_id FROM turn_requests WHERE session_id = ?1 AND key = ?2",
                params![session_id.as_str(), key],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            transaction.commit()?;
            return Ok((self.run(&parse_id(run_id)?)?, false));
        }
        let run = insert_run(&transaction, session_id)?;
        transaction.execute(
            "INSERT INTO turn_requests (session_id, key, run_id) VALUES (?1, ?2, ?3)",
            params![session_id.as_str(), key, run.id.as_str()],
        )?;
        transaction.commit()?;
        Ok((run, true))
    }

    pub fn finish_run(
        &self,
        run_id: &RunId,
        status: RunStatus,
        error: Option<&str>,
    ) -> Result<(), StoreError> {
        if status == RunStatus::Running {
            return Err(StoreError::InvalidData(
                "a finished Run cannot remain running".into(),
            ));
        }
        let transaction = self.connection.unchecked_transaction()?;
        let session_id: String = transaction
            .query_row(
                "SELECT session_id FROM runs WHERE id = ?1 AND completed_at IS NULL",
                [run_id.as_str()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound {
                kind: "active run",
                id: run_id.to_string(),
            })?;
        let changed = transaction.execute(
            "UPDATE runs SET status_json = ?2, error = ?3, completed_at = ?4
             WHERE id = ?1 AND completed_at IS NULL",
            params![
                run_id.as_str(),
                json(&status)?,
                error,
                timestamp(Utc::now())
            ],
        )?;
        debug_assert_eq!(changed, 1);
        let kind = match status {
            RunStatus::Completed => ClientEventKind::RunCompleted {
                run_id: run_id.clone(),
            },
            RunStatus::Failed | RunStatus::Cancelled => ClientEventKind::RunFailed {
                run_id: run_id.clone(),
                error: error.unwrap_or("Run cancelled").to_owned(),
            },
            RunStatus::Running => unreachable!(),
        };
        transaction.execute(
            "INSERT INTO client_events (id, session_id, kind_json, created_at) VALUES (?1, ?2, ?3, ?4)",
            params![EventId::new().as_str(), session_id, json(&kind)?, timestamp(Utc::now())],
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
        let changed = self.connection.execute(
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
        Ok(())
    }

    pub fn run(&self, run_id: &RunId) -> Result<Run, StoreError> {
        self.connection
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
}

fn insert_run(
    connection: &rusqlite::Connection,
    session_id: &SessionId,
) -> Result<Run, StoreError> {
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
    connection.execute(
        "INSERT INTO runs (id, session_id, harness, harness_definition_hash, harness_version,
                           status_json, error, created_at, completed_at)
         VALUES (?1, ?2, NULL, NULL, NULL, ?3, NULL, ?4, NULL)",
        params![
            run.id.as_str(),
            run.session_id.as_str(),
            json(&run.status)?,
            timestamp(run.created_at)
        ],
    )?;
    connection.execute(
        "INSERT INTO client_events (id, session_id, kind_json, created_at)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            EventId::new().as_str(),
            session_id.as_str(),
            json(&ClientEventKind::RunStarted {
                run_id: run.id.clone()
            })?,
            timestamp(Utc::now())
        ],
    )?;
    Ok(run)
}
