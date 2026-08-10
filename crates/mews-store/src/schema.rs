use super::*;

const SCHEMA_VERSION: u32 = 1;

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        let store = Self {
            connection,
            _hub_lock: None,
        };
        store.initialize_schema()?;
        Ok(store)
    }

    /// Opens a connection after the owning process has already initialized and
    /// validated the development schema. This keeps request-scoped connections
    /// from repeating schema DDL and WAL negotiation on the Hub hot path.
    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        Ok(Self {
            connection,
            _hub_lock: None,
        })
    }

    pub fn open_hub(
        path: impl AsRef<Path>,
        lock_path: impl AsRef<Path>,
    ) -> Result<Self, StoreError> {
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .map_err(|error| StoreError::InvalidData(error.to_string()))?;
        lock.try_lock_exclusive().map_err(|_| {
            StoreError::InvalidData("another Hub process already owns this state directory".into())
        })?;
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        let store = Self {
            connection,
            _hub_lock: Some(lock),
        };
        store.initialize_schema()?;
        store.recover_interrupted_work()?;
        Ok(store)
    }

    pub fn open_in_memory() -> Result<Self, StoreError> {
        let connection = Connection::open_in_memory()?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        let store = Self {
            connection,
            _hub_lock: None,
        };
        store.initialize_schema()?;
        Ok(store)
    }

    fn initialize_schema(&self) -> Result<(), StoreError> {
        let version: u32 = self
            .connection
            .pragma_query_value(None, "user_version", |row| row.get(0))?;
        let has_schema: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'hosts')",
            [],
            |row| row.get(0),
        )?;
        if version != SCHEMA_VERSION && (version != 0 || has_schema) {
            return Err(StoreError::InvalidData(format!(
                "development schema changed (found version {version}, expected {SCHEMA_VERSION}); reset MEWS_HOME"
            )));
        }
        self.connection.execute_batch(
            "BEGIN;
             CREATE TABLE IF NOT EXISTS hosts (
                 id TEXT PRIMARY KEY,
                 name TEXT NOT NULL UNIQUE,
                 public_key TEXT NOT NULL UNIQUE,
                 noise_public_key TEXT NOT NULL UNIQUE,
                 relay_url TEXT,
                 revoked INTEGER NOT NULL DEFAULT 0,
                 created_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS installation (
                 singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                 id TEXT NOT NULL,
                 public_key TEXT NOT NULL,
                 relay_url TEXT,
                 hub_host_id TEXT NOT NULL REFERENCES hosts(id),
                 generation INTEGER NOT NULL,
                 created_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS settings (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS agents (
                 id TEXT PRIMARY KEY,
                 slug TEXT NOT NULL UNIQUE,
                 current_revision INTEGER NOT NULL,
                 archived INTEGER NOT NULL DEFAULT 0,
                 created_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS agent_revisions (
                 agent_id TEXT NOT NULL REFERENCES agents(id),
                 revision INTEGER NOT NULL,
                 soul TEXT NOT NULL,
                 config_toml TEXT NOT NULL,
                 content_hash TEXT NOT NULL,
                 author_host_id TEXT NOT NULL REFERENCES hosts(id),
                 created_at TEXT NOT NULL,
                 PRIMARY KEY (agent_id, revision)
             );
             CREATE TABLE IF NOT EXISTS sessions (
                 id TEXT PRIMARY KEY,
                 agent_id TEXT NOT NULL REFERENCES agents(id),
                 agent_revision INTEGER NOT NULL,
                 host_id TEXT NOT NULL REFERENCES hosts(id),
                 working_directory TEXT NOT NULL,
                 model_override TEXT,
                 leaf_entry_id TEXT,
                 created_at TEXT NOT NULL,
                 FOREIGN KEY (agent_id, agent_revision)
                     REFERENCES agent_revisions(agent_id, revision)
             );
             CREATE TABLE IF NOT EXISTS acp_session_bindings (
                 session_id TEXT PRIMARY KEY REFERENCES sessions(id),
                 host_id TEXT NOT NULL REFERENCES hosts(id),
                 harness TEXT NOT NULL,
                 harness_definition_hash TEXT NOT NULL,
                 acp_session_id TEXT NOT NULL,
                 context_version INTEGER NOT NULL,
                 context_hash TEXT NOT NULL,
                 context_channel TEXT NOT NULL,
                 context_text TEXT NOT NULL,
                 context_dispatched INTEGER NOT NULL,
                 created_at TEXT NOT NULL,
                 replaced_at TEXT,
                 last_replacement_reason TEXT,
                 UNIQUE (host_id, harness, acp_session_id)
             );
             CREATE TABLE IF NOT EXISTS session_entries (
                 id TEXT PRIMARY KEY,
                 session_id TEXT NOT NULL REFERENCES sessions(id),
                 sequence INTEGER NOT NULL,
                 parent_id TEXT,
                 kind TEXT NOT NULL,
                 contextual INTEGER NOT NULL,
                 observation_key TEXT,
                 payload_json TEXT NOT NULL,
                 created_at TEXT NOT NULL,
                 UNIQUE (session_id, sequence),
                 UNIQUE (session_id, id),
                 FOREIGN KEY (session_id, parent_id)
                     REFERENCES session_entries(session_id, id)
             );
             CREATE TABLE IF NOT EXISTS runs (
                 id TEXT PRIMARY KEY,
                 session_id TEXT NOT NULL REFERENCES sessions(id),
                 idempotency_key TEXT,
                 harness TEXT,
                 harness_definition_hash TEXT,
                 harness_version TEXT,
                 channel_origin_json TEXT,
                 status_json TEXT NOT NULL,
                 error TEXT,
                 created_at TEXT NOT NULL,
                 completed_at TEXT,
                 UNIQUE (session_id, idempotency_key)
             );
             CREATE UNIQUE INDEX IF NOT EXISTS one_active_run_per_session
                 ON runs(session_id) WHERE completed_at IS NULL;
             CREATE UNIQUE INDEX IF NOT EXISTS acp_observation_idempotency
                 ON session_entries(session_id, observation_key)
                 WHERE observation_key IS NOT NULL;
             CREATE TABLE IF NOT EXISTS invitations (
                 id TEXT PRIMARY KEY,
                 secret_hash TEXT NOT NULL UNIQUE,
                 expires_at TEXT NOT NULL,
                 consumed_at TEXT
             );
             CREATE TABLE IF NOT EXISTS client_events (
                 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                 id TEXT NOT NULL UNIQUE,
                 session_id TEXT NOT NULL REFERENCES sessions(id),
                 entry_id TEXT,
                 idempotency_key TEXT,
                 kind_json TEXT NOT NULL,
                 channel_origin_json TEXT,
                 transient INTEGER NOT NULL,
                 created_at TEXT NOT NULL,
                 FOREIGN KEY (session_id, entry_id)
                     REFERENCES session_entries(session_id, id)
             );
             CREATE UNIQUE INDEX IF NOT EXISTS client_event_idempotency
                 ON client_events(session_id, idempotency_key)
                 WHERE idempotency_key IS NOT NULL;
             CREATE INDEX IF NOT EXISTS client_events_session_sequence
                 ON client_events(session_id, sequence);
             CREATE TABLE IF NOT EXISTS client_consumers (
                 id TEXT PRIMARY KEY,
                 cursor INTEGER NOT NULL,
                 kind TEXT NOT NULL,
                 created_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS client_subscriptions (
                 consumer_id TEXT NOT NULL REFERENCES client_consumers(id) ON DELETE CASCADE,
                 session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                 PRIMARY KEY (consumer_id, session_id)
             );
             CREATE INDEX IF NOT EXISTS client_subscriptions_session
                 ON client_subscriptions(session_id, consumer_id);
             PRAGMA user_version = 1;
             COMMIT;",
        )?;
        Ok(())
    }

    fn recover_interrupted_work(&self) -> Result<(), StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        let interrupted = {
            let mut statement = transaction
                .prepare("SELECT id, session_id FROM runs WHERE completed_at IS NULL")?;
            statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let failed = json(&RunStatus::Failed)?;
        let recovered_at = timestamp(Utc::now());
        transaction.execute(
            "UPDATE runs SET status_json = ?1, error = ?2, completed_at = ?3
             WHERE completed_at IS NULL",
            params![failed, "Hub stopped before the Run completed", recovered_at],
        )?;
        for (run_id, session_id) in interrupted {
            let run_id: RunId = parse_id(run_id)?;
            let session_id: SessionId = parse_id(session_id)?;
            let kind = ClientEventKind::RunFailed {
                run_id: run_id.clone(),
                error: "Hub stopped before the Run completed".into(),
            };
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
            let payload = SessionEntryPayload::RunFailed {
                run_id: run_id.clone(),
                error: "Hub stopped before the Run completed".into(),
            };
            transaction.execute(
                "INSERT INTO session_entries
                 (id, session_id, sequence, parent_id, kind, contextual, payload_json, created_at)
                 VALUES (?1, ?2, ?3, ?4, 'run_failed', 0, ?5, ?6)",
                params![
                    MessageId::new().as_str(),
                    session_id.as_str(),
                    sequence,
                    leaf,
                    json(&payload)?,
                    recovered_at
                ],
            )?;
            transaction.execute(
                "INSERT INTO client_events (id, session_id, kind_json, channel_origin_json, transient, created_at) VALUES (?1, ?2, ?3, ?4, 0, ?5)",
                params![EventId::new().as_str(), session_id.as_str(), json(&kind)?, crate::events::channel_origin_json(&transaction, &kind)?, recovered_at],
            )?;
        }
        transaction.commit()?;

        let session_ids = {
            let mut statement = self
                .connection
                .prepare("SELECT DISTINCT session_id FROM session_entries")?;
            statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut missing = Vec::new();
        for value in session_ids {
            let session_id: SessionId = value
                .parse()
                .map_err(|error: &str| StoreError::InvalidData(error.into()))?;
            let entries = self.session_entries(&session_id)?;
            let completed = entries
                .iter()
                .filter_map(|entry| match &entry.payload {
                    SessionEntryPayload::ToolResult { result, .. } => Some(result.call_id.as_str()),
                    _ => None,
                })
                .collect::<std::collections::HashSet<_>>();
            for entry in &entries {
                if let SessionEntryPayload::ToolStarted { run_id, call } = &entry.payload
                    && !completed.contains(call.call_id.as_str())
                {
                    missing.push((
                        session_id.clone(),
                        run_id.clone(),
                        call.call_id.clone(),
                        call.tool.clone(),
                    ));
                }
            }
        }
        for (session_id, run_id, call_id, tool) in missing {
            self.append_tool_result(
                &session_id,
                &run_id,
                ToolResult {
                    call_id,
                    tool,
                    result: Value::String(
                        "outcome unknown because Hub stopped during tool execution".into(),
                    ),
                    is_error: true,
                },
            )?;
        }
        Ok(())
    }
}
