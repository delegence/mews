use super::*;

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
                 created_at TEXT NOT NULL,
                 replaced_at TEXT,
                 UNIQUE (host_id, harness, acp_session_id)
             );
             CREATE TABLE IF NOT EXISTS acp_session_replacements (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 session_id TEXT NOT NULL REFERENCES sessions(id),
                 old_acp_session_id TEXT NOT NULL,
                 new_acp_session_id TEXT NOT NULL,
                 reason TEXT NOT NULL,
                 created_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS messages (
                 id TEXT PRIMARY KEY,
                 session_id TEXT NOT NULL REFERENCES sessions(id),
                 sequence INTEGER NOT NULL,
                 role_json TEXT NOT NULL,
                 content_json TEXT NOT NULL,
                 metadata_json TEXT NOT NULL,
                 source_json TEXT NOT NULL,
                 created_at TEXT NOT NULL,
                 UNIQUE (session_id, sequence)
             );
             CREATE TABLE IF NOT EXISTS runs (
                 id TEXT PRIMARY KEY,
                 session_id TEXT NOT NULL REFERENCES sessions(id),
                 harness TEXT,
                 harness_definition_hash TEXT,
                 harness_version TEXT,
                 status_json TEXT NOT NULL,
                 error TEXT,
                 created_at TEXT NOT NULL,
                 completed_at TEXT
             );
             CREATE UNIQUE INDEX IF NOT EXISTS one_active_run_per_session
                 ON runs(session_id) WHERE completed_at IS NULL;
             CREATE TABLE IF NOT EXISTS turn_requests (
                 session_id TEXT NOT NULL REFERENCES sessions(id),
                 key TEXT NOT NULL,
                 run_id TEXT NOT NULL UNIQUE REFERENCES runs(id),
                 PRIMARY KEY (session_id, key)
             );
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
                 kind_json TEXT NOT NULL,
                 created_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS client_consumers (
                 id TEXT PRIMARY KEY,
                 cursor INTEGER NOT NULL,
                 created_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS client_subscriptions (
                 consumer_id TEXT NOT NULL REFERENCES client_consumers(id) ON DELETE CASCADE,
                 session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                 PRIMARY KEY (consumer_id, session_id)
             );
             COMMIT;",
        )?;
        Ok(())
    }

    fn recover_interrupted_work(&self) -> Result<(), StoreError> {
        let interrupted = {
            let mut statement = self
                .connection
                .prepare("SELECT id, session_id FROM runs WHERE completed_at IS NULL")?;
            statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let failed = json(&RunStatus::Failed)?;
        self.connection.execute(
            "UPDATE runs SET status_json = ?1, error = ?2, completed_at = ?3
             WHERE completed_at IS NULL",
            params![
                failed,
                "Hub stopped before the Run completed",
                timestamp(Utc::now())
            ],
        )?;
        for (run_id, session_id) in interrupted {
            let run_id: RunId = parse_id(run_id)?;
            let kind = ClientEventKind::RunFailed {
                run_id,
                error: "Hub stopped before the Run completed".into(),
            };
            self.connection.execute(
                "INSERT INTO client_events (id, session_id, kind_json, created_at) VALUES (?1, ?2, ?3, ?4)",
                params![EventId::new().as_str(), session_id, json(&kind)?, timestamp(Utc::now())],
            )?;
        }

        let session_ids = {
            let mut statement = self
                .connection
                .prepare("SELECT DISTINCT session_id FROM messages")?;
            statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut missing = Vec::new();
        for value in session_ids {
            let session_id: SessionId = value
                .parse()
                .map_err(|error: &str| StoreError::InvalidData(error.into()))?;
            let messages = self.messages(&session_id)?;
            let completed = messages
                .iter()
                .filter_map(|message| match &message.content {
                    MessageContent::ToolResult { call_id, .. } => Some(call_id.as_str()),
                    _ => None,
                })
                .collect::<std::collections::HashSet<_>>();
            for message in &messages {
                if let MessageContent::ToolCall { call_id, tool, .. } = &message.content
                    && !completed.contains(call_id.as_str())
                {
                    missing.push((session_id.clone(), call_id.clone(), tool.clone()));
                }
            }
        }
        for (session_id, call_id, tool) in missing {
            self.append_message(
                &session_id,
                MessageRole::Tool,
                MessageContent::ToolResult {
                    call_id,
                    tool,
                    result: Value::String(
                        "outcome unknown because Hub stopped during tool execution".into(),
                    ),
                    is_error: true,
                },
                Value::Null,
                MessageSource {
                    kind: SourceKind::Host,
                    id: "recovery".into(),
                },
            )?;
        }
        Ok(())
    }
}
