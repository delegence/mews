use super::*;

const SCHEMA_VERSION: u32 = 5;

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
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
        connection.pragma_update(None, "synchronous", "FULL")?;
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
        connection.pragma_update(None, "synchronous", "FULL")?;
        let store = Self {
            connection,
            _hub_lock: Some(lock),
        };
        store.initialize_schema()?;
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
                 host_id TEXT NOT NULL REFERENCES hosts(id),
                 working_directory TEXT NOT NULL,
                 model_override TEXT,
                 leaf_entry_id TEXT,
                 created_at TEXT NOT NULL
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
             CREATE TABLE IF NOT EXISTS turns (
                 id TEXT PRIMARY KEY,
                 session_id TEXT NOT NULL REFERENCES sessions(id),
                 agent_revision INTEGER NOT NULL,
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
             CREATE UNIQUE INDEX IF NOT EXISTS one_active_turn_per_session
                 ON turns(session_id) WHERE completed_at IS NULL;
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
                 journal_entry_id TEXT,
                 journal_position INTEGER,
                 idempotency_key TEXT,
                 kind_json TEXT NOT NULL,
                 channel_origin_json TEXT,
                 transient INTEGER NOT NULL,
                 created_at TEXT NOT NULL,
                 FOREIGN KEY (session_id, entry_id)
                     REFERENCES session_entries(session_id, id),
                 FOREIGN KEY (journal_entry_id)
                     REFERENCES journal_entries(id),
                 CHECK (
                     transient = 1
                     OR (journal_entry_id IS NOT NULL AND journal_position IS NOT NULL)
                 )
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
                 created_at TEXT NOT NULL,
                 last_seen_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS client_subscriptions (
                 consumer_id TEXT NOT NULL REFERENCES client_consumers(id) ON DELETE CASCADE,
                 session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
                 PRIMARY KEY (consumer_id, session_id)
             );
             CREATE INDEX IF NOT EXISTS client_subscriptions_session
                 ON client_subscriptions(session_id, consumer_id);
             CREATE TABLE IF NOT EXISTS command_receipts (
                 command_id TEXT PRIMARY KEY,
                 request_hash TEXT NOT NULL,
                 result_json TEXT NOT NULL,
                 first_position INTEGER,
                 last_position INTEGER,
                 completed_at TEXT NOT NULL,
                 CHECK (
                     (first_position IS NULL AND last_position IS NULL)
                     OR (
                         first_position > 0
                         AND last_position >= first_position
                     )
                 )
             );
             CREATE TABLE IF NOT EXISTS journal_entries (
                 position INTEGER PRIMARY KEY AUTOINCREMENT,
                 id TEXT NOT NULL UNIQUE,
                 subject_type TEXT NOT NULL,
                 subject_id TEXT NOT NULL,
                 event_type TEXT NOT NULL,
                 recorded_at TEXT NOT NULL,
                 actor_kind TEXT NOT NULL,
                 actor_id TEXT,
                 command_id TEXT,
                 correlation_id TEXT,
                 payload_json TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS journal_entries_subject
                 ON journal_entries(subject_type, subject_id, position);
             CREATE INDEX IF NOT EXISTS journal_entries_type_position
                 ON journal_entries(event_type, position);
             CREATE TABLE IF NOT EXISTS effects (
                 operation_id TEXT PRIMARY KEY,
                 session_id TEXT NOT NULL REFERENCES sessions(id),
                 turn_id TEXT NOT NULL REFERENCES turns(id),
                 call_id TEXT,
                 tool TEXT,
                 request_json TEXT NOT NULL,
                 status TEXT NOT NULL CHECK (status IN ('scheduled', 'started', 'succeeded', 'failed', 'uncertain')),
                 raw_result_json TEXT,
                 scheduled_journal_entry_id TEXT NOT NULL REFERENCES journal_entries(id),
                 terminal_journal_entry_id TEXT REFERENCES journal_entries(id),
                 scheduled_at TEXT NOT NULL,
                 started_at TEXT,
                 completed_at TEXT,
                 UNIQUE (turn_id, call_id)
             );
             CREATE INDEX IF NOT EXISTS effects_status_turn
                 ON effects(status, turn_id);
             PRAGMA user_version = 5;
             COMMIT;",
        )?;
        Ok(())
    }

    /// Recovers work only after the application has verified that this process
    /// owns the current Hub generation and installation authority.
    pub fn recover_interrupted_work(&self) -> Result<(), StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        // Live streaming fragments never survive a Hub lifecycle boundary.
        transaction.execute("DELETE FROM client_events WHERE transient = 1", [])?;
        let interrupted = {
            let mut statement = transaction
                .prepare("SELECT id, session_id FROM turns WHERE completed_at IS NULL")?;
            statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        for (turn_id, session_id) in interrupted {
            let turn_id: TurnId = parse_id(turn_id)?;
            let session_id: SessionId = parse_id(session_id)?;
            crate::turns::close_open_effects(&transaction, &session_id, &turn_id)?;
            crate::turns::close_orphan_tool_calls(&transaction, &session_id, &turn_id)?;
            let interrupted = crate::sessions::record_session_event(
                &transaction,
                &session_id,
                mews_protocol::EventActor::system(),
                mews_protocol::JournalEvent::TurnInterrupted {
                    turn_id: turn_id.clone(),
                    reason: "Hub stopped before the Turn completed".into(),
                },
            )?;
            crate::sessions::apply_session_journal_entry(&transaction, &interrupted)?;
        }
        transaction.commit()?;
        Ok(())
    }
}
