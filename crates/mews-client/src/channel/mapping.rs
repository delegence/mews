use std::path::Path;

use anyhow::{Context, Result};
use fs2::FileExt;
use mews_protocol::{ConsumerId, SessionId};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

pub(crate) struct MappingStore(Connection, std::fs::File);

impl MappingStore {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.with_extension("lock"))?;
        lock.try_lock_exclusive()
            .context("another Channel process owns this state")?;
        let connection = Connection::open(path)?;
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS mappings (
                 conversation TEXT PRIMARY KEY,
                 session_id TEXT NOT NULL UNIQUE
             );
             CREATE TABLE IF NOT EXISTS deliveries (
                 event_id TEXT PRIMARY KEY,
                 external_id TEXT
             );
             CREATE TABLE IF NOT EXISTS pending (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 external_id TEXT NOT NULL,
                 conversation TEXT NOT NULL,
                 text TEXT NOT NULL,
                 metadata_json TEXT NOT NULL,
                 UNIQUE (conversation, external_id)
             );
             CREATE TABLE IF NOT EXISTS active_runs (
                 session_id TEXT PRIMARY KEY,
                 run_id TEXT NOT NULL UNIQUE
             );
             CREATE TABLE IF NOT EXISTS dead_letters (
                 event_id TEXT PRIMARY KEY,
                 conversation TEXT NOT NULL,
                 text TEXT NOT NULL,
                 error TEXT NOT NULL,
                 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
             );",
        )?;
        Ok(Self(connection, lock))
    }

    pub(crate) fn consumer_id(&mut self) -> Result<ConsumerId> {
        let _held_process_lock = &self.1;
        if let Some(value) = self
            .0
            .query_row(
                "SELECT value FROM metadata WHERE key = 'consumer_id'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            return value.parse().map_err(anyhow::Error::msg);
        }
        let id = ConsumerId::new();
        self.0.execute(
            "INSERT INTO metadata (key, value) VALUES ('consumer_id', ?1)",
            [id.as_str()],
        )?;
        Ok(id)
    }

    pub(crate) fn session(&self, conversation: &str) -> Result<Option<SessionId>> {
        let value = self
            .0
            .query_row(
                "SELECT session_id FROM mappings WHERE conversation = ?1",
                [conversation],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        value
            .map(|value| value.parse().map_err(anyhow::Error::msg))
            .transpose()
    }

    pub(crate) fn conversation(&self, session: &SessionId) -> Result<Option<String>> {
        Ok(self
            .0
            .query_row(
                "SELECT conversation FROM mappings WHERE session_id = ?1",
                [session.as_str()],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub(crate) fn insert(&self, conversation: &str, session: &SessionId) -> Result<()> {
        self.0.execute(
            "INSERT INTO mappings (conversation, session_id) VALUES (?1, ?2)",
            params![conversation, session.as_str()],
        )?;
        Ok(())
    }

    pub(crate) fn mappings(&self) -> Result<Vec<(String, SessionId)>> {
        let mut statement = self
            .0
            .prepare("SELECT conversation, session_id FROM mappings")?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|(conversation, session)| {
                Ok((conversation, session.parse().map_err(anyhow::Error::msg)?))
            })
            .collect()
    }

    pub(crate) fn delivered(&self, event_id: &str) -> Result<bool> {
        Ok(self.0.query_row(
            "SELECT EXISTS(SELECT 1 FROM deliveries WHERE event_id = ?1)",
            [event_id],
            |row| row.get(0),
        )?)
    }

    pub(crate) fn record_delivery(&self, event_id: &str, external_id: Option<&str>) -> Result<()> {
        self.0.execute(
            "INSERT OR IGNORE INTO deliveries (event_id, external_id) VALUES (?1, ?2)",
            params![event_id, external_id],
        )?;
        Ok(())
    }

    pub(crate) fn enqueue(
        &self,
        external_id: &str,
        conversation: &str,
        text: &str,
        metadata: &Value,
    ) -> Result<()> {
        self.0.execute(
            "INSERT OR IGNORE INTO pending (external_id, conversation, text, metadata_json) VALUES (?1, ?2, ?3, ?4)",
            params![external_id, conversation, text, serde_json::to_string(metadata)?],
        )?;
        Ok(())
    }

    pub(crate) fn active(&self, session: &SessionId) -> Result<bool> {
        Ok(self.0.query_row(
            "SELECT EXISTS(SELECT 1 FROM active_runs WHERE session_id = ?1)",
            [session.as_str()],
            |row| row.get(0),
        )?)
    }

    pub(crate) fn next(&self, session: &SessionId) -> Result<Option<(i64, String, String, Value)>> {
        let Some(conversation) = self.conversation(session)? else {
            return Ok(None);
        };
        let row = self.0.query_row(
            "SELECT id, external_id, text, metadata_json FROM pending WHERE conversation = ?1 ORDER BY id LIMIT 1",
            [conversation], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, String>(3)?)),
        ).optional()?;
        row.map(|(id, external_id, text, metadata)| {
            Ok((id, external_id, text, serde_json::from_str(&metadata)?))
        })
        .transpose()
    }

    pub(crate) fn mark_started(
        &mut self,
        pending_id: i64,
        session: &SessionId,
        run_id: &str,
    ) -> Result<()> {
        let transaction = self.0.transaction()?;
        transaction.execute(
            "INSERT INTO active_runs (session_id, run_id) VALUES (?1, ?2)",
            params![session.as_str(), run_id],
        )?;
        transaction.execute("DELETE FROM pending WHERE id = ?1", [pending_id])?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn finish_run(&mut self, run_id: &str) -> Result<Option<SessionId>> {
        let session = self
            .0
            .query_row(
                "SELECT session_id FROM active_runs WHERE run_id = ?1",
                [run_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        self.0
            .execute("DELETE FROM active_runs WHERE run_id = ?1", [run_id])?;
        session
            .map(|value| value.parse().map_err(anyhow::Error::msg))
            .transpose()
    }

    pub(crate) fn active_runs(&self) -> Result<Vec<(SessionId, String)>> {
        let mut statement = self
            .0
            .prepare("SELECT session_id, run_id FROM active_runs")?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|(session, run)| Ok((session.parse().map_err(anyhow::Error::msg)?, run)))
            .collect()
    }

    pub(crate) fn dead_letter(
        &self,
        event_id: &str,
        conversation: &str,
        text: &str,
        error: &str,
    ) -> Result<()> {
        self.0.execute(
            "INSERT OR REPLACE INTO dead_letters (event_id, conversation, text, error) VALUES (?1, ?2, ?3, ?4)",
            params![event_id, conversation, text, error],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapping_store_persists_queue_and_delivery_state() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("channel.db");
        let mut store = MappingStore::open(&path).unwrap();
        assert!(MappingStore::open(&root.path().join("channel.db")).is_err());
        let session = SessionId::new();
        store.insert("chat:thread", &session).unwrap();
        store
            .enqueue(
                "update-1",
                "chat:thread",
                "hello",
                &serde_json::json!({"x":1}),
            )
            .unwrap();
        store
            .enqueue(
                "update-1",
                "chat:thread",
                "duplicate",
                &serde_json::Value::Null,
            )
            .unwrap();
        let (pending, _, text, _) = store.next(&session).unwrap().unwrap();
        assert_eq!(text, "hello");
        store.mark_started(pending, &session, "run-1").unwrap();
        store
            .dead_letter("event-failed", "chat:thread", "answer", "offline")
            .unwrap();
        store.record_delivery("event-1", Some("message-1")).unwrap();
        assert!(store.delivered("event-1").unwrap());
        drop(store);

        let mut reopened = MappingStore::open(&path).unwrap();
        assert!(reopened.active(&session).unwrap());
        assert_eq!(reopened.next(&session).unwrap(), None);
        assert_eq!(
            reopened
                .0
                .query_row("SELECT COUNT(*) FROM dead_letters", [], |row| row
                    .get::<_, u64>(0))
                .unwrap(),
            1
        );
        assert_eq!(reopened.finish_run("run-1").unwrap(), Some(session));
    }

    #[test]
    fn external_message_ids_are_scoped_to_the_conversation() {
        let root = tempfile::tempdir().unwrap();
        let store = MappingStore::open(&root.path().join("channel.db")).unwrap();
        let first = SessionId::new();
        let second = SessionId::new();
        store.insert("first", &first).unwrap();
        store.insert("second", &second).unwrap();
        store
            .enqueue("message-1", "first", "one", &Value::Null)
            .unwrap();
        store
            .enqueue("message-1", "second", "two", &Value::Null)
            .unwrap();

        assert_eq!(store.next(&first).unwrap().unwrap().2, "one");
        assert_eq!(store.next(&second).unwrap().unwrap().2, "two");
    }
}
