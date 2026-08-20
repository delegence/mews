use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};

use anyhow::{Context, Result, bail};
use fs2::FileExt;
use mews_protocol::{ConsumerId, SessionId};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

pub(crate) struct MappingStore(Connection, std::fs::File);

impl MappingStore {
    pub(crate) fn open(directory: &Path) -> Result<Self> {
        create_private_directory(directory)?;
        let directory = directory
            .canonicalize()
            .with_context(|| format!("resolve Channel state directory {}", directory.display()))?;
        let path = directory.join("channel.db");
        let lock_path = directory.join("channel.lock");
        let _database_file = create_private_file(&path)?;
        let lock = create_private_file(&lock_path)?;
        secure_file_if_present(&directory.join("channel.db-wal"))?;
        secure_file_if_present(&directory.join("channel.db-shm"))?;
        lock.try_lock_exclusive()
            .context("another Channel process owns this state")?;
        let connection = Connection::open_with_flags(
            &path,
            rusqlite::OpenFlags::default() | rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )?;
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS mappings (
                 conversation TEXT PRIMARY KEY,
                 session_id TEXT NOT NULL UNIQUE
             );
             CREATE TABLE IF NOT EXISTS pending (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 external_id TEXT NOT NULL,
                 conversation TEXT NOT NULL,
                 text TEXT NOT NULL,
                 metadata_json TEXT NOT NULL,
                 UNIQUE (conversation, external_id)
             );
             CREATE TABLE IF NOT EXISTS active_turns (
                 session_id TEXT PRIMARY KEY,
                 turn_id TEXT NOT NULL UNIQUE
             );",
        )?;
        secure_file_if_present(&directory.join("channel.db-wal"))?;
        secure_file_if_present(&directory.join("channel.db-shm"))?;
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
            "SELECT EXISTS(SELECT 1 FROM active_turns WHERE session_id = ?1)",
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
        turn_id: &str,
    ) -> Result<()> {
        let transaction = self.0.transaction()?;
        transaction.execute(
            "INSERT INTO active_turns (session_id, turn_id) VALUES (?1, ?2)",
            params![session.as_str(), turn_id],
        )?;
        transaction.execute("DELETE FROM pending WHERE id = ?1", [pending_id])?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn discard_pending(&self, pending_id: i64) -> Result<()> {
        self.0
            .execute("DELETE FROM pending WHERE id = ?1", [pending_id])?;
        Ok(())
    }

    pub(crate) fn finish_turn(&mut self, turn_id: &str) -> Result<Option<SessionId>> {
        let session = self
            .0
            .query_row(
                "SELECT session_id FROM active_turns WHERE turn_id = ?1",
                [turn_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        self.0
            .execute("DELETE FROM active_turns WHERE turn_id = ?1", [turn_id])?;
        session
            .map(|value| value.parse().map_err(anyhow::Error::msg))
            .transpose()
    }

    pub(crate) fn active_turns(&self) -> Result<Vec<(SessionId, String)>> {
        let mut statement = self
            .0
            .prepare("SELECT session_id, turn_id FROM active_turns")?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter()
            .map(|(session, turn)| Ok((session.parse().map_err(anyhow::Error::msg)?, turn)))
            .collect()
    }
}

fn create_private_directory(path: &Path) -> Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    builder.mode(0o700);
    builder.create(path)?;

    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("inspect Channel state directory {}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!(
            "Channel state path {} must be a directory, not a symbolic link",
            path.display()
        );
    }
    #[cfg(unix)]
    {
        let directory = std::fs::File::open(path)?;
        ensure_same_file(path, &metadata, &directory.metadata()?)?;
        directory.set_permissions(std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn create_private_file(path: &Path) -> Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options
        .create_new(true)
        .truncate(false)
        .read(true)
        .write(true);
    #[cfg(unix)]
    options.mode(0o600);
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            std::fs::OpenOptions::new()
                .truncate(false)
                .read(true)
                .write(true)
                .open(path)?
        }
        Err(error) => return Err(error.into()),
    };
    secure_open_file(path, file)
}

fn secure_file_if_present(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => {
            let file = std::fs::OpenOptions::new()
                .truncate(false)
                .read(true)
                .write(true)
                .open(path)?;
            secure_open_file(path, file)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn secure_open_file(path: &Path, file: std::fs::File) -> Result<std::fs::File> {
    let path_metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("inspect Channel state file {}", path.display()))?;
    let file_metadata = file.metadata()?;
    if path_metadata.file_type().is_symlink()
        || !path_metadata.is_file()
        || !file_metadata.is_file()
    {
        bail!(
            "Channel state path {} must be a regular file, not a symbolic link",
            path.display()
        );
    }
    #[cfg(unix)]
    {
        ensure_same_file(path, &path_metadata, &file_metadata)?;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

#[cfg(unix)]
fn ensure_same_file(
    path: &Path,
    path_metadata: &std::fs::Metadata,
    file_metadata: &std::fs::Metadata,
) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    if path_metadata.dev() != file_metadata.dev() || path_metadata.ino() != file_metadata.ino() {
        bail!(
            "Channel state path {} changed while opening",
            path.display()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapping_store_persists_queue_and_delivery_state() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("channel-state");
        let mut store = MappingStore::open(&path).unwrap();
        assert!(MappingStore::open(&path).is_err());
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
        store.mark_started(pending, &session, "turn-1").unwrap();
        drop(store);

        let mut reopened = MappingStore::open(&path).unwrap();
        assert!(reopened.active(&session).unwrap());
        assert_eq!(reopened.next(&session).unwrap(), None);
        assert_eq!(reopened.finish_turn("turn-1").unwrap(), Some(session));
    }

    #[test]
    fn external_message_ids_are_scoped_to_the_conversation() {
        let root = tempfile::tempdir().unwrap();
        let store = MappingStore::open(&root.path().join("channel-state")).unwrap();
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

    #[test]
    fn discarding_one_pending_row_preserves_fifo_progress() {
        let root = tempfile::tempdir().unwrap();
        let store = MappingStore::open(&root.path().join("channel-state")).unwrap();
        let session = SessionId::new();
        store.insert("first", &session).unwrap();
        store
            .enqueue("invalid", "first", " ", &Value::Null)
            .unwrap();
        store
            .enqueue("valid", "first", "hello", &Value::Null)
            .unwrap();

        let invalid = store.next(&session).unwrap().unwrap();
        store.discard_pending(invalid.0).unwrap();

        let valid = store.next(&session).unwrap().unwrap();
        assert_eq!(valid.1, "valid");
        assert_eq!(valid.2, "hello");
    }

    #[cfg(unix)]
    #[test]
    fn state_directory_and_files_are_owner_only() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("channel-state");
        std::fs::create_dir(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o755)).unwrap();
        for name in ["channel.db", "channel.lock"] {
            let path = directory.join(name);
            std::fs::write(&path, []).unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o644)).unwrap();
        }

        let _store = MappingStore::open(&directory).unwrap();

        assert_eq!(
            std::fs::metadata(&directory).unwrap().permissions().mode() & 0o777,
            0o700
        );
        for name in [
            "channel.db",
            "channel.lock",
            "channel.db-wal",
            "channel.db-shm",
        ] {
            let path = directory.join(name);
            assert!(path.exists(), "{} was not created", path.display());
            assert_eq!(
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn state_directory_symbolic_link_is_rejected_without_touching_target() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("target");
        std::fs::create_dir(&target).unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755)).unwrap();
        let state = root.path().join("channel-state");
        symlink(&target, &state).unwrap();

        assert!(MappingStore::open(&state).is_err());
        assert!(!target.join("channel.db").exists());
        assert_eq!(
            std::fs::metadata(target).unwrap().permissions().mode() & 0o777,
            0o755
        );
    }

    #[cfg(unix)]
    #[test]
    fn state_file_symbolic_links_are_rejected_without_touching_targets() {
        use std::os::unix::fs::symlink;

        for name in ["channel.db", "channel.lock"] {
            let root = tempfile::tempdir().unwrap();
            let state = root.path().join("channel-state");
            std::fs::create_dir(&state).unwrap();
            let target = root.path().join("target");
            std::fs::write(&target, b"unchanged").unwrap();
            std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644)).unwrap();
            symlink(&target, state.join(name)).unwrap();

            assert!(MappingStore::open(&state).is_err(), "accepted {name}");
            assert_eq!(std::fs::read(&target).unwrap(), b"unchanged");
            assert_eq!(
                std::fs::metadata(target).unwrap().permissions().mode() & 0o777,
                0o644
            );
        }
    }
}
