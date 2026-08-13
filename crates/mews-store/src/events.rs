use std::collections::HashSet;

use chrono::{DateTime, Utc};
use mews_protocol::{
    EventActor, EventActorKind, EventId, JournalEntry, JournalEvent, JournalSubject,
    JournalSubjectType, RequestId, SessionId,
};
use rusqlite::{OptionalExtension, Transaction, params};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::*;

/// Provenance and retry identity for one application command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandContext {
    pub command_id: String,
    pub actor: EventActor,
    pub correlation_id: Option<String>,
}

impl CommandContext {
    pub fn new(command_id: impl Into<String>, actor: EventActor) -> Self {
        Self {
            command_id: command_id.into(),
            actor,
            correlation_id: None,
        }
    }

    pub fn system() -> Self {
        Self::new(RequestId::new().to_string(), EventActor::system())
    }

    pub fn operation_id(&self, operation: &str) -> String {
        format!("{operation}:{}", self.command_id)
    }

    pub fn decorate(&self, append: &mut JournalAppend) {
        for entry in append
            .subjects
            .iter_mut()
            .flat_map(|subject| &mut subject.entries)
        {
            entry.actor = self.actor.clone();
            entry.correlation_id = self.correlation_id.clone();
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct NewJournalEntry {
    pub id: EventId,
    pub actor: EventActor,
    pub correlation_id: Option<String>,
    pub payload: JournalEvent,
}

impl NewJournalEntry {
    pub fn new(actor: EventActor, payload: JournalEvent) -> Self {
        Self {
            id: EventId::new(),
            actor,
            correlation_id: None,
            payload,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct JournalSubjectAppend {
    pub subject_type: JournalSubjectType,
    pub subject_id: String,
    pub entries: Vec<NewJournalEntry>,
}

/// State mutations, journal entries, and the retry receipt commit together.
#[derive(Clone, Debug, PartialEq)]
pub struct JournalAppend {
    pub command_id: String,
    pub request_hash: String,
    pub result: Value,
    pub subjects: Vec<JournalSubjectAppend>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CommandReceipt {
    pub command_id: String,
    pub request_hash: String,
    pub result: Value,
    pub first_position: Option<u64>,
    pub last_position: Option<u64>,
    pub completed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AppendJournalEntriesOutcome {
    Appended {
        entries: Vec<JournalEntry>,
        receipt: CommandReceipt,
    },
    Replayed(CommandReceipt),
}

impl AppendJournalEntriesOutcome {
    pub fn receipt(&self) -> &CommandReceipt {
        match self {
            Self::Appended { receipt, .. } | Self::Replayed(receipt) => receipt,
        }
    }

    pub fn was_replayed(&self) -> bool {
        matches!(self, Self::Replayed(_))
    }
}

pub fn command_request_hash(request: &impl Serialize) -> Result<String, StoreError> {
    let bytes =
        serde_json::to_vec(request).map_err(|error| StoreError::InvalidData(error.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

impl Store {
    pub fn append_journal_entries_with<F>(
        &self,
        append: &JournalAppend,
        mutate: F,
    ) -> Result<AppendJournalEntriesOutcome, StoreError>
    where
        F: FnOnce(&Transaction<'_>, &[JournalEntry]) -> Result<(), StoreError>,
    {
        let transaction =
            Transaction::new_unchecked(&self.connection, rusqlite::TransactionBehavior::Immediate)?;
        let outcome = append_journal_entries_in(&transaction, append, mutate)?;
        transaction.commit()?;
        Ok(outcome)
    }

    /// Runs an idempotent command under one reserved SQLite write transaction.
    /// The decision reads authoritative state only after retry replay is ruled out.
    pub(crate) fn transact_command<T, D, M>(
        &self,
        context: &CommandContext,
        operation: &str,
        request_hash: String,
        decide: D,
        mutate: M,
    ) -> Result<(T, bool), StoreError>
    where
        T: Serialize + DeserializeOwned,
        D: FnOnce(&Transaction<'_>) -> Result<(T, Vec<JournalSubjectAppend>), StoreError>,
        M: FnOnce(&Transaction<'_>, &[JournalEntry]) -> Result<(), StoreError>,
    {
        let command_id = context.operation_id(operation);
        let transaction =
            Transaction::new_unchecked(&self.connection, rusqlite::TransactionBehavior::Immediate)?;
        if let Some(receipt) = select_command_receipt(&transaction, &command_id)? {
            if receipt.request_hash != request_hash {
                return Err(StoreError::CommandConflict { command_id });
            }
            let result = serde_json::from_value(receipt.result)
                .map_err(|error| StoreError::InvalidData(error.to_string()))?;
            return Ok((result, true));
        }

        let (result, subjects) = decide(&transaction)?;
        let mut append = JournalAppend {
            command_id,
            request_hash,
            result: serde_json::to_value(&result)
                .map_err(|error| StoreError::InvalidData(error.to_string()))?,
            subjects,
        };
        context.decorate(&mut append);
        append_journal_entries_in(&transaction, &append, mutate)?;
        transaction.commit()?;
        Ok((result, false))
    }

    pub fn command_receipt(&self, command_id: &str) -> Result<Option<CommandReceipt>, StoreError> {
        select_command_receipt(&self.connection, command_id)
    }

    pub fn journal_entries_after(
        &self,
        position: u64,
        limit: usize,
    ) -> Result<Vec<JournalEntry>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut statement = self.connection.prepare(
            "SELECT position, id, subject_type, subject_id, event_type, recorded_at,
                    actor_kind, actor_id, command_id, correlation_id, payload_json
             FROM journal_entries
             WHERE position > ?1
             ORDER BY position
             LIMIT ?2",
        )?;
        statement
            .query_map(params![position, limit as u64], entry_from_row)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    pub fn journal_entries_for_subject(
        &self,
        subject_type: JournalSubjectType,
        subject_id: &str,
        after: u64,
    ) -> Result<Vec<JournalEntry>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT position, id, subject_type, subject_id, event_type, recorded_at,
                    actor_kind, actor_id, command_id, correlation_id, payload_json
             FROM journal_entries
             WHERE subject_type = ?1 AND subject_id = ?2 AND position > ?3
             ORDER BY position",
        )?;
        statement
            .query_map(
                params![subject_type.as_str(), subject_id, after],
                entry_from_row,
            )?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }
}

pub(crate) fn append_journal_entries_in<F>(
    transaction: &Transaction<'_>,
    append: &JournalAppend,
    mutate: F,
) -> Result<AppendJournalEntriesOutcome, StoreError>
where
    F: FnOnce(&Transaction<'_>, &[JournalEntry]) -> Result<(), StoreError>,
{
    validate_append(append)?;
    if let Some(receipt) = select_command_receipt(transaction, &append.command_id)? {
        if receipt.request_hash != append.request_hash {
            return Err(StoreError::CommandConflict {
                command_id: append.command_id.clone(),
            });
        }
        return Ok(AppendJournalEntriesOutcome::Replayed(receipt));
    }

    let recorded_at = Utc::now();
    let mut committed = Vec::new();
    for subject in &append.subjects {
        for entry in &subject.entries {
            committed.push(insert_entry(
                transaction,
                Some(&append.command_id),
                subject.subject_type,
                &subject.subject_id,
                recorded_at,
                entry,
            )?);
        }
    }

    mutate(transaction, &committed)?;

    let first_position = committed.first().map(|entry| entry.position);
    let last_position = committed.last().map(|entry| entry.position);
    transaction.execute(
        "INSERT INTO command_receipts
         (command_id, request_hash, result_json, first_position, last_position, completed_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            append.command_id,
            append.request_hash,
            json(&append.result)?,
            first_position,
            last_position,
            timestamp(recorded_at),
        ],
    )?;
    let receipt = CommandReceipt {
        command_id: append.command_id.clone(),
        request_hash: append.request_hash.clone(),
        result: append.result.clone(),
        first_position,
        last_position,
        completed_at: recorded_at,
    };
    Ok(AppendJournalEntriesOutcome::Appended {
        entries: committed,
        receipt,
    })
}

/// Adds a journal entry inside an existing authoritative-state transaction.
pub(crate) fn record_journal_entry(
    transaction: &Transaction<'_>,
    subject_type: JournalSubjectType,
    subject_id: &str,
    mut entry: NewJournalEntry,
) -> Result<JournalEntry, StoreError> {
    if entry.correlation_id.is_none() {
        entry.correlation_id = payload_turn_id(&entry.payload).map(ToString::to_string);
    }
    validate_new_entry(subject_type, subject_id, &entry)?;
    insert_entry(
        transaction,
        None,
        subject_type,
        subject_id,
        Utc::now(),
        &entry,
    )
}

fn validate_append(append: &JournalAppend) -> Result<(), StoreError> {
    validate_non_empty("command ID", &append.command_id)?;
    if append.request_hash.len() != 64
        || !append
            .request_hash
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(StoreError::InvalidData(
            "command request hash must be a 64-character SHA-256 hex digest".into(),
        ));
    }
    let mut subjects = HashSet::new();
    let mut ids = HashSet::new();
    for subject in &append.subjects {
        validate_non_empty("journal subject ID", &subject.subject_id)?;
        if !subjects.insert((subject.subject_type, subject.subject_id.as_str())) {
            return Err(StoreError::InvalidData(format!(
                "journal subject appears more than once in one append: {}",
                subject.subject_id
            )));
        }
        for entry in &subject.entries {
            if !ids.insert(entry.id.as_str()) {
                return Err(StoreError::InvalidData(
                    "journal entry ID appears more than once in one append".into(),
                ));
            }
            validate_new_entry(subject.subject_type, &subject.subject_id, entry)?;
        }
    }
    Ok(())
}

fn validate_non_empty(label: &str, value: &str) -> Result<(), StoreError> {
    if value.trim().is_empty() {
        return Err(StoreError::InvalidData(format!("{label} cannot be empty")));
    }
    Ok(())
}

fn validate_new_entry(
    subject_type: JournalSubjectType,
    subject_id: &str,
    entry: &NewJournalEntry,
) -> Result<(), StoreError> {
    if entry.payload.subject_type() != subject_type {
        return Err(StoreError::InvalidData(format!(
            "event {} belongs to a {} subject, not {}",
            entry.payload.event_type(),
            entry.payload.subject_type(),
            subject_type
        )));
    }
    entry
        .payload
        .validate_subject_id(subject_id)
        .map_err(|error| StoreError::InvalidData(error.into()))?;
    Ok(())
}

fn insert_entry(
    transaction: &Transaction<'_>,
    command_id: Option<&str>,
    subject_type: JournalSubjectType,
    subject_id: &str,
    recorded_at: DateTime<Utc>,
    entry: &NewJournalEntry,
) -> Result<JournalEntry, StoreError> {
    let event_type = entry.payload.event_type();
    transaction.execute(
        "INSERT INTO journal_entries
         (id, subject_type, subject_id, event_type, recorded_at, actor_kind, actor_id,
          command_id, correlation_id, payload_json)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            entry.id.as_str(),
            subject_type.as_str(),
            subject_id,
            event_type.as_str(),
            timestamp(recorded_at),
            entry.actor.kind.as_str(),
            entry.actor.id,
            command_id,
            entry.correlation_id,
            json(&entry.payload)?,
        ],
    )?;
    let committed = JournalEntry {
        id: entry.id.clone(),
        position: transaction.last_insert_rowid() as u64,
        subject: JournalSubject {
            kind: subject_type,
            id: subject_id.to_owned(),
        },
        event_type,
        recorded_at,
        actor: entry.actor.clone(),
        command_id: command_id.map(str::to_owned),
        correlation_id: entry.correlation_id.clone(),
        payload: entry.payload.clone(),
    };
    committed
        .validate()
        .map_err(|error| StoreError::InvalidData(error.into()))?;
    Ok(committed)
}

fn select_command_receipt(
    connection: &rusqlite::Connection,
    command_id: &str,
) -> Result<Option<CommandReceipt>, StoreError> {
    connection
        .query_row(
            "SELECT request_hash, result_json, first_position, last_position, completed_at
             FROM command_receipts WHERE command_id = ?1",
            [command_id],
            |row| {
                Ok(CommandReceipt {
                    command_id: command_id.to_owned(),
                    request_hash: row.get(0)?,
                    result: parse_json(row.get::<_, String>(1)?)?,
                    first_position: row.get(2)?,
                    last_position: row.get(3)?,
                    completed_at: parse_time(row.get::<_, String>(4)?)?,
                })
            },
        )
        .optional()
        .map_err(StoreError::from)
}

fn entry_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<JournalEntry> {
    let payload: JournalEvent = parse_json(row.get::<_, String>(10)?)?;
    let entry = JournalEntry {
        id: parse_id(row.get::<_, String>(1)?)?,
        position: row.get(0)?,
        subject: JournalSubject {
            kind: row.get::<_, String>(2)?.parse().map_err(conversion)?,
            id: row.get(3)?,
        },
        event_type: row.get::<_, String>(4)?.parse().map_err(conversion)?,
        recorded_at: parse_time(row.get::<_, String>(5)?)?,
        actor: EventActor {
            kind: row
                .get::<_, String>(6)?
                .parse::<EventActorKind>()
                .map_err(conversion)?,
            id: row.get(7)?,
        },
        command_id: row.get(8)?,
        correlation_id: row.get(9)?,
        payload,
    };
    entry
        .validate()
        .map_err(|error| conversion(error.to_owned()))?;
    Ok(entry)
}

pub(crate) fn journal_session_id(entry: &JournalEntry) -> Result<SessionId, StoreError> {
    if entry.subject.kind != JournalSubjectType::Session {
        return Err(StoreError::InvalidData(
            "journal entry does not belong to a Session".into(),
        ));
    }
    entry
        .subject
        .id
        .parse::<SessionId>()
        .map_err(|error| StoreError::InvalidData(error.to_string()))
}

fn payload_turn_id(payload: &JournalEvent) -> Option<&TurnId> {
    match payload {
        JournalEvent::TurnAccepted { turn_id, .. }
        | JournalEvent::TurnStarted { turn_id, .. }
        | JournalEvent::AssistantResponseRecorded { turn_id, .. }
        | JournalEvent::ToolCallRequested { turn_id, .. }
        | JournalEvent::ToolExecutionCompleted { turn_id, .. }
        | JournalEvent::ToolResultRecorded { turn_id, .. }
        | JournalEvent::ReasoningRecorded { turn_id, .. }
        | JournalEvent::HarnessObservationRecorded { turn_id, .. }
        | JournalEvent::TurnCompleted { turn_id, .. }
        | JournalEvent::TurnFailed { turn_id, .. }
        | JournalEvent::TurnCancelled { turn_id }
        | JournalEvent::TurnInterrupted { turn_id, .. }
        | JournalEvent::EffectScheduled { turn_id, .. }
        | JournalEvent::EffectStarted { turn_id, .. }
        | JournalEvent::EffectSucceeded { turn_id, .. }
        | JournalEvent::EffectFailed { turn_id, .. }
        | JournalEvent::EffectUncertain { turn_id, .. } => Some(turn_id),
        _ => None,
    }
}

fn conversion(error: impl Into<String>) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(StoreError::InvalidData(error.into())),
    )
}
