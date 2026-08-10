//! SQLite persistence for the MEWS Hub.
//!
//! This crate owns the durable schema and transactional invariants. Hub lifecycle,
//! filesystem layout, networking, and runtime execution remain in the `mews` crate.

use std::{
    fs::{File, OpenOptions},
    path::{Component, Path},
    str::FromStr,
    time::Duration,
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use fs2::FileExt;
use rand_core::{OsRng, RngCore};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use mews_protocol::{
    AcpBindingTransition, AcpContextSnapshot, AcpInstructionChannel, AcpObservation,
    AcpSessionBinding, Agent, AgentConfig, AgentId, AgentRevision, AssistantResponse, ClientEvent,
    ClientEventKind, ConsumerId, ConsumerKind, EventBatch, EventId, HarnessProvenance, Host,
    HostId, Installation, InstallationId, InvitationId, Message, MessageContent, MessageId,
    MessageRole, MessageSource, ProviderDefaults, ReasoningEffort, ReasoningProvenance,
    ReasoningVisibility, Run, RunId, RunStatus, Session, SessionEntry, SessionEntryPayload,
    SessionId, SourceKind, ToolCall, ToolResult,
};

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("invalid stored data: {0}")]
    InvalidData(String),
    #[error("{kind} not found: {id}")]
    NotFound { kind: &'static str, id: String },
    #[error("agent slug already exists: {0}")]
    DuplicateAgent(String),
    #[error("agent revision conflict: expected {expected}, current {current}")]
    RevisionConflict { expected: u64, current: u64 },
    #[error("session leaf conflict: expected {expected:?}, current {current:?}")]
    LeafConflict {
        expected: Option<MessageId>,
        current: Option<MessageId>,
    },
    #[error("invalid agent definition: {0}")]
    InvalidAgent(String),
}

pub struct Store {
    connection: Connection,
    _hub_lock: Option<File>,
}

mod agents;
mod events;
mod hosts;
mod installation;
mod runs;
mod schema;
mod sessions;
mod values;

#[cfg(test)]
mod tests;

use values::*;
