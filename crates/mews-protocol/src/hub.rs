use std::{collections::BTreeMap, path::PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    Agent, AuthCredential, AuthStatus, ConsumerId, ConsumerKind, EventBatch, HostHarnessStatus,
    HostId, HostStatus, Installation, MessageSource, ModelInfo, PermissionOutcome,
    ProviderDefaults, ReasoningEffort, Run, RunId, Session, SessionId, SessionModelConfig,
};

pub const PROTOCOL_VERSION: u32 = 1;
pub const MAX_HUB_FRAME_BYTES: usize = 1024 * 1024;

/// A stable, machine-readable error returned by the Hub.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolError {
    pub code: ProtocolErrorCode,
    pub message: String,
    #[serde(default)]
    pub retryable: bool,
}

impl ProtocolError {
    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            code: ProtocolErrorCode::Internal,
            message: message.into(),
            retryable: false,
        }
    }

    pub fn unsupported_version(version: u32) -> Self {
        Self {
            code: ProtocolErrorCode::UnsupportedVersion,
            message: format!(
                "protocol version {version} is incompatible with version {PROTOCOL_VERSION}; restart the MEWS daemon"
            ),
            retryable: false,
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self {
            code: ProtocolErrorCode::Unavailable,
            message: message.into(),
            retryable: true,
        }
    }
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(formatter)
    }
}

impl std::error::Error for ProtocolError {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProtocolErrorCode {
    InvalidRequest,
    NotFound,
    Conflict,
    Unauthorized,
    Unavailable,
    UnsupportedVersion,
    Internal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HubRequest {
    Status,
    ListAgents,
    CreateAgent {
        slug: String,
        /// Absent selects the native Harness. The empty string is deliberately
        /// invalid rather than a compatibility alias.
        #[serde(default)]
        harness: Option<String>,
        /// Harness-owned opaque values. The Hub only validates the portable
        /// map shape; the selected Harness validates its own option IDs.
        #[serde(default)]
        harness_options: BTreeMap<String, String>,
    },
    RenameAgent {
        slug: String,
        new_slug: String,
    },
    ArchiveAgent {
        slug: String,
    },
    SetAuth {
        provider: String,
        credential: AuthCredential,
    },
    RemoveAuth {
        provider: String,
    },
    ListAuth,
    ListModels,
    RefreshModels,
    GetProviderDefaults,
    SetDefaultModel {
        model: String,
    },
    SetDefaultReasoning {
        reasoning: Option<ReasoningEffort>,
    },
    ListSessions,
    GetSession {
        id: SessionId,
    },
    GetSessionModelConfig {
        id: SessionId,
    },
    SetSessionModel {
        id: SessionId,
        model: Option<String>,
    },
    StartSession {
        slug: String,
        working_directory: Option<PathBuf>,
    },
    StartSessionOn {
        slug: String,
        host_id: HostId,
        working_directory: PathBuf,
    },
    StartTurn {
        idempotency_key: String,
        session_id: SessionId,
        prompt: String,
        metadata: Value,
        #[serde(default)]
        source: Option<MessageSource>,
    },
    GetRun {
        id: RunId,
    },
    CancelRun {
        id: RunId,
    },
    ResolvePermission {
        request_id: String,
        outcome: PermissionOutcome,
    },
    SubscribeSession {
        consumer_id: ConsumerId,
        session_id: SessionId,
        #[serde(default)]
        consumer_kind: ConsumerKind,
    },
    UnsubscribeSession {
        consumer_id: ConsumerId,
        session_id: SessionId,
    },
    DeleteConsumer {
        consumer_id: ConsumerId,
    },
    PollEvents {
        consumer_id: ConsumerId,
        #[serde(default = "default_event_limit")]
        limit: u16,
        #[serde(default)]
        wait_ms: u32,
    },
    AcknowledgeEvents {
        consumer_id: ConsumerId,
        checkpoint: u64,
    },
    ListHosts,
    ListHarnesses,
    RefreshHarnesses,
    RemoveHost {
        id: HostId,
    },
    CreateHostInvitation {
        relay_url: Option<String>,
    },
    MoveHub {
        host: String,
    },
    Shutdown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum HubResponse {
    Status(Installation),
    Agents(Vec<Agent>),
    Agent(Agent),
    Sessions(Vec<Session>),
    Session(Session),
    SessionModelConfig(SessionModelConfig),
    Run(Run),
    Events(EventBatch),
    Hosts(Vec<HostStatus>),
    Harnesses(Vec<HostHarnessStatus>),
    Auth(Vec<AuthStatus>),
    Models(Vec<ModelInfo>),
    ProviderDefaults(ProviderDefaults),
    HostInvitation(String),
    Ack,
    Error(ProtocolError),
}

fn default_event_limit() -> u16 {
    100
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Frame<T> {
    pub protocol: u32,
    pub request_id: crate::RequestId,
    pub body: T,
}

impl<T> Frame<T> {
    pub fn with_request_id(body: T, request_id: crate::RequestId) -> Self {
        Self {
            protocol: PROTOCOL_VERSION,
            request_id,
            body,
        }
    }
}

pub fn encode_hub_frame<T: Serialize>(frame: &Frame<T>) -> anyhow::Result<Vec<u8>> {
    let encoded = serde_json::to_vec(frame)?;
    if encoded.len() > MAX_HUB_FRAME_BYTES {
        anyhow::bail!("Hub protocol frame exceeds 1 MiB");
    }
    Ok(encoded)
}

pub fn decode_hub_frame<T: serde::de::DeserializeOwned>(
    encoded: &[u8],
) -> anyhow::Result<Frame<T>> {
    if encoded.len() > MAX_HUB_FRAME_BYTES {
        anyhow::bail!("Hub protocol frame exceeds 1 MiB");
    }
    Ok(serde_json::from_slice(encoded)?)
}

/// Decode a version-independent frame envelope before interpreting its body.
pub fn decode_hub_envelope(encoded: &[u8]) -> anyhow::Result<Frame<Value>> {
    decode_hub_frame(encoded)
}

pub fn decode_hub_body<T: serde::de::DeserializeOwned>(
    frame: Frame<Value>,
) -> anyhow::Result<Frame<T>> {
    Ok(Frame {
        protocol: frame.protocol,
        request_id: frame.request_id,
        body: serde_json::from_value(frame.body)?,
    })
}

pub fn validate_hub_version<T>(frame: &Frame<T>) -> Result<(), ProtocolError> {
    if frame.protocol == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ProtocolError::unsupported_version(frame.protocol))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_request_has_stable_json() {
        let id: crate::RequestId = "req_0198f73b-9c31-7c01-8000-000000000000".parse().unwrap();
        let encoded = encode_hub_frame(&Frame::with_request_id(HubRequest::Status, id)).unwrap();
        assert_eq!(
            String::from_utf8(encoded).unwrap(),
            r#"{"protocol":1,"request_id":"req_0198f73b-9c31-7c01-8000-000000000000","body":{"type":"status"}}"#
        );
    }

    #[test]
    fn structured_error_has_stable_json() {
        let encoded =
            serde_json::to_string(&HubResponse::Error(ProtocolError::unsupported_version(2)))
                .unwrap();
        assert_eq!(
            encoded,
            r#"{"type":"error","data":{"code":"unsupported_version","message":"protocol version 2 is incompatible with version 1; restart the MEWS daemon","retryable":false}}"#
        );
    }

    #[test]
    fn malformed_and_oversized_hub_frames_are_rejected() {
        assert!(decode_hub_frame::<HubRequest>(b"not-json").is_err());
        let oversized = vec![b' '; MAX_HUB_FRAME_BYTES + 1];
        assert!(decode_hub_frame::<HubRequest>(&oversized).is_err());
    }

    #[test]
    fn incompatible_versions_are_typed_errors() {
        let frame = Frame {
            protocol: 2,
            request_id: crate::RequestId::new(),
            body: HubRequest::Status,
        };
        let error = validate_hub_version(&frame).unwrap_err();
        assert_eq!(error.code, ProtocolErrorCode::UnsupportedVersion);
    }

    #[test]
    fn envelope_decodes_before_an_unknown_body() {
        let encoded = br#"{"protocol":999,"request_id":"req_0198f73b-9c31-7c01-8000-000000000000","body":{"type":"future_request"}}"#;
        let envelope = decode_hub_envelope(encoded).unwrap();
        let error = validate_hub_version(&envelope).unwrap_err();
        assert_eq!(error.code, ProtocolErrorCode::UnsupportedVersion);
    }
}
