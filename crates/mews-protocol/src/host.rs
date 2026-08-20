use std::{collections::BTreeMap, path::PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    Agent, AgentId, AgentRevision, HarnessDescriptor, HostId, HubRequest, HubResponse,
    InstallationId, RequestId, ToolCatalogSnapshot,
};

pub const HOST_PROTOCOL_VERSION: u32 = 4;
pub const MAX_HOST_FRAME_BYTES: usize = 256 * 1024;
/// Reserved in Host frames for the request envelope, Agent configuration,
/// tools, and other metadata surrounding project instructions.
pub const HOST_REQUEST_ENVELOPE_RESERVE_BYTES: usize = 64 * 1024;
/// Aggregate project context budget within one Host protocol frame.
pub const MAX_PROJECT_CONTEXT_BYTES: usize =
    MAX_HOST_FRAME_BYTES - HOST_REQUEST_ENVELOPE_RESERVE_BYTES;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostFrame<T> {
    pub version: u32,
    pub body: T,
}

impl<T> HostFrame<T> {
    pub fn new(body: T) -> Self {
        Self {
            version: HOST_PROTOCOL_VERSION,
            body,
        }
    }
}

pub fn encode<T: Serialize>(body: T) -> anyhow::Result<Vec<u8>> {
    let encoded = serde_json::to_vec(&HostFrame::new(body))?;
    if encoded.len() > MAX_HOST_FRAME_BYTES {
        anyhow::bail!("Host protocol frame exceeds 256 KiB");
    }
    Ok(encoded)
}

pub fn decode<T: serde::de::DeserializeOwned>(encoded: &[u8]) -> anyhow::Result<T> {
    if encoded.len() > MAX_HOST_FRAME_BYTES {
        anyhow::bail!("Host protocol frame exceeds 256 KiB");
    }
    let frame: HostFrame<T> = serde_json::from_slice(encoded)?;
    if frame.version != HOST_PROTOCOL_VERSION {
        anyhow::bail!("unsupported Host protocol version {}", frame.version);
    }
    Ok(frame.body)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HubToHost {
    BeginHubTransfer {
        request_id: RequestId,
        transfer: HubTransferStart,
    },
    WriteHubTransfer {
        request_id: RequestId,
        offset: u64,
        #[serde(with = "base64_bytes")]
        data: Vec<u8>,
    },
    CommitHubTransfer {
        request_id: RequestId,
    },
    ArmHubTransfer {
        request_id: RequestId,
        move_nonce: String,
    },
    ActivateHubTransfer {
        request_id: RequestId,
    },
    ConfigureRelay {
        request_id: RequestId,
        active: bool,
        stop_at: Option<chrono::DateTime<chrono::Utc>>,
    },
    UpdateRelayCandidates {
        request_id: RequestId,
        relay_urls: Vec<String>,
    },
    ReadAgentReplica {
        request_id: RequestId,
        slug: String,
    },
    ReadProjectContext {
        request_id: RequestId,
        agent_slug: String,
        canonical_cwd: PathBuf,
    },
    ExecuteTool {
        request_id: RequestId,
        agent_id: AgentId,
        catalog_generation: u64,
        tool: String,
        arguments: Value,
        canonical_cwd: PathBuf,
    },
    CancelTool {
        request_id: RequestId,
    },
    ExecuteHook {
        request_id: RequestId,
        agent_id: AgentId,
        hook: String,
        payload: Value,
        canonical_cwd: PathBuf,
        catalog_generation: Option<u64>,
    },
    /// Execute an external ACP Harness on this Host. The prompt is already
    /// canonicalized by the Hub; launch details remain Host-local.
    ExecuteAcpTurn {
        request_id: RequestId,
        harness: String,
        harness_options: BTreeMap<String, String>,
        tools: Vec<String>,
        canonical_cwd: PathBuf,
        prompt: String,
        recovery_prompt: String,
        agent_id: AgentId,
        agent_slug: String,
        system_instructions: String,
        soul: String,
        mews_session_id: String,
        turn_id: String,
        transition: crate::AcpBindingTransition,
        /// Present only for a compatible resume. New/replaced Sessions are
        /// rendered by the executing Host from its selected-Agent skill scope.
        context: Option<crate::AcpBindingContext>,
    },
    CancelAcp {
        request_id: RequestId,
    },
    AcknowledgeAcpSessionBinding {
        acknowledgement_id: String,
    },
    AttestDirectory {
        request_id: RequestId,
        path: PathBuf,
    },
    SynchronizeAgent {
        request_id: RequestId,
        agent: Agent,
        revision: AgentRevision,
        expected_replica: Option<AgentReplica>,
        /// When renaming, the Host verifies and retires this old replica only
        /// after the new canonical replica is durable.
        previous_slug: Option<String>,
    },
    RefreshHarnessCatalog {
        request_id: RequestId,
    },
    Ping {
        nonce: u64,
    },
}

mod base64_bytes {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        STANDARD.decode(encoded).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worst_case_hub_transfer_chunk_fits_the_host_frame() {
        let data = vec![u8::MAX; 128 * 1024];
        let encoded = encode(HubToHost::WriteHubTransfer {
            request_id: RequestId::new(),
            offset: 0,
            data: data.clone(),
        })
        .unwrap();
        assert!(encoded.len() < MAX_HOST_FRAME_BYTES);
        let decoded: HubToHost = decode(&encoded).unwrap();
        assert!(
            matches!(decoded, HubToHost::WriteHubTransfer { data: value, .. } if value == data)
        );
    }

    #[test]
    fn project_context_budget_leaves_room_in_a_host_frame() {
        assert_eq!(
            MAX_PROJECT_CONTEXT_BYTES + HOST_REQUEST_ENVELOPE_RESERVE_BYTES,
            MAX_HOST_FRAME_BYTES
        );
        let encoded = encode(HostToHub::ProjectContext {
            request_id: RequestId::new(),
            context: Some("x".repeat(MAX_PROJECT_CONTEXT_BYTES)),
            error: None,
        })
        .unwrap();
        assert!(encoded.len() <= MAX_HOST_FRAME_BYTES);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubTransferStart {
    pub move_nonce: String,
    pub installation_id: InstallationId,
    pub generation: u64,
    pub target_host_id: HostId,
    pub database_size: u64,
    pub database_sha256: String,
    pub installation_key: Vec<u8>,
    pub hub_noise_key: Vec<u8>,
    pub credentials: Vec<u8>,
    pub credentials_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HostToHub {
    ConfigurationResult {
        request_id: RequestId,
        error: Option<String>,
    },
    HubTransferResult {
        request_id: RequestId,
        next_offset: Option<u64>,
        error: Option<String>,
    },
    Ready {
        tools: ToolCatalogSnapshot,
        harnesses: Vec<HarnessDescriptor>,
    },
    ToolCatalogChanged {
        tools: ToolCatalogSnapshot,
    },
    HarnessCatalog {
        request_id: RequestId,
        harnesses: Vec<HarnessDescriptor>,
        error: Option<String>,
    },
    ToolResult {
        request_id: RequestId,
        result: Value,
        error: Option<String>,
    },
    HookResult {
        request_id: RequestId,
        payload: Option<Value>,
        error: Option<String>,
    },
    AcpResult {
        request_id: RequestId,
        answer: Option<String>,
        acp_session_id: Option<String>,
        #[serde(default)]
        session_replaced: bool,
        stop_reason: Option<AcpStopReason>,
        timings: Option<AcpTimings>,
        error: Option<String>,
    },
    /// A bounded, non-authoritative observation emitted while an ACP Turn is
    /// active. The request ID correlates it with the eventual `AcpResult`.
    AcpEvent {
        request_id: RequestId,
        event: AcpEvent,
    },
    DirectoryAttested {
        request_id: RequestId,
        canonical_path: Option<PathBuf>,
        error: Option<String>,
    },
    AgentSynchronized {
        request_id: RequestId,
        error: Option<String>,
    },
    AgentReplica {
        request_id: RequestId,
        replica: Option<AgentReplica>,
        error: Option<String>,
    },
    ProjectContext {
        request_id: RequestId,
        context: Option<String>,
        error: Option<String>,
    },
    Pong {
        nonce: u64,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AcpStopReason {
    EndTurn,
    MaxTokens,
    MaxTurnRequests,
    Refusal,
    Cancelled,
    Other,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AcpTimings {
    #[serde(default)]
    pub queue_ms: u64,
    pub spawn_ms: u64,
    pub initialize_ms: u64,
    pub continuation_ms: u64,
    #[serde(default)]
    pub prompt_to_first_update_ms: Option<u64>,
    #[serde(default)]
    pub prompt_to_first_token_ms: Option<u64>,
    #[serde(default)]
    pub prompt_ms: u64,
    #[serde(default)]
    pub total_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AcpEvent {
    PromptDispatched {
        event_key: crate::AcpEventKey,
        session_id: String,
    },
    AssistantDelta {
        event_key: crate::AcpEventKey,
        delta: String,
        message_id: Option<String>,
        raw: Value,
    },
    ProviderState {
        event_key: crate::AcpEventKey,
        data: Value,
    },
    ReasoningDelta {
        event_key: crate::AcpEventKey,
        delta: String,
        message_id: Option<String>,
        raw: Value,
    },
    ToolActivity {
        event_key: crate::AcpEventKey,
        activity: crate::ToolActivity,
    },
    HookOutcome {
        event_key: crate::AcpEventKey,
        hook: String,
        ok: bool,
        detail: Option<String>,
        tool: Option<String>,
        call_id: Option<String>,
    },
    ContextDispatched {
        event_key: crate::AcpEventKey,
        acknowledgement_id: String,
        session_id: String,
    },
    SessionBound {
        event_key: crate::AcpEventKey,
        acknowledgement_id: String,
        session_id: String,
        transition: crate::AcpBindingTransition,
        context: crate::AcpBindingContext,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentReplica {
    pub revision: u64,
    pub soul: String,
    pub config_toml: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PeerEnvelope {
    Heartbeat {
        nonce: u64,
    },
    ToolRequest {
        body: HubToHost,
    },
    ToolResponse {
        body: HostToHub,
    },
    ClientRequest {
        request_id: RequestId,
        body: HubRequest,
    },
    ClientResponse {
        request_id: RequestId,
        body: HubResponse,
    },
}
