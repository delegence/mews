use std::{collections::BTreeMap, path::PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    Agent, AgentRevision, HarnessDescriptor, HostId, HubRequest, HubResponse, InstallationId,
    PermissionRequest, RequestId, ToolDefinition,
};

pub const HOST_PROTOCOL_VERSION: u32 = 1;
pub const MAX_HOST_FRAME_BYTES: usize = 256 * 1024;

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
        canonical_cwd: PathBuf,
    },
    ReadPrompt {
        request_id: RequestId,
        name: String,
        canonical_cwd: PathBuf,
    },
    ExecuteTool {
        request_id: RequestId,
        tool: String,
        arguments: Value,
        canonical_cwd: PathBuf,
    },
    ExecuteHook {
        request_id: RequestId,
        hook: String,
        payload: Value,
        canonical_cwd: PathBuf,
    },
    /// Execute an external ACP Harness on this Host. The prompt is already
    /// canonicalized by the Hub; launch details remain Host-local.
    RunAcp {
        request_id: RequestId,
        harness: String,
        harness_options: BTreeMap<String, String>,
        tools: Vec<String>,
        canonical_cwd: PathBuf,
        prompt: String,
        recovery_prompt: String,
        #[serde(default)]
        acp_session_id: Option<String>,
    },
    ResolveAcpPermission {
        permission_id: String,
        #[serde(default)]
        option_id: Option<String>,
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
    },
    RefreshHarnessCatalog {
        request_id: RequestId,
    },
    Ping {
        nonce: u64,
    },
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
        tools: Vec<ToolDefinition>,
        harnesses: Vec<HarnessDescriptor>,
    },
    ToolCatalogChanged {
        tools: Vec<ToolDefinition>,
    },
    HarnessCatalogChanged {
        harnesses: Vec<HarnessDescriptor>,
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
        error: Option<String>,
    },
    /// A bounded, non-authoritative observation emitted while an ACP run is
    /// active. The request ID correlates it with the eventual `AcpResult`.
    AcpEvent {
        request_id: RequestId,
        event: AcpEvent,
    },
    AcpPermissionRequested {
        request_id: RequestId,
        request: PermissionRequest,
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
    Prompt {
        request_id: RequestId,
        content: Option<String>,
        error: Option<String>,
    },
    Pong {
        nonce: u64,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AcpEvent {
    AssistantDelta {
        delta: String,
        message_id: Option<String>,
    },
    ProviderState {
        data: Value,
    },
    ReasoningDelta {
        delta: String,
        message_id: Option<String>,
    },
    ToolActivity {
        activity: crate::ToolActivity,
    },
    SessionBound {
        acknowledgement_id: String,
        session_id: String,
        replaced: bool,
    },
    PermissionRequested {
        request: PermissionRequest,
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
