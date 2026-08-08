use std::{collections::BTreeMap, fmt, path::PathBuf, str::FromStr};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

// These types are persisted and sent over the wire. Development-state schema
// changes are intentionally breaking: reset local MEWS state instead of
// accepting legacy representations.

macro_rules! id {
    ($name:ident, $prefix:literal) => {
        #[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new() -> Self {
                Self(format!("{}{}", $prefix, Uuid::now_v7()))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl FromStr for $name {
            type Err = &'static str;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let uuid = value.strip_prefix($prefix).ok_or("invalid ID prefix")?;
                Uuid::parse_str(uuid).map_err(|_| "invalid ID")?;
                Ok(Self(value.to_owned()))
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                value.parse().map_err(serde::de::Error::custom)
            }
        }
    };
}

id!(InstallationId, "ins_");
id!(HostId, "hst_");
id!(AgentId, "agt_");
id!(SessionId, "ses_");
id!(MessageId, "msg_");
id!(RunId, "run_");
id!(InvitationId, "inv_");
id!(RequestId, "req_");
id!(ConsumerId, "con_");
id!(EventId, "evt_");

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Installation {
    pub id: InstallationId,
    pub public_key: String,
    pub relay_url: Option<String>,
    pub hub_host_id: HostId,
    pub generation: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Host {
    pub id: HostId,
    pub name: String,
    pub public_key: String,
    pub noise_public_key: String,
    pub relay_url: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostStatus {
    pub host: Host,
    pub connected: bool,
}

/// One live Harness descriptor paired with the Host that published it. The
/// catalog is connection state, so offline Hosts intentionally have no rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostHarnessStatus {
    pub host: Host,
    pub descriptor: HarnessDescriptor,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Agent {
    pub id: AgentId,
    pub slug: String,
    pub current_revision: u64,
    pub archived: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRevision {
    pub agent_id: AgentId,
    pub revision: u64,
    pub soul: String,
    pub config_toml: String,
    pub content_hash: String,
    pub author_host_id: HostId,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    pub harness: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub harness_options: BTreeMap<String, String>,
    #[serde(default = "default_tools")]
    pub tools: Vec<String>,
    #[serde(default)]
    pub tool_execution: ToolExecutionMode,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolExecutionMode {
    Sequential,
    #[default]
    Parallel,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEffort {
    None,
    Auto,
    Minimal,
    Low,
    Medium,
    High,
    XHigh,
    Max,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelInfo {
    pub id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub reasoning: Vec<ReasoningEffort>,
    #[serde(default)]
    pub default_reasoning: Option<ReasoningEffort>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderDefaults {
    pub model: Option<String>,
    pub reasoning: Option<ReasoningEffort>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionModelConfig {
    pub model: Option<String>,
    pub reasoning: Option<ReasoningEffort>,
}

fn default_tools() -> Vec<String> {
    vec!["*".to_owned()]
}

impl AgentConfig {
    pub fn parse(input: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(input)
    }

    pub fn validate(&self) -> Result<(), String> {
        if !valid_name(&self.harness) {
            return Err("invalid harness name".into());
        }
        for (name, value) in &self.harness_options {
            if !valid_name(name) {
                return Err(format!("invalid harness option name {name:?}"));
            }
            if value.trim().is_empty() {
                return Err(format!("harness option {name:?} must not be empty"));
            }
        }
        let mut unique = std::collections::HashSet::new();
        for tool in &self.tools {
            let base = tool.strip_suffix('*').unwrap_or(tool);
            if tool.is_empty()
                || (base.is_empty() && tool != "*")
                || !base.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'-' | b'_' | b'.')
                })
            {
                return Err(format!("invalid tool name {tool:?}"));
            }
            if !unique.insert(tool) {
                return Err(format!("duplicate tool name {tool:?}"));
            }
        }
        Ok(())
    }
}

fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    pub id: SessionId,
    pub agent_id: AgentId,
    pub agent_revision: u64,
    pub host_id: HostId,
    pub working_directory: PathBuf,
    pub model_override: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcpSessionBinding {
    pub session_id: SessionId,
    pub host_id: HostId,
    pub harness: String,
    pub harness_definition_hash: String,
    pub acp_session_id: String,
    pub created_at: DateTime<Utc>,
    pub replaced_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Run {
    pub id: RunId,
    pub session_id: SessionId,
    /// Exact Host-local Harness definition selected for this Run. These are
    /// filled before execution begins and remain stable even if the Host
    /// catalog changes while the Run is active.
    pub harness: Option<String>,
    pub harness_definition_hash: Option<String>,
    pub harness_version: Option<String>,
    pub status: RunStatus,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// Serializable tool metadata shared by model requests and Host catalogs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub schema: Value,
}

/// The wire-level protocol used by a Host Harness. Agent configuration keeps
/// the logical Harness name; Hosts keep the executable details private.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessProtocol {
    Mews,
    Acp,
}

/// Readiness of one independently observable part of a Harness installation.
/// A descriptor is ready for execution only when all applicable parts are
/// ready. `NotApplicable` is used by the native Harness for adapter and auth.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessReadiness {
    NotApplicable,
    Ready,
    Missing,
    Required,
    Stale,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessAvailability {
    pub runtime: HarnessReadiness,
    pub adapter: HarnessReadiness,
    pub authentication: HarnessReadiness,
    pub catalog: HarnessReadiness,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl HarnessAvailability {
    pub fn ready(&self) -> bool {
        [
            self.runtime,
            self.adapter,
            self.authentication,
            self.catalog,
        ]
        .into_iter()
        .all(|state| {
            matches!(
                state,
                HarnessReadiness::Ready | HarnessReadiness::NotApplicable
            )
        })
    }
}

/// Opaque model configuration advertised by an external Harness. Its IDs are
/// intentionally strings: Hub must preserve, not reinterpret, provider values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessModelCapability {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reasoning: Vec<String>,
}

/// A bounded, Host-published description of one logical Harness. It never
/// includes paths, launch arguments, credentials, or other Host authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessDescriptor {
    pub name: String,
    pub protocol: HarnessProtocol,
    pub definition_hash: String,
    pub availability: HarnessAvailability,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable_version: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub native_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub modes: Vec<String>,
    pub supports_mcp: bool,
    pub supports_continuation: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<HarnessModelCapability>,
    /// Bounded, Harness-owned ACP Session Config Options. The Hub preserves
    /// these verbatim rather than assigning provider-specific core fields.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub config_options: Vec<Value>,
    /// Host-local Unix timestamp of the cached ACP probe, if this descriptor
    /// was discovered by starting the adapter rather than static detection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probed_at: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub id: MessageId,
    pub session_id: SessionId,
    pub sequence: u64,
    pub role: MessageRole,
    pub content: MessageContent,
    pub metadata: Value,
    pub source: MessageSource,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    User,
    Assistant,
    Tool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageContent {
    Text {
        text: String,
    },
    ToolCall {
        call_id: String,
        tool: String,
        arguments: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thought_signature: Option<String>,
    },
    ToolResult {
        call_id: String,
        tool: String,
        result: Value,
        is_error: bool,
    },
    ProviderState {
        provider: String,
        model: String,
        data: Value,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageSource {
    pub kind: SourceKind,
    pub id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    Client,
    Channel,
    Harness,
    Host,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClientEvent {
    pub id: EventId,
    pub sequence: u64,
    pub session_id: SessionId,
    pub kind: ClientEventKind,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientEventKind {
    RunStarted {
        run_id: RunId,
    },
    /// Ephemeral-sized response fragment. The completed assistant message is
    /// still journaled separately as the durable conversation record.
    AssistantDelta {
        delta: String,
        message_id: Option<String>,
    },
    ReasoningDelta {
        run_id: RunId,
        delta: String,
        message_id: Option<String>,
    },
    ToolActivity {
        run_id: RunId,
        activity: ToolActivity,
    },
    AssistantMessage {
        message: Message,
    },
    ToolStarted {
        run_id: RunId,
        message: Message,
    },
    ToolCompleted {
        run_id: RunId,
        message: Message,
    },
    PermissionRequested {
        run_id: RunId,
        request: PermissionRequest,
    },
    RunCompleted {
        run_id: RunId,
    },
    RunFailed {
        run_id: RunId,
        error: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolActivity {
    pub call_id: String,
    pub title: String,
    pub kind: Option<String>,
    pub status: Option<String>,
    pub input: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PermissionRequest {
    pub id: String,
    pub tool_call: Value,
    pub options: Vec<PermissionOption>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionOption {
    pub id: String,
    pub name: String,
    pub kind: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventBatch {
    pub events: Vec<ClientEvent>,
    /// Acknowledging this advances past everything scanned, including events
    /// for Sessions to which this consumer is not subscribed.
    pub checkpoint: u64,
    pub advanced: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthCredential {
    ApiKey {
        key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        base_url: Option<String>,
    },
    Oauth {
        access: String,
        refresh: String,
        expires: u64,
        #[serde(rename = "accountId")]
        account_id: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthStatus {
    pub provider: String,
    pub kind: String,
}
