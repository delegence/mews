use std::{collections::BTreeMap, fmt, path::PathBuf, str::FromStr};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
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
    pub harness: String,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acp_context_is_sorted_and_hashes_exact_rendering() {
        let context = AcpContextSnapshot {
            version: ACP_CONTEXT_VERSION,
            agent_slug: "coder".into(),
            soul: "Be useful.".into(),
            skills: vec![
                AcpSkillInventoryItem {
                    name: "zeta".into(),
                    description: "Z".into(),
                    hash: "z".into(),
                },
                AcpSkillInventoryItem {
                    name: "alpha".into(),
                    description: "A".into(),
                    hash: "a".into(),
                },
            ],
        };
        let rendered = context.render().unwrap();
        assert!(rendered.find("alpha").unwrap() < rendered.find("zeta").unwrap());
        assert_eq!(AcpContextSnapshot::hash_rendered(&rendered).len(), 64);
        assert!(rendered.contains("mews_read_skill"));
    }

    #[test]
    fn empty_acp_skill_context_does_not_advertise_skill_tools() {
        let rendered = AcpContextSnapshot {
            version: ACP_CONTEXT_VERSION,
            agent_slug: "coder".into(),
            soul: "Be useful.".into(),
            skills: Vec::new(),
        }
        .render()
        .unwrap();
        assert!(!rendered.contains("mews_list_skills"));
        assert!(!rendered.contains("mews_read_skill"));
    }

    #[test]
    fn portable_projection_strips_private_native_state() {
        let session_id = SessionId::new();
        let entry = SessionEntry {
            id: MessageId::new(),
            session_id,
            sequence: 1,
            parent_id: None,
            created_at: Utc::now(),
            payload: SessionEntryPayload::AssistantResponse {
                run_id: RunId::new(),
                response: AssistantResponse {
                    provider: "google".into(),
                    model: "gemini".into(),
                    api: "generate_content".into(),
                    response_id: Some("private".into()),
                    blocks: vec![
                        AssistantResponseBlock::Reasoning {
                            text: "secret thought".into(),
                            signature: Some("sig".into()),
                        },
                        AssistantResponseBlock::Text {
                            text: "answer".into(),
                        },
                        AssistantResponseBlock::ToolCall {
                            call_id: "call".into(),
                            tool: "read".into(),
                            arguments: serde_json::json!({}),
                            thought_signature: Some("thought-sig".into()),
                        },
                        AssistantResponseBlock::OpaqueState {
                            provider: "google".into(),
                            model: "gemini".into(),
                            data: serde_json::json!({"secret":true}),
                        },
                    ],
                    usage: None,
                    stop_reason: None,
                },
            },
        };
        let projected = portable_history(&[entry]);
        assert_eq!(projected.len(), 2);
        assert!(matches!(&projected[0].content, MessageContent::Text { text } if text == "answer"));
        assert!(matches!(
            &projected[1].content,
            MessageContent::ToolCall {
                thought_signature: None,
                ..
            }
        ));
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    pub id: SessionId,
    pub agent_id: AgentId,
    pub agent_revision: u64,
    pub host_id: HostId,
    pub working_directory: PathBuf,
    pub model_override: Option<String>,
    /// The contextual leaf used to reconstruct this Session's active history.
    pub leaf_entry_id: Option<MessageId>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcpSessionBinding {
    pub session_id: SessionId,
    pub host_id: HostId,
    pub harness: String,
    pub harness_definition_hash: String,
    pub acp_session_id: String,
    pub context_version: u32,
    pub context_hash: String,
    pub context_channel: AcpInstructionChannel,
    pub context_text: String,
    /// A FirstPrompt binding exists before its context-bearing prompt is
    /// dispatched, but is never eligible for resume until this is true.
    pub context_dispatched: bool,
    pub created_at: DateTime<Utc>,
    pub replaced_at: Option<DateTime<Utc>>,
    pub last_replacement_reason: Option<AcpReplacementReason>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AcpInstructionChannel {
    CodexDeveloper,
    ClaudeSystemAppend,
    FirstPrompt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AcpReplacementReason {
    ResourceNotFound,
    HarnessDefinitionChanged,
    ContextNotDispatched,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum AcpBindingTransition {
    New,
    Resume { acp_session_id: String },
    Replace { reason: AcpReplacementReason },
}

pub const ACP_CONTEXT_VERSION: u32 = 1;
pub const MAX_ACP_CONTEXT_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcpSkillInventoryItem {
    pub name: String,
    pub description: String,
    pub hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcpContextSnapshot {
    pub version: u32,
    pub agent_slug: String,
    pub soul: String,
    pub skills: Vec<AcpSkillInventoryItem>,
}

/// The exact private instruction material attached to an ACP binding. The Hub
/// stores this after the executing Host has rendered host-local skill inventory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcpBindingContext {
    pub version: u32,
    pub hash: String,
    pub channel: AcpInstructionChannel,
    pub text: String,
}

impl AcpBindingContext {
    pub fn from_snapshot(
        snapshot: &AcpContextSnapshot,
        channel: AcpInstructionChannel,
    ) -> Result<Self, String> {
        let text = snapshot.render()?;
        Ok(Self {
            version: snapshot.version,
            hash: AcpContextSnapshot::hash_rendered(&text),
            channel,
            text,
        })
    }
}

impl AcpContextSnapshot {
    pub fn render(&self) -> Result<String, String> {
        let mut skills = self.skills.clone();
        skills.sort_by(|left, right| left.name.cmp(&right.name));
        let mut text = format!(
            "<mews_context version=\"{}\">\n<agent slug={:?}>\n<soul>{}</soul>\n<skills>\n",
            self.version, self.agent_slug, self.soul
        );
        for skill in skills {
            text.push_str(&format!(
                "<skill name={:?} description={:?} hash=\"{}\" />\n",
                skill.name, skill.description, skill.hash
            ));
        }
        text.push_str("</skills>\n");
        // Do not advertise a capability when this selected agent has none.
        // Some ACP adapters intentionally run without an MCP transport.
        if !self.skills.is_empty() {
            text.push_str(
                "Read full selected-agent skill bodies only through mews_list_skills and mews_read_skill.\n",
            );
        }
        text.push_str("</agent>\n</mews_context>");
        if text.len() > MAX_ACP_CONTEXT_BYTES {
            return Err("ACP context exceeds 64 KiB".into());
        }
        Ok(text)
    }

    pub fn hash_rendered(text: &str) -> String {
        format!("{:x}", Sha256::digest(text.as_bytes()))
    }
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

/// One durable item in a Session timeline. This initial timeline stores only
/// messages; future non-contextual entries can extend the payload enum.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionEntry {
    pub id: MessageId,
    pub session_id: SessionId,
    pub sequence: u64,
    pub parent_id: Option<MessageId>,
    pub payload: SessionEntryPayload,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionEntryPayload {
    UserMessage {
        content: MessageContent,
        metadata: Value,
        source: MessageSource,
    },
    RunStarted {
        run_id: RunId,
        harness: HarnessProvenance,
    },
    /// One provider invocation. Blocks remain in provider order so replay can
    /// preserve signatures and opaque state at their exact boundaries.
    AssistantResponse {
        run_id: RunId,
        response: AssistantResponse,
    },
    ToolStarted {
        run_id: RunId,
        call: ToolCall,
    },
    ToolResult {
        run_id: RunId,
        result: ToolResult,
    },
    Reasoning {
        run_id: RunId,
        text: String,
        visibility: ReasoningVisibility,
        provenance: ReasoningProvenance,
    },
    PermissionRequested {
        run_id: RunId,
        request: PermissionRequest,
    },
    PermissionResolved {
        run_id: RunId,
        outcome: PermissionOutcome,
    },
    RunCompleted {
        run_id: RunId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stop_reason: Option<String>,
    },
    RunFailed {
        run_id: RunId,
        error: String,
    },
    RunCancelled {
        run_id: RunId,
    },
    ContextCompaction {
        summary: String,
        first_kept_entry_id: MessageId,
        tokens_before: u64,
    },
    HarnessObservation {
        run_id: RunId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        harness_session_id: Option<String>,
        kind: String,
        data: Value,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessProvenance {
    pub name: String,
    pub definition_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    pub call_id: String,
    pub tool: String,
    pub arguments: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thought_signature: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolResult {
    pub call_id: String,
    pub tool: String,
    pub result: Value,
    pub is_error: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReasoningProvenance {
    Provider {
        provider: String,
        model: String,
    },
    Harness {
        harness: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AssistantResponse {
    pub provider: String,
    pub model: String,
    /// Identifies the concrete provider API whose cursor/replay contract was
    /// used (for example `responses` or `messages`).
    pub api: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_id: Option<String>,
    pub blocks: Vec<AssistantResponseBlock>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<ModelUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AssistantResponseBlock {
    Text {
        text: String,
    },
    /// Reasoning safe to show to the user. This is deliberately distinct from
    /// provider state needed only for replay.
    Reasoning {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
    },
    ToolCall {
        call_id: String,
        tool: String,
        arguments: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thought_signature: Option<String>,
    },
    OpaqueState {
        provider: String,
        model: String,
        data: Value,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(default)]
    pub cached_input_tokens: u64,
    #[serde(default)]
    pub reasoning_tokens: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PortableHistoryItem {
    pub role: MessageRole,
    pub content: MessageContent,
}

/// Provider-neutral projection used when native opaque state is not valid and
/// when reconstructing ACP prompts. It intentionally excludes cursor IDs,
/// signatures, opaque reasoning, and private metadata.
pub fn portable_history(entries: &[SessionEntry]) -> Vec<PortableHistoryItem> {
    let mut projected = Vec::new();
    for entry in entries {
        match &entry.payload {
            SessionEntryPayload::UserMessage { content, .. } => {
                if let MessageContent::Text { text } = content {
                    projected.push(PortableHistoryItem {
                        role: MessageRole::User,
                        content: MessageContent::Text { text: text.clone() },
                    });
                }
            }
            SessionEntryPayload::AssistantResponse { response, .. } => {
                for block in &response.blocks {
                    match block {
                        AssistantResponseBlock::Text { text } => {
                            projected.push(PortableHistoryItem {
                                role: MessageRole::Assistant,
                                content: MessageContent::Text { text: text.clone() },
                            })
                        }
                        AssistantResponseBlock::ToolCall {
                            call_id,
                            tool,
                            arguments,
                            ..
                        } => {
                            projected.push(PortableHistoryItem {
                                role: MessageRole::Assistant,
                                content: MessageContent::ToolCall {
                                    call_id: call_id.clone(),
                                    tool: tool.clone(),
                                    arguments: arguments.clone(),
                                    thought_signature: None,
                                },
                            });
                        }
                        AssistantResponseBlock::Reasoning { .. }
                        | AssistantResponseBlock::OpaqueState { .. } => {}
                    }
                }
            }
            SessionEntryPayload::ToolResult { result, .. } => {
                projected.push(PortableHistoryItem {
                    role: MessageRole::Tool,
                    content: MessageContent::ToolResult {
                        call_id: result.call_id.clone(),
                        tool: result.tool.clone(),
                        result: result.result.clone(),
                        is_error: result.is_error,
                    },
                });
            }
            SessionEntryPayload::ContextCompaction { summary, .. } => {
                projected.push(PortableHistoryItem {
                    role: MessageRole::User,
                    content: MessageContent::Text {
                        text: format!("[Earlier context summary]\n{summary}"),
                    },
                })
            }
            SessionEntryPayload::RunStarted { .. }
            | SessionEntryPayload::ToolStarted { .. }
            | SessionEntryPayload::Reasoning { .. }
            | SessionEntryPayload::PermissionRequested { .. }
            | SessionEntryPayload::PermissionResolved { .. }
            | SessionEntryPayload::RunCompleted { .. }
            | SessionEntryPayload::RunFailed { .. }
            | SessionEntryPayload::RunCancelled { .. }
            | SessionEntryPayload::HarnessObservation { .. } => {}
        }
    }
    projected
}

/// A run-scoped identity assigned when MEWS accepts an ACP event. It is
/// deliberately independent of the event payload so repeated chunks survive.
pub type AcpEventKey = String;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AcpObservation {
    AssistantDelta {
        delta: String,
        message_id: Option<String>,
        raw: Value,
    },
    CompletedReasoning {
        text: String,
        message_id: Option<String>,
        visibility: ReasoningVisibility,
    },
    ContextDispatched {
        version: u32,
        hash: String,
        channel: AcpInstructionChannel,
        /// Lossless, bounded record of the exact initialization context.
        text: String,
    },
    BindingChanged {
        transition: AcpBindingTransition,
    },
    ProviderUpdate {
        data: Value,
    },
    ToolActivity {
        activity: ToolActivity,
    },
    PermissionRequested {
        request: PermissionRequest,
    },
    PermissionResolved {
        request_id: String,
        outcome: PermissionOutcome,
    },
    HookOutcome {
        hook: String,
        ok: bool,
        detail: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tool: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_id: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningVisibility {
    Visible,
    Redacted,
    Omitted,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_origin: Option<ChannelOrigin>,
}

/// The standalone Channel identity and destination that originated a Run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelOrigin {
    pub consumer_id: ConsumerId,
    pub conversation: String,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_origin: Option<ChannelOrigin>,
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
        run_id: RunId,
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
        run_id: RunId,
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
    PermissionResolved {
        run_id: RunId,
        request_id: String,
        outcome: PermissionOutcome,
    },
    RunCompleted {
        run_id: RunId,
    },
    RunFailed {
        run_id: RunId,
        error: String,
    },
    RunCancelled {
        run_id: RunId,
    },
}

impl ClientEventKind {
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::AssistantDelta { .. } | Self::ReasoningDelta { .. } | Self::ToolActivity { .. }
        )
    }

    pub fn run_id(&self) -> Option<&RunId> {
        match self {
            Self::RunStarted { run_id }
            | Self::AssistantDelta { run_id, .. }
            | Self::ReasoningDelta { run_id, .. }
            | Self::ToolActivity { run_id, .. }
            | Self::AssistantMessage { run_id, .. }
            | Self::ToolStarted { run_id, .. }
            | Self::ToolCompleted { run_id, .. }
            | Self::PermissionRequested { run_id, .. }
            | Self::PermissionResolved { run_id, .. }
            | Self::RunCompleted { run_id }
            | Self::RunFailed { run_id, .. }
            | Self::RunCancelled { run_id } => Some(run_id),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PermissionOutcome {
    Selected { option_id: String },
    Cancelled,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerKind {
    #[default]
    Durable,
    Ephemeral,
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
    /// Acknowledging this advances past the subscribed events returned by the Hub.
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
