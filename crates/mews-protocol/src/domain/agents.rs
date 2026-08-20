use super::*;

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

/// Canonical Agent configuration plus one optional Host resolution.
/// Clients inspect Hosts one at a time so a valid response always fits in one
/// Hub frame.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentInspection {
    pub agent: Agent,
    pub revision_hash: String,
    pub author_host_id: HostId,
    pub config: AgentConfig,
    pub host: Option<AgentHostInspection>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentHostInspection {
    pub host: Host,
    pub connected: bool,
    pub harness: Option<AgentHarnessInspection>,
    pub harness_native_authority: HarnessNativeAuthority,
    pub acp_skill_tools: AcpSkillToolsInspection,
    pub tool_catalog_generation: Option<u64>,
    pub tools: AgentToolInspectionPage,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentToolInspectionPage {
    pub tools: Vec<AgentToolInspection>,
    pub next: Option<AgentToolCursor>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentToolCursor {
    pub snapshot_hash: [u8; 32],
    pub offset: u32,
}

pub const ACP_SKILL_TOOL_NAMES: [&str; 2] = ["mews_list_skills", "mews_read_skill"];

pub fn is_reserved_acp_skill_tool(name: &str) -> bool {
    ACP_SKILL_TOOL_NAMES.contains(&name)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcpSkillToolsInspection {
    pub names: Vec<String>,
    pub state: AcpSkillToolsState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AcpSkillToolsState {
    NotApplicable,
    NoneKnown,
    Conditional,
    Exposed,
    HarnessUnavailable,
    UnsupportedTransport,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentHarnessInspection {
    pub name: String,
    pub protocol: HarnessProtocol,
    pub availability: HarnessAvailability,
    pub supports_http_mcp: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentToolInspection {
    pub name: String,
    pub source: AgentToolSource,
    pub allowlist_match: bool,
    pub exposure: AgentToolExposure,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentToolSource {
    MewsNative,
    HarnessNative,
    AgentExtension,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentToolExposure {
    Exposed,
    ExcludedByAllowlist,
    HarnessUnavailable,
    UnsupportedTransport,
    HarnessControlled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessNativeAuthority {
    NotApplicable,
    KnownUncontrolled,
    UnknownUncontrolled,
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
            system_instructions: "System.".into(),
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
        assert!(rendered.find("System.").unwrap() < rendered.find("Be useful.").unwrap());
        assert!(rendered.find("alpha").unwrap() < rendered.find("zeta").unwrap());
        assert_eq!(AcpContextSnapshot::hash_rendered(&rendered).len(), 64);
        assert!(rendered.contains("mews_read_skill"));
    }

    #[test]
    fn empty_acp_skill_context_does_not_advertise_skill_tools() {
        let rendered = AcpContextSnapshot {
            version: ACP_CONTEXT_VERSION,
            agent_slug: "coder".into(),
            system_instructions: "System.".into(),
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
                turn_id: TurnId::new(),
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
