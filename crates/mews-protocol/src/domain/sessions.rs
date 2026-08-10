use super::*;

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
