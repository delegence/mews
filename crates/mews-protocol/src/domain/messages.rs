use super::*;

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
