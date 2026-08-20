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
    TurnStarted {
        turn_id: TurnId,
        harness: HarnessProvenance,
    },
    /// One provider invocation. Blocks remain in provider order so replay can
    /// preserve signatures and opaque state at their exact boundaries.
    AssistantResponse {
        turn_id: TurnId,
        response: AssistantResponse,
    },
    ToolStarted {
        turn_id: TurnId,
        call: ToolCall,
    },
    ToolResult {
        turn_id: TurnId,
        result: ToolResult,
    },
    Reasoning {
        turn_id: TurnId,
        text: String,
        visibility: ReasoningVisibility,
        provenance: ReasoningProvenance,
    },
    TurnCompleted {
        turn_id: TurnId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stop_reason: Option<String>,
    },
    TurnFailed {
        turn_id: TurnId,
        error: String,
    },
    TurnCancelled {
        turn_id: TurnId,
    },
    ContextCompaction {
        summary: String,
        first_kept_entry_id: MessageId,
        tokens_before: u64,
    },
    HarnessObservation {
        turn_id: TurnId,
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
    /// The effect may have happened, but no definitive result was observed.
    #[serde(default)]
    pub uncertain: bool,
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
                        uncertain: result.uncertain,
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
            SessionEntryPayload::TurnStarted { .. }
            | SessionEntryPayload::ToolStarted { .. }
            | SessionEntryPayload::Reasoning { .. }
            | SessionEntryPayload::TurnCompleted { .. }
            | SessionEntryPayload::TurnFailed { .. }
            | SessionEntryPayload::TurnCancelled { .. }
            | SessionEntryPayload::HarnessObservation { .. } => {}
        }
    }
    projected
}

/// A Turn-scoped identity assigned when MEWS accepts an ACP event. It is
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
        #[serde(default)]
        uncertain: bool,
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

/// The standalone Channel identity and destination that originated a Turn.
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnInputValidationError(String);

impl std::fmt::Display for TurnInputValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for TurnInputValidationError {}

/// Applies the same Turn admission limits at every ingress boundary.
pub fn validate_turn_input(
    idempotency_key: &str,
    content: &MessageContent,
    metadata: &Value,
    source: &MessageSource,
) -> Result<(), TurnInputValidationError> {
    if idempotency_key.is_empty() || idempotency_key.len() > 200 {
        return Err(TurnInputValidationError(
            "invalid turn idempotency key".into(),
        ));
    }
    if !matches!(source.kind, SourceKind::Client | SourceKind::Channel)
        || source.id.is_empty()
        || source.id.len() > 256
    {
        return Err(TurnInputValidationError(
            "turn source must be a Client or Channel with a valid ID".into(),
        ));
    }
    let metadata_bytes = serde_json::to_vec(metadata)
        .map_err(|error| TurnInputValidationError(error.to_string()))?;
    if metadata_bytes.len() > 64 * 1024 {
        return Err(TurnInputValidationError(
            "message metadata exceeds 64 KiB".into(),
        ));
    }
    if matches!(content, MessageContent::Text { text } if text.trim().is_empty()) {
        return Err(TurnInputValidationError(
            "message text cannot be empty".into(),
        ));
    }
    let entry = SessionEntryPayload::UserMessage {
        content: content.clone(),
        metadata: metadata.clone(),
        source: source.clone(),
    };
    let entry_bytes =
        serde_json::to_vec(&entry).map_err(|error| TurnInputValidationError(error.to_string()))?;
    if entry_bytes.len() > crate::MAX_SESSION_ITEM_BYTES {
        return Err(TurnInputValidationError(
            "session item exceeds the page-safe size limit".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    #[test]
    fn legacy_tool_results_default_to_definitive() {
        let result: ToolResult = serde_json::from_value(serde_json::json!({
            "call_id": "call-1",
            "tool": "write",
            "result": null,
            "is_error": true
        }))
        .unwrap();

        assert!(!result.uncertain);

        let content: super::MessageContent = serde_json::from_value(serde_json::json!({
            "type": "tool_result",
            "call_id": "call-1",
            "tool": "write",
            "result": null,
            "is_error": true
        }))
        .unwrap();
        assert!(matches!(
            content,
            super::MessageContent::ToolResult {
                uncertain: false,
                ..
            }
        ));
    }

    #[test]
    fn turn_input_validation_is_shared_and_page_safe() {
        let source = MessageSource {
            kind: SourceKind::Channel,
            id: "channel-1".into(),
            channel_origin: None,
        };
        validate_turn_input(
            "key-1",
            &MessageContent::Text {
                text: "hello".into(),
            },
            &json!({"source":"test"}),
            &source,
        )
        .unwrap();

        assert!(
            validate_turn_input(
                "key-2",
                &MessageContent::Text { text: " ".into() },
                &Value::Null,
                &source,
            )
            .is_err()
        );
        assert!(
            validate_turn_input(
                "key-3",
                &MessageContent::Text {
                    text: "x".repeat(crate::MAX_SESSION_ITEM_BYTES),
                },
                &Value::Null,
                &source,
            )
            .is_err()
        );
    }
}
