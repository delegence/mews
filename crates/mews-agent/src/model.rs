use async_trait::async_trait;
use futures_util::Stream;
use mews_protocol::{ModelUsage, ReasoningEffort, ToolDefinition};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelRequest {
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<ReasoningEffort>,
    pub system: String,
    pub messages: Vec<ModelMessage>,
    pub tools: Vec<ToolDefinition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<ResponseContinuation>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponseContinuation {
    pub response_id: String,
    pub provider: String,
    pub model: String,
    pub api: String,
    /// Canonical full replay retained locally for the one safe cursor fallback.
    #[serde(skip)]
    pub fallback_messages: Vec<ModelMessage>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContinuationCapability {
    None,
    ResponseId { provider: String, api: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelMessage {
    pub role: MessageRole,
    pub content: MessageContent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageRole {
    User,
    Assistant,
    Tool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelResponse {
    pub provider: String,
    pub model: String,
    pub api: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_id: Option<String>,
    pub parts: Vec<ModelPart>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<ModelUsage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ModelStreamEvent {
    Start,
    ResponseMetadata {
        provider: String,
        model: String,
        api: String,
        response_id: Option<String>,
    },
    TextDelta(String),
    Reasoning {
        text: String,
        signature: Option<String>,
    },
    ToolCall {
        id: String,
        name: String,
        arguments: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thought_signature: Option<String>,
    },
    ProviderState {
        provider: String,
        model: String,
        data: Value,
    },
    ResponseCompleted {
        usage: Option<ModelUsage>,
        stop_reason: Option<String>,
    },
    Done,
}

pub type ModelStream =
    std::pin::Pin<Box<dyn Stream<Item = ProviderResult<ModelStreamEvent>> + Send + 'static>>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ModelPart {
    Text {
        text: String,
    },
    Reasoning {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
    },
    ToolCall {
        id: String,
        name: String,
        arguments: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thought_signature: Option<String>,
    },
    ProviderState {
        provider: String,
        model: String,
        data: Value,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "code", content = "message", rename_all = "snake_case")]
pub enum ProviderError {
    #[error("invalid model request: {0}")]
    InvalidRequest(String),
    #[error("provider authentication failed: {0}")]
    Authentication(String),
    #[error("unsupported provider {0:?}")]
    UnsupportedProvider(String),
    #[error("provider rate limited the request{retry}", retry = retry_after.map(|seconds| format!("; retry after {seconds}s")).unwrap_or_default())]
    RateLimited { retry_after: Option<u64> },
    #[error("provider request failed: {0}")]
    Http(String),
    #[error("invalid provider response: {0}")]
    InvalidResponse(String),
    #[error("provider operation was cancelled")]
    Cancelled,
    #[error("provider continuation cursor is missing, expired, or incompatible: {0}")]
    CursorRejected(String),
}

pub type ProviderResult<T> = std::result::Result<T, ProviderError>;

#[async_trait]
pub trait Provider: Send + Sync {
    fn continuation_capability(&self, _model: &str) -> ContinuationCapability {
        ContinuationCapability::None
    }

    async fn generate(&self, request: ModelRequest) -> ProviderResult<ModelResponse>;

    async fn stream(&self, request: ModelRequest) -> ProviderResult<ModelStream> {
        let response = self.generate(request).await?;
        let mut events = vec![
            Ok(ModelStreamEvent::Start),
            Ok(ModelStreamEvent::ResponseMetadata {
                provider: response.provider.clone(),
                model: response.model.clone(),
                api: response.api.clone(),
                response_id: response.response_id.clone(),
            }),
        ];
        events.extend(response.parts.into_iter().map(|part| {
            Ok(match part {
                ModelPart::Text { text } => ModelStreamEvent::TextDelta(text),
                ModelPart::Reasoning { text, signature } => {
                    ModelStreamEvent::Reasoning { text, signature }
                }
                ModelPart::ToolCall {
                    id,
                    name,
                    arguments,
                    thought_signature,
                } => ModelStreamEvent::ToolCall {
                    id,
                    name,
                    arguments,
                    thought_signature,
                },
                ModelPart::ProviderState {
                    provider,
                    model,
                    data,
                } => ModelStreamEvent::ProviderState {
                    provider,
                    model,
                    data,
                },
            })
        }));
        events.push(Ok(ModelStreamEvent::ResponseCompleted {
            usage: response.usage,
            stop_reason: response.stop_reason,
        }));
        events.push(Ok(ModelStreamEvent::Done));
        Ok(Box::pin(futures_util::stream::iter(events)))
    }
}
