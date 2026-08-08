use async_trait::async_trait;
use futures_util::Stream;
use mews_protocol::{ReasoningEffort, ToolDefinition};
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
    },
    ProviderState {
        provider: String,
        model: String,
        data: Value,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelResponse {
    pub parts: Vec<ModelPart>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ModelStreamEvent {
    Start,
    TextDelta(String),
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
}

pub type ProviderResult<T> = std::result::Result<T, ProviderError>;

#[async_trait]
pub trait Provider: Send + Sync {
    async fn generate(&self, request: ModelRequest) -> ProviderResult<ModelResponse>;

    async fn stream(&self, request: ModelRequest) -> ProviderResult<ModelStream> {
        let response = self.generate(request).await?;
        let mut events = vec![Ok(ModelStreamEvent::Start)];
        events.extend(response.parts.into_iter().map(|part| {
            Ok(match part {
                ModelPart::Text { text } => ModelStreamEvent::TextDelta(text),
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
        events.push(Ok(ModelStreamEvent::Done));
        Ok(Box::pin(futures_util::stream::iter(events)))
    }
}
