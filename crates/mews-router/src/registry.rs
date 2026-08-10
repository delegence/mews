use std::{collections::BTreeMap, path::PathBuf};

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    MessageContent, MessageRole, ModelInfo, ModelPart, ModelRequest, ModelResponse, ModelStream,
    Provider, ProviderError, ProviderResult,
    providers::{anthropic, gemini, openai},
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderInfo {
    pub id: String,
    pub auth: String,
    pub default_base_url: String,
}

#[derive(Clone, Copy)]
enum Adapter {
    Test,
    OpenAi,
    OpenAiCodex,
    Anthropic,
    Gemini,
}

pub(crate) struct ProviderRegistry {
    pub(crate) root: PathBuf,
    pub(crate) catalog_lock: tokio::sync::Mutex<()>,
    client: Client,
    adapters: BTreeMap<&'static str, Adapter>,
}

impl ProviderRegistry {
    pub(crate) fn new(root: PathBuf) -> Self {
        let mut adapters = BTreeMap::from([
            ("openai", Adapter::OpenAi),
            ("openai-codex", Adapter::OpenAiCodex),
            ("anthropic", Adapter::Anthropic),
            ("google", Adapter::Gemini),
        ]);
        if test_provider_enabled(&root) {
            adapters.insert("test", Adapter::Test);
        }
        Self {
            root,
            catalog_lock: tokio::sync::Mutex::new(()),
            client: Client::new(),
            adapters,
        }
    }

    pub(crate) async fn discover_models(&self, provider: &str) -> ProviderResult<Vec<ModelInfo>> {
        match self.adapters.get(provider).copied() {
            Some(Adapter::OpenAi | Adapter::OpenAiCodex) => {
                openai::models(&self.client, &self.root, provider).await
            }
            Some(Adapter::Anthropic) => anthropic::models(&self.client, &self.root).await,
            Some(Adapter::Gemini) => gemini::models(&self.client, &self.root).await,
            Some(Adapter::Test) => Ok(vec![test_model()]),
            None => Err(ProviderError::UnsupportedProvider(provider.into())),
        }
    }
}

pub fn implemented_providers() -> Vec<ProviderInfo> {
    [
        ("openai", "api_key", "https://api.openai.com/v1"),
        ("openai-codex", "oauth", "https://chatgpt.com/backend-api"),
        (
            "anthropic",
            "oauth_or_api_key",
            "https://api.anthropic.com/v1",
        ),
        (
            "google",
            "api_key",
            "https://generativelanguage.googleapis.com",
        ),
    ]
    .into_iter()
    .map(|(id, auth, base)| ProviderInfo {
        id: id.into(),
        auth: auth.into(),
        default_base_url: base.into(),
    })
    .collect()
}

pub(crate) fn test_provider_enabled(root: &std::path::Path) -> bool {
    cfg!(debug_assertions) && root.join(".test-provider").is_file()
}

pub(crate) fn test_model() -> ModelInfo {
    ModelInfo {
        id: "test".into(),
        display_name: Some("Test".into()),
        reasoning: vec![],
        default_reasoning: None,
    }
}

#[async_trait]
impl Provider for ProviderRegistry {
    fn continuation_capability(&self, model: &str) -> mews_agent::ContinuationCapability {
        match model.split_once('/').map(|(provider, _)| provider) {
            Some(provider @ ("openai" | "openai-codex")) => {
                mews_agent::ContinuationCapability::ResponseId {
                    provider: provider.into(),
                    api: "responses".into(),
                }
            }
            _ => mews_agent::ContinuationCapability::None,
        }
    }

    async fn generate(&self, request: ModelRequest) -> ProviderResult<ModelResponse> {
        let prefix = if request.model == "test" {
            "test".to_owned()
        } else {
            request
                .model
                .split_once('/')
                .map(|(provider, _)| provider.to_owned())
                .ok_or_else(|| {
                    ProviderError::InvalidRequest("model must use provider/model format".into())
                })?
        };
        match self.adapters.get(prefix.as_str()).copied() {
            Some(Adapter::Test) if request.reasoning.is_some() => {
                Err(ProviderError::InvalidRequest(
                    "test provider does not support reasoning effort".into(),
                ))
            }
            Some(Adapter::Test) => Ok(test_response(request)),
            Some(Adapter::OpenAi | Adapter::OpenAiCodex) => {
                openai::generate(&self.client, &self.root, &prefix, request).await
            }
            Some(Adapter::Anthropic) => {
                anthropic::generate(&self.client, &self.root, request).await
            }
            Some(Adapter::Gemini) => gemini::generate(&self.client, &self.root, request).await,
            None => Err(ProviderError::UnsupportedProvider(prefix)),
        }
    }

    async fn stream(&self, request: ModelRequest) -> ProviderResult<ModelStream> {
        let prefix = if request.model == "test" {
            "test".to_owned()
        } else {
            request
                .model
                .split_once('/')
                .map(|(provider, _)| provider.to_owned())
                .ok_or_else(|| {
                    ProviderError::InvalidRequest("model must use provider/model format".into())
                })?
        };
        if matches!(
            self.adapters.get(prefix.as_str()),
            Some(Adapter::OpenAi | Adapter::OpenAiCodex)
        ) {
            return openai::stream(&self.client, &self.root, &prefix, request).await;
        }
        let response = self.generate(request).await?;
        let mut events = vec![
            Ok(crate::ModelStreamEvent::Start),
            Ok(crate::ModelStreamEvent::ResponseMetadata {
                provider: response.provider.clone(),
                model: response.model.clone(),
                api: response.api.clone(),
                response_id: response.response_id.clone(),
            }),
        ];
        events.extend(response.parts.into_iter().map(|part| {
            Ok(match part {
                ModelPart::Text { text } => crate::ModelStreamEvent::TextDelta(text),
                ModelPart::Reasoning { text, signature } => {
                    crate::ModelStreamEvent::Reasoning { text, signature }
                }
                ModelPart::ToolCall {
                    id,
                    name,
                    arguments,
                    thought_signature,
                } => crate::ModelStreamEvent::ToolCall {
                    id,
                    name,
                    arguments,
                    thought_signature,
                },
                ModelPart::ProviderState {
                    provider,
                    model,
                    data,
                } => crate::ModelStreamEvent::ProviderState {
                    provider,
                    model,
                    data,
                },
            })
        }));
        events.push(Ok(crate::ModelStreamEvent::ResponseCompleted {
            usage: response.usage,
            stop_reason: response.stop_reason,
        }));
        events.push(Ok(crate::ModelStreamEvent::Done));
        Ok(Box::pin(futures_util::stream::iter(events)))
    }
}

fn test_response(request: ModelRequest) -> ModelResponse {
    if let Some(MessageContent::ToolResult { result, .. }) =
        request.messages.last().map(|m| &m.content)
    {
        return ModelResponse {
            provider: "test".into(),
            model: request.model.clone(),
            api: "test".into(),
            response_id: None,
            usage: None,
            stop_reason: Some("stop".into()),
            parts: vec![ModelPart::Text {
                text: result
                    .get("content")
                    .and_then(Value::as_str)
                    .unwrap_or_else(|| result.as_str().unwrap_or("tool completed"))
                    .to_owned(),
            }],
        };
    }
    let prompt = request
        .messages
        .iter()
        .rev()
        .find_map(|message| match &message.content {
            MessageContent::Text { text } if message.role == MessageRole::User => {
                Some(text.as_str())
            }
            _ => None,
        })
        .unwrap_or_default();
    if let Some(path) = prompt.strip_prefix("test:read ") {
        return ModelResponse {
            provider: "test".into(),
            model: request.model.clone(),
            api: "test".into(),
            response_id: None,
            usage: None,
            stop_reason: Some("tool_call".into()),
            parts: vec![ModelPart::ToolCall {
                id: "test-call-1".into(),
                name: "read".into(),
                arguments: json!({"path":path}),
                thought_signature: None,
            }],
        };
    }
    ModelResponse {
        provider: "test".into(),
        model: request.model.clone(),
        api: "test".into(),
        response_id: None,
        usage: None,
        stop_reason: Some("stop".into()),
        parts: vec![ModelPart::Text {
            text: format!("{prompt} [{}]", request.model),
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ModelMessage;

    #[tokio::test]
    async fn test_provider_preserves_tool_calls() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join(".test-provider"), []).unwrap();
        let provider = ProviderRegistry::new(root.path().to_path_buf());
        let response = provider
            .generate(ModelRequest {
                model: "test".into(),
                reasoning: None,
                system: String::new(),
                messages: vec![ModelMessage {
                    role: MessageRole::User,
                    content: MessageContent::Text {
                        text: "test:read a.txt".into(),
                    },
                }],
                tools: Vec::new(),
                continuation: None,
            })
            .await
            .unwrap();
        assert!(matches!(&response.parts[0], ModelPart::ToolCall { name, .. } if name == "read"));
    }

    #[tokio::test]
    async fn test_provider_rejects_reasoning_effort() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join(".test-provider"), []).unwrap();
        let provider = ProviderRegistry::new(root.path().to_path_buf());
        let result = provider
            .generate(ModelRequest {
                model: "test".into(),
                reasoning: Some(crate::ReasoningEffort::Low),
                system: String::new(),
                messages: vec![],
                tools: vec![],
                continuation: None,
            })
            .await;
        assert!(matches!(result, Err(ProviderError::InvalidRequest(_))));
    }

    #[tokio::test]
    async fn catalog_does_not_add_a_synthetic_model() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("models.json"),
            r#"{"providers":{"openai":{"models":[{"id":"openai/example","display_name":"Example"}]}}}"#,
        )
        .unwrap();
        let models = crate::service::load_models(root.path()).unwrap();
        assert_eq!(
            models.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            ["openai/example"]
        );
    }

    /// Opt-in check against a real provider. Example:
    /// MEWS_LIVE_MODEL=openai/gpt-5 cargo test -p mews-router live_provider -- --ignored
    #[tokio::test]
    #[ignore = "requires MEWS_LIVE_MODEL and matching provider credentials"]
    async fn live_provider_returns_text() {
        let model = std::env::var("MEWS_LIVE_MODEL").expect("set MEWS_LIVE_MODEL");
        let root = tempfile::tempdir().unwrap();
        crate::AuthStore::initialize(root.path()).unwrap();
        let provider = ProviderRegistry::new(root.path().to_path_buf());
        let response = provider
            .generate(ModelRequest {
                model,
                reasoning: None,
                system: "Reply with the word ok.".into(),
                messages: vec![crate::ModelMessage {
                    role: MessageRole::User,
                    content: MessageContent::Text {
                        text: "Compatibility check".into(),
                    },
                }],
                tools: vec![],
                continuation: None,
            })
            .await
            .unwrap();
        assert!(
            response
                .parts
                .iter()
                .any(|part| matches!(part, ModelPart::Text { text } if !text.trim().is_empty()))
        );
    }
}
