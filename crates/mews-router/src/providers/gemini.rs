use std::path::Path;

use anyhow::{Context, Result};
use mews_protocol::AuthCredential;
use reqwest::Client;
use serde_json::{Value, json};

use crate::{
    AuthStore, MessageContent, MessageRole, ModelPart, ModelRequest, ModelResponse, ProviderError,
    ProviderResult, ReasoningEffort, http::send_with_retry,
};

const DEFAULT_BASE_URL: &str = "https://generativelanguage.googleapis.com";

pub(crate) async fn generate(
    client: &Client,
    root: &Path,
    request: ModelRequest,
) -> ProviderResult<ModelResponse> {
    let (key, base) = credential(root)?;
    let model = request
        .model
        .split_once('/')
        .map_or(request.model.as_str(), |(_, model)| model);
    let mut body = json!({
        "contents": messages(&request.messages),
        "tools": [{"functionDeclarations": request.tools.into_iter().map(|tool| json!({
            "name": tool.name,
            "description": tool.description,
            "parametersJsonSchema": tool.schema,
        })).collect::<Vec<_>>() }],
    });
    if !request.system.is_empty() {
        body["systemInstruction"] = json!({"parts": [{"text": request.system}]});
    }
    if body
        .pointer("/tools/0/functionDeclarations")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
    {
        body.as_object_mut()
            .expect("Gemini request is an object")
            .remove("tools");
    }
    apply_reasoning(&mut body, model, request.reasoning);

    let response: Value = send_with_retry(
        client
            .post(format!(
                "{}/v1beta/models/{model}:generateContent",
                base.trim_end_matches('/')
            ))
            .header("x-goog-api-key", key)
            .json(&body),
    )
    .await?
    .json()
    .await
    .map_err(|error| ProviderError::InvalidResponse(error.to_string()))?;
    parse(response).map_err(|error| ProviderError::InvalidResponse(error.to_string()))
}

pub(crate) async fn models(client: &Client, root: &Path) -> ProviderResult<Vec<crate::ModelInfo>> {
    let (key, base) = credential(root)?;
    let mut models = Vec::new();
    let mut page_token: Option<String> = None;
    loop {
        let mut request = client
            .get(format!("{}/v1beta/models", base.trim_end_matches('/')))
            .header("x-goog-api-key", &key)
            .query(&[("pageSize", "1000")]);
        if let Some(token) = &page_token {
            request = request.query(&[("pageToken", token)]);
        }
        let payload: Value = send_with_retry(request)
            .await?
            .json()
            .await
            .map_err(|error| ProviderError::InvalidResponse(error.to_string()))?;
        let entries = payload
            .get("models")
            .and_then(Value::as_array)
            .context("Gemini model catalog has no models")
            .map_err(|error| ProviderError::InvalidResponse(error.to_string()))?;
        models.extend(entries.iter().filter_map(model_info));
        page_token = payload
            .get("nextPageToken")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if page_token.is_none() {
            break;
        }
    }
    models.sort_by(|a, b| a.id.cmp(&b.id));
    models.dedup_by(|a, b| a.id == b.id);
    Ok(models)
}

fn credential(root: &Path) -> ProviderResult<(String, String)> {
    match AuthStore::load(root)
        .and_then(|store| store.credential("google"))
        .map_err(|error| ProviderError::Authentication(format!("{error:#}")))?
    {
        AuthCredential::ApiKey { key, base_url } => {
            Ok((key, base_url.unwrap_or_else(|| DEFAULT_BASE_URL.into())))
        }
        AuthCredential::Oauth { .. } => Err(ProviderError::Authentication(
            "Gemini requires an API key".into(),
        )),
    }
}

fn messages(messages: &[crate::ModelMessage]) -> Vec<Value> {
    messages
        .iter()
        .filter_map(|message| {
            Some(match &message.content {
                MessageContent::Text { text } => json!({
                    "role": if message.role == MessageRole::Assistant { "model" } else { "user" },
                    "parts": [{"text": text}],
                }),
                MessageContent::ToolCall {
                    call_id,
                    tool,
                    arguments,
                    thought_signature,
                } => {
                    let mut part = json!({
                        "functionCall": {"id": call_id, "name": tool, "args": arguments}
                    });
                    if let Some(signature) = thought_signature {
                        part["thoughtSignature"] = Value::String(signature.clone());
                    }
                    json!({"role": "model", "parts": [part]})
                }
                MessageContent::ToolResult {
                    call_id,
                    tool,
                    result,
                    ..
                } => json!({
                    "role": "user",
                    "parts": [{"functionResponse": {
                        "id": call_id,
                        "name": tool,
                        "response": {"result": result},
                    }}],
                }),
                MessageContent::ProviderState { .. } => return None,
            })
        })
        .collect()
}

fn parse(response: Value) -> Result<ModelResponse> {
    let parts = response
        .pointer("/candidates/0/content/parts")
        .and_then(Value::as_array)
        .context("Gemini response has no candidate content")?
        .iter()
        .filter_map(|part| {
            if let Some(text) = part.get("text").and_then(Value::as_str) {
                return Some(ModelPart::Text { text: text.into() });
            }
            let call = part.get("functionCall")?;
            Some(ModelPart::ToolCall {
                id: call
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| uuid::Uuid::now_v7().to_string()),
                name: call.get("name")?.as_str()?.to_owned(),
                arguments: call.get("args").cloned().unwrap_or_else(|| json!({})),
                thought_signature: part
                    .get("thoughtSignature")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            })
        })
        .collect();
    Ok(ModelResponse { parts })
}

fn model_info(entry: &Value) -> Option<crate::ModelInfo> {
    let supported = entry.get("supportedGenerationMethods")?.as_array()?;
    if !supported.iter().any(|method| method == "generateContent") {
        return None;
    }
    let name = entry.get("name")?.as_str()?.strip_prefix("models/")?;
    if !name.starts_with("gemini-") {
        return None;
    }
    let reasoning = if name.starts_with("gemini-2.5")
        || name.starts_with("gemini-3")
        || name.contains("thinking")
    {
        vec![
            ReasoningEffort::None,
            ReasoningEffort::Auto,
            ReasoningEffort::Minimal,
            ReasoningEffort::Low,
            ReasoningEffort::Medium,
            ReasoningEffort::High,
            ReasoningEffort::XHigh,
            ReasoningEffort::Max,
        ]
    } else {
        Vec::new()
    };
    Some(crate::ModelInfo {
        id: format!("google/{name}"),
        display_name: entry
            .get("displayName")
            .and_then(Value::as_str)
            .map(str::to_owned),
        reasoning,
        default_reasoning: None,
    })
}

fn apply_reasoning(body: &mut Value, model: &str, effort: Option<ReasoningEffort>) {
    let Some(effort) = effort else { return };
    let config = if model.starts_with("gemini-3") {
        let level = match effort {
            ReasoningEffort::None => "none",
            ReasoningEffort::Auto => "auto",
            ReasoningEffort::Minimal => "minimal",
            ReasoningEffort::Low => "low",
            ReasoningEffort::Medium => "medium",
            ReasoningEffort::High | ReasoningEffort::XHigh | ReasoningEffort::Max => "high",
        };
        json!({"thinkingLevel": level})
    } else {
        let budget: i64 = match effort {
            ReasoningEffort::None => 0,
            ReasoningEffort::Auto => -1,
            ReasoningEffort::Minimal => 512,
            ReasoningEffort::Low => 1024,
            ReasoningEffort::Medium => 8192,
            ReasoningEffort::High => 24_576,
            ReasoningEffort::XHigh | ReasoningEffort::Max => 32_768,
        };
        json!({"thinkingBudget": budget})
    };
    body["generationConfig"]["thinkingConfig"] = config;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ModelMessage;
    use mews_protocol::ToolDefinition;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn translates_and_parses_tool_calls() {
        let input = messages(&[ModelMessage {
            role: MessageRole::Tool,
            content: MessageContent::ToolResult {
                call_id: "call-1".into(),
                tool: "read".into(),
                result: json!({"content": "hello"}),
                is_error: false,
            },
        }]);
        assert_eq!(input[0]["parts"][0]["functionResponse"]["id"], "call-1");
        let response = parse(json!({"candidates":[{"content":{"parts":[
            {"text":"checking"},
            {"thoughtSignature":"signed-thought","functionCall":{"id":"call-2","name":"write","args":{"path":"b"}}}
        ]}}]}))
        .unwrap();
        assert!(
            matches!(&response.parts[1], ModelPart::ToolCall { id, name, arguments, thought_signature } if id == "call-2" && name == "write" && arguments["path"] == "b" && thought_signature.as_deref() == Some("signed-thought"))
        );

        let replay = messages(&[ModelMessage {
            role: MessageRole::Assistant,
            content: MessageContent::ToolCall {
                call_id: "call-2".into(),
                tool: "write".into(),
                arguments: json!({"path": "b"}),
                thought_signature: Some("signed-thought".into()),
            },
        }]);
        assert_eq!(replay[0]["parts"][0]["thoughtSignature"], "signed-thought");
    }

    #[test]
    fn uses_level_reasoning_for_gemini_3_and_budget_for_older_models() {
        let mut current = json!({});
        apply_reasoning(&mut current, "gemini-3-pro", Some(ReasoningEffort::High));
        assert_eq!(
            current["generationConfig"]["thinkingConfig"]["thinkingLevel"],
            "high"
        );
        let mut legacy = json!({});
        apply_reasoning(&mut legacy, "gemini-2.5-pro", Some(ReasoningEffort::Auto));
        assert_eq!(
            legacy["generationConfig"]["thinkingConfig"]["thinkingBudget"],
            -1
        );
    }

    #[test]
    fn filters_the_model_catalog() {
        assert!(
            model_info(&json!({
                "name":"models/gemini-2.5-pro", "displayName":"Gemini 2.5 Pro",
                "supportedGenerationMethods":["generateContent"]
            }))
            .is_some()
        );
        assert!(
            model_info(&json!({
                "name":"models/embedding-001", "supportedGenerationMethods":["embedContent"]
            }))
            .is_none()
        );
    }

    #[tokio::test]
    async fn calls_native_gemini_api_with_key_header() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0_u8; 8192];
            let read = stream.read(&mut request).await.unwrap();
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(request.starts_with("POST /v1beta/models/gemini-test:generateContent "));
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("x-goog-api-key: secret")
            );
            assert!(request.contains("systemInstruction"));
            let payload: Value = serde_json::from_str(
                request
                    .split_once("\r\n\r\n")
                    .expect("HTTP request has a body")
                    .1,
            )
            .unwrap();
            let declaration = &payload["tools"][0]["functionDeclarations"][0];
            assert!(declaration.get("parameters").is_none());
            assert_eq!(
                declaration["parametersJsonSchema"]["additionalProperties"],
                false
            );
            assert_eq!(
                declaration["parametersJsonSchema"]["properties"]["timeout_seconds"]["type"],
                json!(["integer", "null"])
            );
            let body = r#"{"candidates":[{"content":{"parts":[{"text":"hello"}]}}]}"#;
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let root = tempfile::tempdir().unwrap();
        AuthStore::set(
            root.path(),
            "google",
            &AuthCredential::ApiKey {
                key: "secret".into(),
                base_url: Some(format!("http://{address}")),
            },
        )
        .unwrap();
        let response = generate(
            &Client::new(),
            root.path(),
            ModelRequest {
                model: "google/gemini-test".into(),
                reasoning: None,
                system: "Be concise".into(),
                messages: vec![ModelMessage {
                    role: MessageRole::User,
                    content: MessageContent::Text { text: "Hi".into() },
                }],
                tools: vec![ToolDefinition {
                    name: "bash".into(),
                    description: "Run a command".into(),
                    schema: json!({
                        "type": "object",
                        "properties": {
                            "command": {"type": "string"},
                            "timeout_seconds": {"type": ["integer", "null"]}
                        },
                        "required": ["command", "timeout_seconds"],
                        "additionalProperties": false
                    }),
                }],
            },
        )
        .await
        .unwrap();
        assert_eq!(
            response.parts,
            vec![ModelPart::Text {
                text: "hello".into()
            }]
        );
        server.await.unwrap();
    }
}
