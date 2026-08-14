use std::{path::Path, time::Duration};

use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use mews_protocol::AuthCredential;
use reqwest::Client;
use serde_json::{Value, json};

use crate::{
    AuthStore, MessageContent, MessageRole, ModelPart, ModelRequest, ModelResponse, ModelStream,
    ModelStreamEvent, ProviderError, ProviderResult, ReasoningEffort,
    http::{response_json, response_text_limited, send_with_retry},
};

const CODEX_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const CODEX_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
// The Codex catalog filters models by the requesting client's protocol version.
const CODEX_CLIENT_VERSION: &str = "0.147.0";
const MAX_NONSTREAM_BODY_BYTES: usize = 8 * 1024 * 1024;
const MAX_SSE_EVENT_BYTES: usize = 1024 * 1024;
static CODEX_REFRESH: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DeviceAuthorization {
    pub verification_uri: String,
    pub user_code: String,
}

pub(crate) async fn generate(
    client: &Client,
    root: &Path,
    provider: &str,
    request: ModelRequest,
) -> ProviderResult<ModelResponse> {
    let credential = current_credential(root, provider, client)
        .await
        .map_err(|error| ProviderError::Authentication(format!("{error:#}")))?;
    let (key, base, account_id, codex) = match credential {
        AuthCredential::ApiKey { key, base_url } => (
            key,
            base_url.unwrap_or_else(|| "https://api.openai.com/v1".into()),
            None,
            false,
        ),
        AuthCredential::Oauth {
            access, account_id, ..
        } => (
            access,
            "https://chatgpt.com/backend-api".into(),
            Some(account_id),
            true,
        ),
    };
    let model = request
        .model
        .split_once('/')
        .map_or(request.model.as_str(), |(_, model)| model)
        .to_owned();
    let include_reasoning = codex
        || request
            .reasoning
            .is_some_and(|effort| effort != ReasoningEffort::None);
    let mut body = json!({
        "model": &model,
        "instructions": request.system,
        "input": messages(&request.messages, provider, &model).map_err(|error| ProviderError::InvalidRequest(error.to_string()))?,
        "tools": request.tools.into_iter().map(|tool| json!({
            "type":"function", "name":tool.name, "description":tool.description,
            "parameters":tool.schema, "strict":true
        })).collect::<Vec<_>>(),
        "store": !codex,
    });
    if let Some(cursor) = request.continuation.as_ref().filter(|cursor| {
        !codex && cursor.provider == provider && cursor.model == model && cursor.api == "responses"
    }) {
        body["previous_response_id"] = Value::String(cursor.response_id.clone());
    }
    if include_reasoning {
        body["include"] = json!(["reasoning.encrypted_content"]);
    }
    if codex {
        body["parallel_tool_calls"] = Value::Bool(true);
    }
    apply_reasoning(&mut body, request.reasoning)?;
    if codex {
        body["stream"] = Value::Bool(true);
    }
    let endpoint = if codex {
        "codex/responses"
    } else {
        "responses"
    };
    let mut call = client
        .post(format!("{}/{endpoint}", base.trim_end_matches('/')))
        .bearer_auth(key)
        .json(&body);
    if let Some(account_id) = account_id {
        call = call
            .header("chatgpt-account-id", account_id)
            .header("originator", "mews")
            .header("OpenAI-Beta", "responses=experimental");
    }
    let response = send_with_retry(call).await?;
    if codex {
        let body = response_text_limited(response, MAX_NONSTREAM_BODY_BYTES).await?;
        parse_sse(&body, provider, &model)
            .map_err(|error| ProviderError::InvalidResponse(error.to_string()))
    } else {
        let value = response_json(response).await?;
        parse(value, provider, &model)
            .map_err(|error| ProviderError::InvalidResponse(error.to_string()))
    }
}

/// Streams OpenAI Responses API text and completed function calls as they arrive.
pub(crate) async fn stream(
    client: &Client,
    root: &Path,
    provider: &str,
    request: ModelRequest,
) -> ProviderResult<ModelStream> {
    let credential = current_credential(root, provider, client)
        .await
        .map_err(|error| ProviderError::Authentication(format!("{error:#}")))?;
    let (key, base, account_id, codex) = match credential {
        AuthCredential::ApiKey { key, base_url } => (
            key,
            base_url.unwrap_or_else(|| "https://api.openai.com/v1".into()),
            None,
            false,
        ),
        AuthCredential::Oauth {
            access, account_id, ..
        } => (
            access,
            "https://chatgpt.com/backend-api".into(),
            Some(account_id),
            true,
        ),
    };
    let model = request
        .model
        .split_once('/')
        .map_or(request.model.as_str(), |(_, model)| model)
        .to_owned();
    let include_reasoning = codex
        || request
            .reasoning
            .is_some_and(|effort| effort != ReasoningEffort::None);
    let mut body = json!({
        "model": &model,
        "instructions": request.system,
        "input": messages(&request.messages, provider, &model).map_err(|error| ProviderError::InvalidRequest(error.to_string()))?,
        "tools": request.tools.into_iter().map(|tool| json!({
            "type":"function", "name":tool.name, "description":tool.description,
            "parameters":tool.schema, "strict":true
        })).collect::<Vec<_>>(),
        "store": !codex,
        "stream": true,
    });
    if let Some(cursor) = request.continuation.as_ref().filter(|cursor| {
        !codex && cursor.provider == provider && cursor.model == model && cursor.api == "responses"
    }) {
        body["previous_response_id"] = Value::String(cursor.response_id.clone());
    }
    if include_reasoning {
        body["include"] = json!(["reasoning.encrypted_content"]);
    }
    if codex {
        body["parallel_tool_calls"] = Value::Bool(true);
    }
    apply_reasoning(&mut body, request.reasoning)?;
    let endpoint = if codex {
        "codex/responses"
    } else {
        "responses"
    };
    let mut call = client
        .post(format!("{}/{endpoint}", base.trim_end_matches('/')))
        .bearer_auth(key)
        .json(&body);
    if let Some(account_id) = account_id {
        call = call
            .header("chatgpt-account-id", account_id)
            .header("originator", "mews")
            .header("OpenAI-Beta", "responses=experimental");
    }
    let response = send_with_retry(call).await?;
    Ok(openai_response_stream(response, provider.to_owned(), model))
}

fn openai_response_stream(
    response: reqwest::Response,
    state_provider: String,
    state_model: String,
) -> ModelStream {
    let (sender, receiver) = tokio::sync::mpsc::channel(32);
    tokio::spawn(async move {
        use futures_util::StreamExt;
        let _ = sender.send(Ok(ModelStreamEvent::Start)).await;
        let mut bytes = response.bytes_stream();
        let mut buffer = Vec::new();
        while let Some(chunk) = bytes.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    let _ = sender
                        .send(Err(ProviderError::Http(error.to_string())))
                        .await;
                    return;
                }
            };
            buffer.extend_from_slice(&chunk);
            while let Some((end, delimiter_len)) = sse_frame_end(&buffer) {
                if end > MAX_SSE_EVENT_BYTES {
                    let _ = sender.send(Err(sse_event_too_large())).await;
                    return;
                }
                let block = match String::from_utf8(buffer[..end].to_vec()) {
                    Ok(block) => block.replace('\r', ""),
                    Err(error) => {
                        let _ = sender
                            .send(Err(ProviderError::InvalidResponse(format!(
                                "OpenAI stream is not UTF-8: {error}"
                            ))))
                            .await;
                        return;
                    }
                };
                buffer.drain(..end + delimiter_len);
                let Some(data) = block.lines().find_map(|line| line.strip_prefix("data: ")) else {
                    continue;
                };
                if data == "[DONE]" {
                    continue;
                }
                let event = match serde_json::from_str::<Value>(data) {
                    Ok(event) => event,
                    Err(error) => {
                        let _ = sender
                            .send(Err(ProviderError::InvalidResponse(format!(
                                "invalid OpenAI stream event: {error}"
                            ))))
                            .await;
                        return;
                    }
                };
                let event_type = event.get("type").and_then(Value::as_str);
                if matches!(
                    event_type,
                    Some("response.failed" | "response.incomplete" | "error")
                ) {
                    let _ = sender.send(Err(openai_stream_error(&event))).await;
                    return;
                }
                let item = match event_type {
                    Some("response.created") => {
                        event
                            .get("response")
                            .map(|response| ModelStreamEvent::ResponseMetadata {
                                provider: state_provider.clone(),
                                model: state_model.clone(),
                                api: "responses".into(),
                                response_id: response
                                    .get("id")
                                    .and_then(Value::as_str)
                                    .map(str::to_owned),
                            })
                    }
                    Some("response.output_text.delta") => event
                        .get("delta")
                        .and_then(Value::as_str)
                        .map(|delta| ModelStreamEvent::TextDelta(delta.to_owned())),
                    Some("response.reasoning_summary_text.delta") => event
                        .get("delta")
                        .and_then(Value::as_str)
                        .map(|text| ModelStreamEvent::Reasoning {
                            text: text.to_owned(),
                            signature: None,
                        }),
                    Some("response.output_item.done")
                        if event.pointer("/item/type").and_then(Value::as_str)
                            == Some("function_call") =>
                    {
                        event.get("item").and_then(|item| {
                            Some(ModelStreamEvent::ToolCall {
                                id: item.get("call_id")?.as_str()?.to_owned(),
                                name: item.get("name")?.as_str()?.to_owned(),
                                arguments: serde_json::from_str(item.get("arguments")?.as_str()?)
                                    .ok()?,
                                thought_signature: None,
                            })
                        })
                    }
                    Some("response.output_item.done")
                        if event.pointer("/item/type").and_then(Value::as_str)
                            == Some("reasoning") =>
                    {
                        event.get("item").and_then(|item| {
                            item.get("encrypted_content")
                                .and_then(Value::as_str)
                                .filter(|value| !value.is_empty())?;
                            Some(ModelStreamEvent::ProviderState {
                                provider: state_provider.clone(),
                                model: state_model.clone(),
                                data: item.clone(),
                            })
                        })
                    }
                    Some("response.completed") => {
                        let response = event.get("response").unwrap_or(&Value::Null);
                        let _ = sender
                            .send(Ok(ModelStreamEvent::ResponseCompleted {
                                usage: openai_usage(response),
                                stop_reason: response
                                    .get("status")
                                    .and_then(Value::as_str)
                                    .map(str::to_owned),
                            }))
                            .await;
                        let _ = sender.send(Ok(ModelStreamEvent::Done)).await;
                        return;
                    }
                    _ => None,
                };
                if let Some(item) = item
                    && sender.send(Ok(item)).await.is_err()
                {
                    return;
                }
            }
            // Completed frames were drained above, so `buffer` now contains only
            // an incomplete event. Cap it here as well: a peer may never send a
            // delimiter, and waiting for `sse_frame_end` must not grow memory
            // without bound.
            if buffer.len() > MAX_SSE_EVENT_BYTES {
                let _ = sender.send(Err(sse_event_too_large())).await;
                return;
            }
        }
        let _ = sender
            .send(Err(ProviderError::InvalidResponse(
                "OpenAI stream ended before response.completed".into(),
            )))
            .await;
    });
    Box::pin(futures_util::stream::unfold(
        receiver,
        |mut receiver| async { receiver.recv().await.map(|event| (event, receiver)) },
    ))
}

fn sse_event_too_large() -> ProviderError {
    ProviderError::InvalidResponse(format!(
        "OpenAI stream event exceeds {MAX_SSE_EVENT_BYTES} bytes"
    ))
}

fn openai_stream_error(event: &Value) -> ProviderError {
    let event_type = event.get("type").and_then(Value::as_str).unwrap_or("error");
    let detail = [
        "/response/error/message",
        "/response/incomplete_details/reason",
        "/error/message",
        "/message",
    ]
    .into_iter()
    .find_map(|pointer| event.pointer(pointer).and_then(Value::as_str))
    .unwrap_or("provider reported a terminal error");
    ProviderError::InvalidResponse(format!("OpenAI {event_type}: {detail}"))
}

fn sse_frame_end(buffer: &[u8]) -> Option<(usize, usize)> {
    buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|end| (end, 4))
        .or_else(|| {
            buffer
                .windows(2)
                .position(|window| window == b"\n\n")
                .map(|end| (end, 2))
        })
}

pub(crate) async fn models(
    client: &Client,
    root: &Path,
    provider: &str,
) -> ProviderResult<Vec<crate::ModelInfo>> {
    let credential = current_credential(root, provider, client)
        .await
        .map_err(|error| ProviderError::Authentication(format!("{error:#}")))?;
    let (token, url, codex, account_id) = match credential {
        AuthCredential::ApiKey { key, base_url } => (
            key,
            format!(
                "{}/models",
                base_url
                    .unwrap_or_else(|| "https://api.openai.com/v1".into())
                    .trim_end_matches('/')
            ),
            false,
            None,
        ),
        AuthCredential::Oauth {
            access, account_id, ..
        } => (
            access,
            "https://chatgpt.com/backend-api/codex/models".into(),
            true,
            Some(account_id),
        ),
    };
    let mut request = add_codex_client_version(client.get(url).bearer_auth(token), codex);
    if let Some(account_id) = account_id {
        request = request
            .header("chatgpt-account-id", account_id)
            .header("originator", "mews");
    }
    let response = send_with_retry(request).await?;
    let payload: Value = response_json(response).await?;
    let entries = payload
        .get(if codex { "models" } else { "data" })
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ProviderError::InvalidResponse("OpenAI model catalog has no models".into())
        })?;
    let mut models = entries
        .iter()
        .filter_map(|entry| {
            let raw_id = entry.get(if codex { "slug" } else { "id" })?.as_str()?;
            if !codex && !openai_agent_model(raw_id) {
                return None;
            }
            let reasoning = entry
                .get("supported_reasoning_levels")
                .and_then(Value::as_array)
                .map(|levels| {
                    levels
                        .iter()
                        .filter_map(|level| {
                            parse_reasoning(level.get("effort").unwrap_or(level).as_str()?)
                        })
                        .collect()
                })
                .unwrap_or_default();
            Some(crate::ModelInfo {
                id: format!("{provider}/{raw_id}"),
                display_name: entry
                    .get("display_name")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                reasoning,
                default_reasoning: entry
                    .get("default_reasoning_level")
                    .and_then(Value::as_str)
                    .and_then(parse_reasoning),
            })
        })
        .collect::<Vec<_>>();
    models.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(models)
}

fn add_codex_client_version(
    request: reqwest::RequestBuilder,
    codex: bool,
) -> reqwest::RequestBuilder {
    if codex {
        request.query(&[("client_version", CODEX_CLIENT_VERSION)])
    } else {
        request
    }
}

fn openai_agent_model(id: &str) -> bool {
    ["gpt-", "o1", "o3", "o4", "codex-"]
        .iter()
        .any(|prefix| id.starts_with(prefix))
        && !["audio", "realtime", "transcribe", "tts", "image", "search"]
            .iter()
            .any(|kind| id.contains(kind))
}

fn parse_reasoning(value: &str) -> Option<ReasoningEffort> {
    Some(match value {
        "none" => ReasoningEffort::None,
        "auto" => ReasoningEffort::Auto,
        "minimal" => ReasoningEffort::Minimal,
        "low" => ReasoningEffort::Low,
        "medium" => ReasoningEffort::Medium,
        "high" => ReasoningEffort::High,
        "xhigh" => ReasoningEffort::XHigh,
        "max" | "ultra" => ReasoningEffort::Max,
        _ => return None,
    })
}

fn apply_reasoning(body: &mut Value, effort: Option<ReasoningEffort>) -> ProviderResult<()> {
    let Some(effort) = effort else {
        return Ok(());
    };
    let effort = match effort {
        ReasoningEffort::None => "none",
        ReasoningEffort::Minimal => "minimal",
        ReasoningEffort::Low => "low",
        ReasoningEffort::Medium => "medium",
        ReasoningEffort::High => "high",
        ReasoningEffort::XHigh => "xhigh",
        ReasoningEffort::Max => "max",
        ReasoningEffort::Auto => {
            return Err(ProviderError::InvalidRequest(
                "OpenAI does not support reasoning effort auto; omit reasoning to use the model default"
                    .into(),
            ));
        }
    };
    body["reasoning"] = json!({ "effort": effort });
    Ok(())
}

fn messages(messages: &[crate::ModelMessage], provider: &str, model: &str) -> Result<Vec<Value>> {
    messages
        .iter()
        .filter_map(|message| {
            Some(Ok(match &message.content {
                MessageContent::Text { text } => json!({"role":match message.role { MessageRole::User => "user", _ => "assistant" },"content":[{"type":if message.role == MessageRole::User {"input_text"} else {"output_text"},"text":text}]}),
                MessageContent::ToolCall { call_id, tool, arguments, .. } => json!({"type":"function_call","call_id":call_id,"name":tool,"arguments":serde_json::to_string(arguments).ok()?}),
                MessageContent::ToolResult { call_id, result, is_error, uncertain, .. } => json!({"type":"function_call_output","call_id":call_id,"output":serde_json::to_string(&mews_agent::tool_result_for_model(result, *is_error, *uncertain)).ok()?}),
                MessageContent::ProviderState { provider: state_provider, model: state_model, data }
                    if state_provider == provider && state_model == model => data.clone(),
                MessageContent::ProviderState { .. } => return None,
            }))
        })
        .collect()
}

fn parse(response: Value, provider: &str, model: &str) -> Result<ModelResponse> {
    let output = response
        .get("output")
        .and_then(Value::as_array)
        .context("OpenAI response has no output")?;
    let mut parts = Vec::new();
    for part in output {
        match part.get("type").and_then(Value::as_str) {
            Some("message") => {
                for content in part
                    .get("content")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    if let Some(text) = content.get("text").and_then(Value::as_str) {
                        parts.push(ModelPart::Text {
                            text: text.to_owned(),
                        });
                    }
                }
            }
            Some("function_call") => {
                if let (Some(id), Some(name), Some(arguments)) = (
                    part.get("call_id").and_then(Value::as_str),
                    part.get("name").and_then(Value::as_str),
                    part.get("arguments")
                        .and_then(Value::as_str)
                        .and_then(|value| serde_json::from_str(value).ok()),
                ) {
                    parts.push(ModelPart::ToolCall {
                        id: id.into(),
                        name: name.into(),
                        arguments,
                        thought_signature: None,
                    });
                }
            }
            Some("reasoning") => {
                for summary in part
                    .get("summary")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    if let Some(text) = summary.get("text").and_then(Value::as_str) {
                        parts.push(ModelPart::Reasoning {
                            text: text.into(),
                            signature: None,
                        });
                    }
                }
                if part
                    .get("encrypted_content")
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.is_empty())
                {
                    parts.push(ModelPart::ProviderState {
                        provider: provider.into(),
                        model: model.into(),
                        data: part.clone(),
                    });
                }
            }
            _ => {}
        }
    }
    Ok(ModelResponse {
        provider: provider.into(),
        model: model.into(),
        api: "responses".into(),
        response_id: response
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        usage: openai_usage(&response),
        stop_reason: response
            .get("status")
            .and_then(Value::as_str)
            .map(str::to_owned),
        parts,
    })
}

fn openai_usage(response: &Value) -> Option<mews_protocol::ModelUsage> {
    let usage = response.get("usage")?;
    Some(mews_protocol::ModelUsage {
        input_tokens: usage
            .get("input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        output_tokens: usage
            .get("output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cached_input_tokens: usage
            .pointer("/input_tokens_details/cached_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        reasoning_tokens: usage
            .pointer("/output_tokens_details/reasoning_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
    })
}

fn parse_sse(body: &str, provider: &str, model: &str) -> Result<ModelResponse> {
    let body = body.replace("\r\n", "\n");
    for block in body.split("\n\n") {
        anyhow::ensure!(
            block.len() <= MAX_SSE_EVENT_BYTES,
            "OpenAI stream event exceeds {MAX_SSE_EVENT_BYTES} bytes"
        );
        let Some(data) = block.lines().find_map(|line| {
            line.strip_suffix('\r')
                .unwrap_or(line)
                .strip_prefix("data: ")
        }) else {
            continue;
        };
        if data == "[DONE]" {
            continue;
        }
        let event: Value = serde_json::from_str(data).context("invalid OpenAI stream event")?;
        match event.get("type").and_then(Value::as_str) {
            Some("response.completed") => {
                return parse(
                    event
                        .get("response")
                        .cloned()
                        .context("completed OpenAI response has no response body")?,
                    provider,
                    model,
                );
            }
            Some("response.failed" | "response.incomplete" | "error") => {
                anyhow::bail!(openai_stream_error(&event));
            }
            _ => {}
        }
    }
    anyhow::bail!("OpenAI Codex stream ended without a completed response")
}

pub async fn login_openai<F>(mut notify: F) -> Result<AuthCredential>
where
    F: FnMut(DeviceAuthorization),
{
    let (_cancel, cancellation) = tokio::sync::watch::channel(false);
    login_with_client(
        &Client::new(),
        &mut notify,
        &cancellation,
        &OpenAiAuthEndpoints::default(),
    )
    .await
    .map_err(anyhow::Error::new)
}

async fn login_with_client<F>(
    client: &Client,
    notify: &mut F,
    cancellation: &tokio::sync::watch::Receiver<bool>,
    endpoints: &OpenAiAuthEndpoints,
) -> ProviderResult<AuthCredential>
where
    F: FnMut(DeviceAuthorization),
{
    let response: Value = response_json(
        send_with_retry(
            client
                .post(&endpoints.user_code)
                .json(&json!({"client_id":CODEX_CLIENT_ID})),
        )
        .await?,
    )
    .await?;
    let device_auth_id = response
        .get("device_auth_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ProviderError::InvalidResponse("device login response has no device_auth_id".into())
        })?
        .to_owned();
    let user_code = response
        .get("user_code")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ProviderError::InvalidResponse("device login response has no user_code".into())
        })?
        .to_owned();
    let mut interval = response
        .get("interval")
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
        .unwrap_or(5);
    notify(DeviceAuthorization {
        verification_uri: "https://auth.openai.com/codex/device".into(),
        user_code: user_code.clone(),
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15 * 60);
    let mut cancellation = cancellation.clone();
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(ProviderError::Authentication(
                "OpenAI Codex device login timed out".into(),
            ));
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(interval.max(1))) => {}
            changed = cancellation.changed() => {
                if changed.is_err() || *cancellation.borrow() {
                    return Err(ProviderError::Cancelled);
                }
                continue;
            }
        }
        let response = client
            .post(&endpoints.device_token)
            .json(&json!({"device_auth_id":device_auth_id,"user_code":user_code}))
            .send()
            .await
            .map_err(|error| ProviderError::Http(error.to_string()))?;
        let status = response.status();
        let value: Value = response_json(response).await.unwrap_or(Value::Null);
        let state = value.get("error").and_then(Value::as_str);
        if matches!(state, Some("authorization_pending")) || matches!(status.as_u16(), 403 | 404) {
            continue;
        }
        if state == Some("slow_down") {
            interval = interval.saturating_add(5);
            continue;
        }
        if let Some(error) = state {
            return Err(ProviderError::Authentication(format!(
                "device login failed: {error}"
            )));
        }
        if !status.is_success() {
            return Err(ProviderError::Http(format!(
                "device login returned HTTP {status}"
            )));
        }
        let code = value
            .get("authorization_code")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ProviderError::InvalidResponse("device token has no authorization_code".into())
            })?;
        let verifier = value
            .get("code_verifier")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                ProviderError::InvalidResponse("device token has no code_verifier".into())
            })?;
        let token = exchange_code(client, code, verifier, &endpoints.oauth_token)
            .await
            .map_err(|error| ProviderError::Authentication(format!("{error:#}")))?;
        return credential_from_token(token)
            .map_err(|error| ProviderError::InvalidResponse(error.to_string()));
    }
}

/// Device login variant that can be cancelled without aborting its task.
pub async fn login_openai_cancellable<F>(
    mut notify: F,
    cancellation: &tokio::sync::watch::Receiver<bool>,
) -> ProviderResult<AuthCredential>
where
    F: FnMut(DeviceAuthorization),
{
    login_with_client(
        &Client::new(),
        &mut notify,
        cancellation,
        &OpenAiAuthEndpoints::default(),
    )
    .await
}

#[derive(Clone)]
struct OpenAiAuthEndpoints {
    user_code: String,
    device_token: String,
    oauth_token: String,
}

impl Default for OpenAiAuthEndpoints {
    fn default() -> Self {
        Self {
            user_code: "https://auth.openai.com/api/accounts/deviceauth/usercode".into(),
            device_token: "https://auth.openai.com/api/accounts/deviceauth/token".into(),
            oauth_token: CODEX_TOKEN_URL.into(),
        }
    }
}

async fn exchange_code(
    client: &Client,
    code: &str,
    verifier: &str,
    token_url: &str,
) -> Result<Value> {
    let response = client
        .post(token_url)
        .form(&[
            ("grant_type", "authorization_code"),
            ("client_id", CODEX_CLIENT_ID),
            ("code", code),
            ("code_verifier", verifier),
            (
                "redirect_uri",
                "https://auth.openai.com/deviceauth/callback",
            ),
        ])
        .send()
        .await?
        .error_for_status()?;
    Ok(response_json(response).await?)
}

async fn refresh(client: &Client, refresh: &str) -> Result<AuthCredential> {
    let response = client
        .post(CODEX_TOKEN_URL)
        .form(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh),
            ("client_id", CODEX_CLIENT_ID),
        ])
        .send()
        .await?
        .error_for_status()?;
    let token = response_json(response).await?;
    credential_from_token(token)
}

fn credential_from_token(token: Value) -> Result<AuthCredential> {
    let access = token
        .get("access_token")
        .and_then(Value::as_str)
        .context("token has no access_token")?
        .to_owned();
    let refresh = token
        .get("refresh_token")
        .and_then(Value::as_str)
        .context("token has no refresh_token")?
        .to_owned();
    let expires = now_ms()
        + token
            .get("expires_in")
            .and_then(Value::as_u64)
            .context("token has no expires_in")?
            * 1000;
    let payload = access
        .split('.')
        .nth(1)
        .context("access token is not a JWT")?;
    let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload)?)?;
    let account_id = claims
        .pointer("/https:~1~1api.openai.com~1auth/chatgpt_account_id")
        .and_then(Value::as_str)
        .context("access token has no ChatGPT account ID")?
        .to_owned();
    Ok(AuthCredential::Oauth {
        access,
        refresh,
        expires,
        account_id,
    })
}

async fn current_credential(
    root: &Path,
    provider: &str,
    client: &Client,
) -> Result<AuthCredential> {
    let mut credential = AuthStore::load(root)?.credential(provider)?;
    if let AuthCredential::Oauth {
        refresh: _,
        expires,
        ..
    } = &credential
        && *expires <= now_ms() + 60_000
    {
        let _guard = CODEX_REFRESH.lock().await;
        // Another request may have refreshed while this request waited.
        credential = AuthStore::load(root)?.credential(provider)?;
        let AuthCredential::Oauth {
            refresh: token,
            expires,
            ..
        } = &credential
        else {
            return Ok(credential);
        };
        if *expires > now_ms() + 60_000 {
            return Ok(credential);
        }
        let refreshed = refresh(client, token).await?;
        AuthStore::set(root, provider, &refreshed)?;
        return Ok(refreshed);
    }
    Ok(credential)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_model_catalog_sends_client_version() {
        let request = add_codex_client_version(
            reqwest::Client::new().get("https://example.com/models"),
            true,
        )
        .build()
        .unwrap();

        assert_eq!(request.url().query(), Some("client_version=0.147.0"));
    }

    async fn collect_stream(body: String) -> Vec<ProviderResult<ModelStreamEvent>> {
        use futures_util::StreamExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request).await.unwrap();
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(headers.as_bytes()).await;
            let _ = stream.write_all(body.as_bytes()).await;
        });
        let response = reqwest::Client::new()
            .get(format!("http://{address}"))
            .send()
            .await
            .unwrap();
        let events = openai_response_stream(response, "openai".into(), "gpt-test".into())
            .collect()
            .await;
        server.await.unwrap();
        events
    }

    #[test]
    fn sse_framing_waits_for_complete_utf8_and_accepts_crlf() {
        let event = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Привет\"}\r\n\r\n";
        let bytes = event.as_bytes();
        let split = event.find('П').unwrap() + 1;
        assert_eq!(sse_frame_end(&bytes[..split]), None);
        assert_eq!(sse_frame_end(bytes), Some((bytes.len() - 4, 4)));
        assert!(std::str::from_utf8(&bytes[..bytes.len() - 4]).is_ok());
    }

    #[tokio::test]
    async fn stream_requires_explicit_completion() {
        let events = collect_stream(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n".into(),
        )
        .await;

        assert!(matches!(events[0], Ok(ModelStreamEvent::Start)));
        assert!(matches!(
            &events[1],
            Ok(ModelStreamEvent::TextDelta(text)) if text == "partial"
        ));
        assert!(matches!(
            &events[2],
            Err(ProviderError::InvalidResponse(message))
                if message.contains("before response.completed")
        ));
        assert_eq!(events.len(), 3);
    }

    #[tokio::test]
    async fn stream_completion_is_terminal() {
        let events = collect_stream(
            concat!(
                "data: {\"type\":\"response.completed\"}\n\n",
                "data: {\"type\":\"error\",\"message\":\"late\"}\n\n"
            )
            .into(),
        )
        .await;

        assert!(matches!(
            events.as_slice(),
            [
                Ok(ModelStreamEvent::Start),
                Ok(ModelStreamEvent::ResponseCompleted { .. }),
                Ok(ModelStreamEvent::Done)
            ]
        ));
    }

    #[tokio::test]
    async fn stream_maps_openai_terminal_errors() {
        let cases = [
            (
                r#"{"type":"response.failed","response":{"error":{"message":"failed"}}}"#,
                "response.failed: failed",
            ),
            (
                r#"{"type":"response.incomplete","response":{"incomplete_details":{"reason":"max_output_tokens"}}}"#,
                "response.incomplete: max_output_tokens",
            ),
            (
                r#"{"type":"error","message":"overloaded"}"#,
                "error: overloaded",
            ),
        ];

        for (event, expected) in cases {
            let events = collect_stream(format!("data: {event}\n\n")).await;
            assert!(matches!(events[0], Ok(ModelStreamEvent::Start)));
            assert!(matches!(
                &events[1],
                Err(ProviderError::InvalidResponse(message)) if message.contains(expected)
            ));
            assert_eq!(events.len(), 2);
        }
    }

    #[tokio::test]
    async fn stream_caps_a_single_sse_event() {
        let events = collect_stream(format!("data: {}", "x".repeat(MAX_SSE_EVENT_BYTES))).await;

        assert!(matches!(events[0], Ok(ModelStreamEvent::Start)));
        assert!(matches!(
            &events[1],
            Err(ProviderError::InvalidResponse(message)) if message.contains("event exceeds")
        ));
        assert_eq!(events.len(), 2);
    }
    use crate::{ModelMessage, ToolDefinition};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn translates_tool_history_and_parses_tool_call() {
        let input = messages(&[
            ModelMessage {
                role: MessageRole::Assistant,
                content: MessageContent::ProviderState {
                    provider: "openai-codex".into(),
                    model: "gpt-test".into(),
                    data: json!({"type":"reasoning","id":"rs_1","encrypted_content":"opaque","summary":[]}),
                },
            },
            ModelMessage {
                role: MessageRole::Assistant,
                content: MessageContent::ToolCall {
                    call_id: "call-1".into(),
                    tool: "read".into(),
                    arguments: json!({"path":"a"}),
                    thought_signature: None,
                },
            },
            ModelMessage {
                role: MessageRole::Tool,
                content: MessageContent::ToolResult {
                    call_id: "call-1".into(),
                    tool: "read".into(),
                    result: json!({"content":"hello"}),
                    is_error: false,
                    uncertain: false,
                },
            },
        ], "openai-codex", "gpt-test")
        .unwrap();
        assert_eq!(input[0]["encrypted_content"], "opaque");
        assert_eq!(input[1]["type"], "function_call");
        assert_eq!(input[2]["type"], "function_call_output");

        let uncertain = messages(
            &[ModelMessage {
                role: MessageRole::Tool,
                content: MessageContent::ToolResult {
                    call_id: "call-uncertain".into(),
                    tool: "write".into(),
                    result: json!("reply lost"),
                    is_error: true,
                    uncertain: true,
                },
            }],
            "openai-codex",
            "gpt-test",
        )
        .unwrap();
        let output: Value = serde_json::from_str(uncertain[0]["output"].as_str().unwrap()).unwrap();
        assert_eq!(output["outcome"], "uncertain");
        assert!(
            output["instruction"]
                .as_str()
                .unwrap()
                .contains("Do not retry automatically")
        );

        let response = parse(json!({"output":[
            {"type":"reasoning","id":"rs_2","encrypted_content":"next-opaque","summary":[]},
            {"type":"function_call","call_id":"call-2","name":"write","arguments":"{\"path\":\"b\"}"}
        ]}), "openai-codex", "gpt-test").unwrap();
        assert!(
            matches!(&response.parts[0], ModelPart::ProviderState { data, .. } if data["encrypted_content"] == "next-opaque")
        );
        assert!(
            matches!(&response.parts[1], ModelPart::ToolCall { id, name, arguments, .. } if id == "call-2" && name == "write" && arguments["path"] == "b")
        );
        let _ = ToolDefinition {
            agent_id: None,
            name: "read".into(),
            description: "read".into(),
            schema: json!({}),
        };
    }

    #[test]
    fn translates_reasoning_effort_for_responses() {
        let mut body = json!({});
        apply_reasoning(&mut body, Some(ReasoningEffort::High)).unwrap();
        assert_eq!(body["reasoning"]["effort"], "high");

        assert!(matches!(
            apply_reasoning(&mut body, Some(ReasoningEffort::Auto)),
            Err(ProviderError::InvalidRequest(_))
        ));
    }

    #[test]
    fn parses_codex_completed_sse() {
        let body = "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":4},\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"done\"}]}]}}\n\ndata: [DONE]\n";
        let response = parse_sse(body, "openai-codex", "gpt-test").unwrap();
        assert_eq!(response.response_id.as_deref(), Some("resp_1"));
        assert_eq!(
            response.usage.as_ref().map(|usage| usage.output_tokens),
            Some(4)
        );
        assert_eq!(
            response.parts,
            vec![ModelPart::Text {
                text: "done".into()
            }]
        );
    }

    #[tokio::test]
    async fn device_login_can_be_cancelled() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request).await.unwrap();
            let body = r#"{"device_auth_id":"device","user_code":"CODE","interval":5}"#;
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        });
        let endpoint = format!("http://{address}");
        let endpoints = OpenAiAuthEndpoints {
            user_code: endpoint.clone(),
            device_token: endpoint.clone(),
            oauth_token: endpoint,
        };
        let (cancel, cancellation) = tokio::sync::watch::channel(false);
        let result = login_with_client(
            &Client::new(),
            &mut |_| {
                cancel.send(true).unwrap();
            },
            &cancellation,
            &endpoints,
        )
        .await;
        assert!(matches!(result, Err(ProviderError::Cancelled)));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn calls_openai_compatible_http_endpoint() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0_u8; 8192];
            let read = stream.read(&mut request).await.unwrap();
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(request.starts_with("POST /responses "));
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("authorization: bearer secret")
            );
            let body = r#"{"output":[{"type":"message","content":[{"type":"output_text","text":"hello"}]}]}"#;
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        });
        let root = tempfile::tempdir().unwrap();
        AuthStore::set(
            root.path(),
            "openai",
            &AuthCredential::ApiKey {
                key: "secret".into(),
                base_url: Some(format!("http://{address}")),
            },
        )
        .unwrap();
        let response = generate(
            &Client::new(),
            root.path(),
            "openai",
            ModelRequest {
                model: "openai/test".into(),
                reasoning: None,
                system: "help".into(),
                messages: vec![],
                tools: vec![],
                continuation: None,
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
