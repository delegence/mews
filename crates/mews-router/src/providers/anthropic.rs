use std::{path::Path, time::Duration};

use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use mews_protocol::AuthCredential;
use rand_core::{OsRng, RngCore};
use reqwest::Client;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use crate::{
    AuthStore, MessageContent, MessageRole, ModelPart, ModelRequest, ModelResponse, ProviderError,
    ProviderResult, ReasoningEffort,
    http::{response_json, send_with_retry},
};

const CLIENT_ID: &str = "9d1c250a-e61b-44d9-88ed-5944d1962f5e";
const AUTH_URL: &str = "https://claude.ai/oauth/authorize";
const TOKEN_URL: &str = "https://platform.claude.com/v1/oauth/token";
const PROFILE_URL: &str = "https://api.anthropic.com/api/oauth/profile";
const ROLES_URL: &str = "https://api.anthropic.com/api/oauth/claude_cli/roles";
const REDIRECT_URI: &str = "http://localhost:54545/callback";
const OAUTH_SCOPE: &str =
    "user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload";
const OAUTH_BETAS: &str = "claude-code-20250219,oauth-2025-04-20";
static REFRESH: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BrowserAuthorization {
    pub authorization_uri: String,
}

pub(crate) async fn generate(
    client: &Client,
    root: &Path,
    request: ModelRequest,
) -> ProviderResult<ModelResponse> {
    let credential = current_credential(client, root)
        .await
        .map_err(|error| ProviderError::Authentication(format!("{error:#}")))?;
    let model = request
        .model
        .split_once('/')
        .map_or(request.model.as_str(), |(_, model)| model)
        .to_owned();
    let oauth = matches!(&credential, AuthCredential::Oauth { .. });
    let tools = request.tools;
    let mut body = json!({
        "model": &model,
        "system": request.system,
        "max_tokens": 8192,
        "messages": messages(&request.messages, &model, oauth).map_err(|error| ProviderError::InvalidRequest(error.to_string()))?,
        "tools": tools.iter().map(|tool| json!({
            "name":if oauth { claude_code_tool_name(&tool.name) } else { &tool.name },"description":tool.description,"input_schema":tool.schema
        })).collect::<Vec<_>>()
    });
    apply_reasoning(&mut body, &model, request.reasoning)?;
    let mut request = authenticate(
        client,
        credential,
        |client, url| client.post(url).json(&body),
        "messages",
    )?;
    if oauth {
        request = request.header("anthropic-beta", oauth_betas(&body));
    }
    let response: Value = response_json(send_with_retry(request).await?).await?;
    parse(response, &model, &tools)
        .map_err(|error| ProviderError::InvalidResponse(error.to_string()))
}

pub(crate) async fn models(client: &Client, root: &Path) -> ProviderResult<Vec<crate::ModelInfo>> {
    let credential = current_credential(client, root)
        .await
        .map_err(|error| ProviderError::Authentication(format!("{error:#}")))?;
    let mut entries = Vec::new();
    let mut after_id: Option<String> = None;
    loop {
        let mut request = authenticate(
            client,
            credential.clone(),
            |client, url| client.get(url),
            "models",
        )?;
        request = request.query(&[("limit", "100")]);
        if let Some(after_id) = &after_id {
            request = request.query(&[("after_id", after_id)]);
        }
        let payload: Value = response_json(send_with_retry(request).await?).await?;
        let page = payload
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                ProviderError::InvalidResponse("Anthropic model catalog has no data".into())
            })?;
        entries.extend(page.iter().cloned());
        if !payload
            .get("has_more")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            break;
        }
        after_id = payload
            .get("last_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if after_id.is_none() {
            return Err(ProviderError::InvalidResponse(
                "Anthropic model catalog is missing last_id".into(),
            ));
        }
    }
    let mut models = entries
        .iter()
        .filter_map(|entry| {
            let id = entry.get("id")?.as_str()?;
            if !id.starts_with("claude-") {
                return None;
            }
            let adaptive = supports_adaptive_thinking(id);
            Some(crate::ModelInfo {
                id: format!("anthropic/{id}"),
                display_name: entry
                    .get("display_name")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                reasoning: if adaptive {
                    vec![
                        ReasoningEffort::None,
                        ReasoningEffort::Auto,
                        ReasoningEffort::Low,
                        ReasoningEffort::Medium,
                        ReasoningEffort::High,
                        ReasoningEffort::Max,
                    ]
                } else {
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
                },
                default_reasoning: None,
            })
        })
        .collect::<Vec<_>>();
    models.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(models)
}

fn endpoint(credential: &AuthCredential, path: &str) -> String {
    let base = match credential {
        AuthCredential::ApiKey { base_url, .. } => base_url
            .as_deref()
            .unwrap_or("https://api.anthropic.com/v1"),
        AuthCredential::Oauth { .. } => "https://api.anthropic.com/v1",
    };
    format!("{}/{path}", base.trim_end_matches('/'))
}

fn authenticate(
    client: &Client,
    credential: AuthCredential,
    build: impl FnOnce(&Client, String) -> reqwest::RequestBuilder,
    path: &str,
) -> ProviderResult<reqwest::RequestBuilder> {
    let url = endpoint(&credential, path);
    let request = build(client, url.clone()).header("anthropic-version", "2023-06-01");
    let request = match credential {
        AuthCredential::ApiKey { key, .. } => request.header("x-api-key", key),
        AuthCredential::Oauth { access, .. } => request
            .bearer_auth(access)
            .header("anthropic-beta", OAUTH_BETAS)
            .header("anthropic-dangerous-direct-browser-access", "true")
            .header("x-app", "cli")
            .header("user-agent", "claude-cli/2.1.220 (external, cli)")
            .header("x-stainless-retry-count", "0")
            .header("x-stainless-runtime", "node")
            .header("x-stainless-lang", "js")
            .header("x-stainless-timeout", "600")
            .header("x-claude-code-session-id", uuid::Uuid::now_v7().to_string())
            .header("x-client-request-id", uuid::Uuid::now_v7().to_string()),
    };
    Ok(request)
}

fn oauth_betas(body: &Value) -> String {
    let mut betas = vec![
        "claude-code-20250219",
        "oauth-2025-04-20",
        "interleaved-thinking-2025-05-14",
        "redact-thinking-2026-02-12",
        "thinking-token-count-2026-05-13",
        "context-management-2025-06-27",
        "prompt-caching-scope-2026-01-05",
    ];
    if body
        .get("tools")
        .and_then(Value::as_array)
        .is_some_and(|tools| !tools.is_empty())
    {
        betas.push("advanced-tool-use-2025-11-20");
    }
    betas.extend([
        "effort-2025-11-24",
        "fallback-credit-2026-06-01",
        "extended-cache-ttl-2025-04-11",
    ]);
    betas.join(",")
}

pub async fn login_anthropic(notify: impl FnOnce(BrowserAuthorization)) -> Result<AuthCredential> {
    let listener = TcpListener::bind("127.0.0.1:54545")
        .await
        .context("Anthropic OAuth callback port 54545 is unavailable")?;
    let (verifier, challenge) = pkce();
    let state = uuid::Uuid::now_v7().to_string();
    notify(BrowserAuthorization {
        authorization_uri: authorization_url(&state, &challenge)?,
    });
    let code = tokio::time::timeout(Duration::from_secs(5 * 60), callback(listener, &state))
        .await
        .context("Anthropic OAuth login timed out")??;
    exchange(&Client::new(), &code, &state, &verifier).await
}

fn pkce() -> (String, String) {
    let mut bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut bytes);
    let verifier = URL_SAFE_NO_PAD.encode(bytes);
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    (verifier, challenge)
}

fn authorization_url(state: &str, challenge: &str) -> Result<String> {
    let mut url = reqwest::Url::parse(AUTH_URL)?;
    url.query_pairs_mut()
        .append_pair("code", "true")
        .append_pair("client_id", CLIENT_ID)
        .append_pair("response_type", "code")
        .append_pair("redirect_uri", REDIRECT_URI)
        .append_pair("scope", OAUTH_SCOPE)
        .append_pair("code_challenge", challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("state", state);
    Ok(url.into())
}

async fn callback(listener: TcpListener, expected_state: &str) -> Result<String> {
    let (mut stream, _) = listener.accept().await?;
    let mut request = vec![0_u8; 8192];
    let length = stream.read(&mut request).await?;
    let first_line = std::str::from_utf8(&request[..length])?
        .lines()
        .next()
        .context("OAuth callback request is empty")?;
    let path = first_line
        .split_whitespace()
        .nth(1)
        .context("OAuth callback has no path")?;
    let url = reqwest::Url::parse(&format!("http://localhost{path}"))?;
    let query = url
        .query_pairs()
        .collect::<std::collections::HashMap<_, _>>();
    let state = query.get("state").context("OAuth callback has no state")?;
    let result = if state.as_ref() != expected_state {
        Err(anyhow::anyhow!("Anthropic OAuth state mismatch"))
    } else if let Some(error) = query.get("error") {
        Err(anyhow::anyhow!("Anthropic OAuth failed: {error}"))
    } else {
        query
            .get("code")
            .map(|code| code.to_string())
            .context("OAuth callback has no authorization code")
    };
    let (status, message) = if result.is_ok() {
        (
            "200 OK",
            "Anthropic authentication complete. You can close this window.",
        )
    } else {
        (
            "400 Bad Request",
            "Anthropic authentication failed. Return to the terminal.",
        )
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{message}",
        message.len()
    );
    stream.write_all(response.as_bytes()).await?;
    result
}

async fn exchange(
    client: &Client,
    code: &str,
    state: &str,
    verifier: &str,
) -> Result<AuthCredential> {
    let token: Value = response_json(
        send_with_retry(
            client
                .post(TOKEN_URL)
                .header("accept", "application/json, text/plain, */*")
                .header("user-agent", "axios/1.15.2")
                .json(&json!({
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": REDIRECT_URI,
                    "client_id": CLIENT_ID,
                    "code_verifier": verifier,
                    "state": state,
                })),
        )
        .await?,
    )
    .await?;
    let mut credential = credential_from_token(token, None)?;
    if let AuthCredential::Oauth {
        access, account_id, ..
    } = &mut credential
        && let Some(profile_account) = inspect_account(client, access).await
    {
        *account_id = profile_account;
    }
    Ok(credential)
}

async fn inspect_account(client: &Client, access: &str) -> Option<String> {
    let request = |url| {
        client
            .get(url)
            .bearer_auth(access)
            .header("accept", "application/json, text/plain, */*")
            .header("cache-control", "no-cache")
            .header("user-agent", "axios/1.15.2")
    };
    let profile = request(PROFILE_URL)
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?;
    let profile: Value = response_json(profile).await.ok()?;
    // The roles lookup is advisory, but replaying it keeps login behavior aligned
    // with Claude Code and catches an unusable claude_cli grant early upstream.
    let _ = request(ROLES_URL).send().await;
    profile
        .pointer("/account/uuid")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

async fn refresh(client: &Client, refresh_token: &str, account_id: &str) -> Result<AuthCredential> {
    let token: Value = response_json(
        send_with_retry(
            client
                .post(TOKEN_URL)
                .header("accept", "application/json, text/plain, */*")
                .header("user-agent", "axios/1.15.2")
                .json(&json!({
                    "client_id": CLIENT_ID,
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                    "scope": OAUTH_SCOPE,
                })),
        )
        .await?,
    )
    .await?;
    credential_from_token(token, Some((refresh_token, account_id)))
}

fn credential_from_token(token: Value, previous: Option<(&str, &str)>) -> Result<AuthCredential> {
    let access = token
        .get("access_token")
        .and_then(Value::as_str)
        .context("token has no access_token")?
        .to_owned();
    let refresh = token
        .get("refresh_token")
        .and_then(Value::as_str)
        .or_else(|| previous.map(|(refresh, _)| refresh))
        .context("token has no refresh_token")?
        .to_owned();
    let expires = now_ms()
        + token
            .get("expires_in")
            .and_then(Value::as_u64)
            .context("token has no expires_in")?
            * 1000;
    let account_id = token
        .pointer("/account/uuid")
        .and_then(Value::as_str)
        .or_else(|| previous.map(|(_, account)| account))
        .unwrap_or_default()
        .to_owned();
    Ok(AuthCredential::Oauth {
        access,
        refresh,
        expires,
        account_id,
    })
}

async fn current_credential(client: &Client, root: &Path) -> Result<AuthCredential> {
    let mut credential = AuthStore::load(root)?.credential("anthropic")?;
    if let AuthCredential::Oauth { expires, .. } = &credential
        && *expires <= now_ms() + 60_000
    {
        let _guard = REFRESH.lock().await;
        credential = AuthStore::load(root)?.credential("anthropic")?;
        let AuthCredential::Oauth {
            refresh: token,
            expires,
            account_id,
            ..
        } = &credential
        else {
            return Ok(credential);
        };
        if *expires > now_ms() + 60_000 {
            return Ok(credential);
        }
        let refreshed = refresh(client, token, account_id).await?;
        AuthStore::set(root, "anthropic", &refreshed)?;
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

fn apply_reasoning(
    body: &mut Value,
    model: &str,
    effort: Option<ReasoningEffort>,
) -> ProviderResult<()> {
    let Some(effort) = effort else {
        return Ok(());
    };
    if effort == ReasoningEffort::None {
        body["thinking"] = json!({ "type": "disabled" });
        return Ok(());
    }
    if supports_adaptive_thinking(model) {
        body["thinking"] = json!({ "type": "adaptive" });
        if effort != ReasoningEffort::Auto {
            body["output_config"] = json!({ "effort": anthropic_effort(effort) });
        }
        return Ok(());
    }
    if effort == ReasoningEffort::Auto {
        body["thinking"] = json!({ "type": "enabled" });
        return Ok(());
    }

    let budget = legacy_thinking_budget(effort);
    body["thinking"] = json!({ "type": "enabled", "budget_tokens": budget });
    // Anthropic requires max_tokens to be greater than the thinking budget.
    body["max_tokens"] = Value::from((budget + 1).max(8192));
    Ok(())
}

fn supports_adaptive_thinking(model: &str) -> bool {
    [
        "opus-4-6",
        "opus-4.6",
        "opus-4-7",
        "opus-4.7",
        "opus-4-8",
        "opus-4.8",
        "opus-5",
        "opus.5",
        "sonnet-4-6",
        "sonnet-4.6",
        "sonnet-5",
        "sonnet.5",
        "fable-5",
    ]
    .iter()
    .any(|version| model.contains(version))
}

fn anthropic_effort(effort: ReasoningEffort) -> &'static str {
    match effort {
        ReasoningEffort::Minimal | ReasoningEffort::Low => "low",
        ReasoningEffort::Medium => "medium",
        ReasoningEffort::High => "high",
        ReasoningEffort::XHigh | ReasoningEffort::Max => "max",
        ReasoningEffort::None | ReasoningEffort::Auto => unreachable!(),
    }
}

fn legacy_thinking_budget(effort: ReasoningEffort) -> u64 {
    match effort {
        ReasoningEffort::Minimal => 512,
        ReasoningEffort::Low => 1024,
        ReasoningEffort::Medium => 8192,
        ReasoningEffort::High => 24_576,
        ReasoningEffort::XHigh | ReasoningEffort::Max => 32_768,
        ReasoningEffort::None | ReasoningEffort::Auto => unreachable!(),
    }
}

fn claude_code_tool_name(name: &str) -> &str {
    match name.to_ascii_lowercase().as_str() {
        "read" => "Read",
        "write" => "Write",
        "edit" => "Edit",
        "bash" => "Bash",
        _ => name,
    }
}

fn messages(messages: &[crate::ModelMessage], model: &str, oauth: bool) -> Result<Vec<Value>> {
    let mut output: Vec<Value> = Vec::new();
    for message in messages {
        let (role, block) = match &message.content {
            MessageContent::Text { text } => (
                if message.role == MessageRole::User {
                    "user"
                } else {
                    "assistant"
                },
                json!({"type":"text","text":text}),
            ),
            MessageContent::ToolCall {
                call_id,
                tool,
                arguments,
                ..
            } => (
                "assistant",
                json!({"type":"tool_use","id":call_id,"name":if oauth { claude_code_tool_name(tool) } else { tool },"input":arguments}),
            ),
            MessageContent::ToolResult {
                call_id,
                result,
                is_error,
                ..
            } => (
                "user",
                json!({"type":"tool_result","tool_use_id":call_id,"content":serde_json::to_string(result)?,"is_error":is_error}),
            ),
            MessageContent::ProviderState {
                provider,
                model: state_model,
                data,
            } if provider == "anthropic" && state_model == model => ("assistant", data.clone()),
            MessageContent::ProviderState { .. } => continue,
        };
        if let Some(content) = output
            .last_mut()
            .filter(|entry| entry["role"] == role)
            .and_then(|entry| entry.get_mut("content"))
            .and_then(Value::as_array_mut)
        {
            content.push(block);
        } else {
            output.push(json!({"role": role, "content": [block]}));
        }
    }
    Ok(output)
}

fn parse(
    response: Value,
    model: &str,
    tools: &[mews_protocol::ToolDefinition],
) -> Result<ModelResponse> {
    let parts = response
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("Anthropic response has no content"))?
        .iter()
        .filter_map(|part| match part.get("type").and_then(Value::as_str) {
            Some("text") => Some(ModelPart::Text {
                text: part.get("text")?.as_str()?.to_owned(),
            }),
            Some("tool_use") => Some(ModelPart::ToolCall {
                id: part.get("id")?.as_str()?.to_owned(),
                name: tools
                    .iter()
                    .find(|tool| {
                        part.get("name")
                            .and_then(Value::as_str)
                            .is_some_and(|name| tool.name.eq_ignore_ascii_case(name))
                    })
                    .map(|tool| tool.name.clone())
                    .or_else(|| part.get("name")?.as_str().map(str::to_owned))?,
                arguments: part.get("input")?.clone(),
                thought_signature: None,
            }),
            Some("thinking") => Some(ModelPart::Reasoning {
                text: part.get("thinking")?.as_str()?.to_owned(),
                signature: part
                    .get("signature")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            }),
            Some("redacted_thinking") => Some(ModelPart::ProviderState {
                provider: "anthropic".into(),
                model: model.into(),
                data: part.clone(),
            }),
            _ => None,
        })
        .collect();
    let usage = response
        .get("usage")
        .map(|usage| mews_protocol::ModelUsage {
            input_tokens: usage
                .get("input_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            output_tokens: usage
                .get("output_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            cached_input_tokens: usage
                .get("cache_read_input_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            reasoning_tokens: 0,
        });
    Ok(ModelResponse {
        provider: "anthropic".into(),
        model: model.into(),
        api: "messages".into(),
        response_id: response
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        usage,
        stop_reason: response
            .get("stop_reason")
            .and_then(Value::as_str)
            .map(str::to_owned),
        parts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ModelMessage;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn translates_and_parses_tool_calls() {
        let input = messages(
            &[
                ModelMessage {
                    role: MessageRole::Assistant,
                    content: MessageContent::ProviderState {
                        provider: "anthropic".into(),
                        model: "claude-test".into(),
                        data: json!({"type":"thinking","thinking":"plan","signature":"sig"}),
                    },
                },
                ModelMessage {
                    role: MessageRole::Assistant,
                    content: MessageContent::ToolCall {
                        call_id: "tool-1".into(),
                        tool: "read".into(),
                        arguments: json!({"path":"a"}),
                        thought_signature: None,
                    },
                },
                ModelMessage {
                    role: MessageRole::Tool,
                    content: MessageContent::ToolResult {
                        call_id: "tool-1".into(),
                        tool: "read".into(),
                        result: json!({"content":"hello"}),
                        is_error: false,
                    },
                },
            ],
            "claude-test",
            true,
        )
        .unwrap();
        assert_eq!(input[0]["content"][0]["signature"], "sig");
        assert_eq!(input[0]["content"][1]["id"], "tool-1");
        assert_eq!(input[0]["content"][1]["name"], "Read");
        assert_eq!(input[1]["content"][0]["tool_use_id"], "tool-1");
        let response = parse(
            json!({"content":[
                {"type":"thinking","thinking":"plan","signature":"sig"},
                {"type":"text","text":"checking"},
                {"type":"tool_use","id":"tool-2","name":"Write","input":{"path":"b"}}
            ]}),
            "claude-test",
            &[mews_protocol::ToolDefinition {
                name: "write".into(),
                description: "write".into(),
                schema: json!({}),
            }],
        )
        .unwrap();
        assert_eq!(response.parts.len(), 3);
        assert!(matches!(
            &response.parts[0],
            ModelPart::Reasoning { text, signature }
                if text == "plan" && signature.as_deref() == Some("sig")
        ));
        assert!(
            matches!(&response.parts[2], ModelPart::ToolCall { id, name, arguments, .. } if id == "tool-2" && name == "write" && arguments["path"] == "b")
        );
    }

    #[test]
    fn translates_reasoning_for_adaptive_and_legacy_claude() {
        let mut adaptive = json!({ "max_tokens": 8192 });
        apply_reasoning(
            &mut adaptive,
            "claude-sonnet-4-6",
            Some(ReasoningEffort::XHigh),
        )
        .unwrap();
        assert_eq!(adaptive["thinking"]["type"], "adaptive");
        assert_eq!(adaptive["output_config"]["effort"], "max");

        let mut legacy = json!({ "max_tokens": 8192 });
        apply_reasoning(
            &mut legacy,
            "claude-sonnet-4-5",
            Some(ReasoningEffort::High),
        )
        .unwrap();
        assert_eq!(legacy["thinking"]["type"], "enabled");
        assert_eq!(legacy["thinking"]["budget_tokens"], 24_576);
        assert_eq!(legacy["max_tokens"], 24_577);
    }

    #[test]
    fn builds_pkce_authorization_and_oauth_requests() {
        let url =
            reqwest::Url::parse(&authorization_url("state-1", "challenge-1").unwrap()).unwrap();
        let query = url
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(query.get("client_id").unwrap(), CLIENT_ID);
        assert_eq!(query.get("state").unwrap(), "state-1");
        assert_eq!(query.get("code_challenge").unwrap(), "challenge-1");
        assert_eq!(query.get("code_challenge_method").unwrap(), "S256");

        let request = authenticate(
            &Client::new(),
            AuthCredential::Oauth {
                access: "sk-ant-oat-test".into(),
                refresh: "refresh".into(),
                expires: u64::MAX,
                account_id: String::new(),
            },
            |client, url| client.post(url),
            "messages",
        )
        .unwrap()
        .build()
        .unwrap();
        assert_eq!(
            request.headers().get("authorization").unwrap(),
            "Bearer sk-ant-oat-test"
        );
        assert!(request.headers().get("x-api-key").is_none());
        assert!(
            request
                .headers()
                .get("anthropic-beta")
                .unwrap()
                .to_str()
                .unwrap()
                .contains("oauth-2025-04-20")
        );
    }

    #[test]
    fn oauth_refresh_rotates_tokens_and_preserves_missing_refresh_token() {
        let credential = credential_from_token(
            json!({"access_token":"new-access","expires_in":3600}),
            Some(("old-refresh", "account")),
        )
        .unwrap();
        assert!(matches!(
            credential,
            AuthCredential::Oauth { access, refresh, account_id, .. }
                if access == "new-access" && refresh == "old-refresh" && account_id == "account"
        ));
    }

    #[tokio::test]
    async fn oauth_callback_validates_state_and_returns_code() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let callback = tokio::spawn(async move { callback(listener, "expected").await });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client
            .write_all(
                b"GET /callback?code=authorization-code&state=expected HTTP/1.1\r\nHost: localhost\r\n\r\n",
            )
            .await
            .unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert!(
            String::from_utf8(response)
                .unwrap()
                .starts_with("HTTP/1.1 200 OK")
        );
        assert_eq!(callback.await.unwrap().unwrap(), "authorization-code");
    }

    #[tokio::test]
    async fn calls_anthropic_compatible_http_endpoint() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0_u8; 8192];
            let read = stream.read(&mut request).await.unwrap();
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(request.starts_with("POST /messages "));
            assert!(request.to_ascii_lowercase().contains("x-api-key: secret"));
            let body = r#"{"content":[{"type":"text","text":"hello"}]}"#;
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        });
        let root = tempfile::tempdir().unwrap();
        AuthStore::set(
            root.path(),
            "anthropic",
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
                model: "anthropic/test".into(),
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
