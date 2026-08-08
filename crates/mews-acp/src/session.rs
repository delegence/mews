//! Persistent ACP session execution and recovery.

use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
};

use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Child,
};

use crate::{
    mcp::{RunMcpBridge, RunMcpHttp},
    process::{AcpHarnessConfig, AcpProcess},
    rpc::{RpcClient, is_resource_not_found},
};

// ACP v1 uses a negotiated integer protocol version. The date-shaped value
// belongs to MCP, not ACP.
const ACP_PROTOCOL_VERSION: u16 = 1;
const MAX_METADATA_BYTES: usize = 16 * 1024;

/// Bounded ACP discovery output. Hosts normalize this into their public
/// catalog and persist it, so clients never start an adapter just to redraw a
/// selector.
#[derive(Clone, Debug)]
pub struct AcpProbe {
    pub initialize: Value,
    pub session: Option<Value>,
    pub session_error: Option<String>,
}

pub enum AcpStreamEvent {
    AssistantDelta {
        delta: String,
        message_id: Option<String>,
    },
    ReasoningDelta {
        delta: String,
        message_id: Option<String>,
    },
    ToolActivity {
        call_id: String,
        title: String,
        kind: Option<String>,
        status: Option<String>,
        input: Value,
    },
    ProviderState(Value),
    SessionBound {
        session_id: String,
        replaced: bool,
    },
}

#[derive(Clone, Debug, Default)]
struct ToolActivityState {
    title: String,
    kind: Option<String>,
    status: Option<String>,
    input: Value,
}

#[derive(Clone, Debug)]
pub struct AcpSessionRequest {
    pub prompt: String,
    pub recovery_prompt: String,
    pub session_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcpSessionOutcome {
    pub answer: String,
    pub session_id: String,
    pub session_replaced: bool,
}

pub async fn run_acp_session_with_extensions_and_events(
    config: AcpHarnessConfig,
    cwd: PathBuf,
    harness_options: BTreeMap<String, String>,
    session: AcpSessionRequest,
    environment: &dyn mews_agent::AgentCapabilities,
    allowed_tools: &[String],
    events: &mut dyn FnMut(AcpStreamEvent) -> Result<()>,
) -> Result<AcpSessionOutcome> {
    let harness = AcpProcess::new(config);
    let mut child = harness.spawn(&cwd)?;
    let result = harness
        .run_session_with_extensions(
            &mut child,
            cwd,
            harness_options,
            session,
            environment,
            allowed_tools,
            events,
        )
        .await;
    if let Err(error) = &result {
        let _ = child.kill().await;
        let _ = child.wait().await;
        return Err(anyhow!("ACP Harness failed: {error}"));
    }
    let status = child.wait().await.context("wait for ACP Harness")?;
    if !status.success() {
        bail!("ACP Harness exited with {status}");
    }
    result
}

pub async fn probe_acp(config: AcpHarnessConfig, cwd: PathBuf) -> Result<AcpProbe> {
    let process = AcpProcess::new(config);
    let mut child = process.spawn(&cwd)?;
    let result = async {
        let stdin = child
            .stdin
            .take()
            .context("ACP Harness did not open stdin")?;
        let stdout = child
            .stdout
            .take()
            .context("ACP Harness did not open stdout")?;
        let mut writer = stdin;
        let mut reader = BufReader::new(stdout).lines();
        let cancellation = mews_agent::CancellationToken::new();
        let mut rpc = RpcClient::new(
            &mut writer,
            &mut reader,
            process.config.request_timeout,
            process.config.permission_handler.as_ref(),
        );
        let initialize = rpc
            .request_plain(
                "initialize",
                json!({
                    "protocolVersion": ACP_PROTOCOL_VERSION,
                    "clientInfo": { "name": "mews", "version": env!("CARGO_PKG_VERSION") },
                    "clientCapabilities": { "auth": { "terminal": true } },
                }),
                &cancellation,
                |_| Ok(()),
            )
            .await?;
        let session = rpc
            .request_plain(
                "session/new",
                json!({ "cwd": cwd, "mcpServers": [] }),
                &cancellation,
                |_| Ok(()),
            )
            .await;
        Ok(AcpProbe {
            initialize,
            session: session.as_ref().ok().cloned(),
            session_error: session.err().map(|error| error.to_string()),
        })
    }
    .await;
    let _ = child.kill().await;
    let _ = child.wait().await;
    result
}

impl AcpProcess {
    #[allow(clippy::too_many_arguments)]
    async fn run_session_with_extensions(
        &self,
        child: &mut Child,
        cwd: PathBuf,
        harness_options: BTreeMap<String, String>,
        session: AcpSessionRequest,
        environment: &dyn mews_agent::AgentCapabilities,
        allowed_tools: &[String],
        events: &mut dyn FnMut(AcpStreamEvent) -> Result<()>,
    ) -> Result<AcpSessionOutcome> {
        let stdin = child
            .stdin
            .take()
            .context("ACP Harness did not open stdin")?;
        let stdout = child
            .stdout
            .take()
            .context("ACP Harness did not open stdout")?;
        let mut writer = stdin;
        let mut reader = BufReader::new(stdout).lines();
        let cancellation = mews_agent::CancellationToken::new();
        let mut rpc = RpcClient::new(
            &mut writer,
            &mut reader,
            self.config.request_timeout,
            self.config.permission_handler.as_ref(),
        );
        let initialize = rpc
            .request_plain(
                "initialize",
                json!({
                    "protocolVersion": ACP_PROTOCOL_VERSION,
                    "clientInfo": { "name": "mews", "version": env!("CARGO_PKG_VERSION") },
                    "clientCapabilities": { "auth": { "terminal": true } },
                }),
                &cancellation,
                |_| Ok(()),
            )
            .await?;
        let mcp = RunMcpBridge::for_extensions(
            environment,
            cwd.clone(),
            cancellation.clone(),
            allowed_tools,
        );
        let mcp_http = if mcp.tool_definitions().is_empty() {
            None
        } else if initialize
            .pointer("/agentCapabilities/mcpCapabilities/http")
            .and_then(Value::as_bool)
            == Some(true)
        {
            Some(mcp.bind_http().await?)
        } else {
            bail!("ACP Harness does not support HTTP MCP required for MEWS extensions")
        };
        let mcp_servers = mcp_http.as_ref().map_or_else(Vec::new, |http| {
            vec![json!({ "type": "http", "name": "mews_extensions", "url": http.url(), "headers": [] })]
        });
        let had_binding = session.session_id.is_some();
        let (session_id, session_replaced, prompt) = if let Some(session_id) = session.session_id {
            let method = if initialize
                .pointer("/agentCapabilities/sessionCapabilities/resume")
                .is_some()
            {
                "session/resume"
            } else if initialize
                .pointer("/agentCapabilities/loadSession")
                .and_then(Value::as_bool)
                == Some(true)
            {
                "session/load"
            } else {
                bail!("ACP Harness cannot resume persistent Sessions")
            };
            let resumed = rpc
                .request(
                    method,
                    json!({ "sessionId": session_id, "cwd": cwd.clone(), "mcpServers": mcp_servers.clone() }),
                    &cancellation,
                    Some(&mcp),
                    mcp_http.as_ref(),
                    |_| Ok(()),
                )
                .await;
            match resumed {
                Ok(_) => (session_id, false, session.prompt),
                Err(error) if is_resource_not_found(&error) => {
                    let created = rpc
                        .request(
                            "session/new",
                            json!({ "cwd": cwd.clone(), "mcpServers": mcp_servers }),
                            &cancellation,
                            Some(&mcp),
                            mcp_http.as_ref(),
                            |_| Ok(()),
                        )
                        .await?;
                    (acp_session_id(&created)?, true, session.recovery_prompt)
                }
                Err(error) => return Err(error),
            }
        } else {
            let created = rpc
                .request(
                    "session/new",
                    json!({ "cwd": cwd, "mcpServers": mcp_servers }),
                    &cancellation,
                    Some(&mcp),
                    mcp_http.as_ref(),
                    |_| Ok(()),
                )
                .await?;
            (acp_session_id(&created)?, false, session.prompt)
        };
        if !had_binding || session_replaced {
            events(AcpStreamEvent::SessionBound {
                session_id: session_id.clone(),
                replaced: session_replaced,
            })?;
        }
        apply_harness_options(
            &mut rpc,
            &session_id,
            &harness_options,
            &cancellation,
            Some(&mcp),
            mcp_http.as_ref(),
        )
        .await?;
        let mut assistant = String::new();
        let mut assistant_boundary = false;
        let mut assistant_message_id: Option<String> = None;
        let mut tools = HashMap::<String, ToolActivityState>::new();
        rpc.request(
            "session/prompt",
            json!({ "sessionId": session_id, "prompt": [{ "type": "text", "text": prompt }] }),
            &cancellation,
            Some(&mcp),
            mcp_http.as_ref(),
            |update| {
                match update_kind(update) {
                    Some("agent_message_chunk") => {
                        if let Some(text) = update_text(update) {
                            let message_id = update_message_id(update);
                            let message_changed = assistant_message_id
                                .as_ref()
                                .zip(message_id.as_ref())
                                .is_some_and(|(previous, next)| previous != next);
                            if (assistant_boundary || message_changed) && !assistant.is_empty() {
                                assistant.push_str("\n\n");
                                events(AcpStreamEvent::AssistantDelta {
                                    delta: "\n\n".into(),
                                    message_id: message_id.clone(),
                                })?;
                            }
                            assistant_boundary = false;
                            if message_id.is_some() {
                                assistant_message_id = message_id.clone();
                            }
                            assistant.push_str(text);
                            events(AcpStreamEvent::AssistantDelta {
                                delta: text.to_owned(),
                                message_id,
                            })?;
                        }
                    }
                    Some("agent_thought_chunk") => {
                        if let Some(text) = content_text(update) {
                            events(AcpStreamEvent::ReasoningDelta {
                                delta: text.to_owned(),
                                message_id: update_message_id(update),
                            })?;
                        }
                    }
                    Some("tool_call" | "tool_call_update") => {
                        assistant_boundary = true;
                        let Some(call_id) = update
                            .get("toolCallId")
                            .and_then(Value::as_str)
                            .filter(|call_id| !call_id.trim().is_empty())
                            .map(str::to_owned)
                        else {
                            events(AcpStreamEvent::ProviderState(bounded_json(update)))?;
                            return Ok(());
                        };
                        let state = tools.entry(call_id.clone()).or_default();
                        if let Some(title) = non_empty_string(update, "title") {
                            state.title = title.to_owned();
                        }
                        if let Some(kind) = non_empty_string(update, "kind") {
                            state.kind = Some(kind.to_owned());
                        }
                        if let Some(status) = non_empty_string(update, "status") {
                            state.status = Some(status.to_owned());
                        }
                        if let Some(input) = update.get("rawInput") {
                            merge_json(&mut state.input, input);
                        }
                        events(AcpStreamEvent::ToolActivity {
                            call_id,
                            title: (!state.title.is_empty())
                                .then(|| state.title.clone())
                                .unwrap_or_else(|| "Tool call".into()),
                            kind: state.kind.clone(),
                            status: state.status.clone(),
                            input: state.input.clone(),
                        })?;
                    }
                    _ => events(AcpStreamEvent::ProviderState(bounded_json(update)))?,
                }
                Ok(())
            },
        )
        .await?;
        mcp.revoke();
        Ok(AcpSessionOutcome {
            answer: assistant,
            session_id,
            session_replaced,
        })
    }
}

fn acp_session_id(response: &Value) -> Result<String> {
    response
        .get("sessionId")
        .or_else(|| response.get("session_id"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .context("ACP session/new response did not include sessionId")
}

async fn apply_harness_options<W>(
    rpc: &mut RpcClient<'_, W>,
    session_id: &str,
    options: &BTreeMap<String, String>,
    cancellation: &mews_agent::CancellationToken,
    mcp: Option<&RunMcpBridge<'_>>,
    mcp_http: Option<&RunMcpHttp>,
) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    for (config_id, value) in options {
        rpc.request(
            "session/set_config_option",
            json!({ "sessionId": session_id, "configId": config_id, "value": value }),
            cancellation,
            mcp,
            mcp_http,
            |_| Ok(()),
        )
        .await?;
    }
    Ok(())
}

fn update_text(update: &Value) -> Option<&str> {
    if update_kind(update) != Some("agent_message_chunk") {
        return None;
    }
    content_text(update)
}

fn update_kind(update: &Value) -> Option<&str> {
    update.get("sessionUpdate").and_then(Value::as_str)
}

fn update_message_id(update: &Value) -> Option<String> {
    update
        .get("messageId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
}

fn non_empty_string<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

fn merge_json(current: &mut Value, update: &Value) {
    match (current, update) {
        (Value::Object(current), Value::Object(update)) => {
            for (key, value) in update {
                if value.is_null() || value.as_str().is_some_and(str::is_empty) {
                    continue;
                }
                match current.get_mut(key) {
                    Some(current) => merge_json(current, value),
                    None => {
                        current.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (current, update) if !update.is_null() => *current = update.clone(),
        _ => {}
    }
}

fn content_text(update: &Value) -> Option<&str> {
    update
        .get("content")
        .and_then(|content| {
            content
                .get("text")
                .and_then(Value::as_str)
                .or_else(|| content.as_str())
        })
        .or_else(|| update.get("text").and_then(Value::as_str))
}

fn bounded_json(value: &Value) -> Value {
    let encoded = value.to_string();
    if encoded.len() <= MAX_METADATA_BYTES {
        value.clone()
    } else {
        json!({ "truncated": true, "preview": &encoded[..MAX_METADATA_BYTES] })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AcpHarnessConfig, AcpSessionRequest, AcpStreamEvent, bounded_json,
        run_acp_session_with_extensions_and_events, update_text,
    };
    use crate::rpc::{acp_rpc_error, is_resource_not_found};
    use anyhow::{Result, bail};
    use async_trait::async_trait;
    use mews_agent::{
        AgentCapabilities, CancellationToken, ContextSnapshot, LifecycleHook, ProgressReporter,
        ToolCall, ToolDefinition, ToolResult,
    };
    use serde_json::json;
    use std::{collections::BTreeMap, path::Path};

    #[test]
    fn provider_metadata_is_size_bounded() {
        let value = json!({"body": "x".repeat(20_000)});
        assert_eq!(bounded_json(&value)["truncated"], true);
    }

    #[test]
    fn only_agent_messages_are_answer_text() {
        let message = json!({
            "sessionUpdate": "agent_message_chunk",
            "content": { "type": "text", "text": "answer" }
        });
        let thought = json!({
            "sessionUpdate": "agent_thought_chunk",
            "content": { "type": "text", "text": "internal reasoning" }
        });

        assert_eq!(update_text(&message), Some("answer"));
        assert_eq!(update_text(&thought), None);
    }

    #[test]
    fn only_typed_resource_not_found_is_reconstructable() {
        assert!(is_resource_not_found(&acp_rpc_error(
            "session/resume",
            &json!({"code": -32002, "message": "Resource not found"}),
        )));
        assert!(!is_resource_not_found(&acp_rpc_error(
            "session/resume",
            &json!({"code": -32603, "message": "Session not found"}),
        )));
        assert!(!is_resource_not_found(&anyhow::anyhow!(
            "ACP request timed out"
        )));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn resumes_existing_session_without_replaying_recovery_context() {
        use std::{fs, os::unix::fs::PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("resume-acp");
        fs::write(
            &fixture,
            r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"resume":{}}}}}' ;;
    *'"id":2'*'session/resume'*'native-1'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{}}' ;;
    *'"id":3'*'second turn'*) printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk","content":{"text":"resumed"}}}}'; printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{}}'; exit 0 ;;
    *) exit 9 ;;
  esac
done
"#,
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();
        let outcome = run_acp_session_with_extensions_and_events(
            AcpHarnessConfig::new(vec![fixture.to_string_lossy().into_owned()]).unwrap(),
            directory.path().to_path_buf(),
            BTreeMap::new(),
            AcpSessionRequest {
                prompt: "second turn".into(),
                recovery_prompt: "MUST NOT BE SENT".into(),
                session_id: Some("native-1".into()),
            },
            &NoCapabilities,
            &[],
            &mut |_| Ok(()),
        )
        .await
        .unwrap();
        assert_eq!(outcome.answer, "resumed");
        assert_eq!(outcome.session_id, "native-1");
        assert!(!outcome.session_replaced);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reconstructs_only_after_resource_not_found() {
        use std::{fs, os::unix::fs::PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("recover-acp");
        fs::write(
            &fixture,
            r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"resume":{}}}}}' ;;
    *'"id":2'*'session/resume'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"error":{"code":-32002,"message":"Resource not found"}}' ;;
    *'"id":3'*'session/new'*) printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"sessionId":"native-2"}}' ;;
    *'"id":4'*'recovery history'*) printf '%s\n' '{"jsonrpc":"2.0","id":4,"result":{}}'; exit 0 ;;
    *) exit 9 ;;
  esac
done
"#,
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();
        let mut bound = Vec::new();
        let outcome = run_acp_session_with_extensions_and_events(
            AcpHarnessConfig::new(vec![fixture.to_string_lossy().into_owned()]).unwrap(),
            directory.path().to_path_buf(),
            BTreeMap::new(),
            AcpSessionRequest {
                prompt: "second turn".into(),
                recovery_prompt: "recovery history".into(),
                session_id: Some("native-1".into()),
            },
            &NoCapabilities,
            &[],
            &mut |event| {
                if let super::AcpStreamEvent::SessionBound {
                    session_id,
                    replaced,
                } = event
                {
                    bound.push((session_id, replaced));
                }
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(outcome.session_id, "native-2");
        assert!(outcome.session_replaced);
        assert_eq!(bound, vec![("native-2".into(), true)]);
    }

    struct NoCapabilities;
    #[async_trait(?Send)]
    impl AgentCapabilities for NoCapabilities {
        async fn context(&self, _: &Path) -> Result<ContextSnapshot> {
            Ok(ContextSnapshot::default())
        }
        fn tools(&self) -> Vec<ToolDefinition> {
            Vec::new()
        }
        async fn execute(
            &self,
            _: &ToolCall,
            _: &Path,
            _: &CancellationToken,
            _: &dyn ProgressReporter,
        ) -> Result<ToolResult> {
            bail!("ACP fixture must not call MEWS extension tools")
        }
        async fn hook(
            &self,
            _: LifecycleHook,
            _: serde_json::Value,
            _: &Path,
        ) -> Result<serde_json::Value> {
            Ok(serde_json::Value::Null)
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stdio_fixture_initializes_creates_a_session_and_streams_a_reply() {
        use std::{fs, os::unix::fs::PermissionsExt};

        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("fixture-acp");
        fs::write(
            &fixture,
            r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05"}}' ;;
    *'"id":2'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"fixture"}}' ;;
    *'"id":3'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":90,"method":"session/request_permission","params":{"sessionId":"fixture","toolCall":{"sessionUpdate":"tool_call","toolCallId":"native-1","title":"Run command"},"options":[{"optionId":"allow","name":"Allow Once","kind":"allow_once"},{"optionId":"decline","name":"Decline","kind":"reject_once"}],"_meta":{"provider":"fixture"}}}'
      ;;
    *'"id":90'*'"result"'*'"optionId":"decline"'*)
      printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"agent_message_chunk","messageId":"message-1","content":{"type":"text","text":"intro"}}}}'
      printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"agent_thought_chunk","messageId":"thought-1","content":{"type":"text","text":"checking source"}}}}'
      printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"tool_call","toolCallId":"web-1","title":"Web search","kind":"search","status":"in_progress","rawInput":{"query":""}}}}'
      printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"tool_call_update","toolCallId":"web-1","status":"completed","rawInput":{"query":"weather in Tashkent"}}}}'
      printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"agent_message_chunk","messageId":"message-2","content":{"type":"text","text":"fixture reply"}}}}'
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{}}'
      exit 0
      ;;
  esac
done
"#,
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

        let mut events = Vec::new();
        let outcome = run_acp_session_with_extensions_and_events(
            AcpHarnessConfig::new([fixture.into_os_string()]).unwrap(),
            directory.path().to_owned(),
            BTreeMap::new(),
            AcpSessionRequest {
                prompt: "hello".into(),
                recovery_prompt: "hello".into(),
                session_id: None,
            },
            &NoCapabilities,
            &[],
            &mut |event| {
                events.push(event);
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(outcome.answer, "intro\n\nfixture reply");
        assert!(events.iter().any(|event| {
            matches!(
                event,
                AcpStreamEvent::ProviderState(data)
                    if data["sessionUpdate"] == "permission_request"
                        && data["request"]["_meta"]["provider"] == "fixture"
            )
        }));
        assert!(events.iter().any(
            |event| matches!(event, AcpStreamEvent::AssistantDelta { delta, message_id } if delta == "fixture reply" && message_id.as_deref() == Some("message-2"))
        ));
        assert!(events.iter().any(
            |event| matches!(event, AcpStreamEvent::ReasoningDelta { delta, message_id } if delta == "checking source" && message_id.as_deref() == Some("thought-1"))
        ));
        assert!(events.iter().any(|event| matches!(
            event,
            AcpStreamEvent::ToolActivity { call_id, title, kind, status, input }
                if call_id == "web-1"
                    && title == "Web search"
                    && kind.as_deref() == Some("search")
                    && status.as_deref() == Some("completed")
                    && input["query"] == "weather in Tashkent"
        )));
    }
}
