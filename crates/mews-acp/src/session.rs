//! Persistent ACP session execution and recovery.

use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    process::Child,
};

use crate::{
    mcp::{RunMcpBridge, RunMcpHttp},
    process::{AcpHarnessConfig, AcpProcess, ProcessTreeGuard, terminate_process_tree},
    rpc::{AcpErrorKind, RpcClient, classify_error, is_resource_not_found},
    updates::UpdateState,
};

// ACP v1 uses a negotiated integer protocol version. The date-shaped value
// belongs to MCP, not ACP.
const ACP_PROTOCOL_VERSION: u16 = 1;

/// Bounded ACP discovery output. Hosts normalize this into their public
/// catalog and persist it, so clients never start an adapter just to redraw a
/// selector.
#[derive(Clone, Debug)]
pub struct AcpProbe {
    pub initialize: Value,
    pub session: Option<Value>,
    pub session_error: Option<String>,
    pub session_error_kind: Option<AcpErrorKind>,
    pub timings: AcpProbeTimings,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AcpProbeTimings {
    pub spawn: Duration,
    pub initialize: Duration,
    pub session: Duration,
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
    pub stop_reason: AcpStopReason,
    pub timings: AcpTimings,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AcpTimings {
    pub spawn: Duration,
    pub initialize: Duration,
    pub continuation: Duration,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InitializeResult {
    protocol_version: u16,
    #[serde(default)]
    agent_capabilities: Value,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AcpStopReason {
    EndTurn,
    MaxTokens,
    MaxTurnRequests,
    Refusal,
    Cancelled,
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PromptResult {
    stop_reason: AcpStopReason,
}

#[derive(Clone, Copy, Debug)]
enum ContinuationMethod {
    Resume,
    Load,
}

#[allow(clippy::too_many_arguments)] // This is the public composition boundary for one Run.
pub async fn run_acp_session_with_extensions_and_events(
    config: AcpHarnessConfig,
    cwd: PathBuf,
    harness_options: BTreeMap<String, String>,
    session: AcpSessionRequest,
    environment: &dyn mews_agent::AgentCapabilities,
    allowed_tools: &[String],
    cancellation: mews_agent::CancellationToken,
    events: &mut dyn FnMut(AcpStreamEvent) -> Result<()>,
) -> Result<AcpSessionOutcome> {
    let harness = AcpProcess::new(config);
    let spawn_started = tokio::time::Instant::now();
    let mut child = harness.spawn(&cwd)?;
    let mut process_guard = ProcessTreeGuard::new(&child);
    let spawn = spawn_started.elapsed();
    let result = harness
        .run_session_with_extensions(
            &mut child,
            cwd,
            harness_options,
            session,
            environment,
            allowed_tools,
            cancellation,
            events,
        )
        .await;
    if result.is_err() {
        terminate_process_tree(&mut child).await;
        process_guard.disarm();
    }
    let mut result = result.context("ACP Harness failed")?;
    result.timings.spawn = spawn;
    let status = child.wait().await.context("wait for ACP Harness")?;
    process_guard.disarm();
    if !status.success() {
        bail!("ACP Harness exited with {status}");
    }
    Ok(result)
}

pub async fn probe_acp(config: AcpHarnessConfig, cwd: PathBuf) -> Result<AcpProbe> {
    let process = AcpProcess::new(config);
    let spawn_started = tokio::time::Instant::now();
    let mut child = process.spawn(&cwd)?;
    let mut process_guard = ProcessTreeGuard::new(&child);
    let spawn = spawn_started.elapsed();
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
        let mut reader = BufReader::new(stdout);
        let cancellation = mews_agent::CancellationToken::new();
        let mut rpc = RpcClient::new(
            &mut writer,
            &mut reader,
            process.config.request_timeout,
            process.config.permission_handler.as_ref(),
        );
        let initialize_started = tokio::time::Instant::now();
        let initialize = rpc
            .request(
                "initialize",
                json!({
                    "protocolVersion": ACP_PROTOCOL_VERSION,
                    "clientInfo": { "name": "mews", "version": env!("CARGO_PKG_VERSION") },
                    "clientCapabilities": { "auth": { "terminal": true } },
                }),
                &cancellation,
                None,
                None,
                |_| Ok(()),
            )
            .await?;
        let initialize_elapsed = initialize_started.elapsed();
        let session_started = tokio::time::Instant::now();
        let session = rpc
            .request(
                "session/new",
                json!({ "cwd": cwd, "mcpServers": [] }),
                &cancellation,
                None,
                None,
                |_| Ok(()),
            )
            .await;
        let session_elapsed = session_started.elapsed();
        let session_error_kind = session.as_ref().err().and_then(classify_error);
        Ok(AcpProbe {
            initialize,
            session: session.as_ref().ok().cloned(),
            session_error: session.err().map(|error| error.to_string()),
            session_error_kind,
            timings: AcpProbeTimings {
                spawn,
                initialize: initialize_elapsed,
                session: session_elapsed,
            },
        })
    }
    .await;
    terminate_process_tree(&mut child).await;
    process_guard.disarm();
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
        cancellation: mews_agent::CancellationToken,
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
        let mut reader = BufReader::new(stdout);
        let mut rpc = RpcClient::new(
            &mut writer,
            &mut reader,
            self.config.request_timeout,
            self.config.permission_handler.as_ref(),
        );
        let initialize_started = tokio::time::Instant::now();
        let initialize = rpc
            .request(
                "initialize",
                json!({
                    "protocolVersion": ACP_PROTOCOL_VERSION,
                    "clientInfo": { "name": "mews", "version": env!("CARGO_PKG_VERSION") },
                    "clientCapabilities": { "auth": { "terminal": true } },
                }),
                &cancellation,
                None,
                None,
                |_| Ok(()),
            )
            .await?;
        let initialize_elapsed = initialize_started.elapsed();
        let initialize: InitializeResult =
            serde_json::from_value(initialize).context("invalid ACP initialize result")?;
        if initialize.protocol_version != ACP_PROTOCOL_VERSION {
            bail!(
                "ACP Harness negotiated unsupported protocol version {}",
                initialize.protocol_version
            );
        }
        let mcp = RunMcpBridge::for_extensions(
            environment,
            cwd.clone(),
            cancellation.clone(),
            allowed_tools,
        )?;
        let mcp_http = if mcp.tool_definitions().is_empty() {
            None
        } else if initialize
            .agent_capabilities
            .pointer("/mcpCapabilities/http")
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
        let continuation_started = tokio::time::Instant::now();
        let (session_id, session_replaced, prompt) = if let Some(session_id) = session.session_id {
            let method = if initialize
                .agent_capabilities
                .pointer("/sessionCapabilities/resume")
                .is_some()
            {
                ContinuationMethod::Resume
            } else if initialize
                .agent_capabilities
                .pointer("/loadSession")
                .and_then(Value::as_bool)
                == Some(true)
            {
                ContinuationMethod::Load
            } else {
                bail!("ACP Harness cannot resume persistent Sessions")
            };
            let resumed = rpc
                .request(
                    match method {
                        ContinuationMethod::Resume => "session/resume",
                        ContinuationMethod::Load => "session/load",
                    },
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
        let continuation_elapsed = continuation_started.elapsed();
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
        let mut updates = UpdateState::default();
        let prompt_result = rpc
            .request(
                "session/prompt",
                json!({ "sessionId": session_id, "prompt": [{ "type": "text", "text": prompt }] }),
                &cancellation,
                Some(&mcp),
                mcp_http.as_ref(),
                |update| updates.apply(update, events),
            )
            .await?;
        let prompt_result: PromptResult =
            serde_json::from_value(prompt_result).context("invalid ACP session/prompt result")?;
        mcp.revoke();
        if prompt_result.stop_reason == AcpStopReason::Cancelled {
            bail!("ACP Harness reported that the prompt was cancelled");
        }
        Ok(AcpSessionOutcome {
            answer: updates.answer(),
            session_id,
            session_replaced,
            stop_reason: prompt_result.stop_reason,
            timings: AcpTimings {
                spawn: Duration::ZERO,
                initialize: initialize_elapsed,
                continuation: continuation_elapsed,
            },
        })
    }
}

fn acp_session_id(response: &Value) -> Result<String> {
    response
        .get("sessionId")
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

#[cfg(test)]
mod tests {
    use super::{
        AcpHarnessConfig, AcpSessionRequest, AcpStopReason, AcpStreamEvent, probe_acp,
        run_acp_session_with_extensions_and_events,
    };
    use crate::rpc::{AcpErrorKind, acp_rpc_error, classify_error, is_resource_not_found};
    use crate::updates::update_text;
    use anyhow::{Result, bail};
    use async_trait::async_trait;
    use mews_agent::{
        AgentCapabilities, CancellationToken, ContextSnapshot, LifecycleHook, ProgressReporter,
        ToolCall, ToolDefinition, ToolResult,
    };
    use serde_json::json;
    use std::{collections::BTreeMap, path::Path, time::Duration};

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
        let authentication = acp_rpc_error(
            "session/new",
            &json!({"code":-32000,"message":"any diagnostic prose"}),
        )
        .context("wrapped run failure");
        assert_eq!(
            classify_error(&authentication),
            Some(AcpErrorKind::AuthenticationRequired)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn probe_classifies_authentication_by_acp_error_code_and_reports_timings() {
        use std::{fs, os::unix::fs::PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("auth-acp");
        fs::write(
            &fixture,
            r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"id":1'*) sleep 0.02; printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{}}}' ;;
    *'"id":2'*) sleep 0.02; printf '%s\n' '{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"Provider sign-in required"}}'; sleep 30 ;;
  esac
done
"#,
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

        let probe = probe_acp(
            AcpHarnessConfig::new([fixture.into_os_string()]).unwrap(),
            directory.path().to_owned(),
        )
        .await
        .unwrap();
        assert_eq!(
            probe.session_error_kind,
            Some(AcpErrorKind::AuthenticationRequired)
        );
        assert!(probe.timings.initialize >= Duration::from_millis(10));
        assert!(probe.timings.session >= Duration::from_millis(10));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_an_unsupported_negotiated_protocol_version() {
        use std::{fs, os::unix::fs::PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("wrong-version-acp");
        fs::write(
            &fixture,
            r#"#!/bin/sh
IFS= read -r line
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":2,"agentCapabilities":{}}}'
sleep 30
"#,
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

        let error = run_acp_session_with_extensions_and_events(
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
            CancellationToken::new(),
            &mut |_| Ok(()),
        )
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains("unsupported protocol version 2"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn continuous_updates_do_not_extend_the_absolute_request_deadline() {
        use std::{fs, os::unix::fs::PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("updates-forever-acp");
        fs::write(
            &fixture,
            r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{}}}' ;;
    *'"id":2'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"fixture"}}' ;;
    *'"id":3'*) while true; do printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"agent_thought_chunk","text":"busy"}}}'; sleep 0.01; done ;;
  esac
done
"#,
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();
        let mut config = AcpHarnessConfig::new([fixture.into_os_string()]).unwrap();
        config.request_timeout = Duration::from_millis(100);

        let error = tokio::time::timeout(
            Duration::from_secs(2),
            run_acp_session_with_extensions_and_events(
                config,
                directory.path().to_owned(),
                BTreeMap::new(),
                AcpSessionRequest {
                    prompt: "never finishes".into(),
                    recovery_prompt: "never finishes".into(),
                    session_id: None,
                },
                &NoCapabilities,
                &[],
                CancellationToken::new(),
                &mut |_| Ok(()),
            ),
        )
        .await
        .expect("absolute deadline must not be reset by updates")
        .unwrap_err();
        assert!(format!("{error:#}").contains("timed out"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_terminates_adapter_descendants() {
        use std::{fs, os::unix::fs::PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("cancel-acp");
        let descendant_path = directory.path().join("descendant.pid");
        fs::write(
            &fixture,
            format!(
                r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"id":1'*) printf '%s\n' '{{"jsonrpc":"2.0","id":1,"result":{{"protocolVersion":1,"agentCapabilities":{{}}}}}}' ;;
    *'"id":2'*) printf '%s\n' '{{"jsonrpc":"2.0","id":2,"result":{{"sessionId":"fixture"}}}}' ;;
    *'"id":3'*) sleep 30 & child=$!; printf %s "$child" > {}; wait ;;
  esac
done
"#,
                descendant_path.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();
        let cancellation = CancellationToken::new();
        let mut events = |_| Ok(());
        let execution = run_acp_session_with_extensions_and_events(
            AcpHarnessConfig::new([fixture.into_os_string()]).unwrap(),
            directory.path().to_owned(),
            BTreeMap::new(),
            AcpSessionRequest {
                prompt: "start child".into(),
                recovery_prompt: "start child".into(),
                session_id: None,
            },
            &NoCapabilities,
            &[],
            cancellation.clone(),
            &mut events,
        );
        tokio::pin!(execution);
        let descendant = loop {
            tokio::select! {
                result = &mut execution => panic!("adapter exited before cancellation: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(10)) => {
                    if let Ok(pid) = fs::read_to_string(&descendant_path) {
                        break pid.parse::<i32>().unwrap();
                    }
                }
            }
        };
        cancellation.cancel();
        let error = execution.await.unwrap_err();
        assert!(format!("{error:#}").contains("cancelled"));
        for _ in 0..100 {
            // SAFETY: signal 0 only checks whether this test descendant exists.
            if unsafe { libc::kill(descendant, 0) } == -1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("cancelled ACP descendant {descendant} is still running");
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
    *'"id":1'*) sleep 0.02; printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"resume":{}}}}}' ;;
    *'"id":2'*'session/resume'*'native-1'*) sleep 0.02; printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{}}' ;;
    *'"id":3'*'second turn'*) printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk","content":{"text":"resumed"}}}}'; printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn"}}'; exit 0 ;;
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
            CancellationToken::new(),
            &mut |_| Ok(()),
        )
        .await
        .unwrap();
        assert_eq!(outcome.answer, "resumed");
        assert_eq!(outcome.session_id, "native-1");
        assert!(!outcome.session_replaced);
        assert!(outcome.timings.initialize >= Duration::from_millis(10));
        assert!(outcome.timings.continuation >= Duration::from_millis(10));
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
    *'"id":4'*'recovery history'*) printf '%s\n' '{"jsonrpc":"2.0","id":4,"result":{"stopReason":"end_turn"}}'; exit 0 ;;
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
            CancellationToken::new(),
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
    #[async_trait]
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
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{}}}' ;;
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
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn"}}'
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
            CancellationToken::new(),
            &mut |event| {
                events.push(event);
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(outcome.answer, "intro\n\nfixture reply");
        assert_eq!(outcome.stop_reason, AcpStopReason::EndTurn);
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
