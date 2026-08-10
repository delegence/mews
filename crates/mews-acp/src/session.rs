//! Persistent ACP session execution and recovery.

use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    process::Child,
};

use mews_protocol::{AcpBindingTransition, AcpInstructionChannel, AcpReplacementReason};

use crate::{
    mcp::{RunMcpBridge, RunMcpHttp},
    process::{AcpHarnessConfig, AcpProcess, ProcessTreeGuard, terminate_process_tree},
    rpc::{AcpCancelled, AcpErrorKind, RpcClient, classify_error, is_resource_not_found},
    updates::UpdateState,
};

// ACP v1 uses a negotiated integer protocol version. The date-shaped value
// belongs to MCP, not ACP.
const ACP_PROTOCOL_VERSION: u16 = 1;
const CODEX_RESTART_FOR_RECOVERY: &str = "mews: restart Codex adapter for ACP recovery";

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
        event_key: mews_protocol::AcpEventKey,
        delta: String,
        message_id: Option<String>,
        raw: Value,
    },
    ReasoningDelta {
        event_key: mews_protocol::AcpEventKey,
        delta: String,
        message_id: Option<String>,
        raw: Value,
    },
    ToolActivity {
        event_key: mews_protocol::AcpEventKey,
        call_id: String,
        title: String,
        kind: Option<String>,
        status: Option<String>,
        input: Value,
    },
    ProviderState {
        event_key: mews_protocol::AcpEventKey,
        data: Value,
    },
    SessionBound {
        event_key: mews_protocol::AcpEventKey,
        session_id: String,
        transition: AcpBindingTransition,
    },
    /// The initialization channel has crossed the provider boundary.  This
    /// is deliberately separate from SessionBound for FirstPrompt adapters.
    ContextDispatched {
        event_key: mews_protocol::AcpEventKey,
        session_id: String,
    },
    HookOutcome {
        event_key: mews_protocol::AcpEventKey,
        hook: String,
        ok: bool,
        detail: Option<String>,
        tool: Option<String>,
        call_id: Option<String>,
    },
}

fn accepted_event_key() -> mews_protocol::AcpEventKey {
    // This process is the ACP ingress boundary. A new accepted notification
    // gets one identity here; forwarding and durable replay preserve it.
    uuid::Uuid::now_v7().to_string()
}

/// Stable MEWS identifiers and the initialization boundary supplied by the
/// caller.  ACP owns the exact prompt boundary; callers must not pre-run it.
#[derive(Clone, Debug)]
pub struct AcpHookMetadata {
    pub mews_session_id: String,
    pub run_id: String,
    pub harness: String,
    pub context_hash: String,
    pub context_channel: AcpInstructionChannel,
    pub invoke_run_start: bool,
}

#[derive(Clone, Debug)]
pub struct AcpSessionRequest {
    pub transition: AcpBindingTransition,
    pub prompt: String,
    pub recovery_prompt: String,
    pub context_text: String,
    pub instruction_channel: AcpInstructionChannel,
    pub skills: Vec<crate::mcp::AcpSkill>,
    pub hook_metadata: Option<AcpHookMetadata>,
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
    mut config: AcpHarnessConfig,
    cwd: PathBuf,
    harness_options: BTreeMap<String, String>,
    session: AcpSessionRequest,
    environment: &dyn mews_agent::AgentCapabilities,
    allowed_tools: &[String],
    cancellation: mews_agent::CancellationToken,
    events: &mut dyn FnMut(AcpStreamEvent) -> Result<()>,
) -> Result<AcpSessionOutcome> {
    let replacement_config = config.clone();
    let replacement_session = session.clone();
    let hook_metadata = session.hook_metadata.clone();
    if let Some(metadata) = hook_metadata
        .as_ref()
        .filter(|metadata| metadata.invoke_run_start)
    {
        let payload = json!({
            "session_id": metadata.mews_session_id, "run_id": metadata.run_id,
            "harness": metadata.harness, "cwd": cwd, "binding": session.transition,
            "context_hash": metadata.context_hash, "context_channel": metadata.context_channel,
        });
        match environment
            .hook(
                mews_agent::LifecycleHook::RunStart,
                payload,
                &cwd,
                &cancellation,
            )
            .await
        {
            Ok(_) => events(AcpStreamEvent::HookOutcome {
                event_key: accepted_event_key(),
                hook: "run_start".into(),
                ok: true,
                detail: None,
                tool: None,
                call_id: None,
            })?,
            Err(error) => {
                let detail = bounded_detail(&error.to_string());
                events(AcpStreamEvent::HookOutcome {
                    event_key: accepted_event_key(),
                    hook: "run_start".into(),
                    ok: false,
                    detail: Some(detail.clone()),
                    tool: None,
                    call_id: None,
                })?;
                let _ = record_telemetry_hook(
                    environment,
                    mews_agent::LifecycleHook::RunEnd,
                    "run_end",
                    json!({"session_id": metadata.mews_session_id, "run_id": metadata.run_id,
                        "status": "failed", "outcome": bounded_detail(&error.to_string())}),
                    &cwd,
                    &cancellation,
                    events,
                )
                .await;
                return Err(error).context("ACP run_start hook failed");
            }
        }
    }
    if let Err(error) = prepare_instruction_channel(&mut config, &session) {
        if let Some(metadata) = &hook_metadata {
            let _ = record_telemetry_hook(
                environment,
                mews_agent::LifecycleHook::RunEnd,
                "run_end",
                json!({"session_id": metadata.mews_session_id, "run_id": metadata.run_id,
                    "status":"failed", "outcome": bounded_detail(&error.to_string())}),
                &cwd,
                &cancellation,
                events,
            )
            .await;
        }
        return Err(error);
    }
    let harness = AcpProcess::new(config);
    let spawn_started = tokio::time::Instant::now();
    let mut child = match harness.spawn(&cwd) {
        Ok(child) => child,
        Err(error) => {
            if let Some(metadata) = &hook_metadata {
                let _ = record_telemetry_hook(
                    environment,
                    mews_agent::LifecycleHook::RunEnd,
                    "run_end",
                    json!({"session_id": metadata.mews_session_id, "run_id": metadata.run_id,
                        "status":"failed", "outcome": bounded_detail(&error.to_string())}),
                    &cwd,
                    &cancellation,
                    events,
                )
                .await;
            }
            return Err(error);
        }
    };
    let mut process_guard = ProcessTreeGuard::new(&child);
    let spawn = spawn_started.elapsed();
    let mut observed_activities = Vec::new();
    let result = harness
        .run_session_with_extensions(
            &mut child,
            cwd.clone(),
            harness_options.clone(),
            session,
            environment,
            allowed_tools,
            cancellation.clone(),
            &mut |event| {
                if observed_activities.len() < 64 {
                    match &event {
                        AcpStreamEvent::ProviderState { data, .. } => observed_activities.push(json!({"type":"provider_state","data": crate::updates::bounded_json(data)})),
                        AcpStreamEvent::ToolActivity { call_id, title, status, .. } => observed_activities.push(json!({"type":"tool_activity","call_id":call_id,"title":title,"status":status})),
                        _ => {}
                    }
                }
                events(event)
            },
        )
        .await;
    if result
        .as_ref()
        .err()
        .is_some_and(|error| error.to_string().contains(CODEX_RESTART_FOR_RECOVERY))
    {
        terminate_process_tree(&mut child).await;
        process_guard.disarm();
        let _ = child.wait().await;
        let mut replacement = replacement_session;
        replacement.transition = AcpBindingTransition::Replace {
            reason: AcpReplacementReason::ResourceNotFound,
        };
        if let Some(metadata) = &mut replacement.hook_metadata {
            metadata.invoke_run_start = false;
        }
        return Box::pin(run_acp_session_with_extensions_and_events(
            replacement_config,
            cwd,
            harness_options,
            replacement,
            environment,
            allowed_tools,
            cancellation,
            events,
        ))
        .await;
    }
    if let Some(metadata) = &hook_metadata {
        let after_turn = if let Ok(outcome) = &result {
            record_telemetry_hook(
                environment,
                mews_agent::LifecycleHook::AfterTurn,
                "after_turn",
                json!({"session_id": metadata.mews_session_id, "run_id": metadata.run_id,
                    "acp_session_id": outcome.session_id, "answer": bounded_detail(&outcome.answer),
                    "stop_reason": outcome.stop_reason, "activities": observed_activities}),
                &cwd,
                &cancellation,
                events,
            )
            .await
        } else {
            Ok(())
        };
        let (status, detail) = match &result {
            Ok(outcome) => ("succeeded", Some(bounded_detail(&outcome.answer))),
            Err(error) if crate::rpc::is_cancelled(error) => ("cancelled", None),
            Err(error) => ("failed", Some(bounded_detail(&error.to_string()))),
        };
        let run_end = record_telemetry_hook(
            environment,
            mews_agent::LifecycleHook::RunEnd,
            "run_end",
            json!({"session_id": metadata.mews_session_id, "run_id": metadata.run_id,
                    "status": status, "outcome": detail}),
            &cwd,
            &cancellation,
            events,
        )
        .await;
        match (after_turn, run_end) {
            (Err(after_turn), Err(run_end)) => {
                return Err(after_turn)
                    .context(format!("run_end telemetry also failed: {run_end:#}"));
            }
            (Err(error), Ok(())) | (Ok(()), Err(error)) => return Err(error),
            (Ok(()), Ok(())) => {}
        }
    }
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

async fn record_telemetry_hook(
    environment: &dyn mews_agent::AgentCapabilities,
    lifecycle: mews_agent::LifecycleHook,
    hook: &str,
    payload: Value,
    cwd: &std::path::Path,
    cancellation: &mews_agent::CancellationToken,
    events: &mut dyn FnMut(AcpStreamEvent) -> Result<()>,
) -> Result<()> {
    match environment
        .hook(lifecycle, payload, cwd, cancellation)
        .await
    {
        Ok(_) => events(AcpStreamEvent::HookOutcome {
            event_key: accepted_event_key(),
            hook: hook.into(),
            ok: true,
            detail: None,
            tool: None,
            call_id: None,
        }),
        Err(error) => events(AcpStreamEvent::HookOutcome {
            event_key: accepted_event_key(),
            hook: hook.into(),
            ok: false,
            detail: Some(bounded_detail(&error.to_string())),
            tool: None,
            call_id: None,
        }),
    }
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
        let mut mcp = RunMcpBridge::for_extensions_and_skills(
            environment,
            cwd.clone(),
            cancellation.clone(),
            allowed_tools,
            session.skills.clone(),
        )?;
        if let Some(metadata) = &session.hook_metadata {
            mcp.set_correlation(crate::mcp::McpCorrelation {
                mews_session_id: metadata.mews_session_id.clone(),
                run_id: metadata.run_id.clone(),
                harness: metadata.harness.clone(),
                acp_session_id: std::sync::Arc::new(std::sync::Mutex::new(None)),
            });
        }
        let lifecycle = async {
        let mcp_http = if !mcp.needs_transport() {
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
        let continuation_started = tokio::time::Instant::now();
        let (session_id, session_replaced, prompt, binding_transition) =
            if let AcpBindingTransition::Resume {
                acp_session_id: session_id,
            } = &session.transition
            {
                let method = if initialize
                    .agent_capabilities
                    .pointer("/sessionCapabilities/resume")
                    .is_some_and(|capability| capability.is_object())
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
                    Ok(_) => (session_id.clone(), false, session.prompt.clone(), None),
                    Err(error) if is_resource_not_found(&error) => {
                        if session.instruction_channel == AcpInstructionChannel::CodexDeveloper {
                            bail!(CODEX_RESTART_FOR_RECOVERY);
                        }
                        let created = rpc
                            .request(
                                "session/new",
                                session_new_params(&cwd, mcp_servers, &session),
                                &cancellation,
                                Some(&mcp),
                                mcp_http.as_ref(),
                                |_| Ok(()),
                            )
                            .await?;
                        (
                            acp_session_id(&created)?,
                            true,
                            outbound_prompt(&session, &session.recovery_prompt),
                            Some(AcpBindingTransition::Replace {
                                reason: AcpReplacementReason::ResourceNotFound,
                            }),
                        )
                    }
                    Err(error) => return Err(error),
                }
            } else {
                let created = rpc
                    .request(
                        "session/new",
                        session_new_params(&cwd, mcp_servers, &session),
                        &cancellation,
                        Some(&mcp),
                        mcp_http.as_ref(),
                        |_| Ok(()),
                    )
                    .await?;
                let transition = session.transition.clone();
                let replacement = matches!(transition, AcpBindingTransition::Replace { .. });
                (
                    acp_session_id(&created)?,
                    replacement,
                    outbound_prompt(
                        &session,
                        if replacement {
                            &session.recovery_prompt
                        } else {
                            &session.prompt
                        },
                    ),
                    Some(transition),
                )
            };
        let continuation_elapsed = continuation_started.elapsed();
        if let Some(transition) = &binding_transition {
            events(AcpStreamEvent::SessionBound {
                event_key: format!("binding:{session_id}"),
                session_id: session_id.clone(),
                transition: transition.clone(),
            })?;
        }
        mcp.set_acp_session_id(session_id.clone());
        apply_harness_options(
            &mut rpc,
            &session_id,
            &harness_options,
            &cancellation,
            Some(&mcp),
            mcp_http.as_ref(),
        )
        .await?;
        let mut updates = UpdateState::for_run(
            session
                .hook_metadata
                .as_ref()
                .map_or("local", |metadata| metadata.run_id.as_str()),
        );
        let prompt = before_model(
            environment,
            &session,
            &session_id,
            binding_transition.as_ref().unwrap_or(&session.transition),
            prompt,
            &cwd,
            &cancellation,
            events,
        )
        .await;
        let prompt = match prompt {
            Ok(prompt) => prompt,
            Err(error) => {
                return Err(error);
            }
        };
        if binding_transition.is_some()
            && session.instruction_channel == AcpInstructionChannel::FirstPrompt
        {
            events(AcpStreamEvent::ContextDispatched {
                event_key: format!("context_dispatched:{session_id}"),
                session_id: session_id.clone(),
            })?;
        }
        let prompt_result = rpc
            .request(
                "session/prompt",
                json!({ "sessionId": session_id, "prompt": [{ "type": "text", "text": prompt }] }),
                &cancellation,
                Some(&mcp),
                mcp_http.as_ref(),
                |update| updates.apply(update, events),
            )
            .await;
        let prompt_result = prompt_result?;
        let prompt_result: PromptResult =
            serde_json::from_value(prompt_result).context("invalid ACP session/prompt result")?;
        if prompt_result.stop_reason == AcpStopReason::Cancelled {
            return Err(anyhow::anyhow!(AcpCancelled));
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
        }.await;
        let audit = drain_mcp_hook_outcomes(&mcp, events);
        mcp.revoke();
        match (lifecycle, audit) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Err(primary), Ok(())) => Err(primary),
            (Ok(_), Err(audit)) => Err(audit.context("failed to persist MCP hook audit")),
            (Err(primary), Err(audit)) => Err(primary.context(format!(
                "ACP lifecycle failed; additionally failed to persist MCP hook audit: {audit:#}"
            ))),
        }
    }
}

fn drain_mcp_hook_outcomes(
    mcp: &RunMcpBridge<'_>,
    events: &mut dyn FnMut(AcpStreamEvent) -> Result<()>,
) -> Result<()> {
    for outcome in mcp.drain_hook_outcomes() {
        events(AcpStreamEvent::HookOutcome {
            event_key: accepted_event_key(),
            hook: outcome.hook,
            ok: outcome.ok,
            detail: outcome.detail,
            tool: outcome.tool,
            call_id: outcome.call_id,
        })?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // Run-scoped hook inputs are assembled at the ACP boundary.
async fn before_model(
    environment: &dyn mews_agent::AgentCapabilities,
    session: &AcpSessionRequest,
    acp_session_id: &str,
    transition: &AcpBindingTransition,
    prompt: String,
    cwd: &std::path::Path,
    cancellation: &mews_agent::CancellationToken,
    events: &mut dyn FnMut(AcpStreamEvent) -> Result<()>,
) -> Result<String> {
    let Some(metadata) = &session.hook_metadata else {
        return Ok(prompt);
    };
    let payload = json!({
        "session_id": metadata.mews_session_id, "run_id": metadata.run_id,
        "acp_session_id": acp_session_id, "harness": metadata.harness,
        "mode": transition, "prompt": prompt,
    });
    let response = match environment
        .hook(
            mews_agent::LifecycleHook::BeforeModel,
            payload,
            cwd,
            cancellation,
        )
        .await
    {
        Ok(response) => response,
        Err(error) => {
            let detail = bounded_detail(&error.to_string());
            events(AcpStreamEvent::HookOutcome {
                event_key: accepted_event_key(),
                hook: "before_model".into(),
                ok: false,
                detail: Some(detail.clone()),
                tool: None,
                call_id: None,
            })?;
            return Err(error).context("ACP before_model hook failed");
        }
    };
    let response = match parse_before_model(response) {
        Ok(response) => response,
        Err(detail) => {
            events(AcpStreamEvent::HookOutcome {
                event_key: accepted_event_key(),
                hook: "before_model".into(),
                ok: false,
                detail: Some(detail.clone()),
                tool: None,
                call_id: None,
            })?;
            bail!("invalid ACP before_model hook response: {detail}");
        }
    };
    if let Some(reason) = response.block {
        let detail = bounded_detail(&reason);
        events(AcpStreamEvent::HookOutcome {
            event_key: accepted_event_key(),
            hook: "before_model".into(),
            ok: false,
            detail: Some(detail.clone()),
            tool: None,
            call_id: None,
        })?;
        bail!("ACP before_model hook blocked prompt: {detail}");
    }
    let prompt = response.prompt.unwrap_or(prompt);
    events(AcpStreamEvent::HookOutcome {
        event_key: accepted_event_key(),
        hook: "before_model".into(),
        ok: true,
        detail: None,
        tool: None,
        call_id: None,
    })?;
    Ok(prompt)
}

struct BeforeModel {
    block: Option<String>,
    prompt: Option<String>,
}

fn parse_before_model(value: Value) -> std::result::Result<BeforeModel, String> {
    let object = match value {
        Value::Null => {
            return Ok(BeforeModel {
                block: None,
                prompt: None,
            });
        }
        Value::Object(object) => object,
        _ => return Err("hook response must be an object or null".into()),
    };
    let string = |key: &str| {
        object.get(key).map_or(Ok(None), |value| {
            value
                .as_str()
                .map(str::to_owned)
                .map(Some)
                .ok_or_else(|| format!("{key} must be a string"))
        })
    };
    Ok(BeforeModel {
        block: string("block")?,
        prompt: string("prompt")?,
    })
}

fn bounded_detail(value: &str) -> String {
    value.chars().take(1024).collect()
}

fn acp_session_id(response: &Value) -> Result<String> {
    let session_id = response
        .get("sessionId")
        .and_then(Value::as_str)
        .filter(|session_id| !session_id.is_empty())
        .filter(|session_id| session_id.len() <= mews_protocol::MAX_ACP_SESSION_ID_BYTES)
        .map(str::to_owned)
        .context("ACP session/new response did not include a valid sessionId")?;
    Ok(session_id)
}

fn outbound_prompt(session: &AcpSessionRequest, text: &str) -> String {
    match session.instruction_channel {
        AcpInstructionChannel::FirstPrompt => format!("{}\n\n{}", session.context_text, text),
        AcpInstructionChannel::CodexDeveloper | AcpInstructionChannel::ClaudeSystemAppend => {
            text.to_owned()
        }
    }
}

fn session_new_params(
    cwd: &PathBuf,
    mcp_servers: Vec<Value>,
    session: &AcpSessionRequest,
) -> Value {
    let mut params = json!({ "cwd": cwd, "mcpServers": mcp_servers });
    if session.instruction_channel == AcpInstructionChannel::ClaudeSystemAppend {
        params["_meta"] = json!({ "systemPrompt": { "append": session.context_text } });
    }
    params
}

/// Codex ACP reads CODEX_CONFIG at process start. Merge only the developer
/// channel; base instructions and unrelated trusted recipe configuration win.
fn prepare_instruction_channel(
    config: &mut AcpHarnessConfig,
    session: &AcpSessionRequest,
) -> Result<()> {
    // A successful resume must not inject initialization instructions into an
    // existing provider conversation.  New/replacement paths own the channel.
    if session.instruction_channel != AcpInstructionChannel::CodexDeveloper
        || matches!(session.transition, AcpBindingTransition::Resume { .. })
    {
        return Ok(());
    }
    let key = std::ffi::OsString::from("CODEX_CONFIG");
    let mut value = match config.environment.get(&key) {
        Some(value) => serde_json::from_str::<Value>(&value.to_string_lossy())
            .context("CODEX_CONFIG must be a JSON object")?,
        None => json!({}),
    };
    let object = value
        .as_object_mut()
        .context("CODEX_CONFIG must be a JSON object")?;
    object.insert(
        "developer_instructions".into(),
        Value::String(session.context_text.clone()),
    );
    config
        .environment
        .insert(key, serde_json::to_string(&value)?.into());
    Ok(())
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
        AcpHarnessConfig, AcpSessionRequest, AcpStopReason, AcpStreamEvent,
        prepare_instruction_channel, probe_acp, run_acp_session_with_extensions_and_events,
        session_new_params,
    };
    use crate::rpc::{AcpErrorKind, acp_rpc_error, classify_error, is_resource_not_found};
    use crate::updates::update_text;
    use anyhow::{Result, bail};
    use async_trait::async_trait;
    use mews_agent::{
        AgentCapabilities, CancellationToken, ContextSnapshot, LifecycleHook, ProgressReporter,
        ToolCall, ToolDefinition, ToolResult,
    };
    use mews_protocol::{AcpBindingTransition, AcpInstructionChannel, AcpReplacementReason};
    use serde_json::{Value, json};
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

    #[test]
    fn managed_instruction_channels_initialize_only_new_bindings() {
        let mut config = AcpHarnessConfig::new(["fixture"]).unwrap();
        let codex_config = std::ffi::OsString::from("CODEX_CONFIG");
        config.environment.insert(
            codex_config.clone(),
            r#"{"approval_policy":"never"}"#.into(),
        );
        let request = AcpSessionRequest {
            transition: AcpBindingTransition::New,
            prompt: "user".into(),
            recovery_prompt: String::new(),
            context_text: "MEWS context".into(),
            instruction_channel: AcpInstructionChannel::CodexDeveloper,
            skills: Vec::new(),
            hook_metadata: None,
        };
        prepare_instruction_channel(&mut config, &request).unwrap();
        let merged: Value =
            serde_json::from_str(&config.environment[&codex_config].to_string_lossy()).unwrap();
        assert_eq!(merged["approval_policy"], "never");
        assert_eq!(merged["developer_instructions"], "MEWS context");
        let claude = AcpSessionRequest {
            instruction_channel: AcpInstructionChannel::ClaudeSystemAppend,
            ..request.clone()
        };
        assert_eq!(
            session_new_params(&Path::new("/tmp").to_path_buf(), Vec::new(), &claude)["_meta"]["systemPrompt"]
                ["append"],
            "MEWS context"
        );
        let resume = AcpSessionRequest {
            transition: AcpBindingTransition::Resume {
                acp_session_id: "saved".into(),
            },
            ..request
        };
        let mut resumed_config = AcpHarnessConfig::new(["fixture"]).unwrap();
        prepare_instruction_channel(&mut resumed_config, &resume).unwrap();
        assert!(!resumed_config.environment.contains_key(&codex_config));
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
                transition: AcpBindingTransition::New,
                prompt: "hello".into(),
                recovery_prompt: "hello".into(),
                context_text: String::new(),
                instruction_channel: AcpInstructionChannel::FirstPrompt,
                skills: Vec::new(),
                hook_metadata: None,
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
                    transition: AcpBindingTransition::New,
                    prompt: "never finishes".into(),
                    recovery_prompt: "never finishes".into(),
                    context_text: String::new(),
                    instruction_channel: AcpInstructionChannel::FirstPrompt,
                    skills: Vec::new(),
                    hook_metadata: None,
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
    async fn first_prompt_dispatch_is_reported_before_an_ambiguous_disconnect() {
        use std::{fs, os::unix::fs::PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("disconnect-after-prompt-acp");
        fs::write(
            &fixture,
            r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{}}}' ;;
    *'"id":2'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"fixture"}}' ;;
    *'"id":3'*) exit 0 ;;
  esac
done
"#,
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();
        let mut events = Vec::new();

        run_acp_session_with_extensions_and_events(
            AcpHarnessConfig::new([fixture.into_os_string()]).unwrap(),
            directory.path().to_owned(),
            BTreeMap::new(),
            AcpSessionRequest {
                transition: AcpBindingTransition::New,
                prompt: "may execute".into(),
                recovery_prompt: "may execute".into(),
                context_text: "context".into(),
                instruction_channel: AcpInstructionChannel::FirstPrompt,
                skills: Vec::new(),
                hook_metadata: None,
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
        .unwrap_err();
        assert!(events.iter().any(|event| matches!(
            event,
            AcpStreamEvent::ContextDispatched { session_id, .. } if session_id == "fixture"
        )));
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
                transition: AcpBindingTransition::New,
                prompt: "start child".into(),
                recovery_prompt: "start child".into(),
                context_text: String::new(),
                instruction_channel: AcpInstructionChannel::FirstPrompt,
                skills: Vec::new(),
                hook_metadata: None,
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
                transition: AcpBindingTransition::Resume {
                    acp_session_id: "native-1".into(),
                },
                prompt: "second turn".into(),
                recovery_prompt: "MUST NOT BE SENT".into(),
                context_text: String::new(),
                instruction_channel: AcpInstructionChannel::FirstPrompt,
                skills: Vec::new(),
                hook_metadata: None,
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
    async fn resume_null_uses_load_session_when_advertised() {
        use std::{fs, os::unix::fs::PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("load-acp");
        fs::write(&fixture, r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"resume":null},"loadSession":true}}}' ;;
    *'"id":2'*'session/load'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{}}' ;;
    *'"id":3'*) printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn"}}'; exit 0 ;;
    *) exit 9 ;;
  esac
done
"#).unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();
        run_acp_session_with_extensions_and_events(
            AcpHarnessConfig::new(vec![fixture.to_string_lossy().into_owned()]).unwrap(),
            directory.path().to_path_buf(),
            BTreeMap::new(),
            AcpSessionRequest {
                transition: AcpBindingTransition::Resume {
                    acp_session_id: "saved".into(),
                },
                prompt: "next".into(),
                recovery_prompt: String::new(),
                context_text: String::new(),
                instruction_channel: AcpInstructionChannel::FirstPrompt,
                skills: Vec::new(),
                hook_metadata: None,
            },
            &NoCapabilities,
            &[],
            CancellationToken::new(),
            &mut |_| Ok(()),
        )
        .await
        .unwrap();
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
                transition: AcpBindingTransition::Resume {
                    acp_session_id: "native-1".into(),
                },
                prompt: "second turn".into(),
                recovery_prompt: "recovery history".into(),
                context_text: String::new(),
                instruction_channel: AcpInstructionChannel::FirstPrompt,
                skills: Vec::new(),
                hook_metadata: None,
            },
            &NoCapabilities,
            &[],
            CancellationToken::new(),
            &mut |event| {
                if let super::AcpStreamEvent::SessionBound {
                    session_id,
                    transition,
                    ..
                } = event
                {
                    bound.push((
                        session_id,
                        matches!(transition, AcpBindingTransition::Replace { .. }),
                    ));
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
        async fn context(&self, _: &str, _: &Path) -> Result<ContextSnapshot> {
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
            _: &CancellationToken,
        ) -> Result<serde_json::Value> {
            Ok(serde_json::Value::Null)
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stdio_new_rejects_an_oversized_session_id_before_emitting_a_binding() {
        use std::{fs, os::unix::fs::PermissionsExt};

        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("oversized-session-id-acp");
        let session_id = "x".repeat(mews_protocol::MAX_ACP_SESSION_ID_BYTES + 1);
        let script = r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{}}}' ;;
    *'"id":2'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"__SESSION_ID__"}}'; exit 0 ;;
  esac
done
"#
        .replace("__SESSION_ID__", &session_id);
        fs::write(
            &fixture,
            /* format!(
                "#!/bin/sh\nwhile IFS= read -r line; do\n  case \\\"$line\\\" in\n    *'\\\"id\\\":1'*) printf '%s\\n' '{{\\\"jsonrpc\\\":\\\"2.0\\\",\\\"id\\\":1,\\\"result\\\":{{\\\"protocolVersion\\\":1,\\\"agentCapabilities\\\":{{}}}}}}' ;;\n    *'\\\"id\\\":2'*) printf '%s\\n' '{{\\\"jsonrpc\\\":\\\"2.0\\\",\\\"id\\\":2,\\\"result\\\":{{\\\"sessionId\\\":\\\"{}\\\"}}}}'; exit 0 ;;\n  esac\ndone\n",
                session_id
            ), */
            script,
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

        let mut events = Vec::new();
        let error = run_acp_session_with_extensions_and_events(
            AcpHarnessConfig::new([fixture.into_os_string()]).unwrap(),
            directory.path().to_owned(),
            BTreeMap::new(),
            AcpSessionRequest {
                transition: AcpBindingTransition::New,
                prompt: "hello".into(),
                recovery_prompt: "hello".into(),
                context_text: String::new(),
                instruction_channel: AcpInstructionChannel::FirstPrompt,
                skills: Vec::new(),
                hook_metadata: None,
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
        .unwrap_err();
        assert!(format!("{error:#}").contains("valid sessionId"));
        assert!(events.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_developer_new_injects_config_and_keeps_the_prompt_user_only() {
        use std::{fs, os::unix::fs::PermissionsExt};

        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("capture-codex-acp");
        let environment_path = directory.path().join("environment.json");
        let transcript_path = directory.path().join("transcript.jsonl");
        let script = r#"#!/bin/sh
printf '%s' "$CODEX_CONFIG" > "__ENVIRONMENT__"
while IFS= read -r line; do
  printf '%s\n' "$line" >> "__TRANSCRIPT__"
  case "$line" in
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{}}}' ;;
    *'"id":2'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"fixture"}}' ;;
    *'"id":3'*) printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn"}}'; exit 0 ;;
  esac
done
"#
        .replace("__ENVIRONMENT__", &environment_path.to_string_lossy())
        .replace("__TRANSCRIPT__", &transcript_path.to_string_lossy());
        fs::write(&fixture, script).unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

        let mut config = AcpHarnessConfig::new([fixture.into_os_string()]).unwrap();
        config.environment.insert(
            "CODEX_CONFIG".into(),
            r#"{"approval_policy":"never","unrelated":true}"#.into(),
        );
        run_acp_session_with_extensions_and_events(
            config,
            directory.path().to_owned(),
            BTreeMap::new(),
            AcpSessionRequest {
                transition: AcpBindingTransition::New,
                prompt: "user text".into(),
                recovery_prompt: "recovery text".into(),
                context_text: "EXACT MEWS CONTEXT".into(),
                instruction_channel: AcpInstructionChannel::CodexDeveloper,
                skills: Vec::new(),
                hook_metadata: None,
            },
            &NoCapabilities,
            &[],
            CancellationToken::new(),
            &mut |_| Ok(()),
        )
        .await
        .unwrap();

        let environment: Value =
            serde_json::from_str(&fs::read_to_string(environment_path).unwrap()).unwrap();
        assert_eq!(environment["approval_policy"], "never");
        assert_eq!(environment["unrelated"], true);
        assert_eq!(environment["developer_instructions"], "EXACT MEWS CONTEXT");
        let requests = fs::read_to_string(transcript_path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(requests[1]["method"], "session/new");
        assert_eq!(requests[2]["method"], "session/prompt");
        assert_eq!(requests[2]["params"]["prompt"][0]["text"], "user text");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn claude_system_append_uses_raw_session_new_metadata_for_new_and_replace() {
        use std::{fs, os::unix::fs::PermissionsExt};

        for (transition, expected_prompt) in [
            (AcpBindingTransition::New, "user text"),
            (
                AcpBindingTransition::Replace {
                    reason: AcpReplacementReason::ContextNotDispatched,
                },
                "recovery text",
            ),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let fixture = directory.path().join("capture-claude-acp");
            let transcript_path = directory.path().join("transcript.jsonl");
            let script = r#"#!/bin/sh
while IFS= read -r line; do
  printf '%s\n' "$line" >> "__TRANSCRIPT__"
  case "$line" in
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{}}}' ;;
    *'"id":2'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"fixture"}}' ;;
    *'"id":3'*) printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn"}}'; exit 0 ;;
  esac
done
"#
            .replace("__TRANSCRIPT__", &transcript_path.to_string_lossy());
            fs::write(&fixture, script).unwrap();
            fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

            run_acp_session_with_extensions_and_events(
                AcpHarnessConfig::new([fixture.into_os_string()]).unwrap(),
                directory.path().to_owned(),
                BTreeMap::new(),
                AcpSessionRequest {
                    transition,
                    prompt: "user text".into(),
                    recovery_prompt: "recovery text".into(),
                    context_text: "EXACT MEWS CONTEXT".into(),
                    instruction_channel: AcpInstructionChannel::ClaudeSystemAppend,
                    skills: Vec::new(),
                    hook_metadata: None,
                },
                &NoCapabilities,
                &[],
                CancellationToken::new(),
                &mut |_| Ok(()),
            )
            .await
            .unwrap();

            let requests = fs::read_to_string(transcript_path)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                requests[1]["params"]["_meta"]["systemPrompt"],
                json!({"append": "EXACT MEWS CONTEXT"})
            );
            assert_eq!(requests[2]["params"]["prompt"][0]["text"], expected_prompt);
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
                transition: AcpBindingTransition::New,
                prompt: "hello".into(),
                recovery_prompt: "hello".into(),
                context_text: String::new(),
                instruction_channel: AcpInstructionChannel::FirstPrompt,
                skills: Vec::new(),
                hook_metadata: None,
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
                AcpStreamEvent::ProviderState { data, .. }
                    if data["sessionUpdate"] == "permission_request"
                        && data["request"]["_meta"]["provider"] == "fixture"
            )
        }));
        assert!(events.iter().any(
            |event| matches!(event, AcpStreamEvent::AssistantDelta { delta, message_id, .. } if delta == "fixture reply" && message_id.as_deref() == Some("message-2"))
        ));
        assert!(events.iter().any(
            |event| matches!(event, AcpStreamEvent::ReasoningDelta { delta, message_id, .. } if delta == "checking source" && message_id.as_deref() == Some("thought-1"))
        ));
        assert!(events.iter().any(|event| matches!(
            event,
            AcpStreamEvent::ToolActivity { call_id, title, kind, status, input, .. }
                if call_id == "web-1"
                    && title == "Web search"
                    && kind.as_deref() == Some("search")
                    && status.as_deref() == Some("completed")
                    && input["query"] == "weather in Tashkent"
        )));
    }
}
