use std::collections::HashSet;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::Value;

use crate::{
    AgentEvent, AgentLoopConfig, AgentRuntime, AgentSignal, CancellationToken, EffectUncertain,
    MessageContent, MessageRole, ModelMessage, ModelRequest, ModelStream, ModelStreamEvent,
    NextStepUpdate, ProgressReporter, Provider, ProviderCallOutcome, ProviderError, StepDecision,
    ToolCall, ToolCatalog, ToolDecision, ToolExecutionMode, ToolProgress, ToolResult,
    TurnCancelled, apply_context_budget, effect_uncertainty,
};
use mews_protocol::{AssistantResponse, AssistantResponseBlock, OperationId, ToolCatalogSnapshot};

const MAX_TOOL_CALLS_PER_STEP: usize = 64;
/// Provider tool-call IDs become durable correlation keys. Keep the keys
/// bounded without depending on any provider's opaque ID format.
pub const MAX_TOOL_CALL_ID_BYTES: usize = 256;
const MAX_CONCURRENT_TOOLS: usize = 8;

pub async fn execute_turn(
    provider: &dyn Provider,
    runtime: &dyn AgentRuntime,
    max_steps: usize,
) -> Result<String> {
    execute_turn_with_config(
        provider,
        runtime,
        AgentLoopConfig {
            max_steps,
            ..AgentLoopConfig::default()
        },
    )
    .await
}

pub async fn execute_turn_with_config(
    provider: &dyn Provider,
    runtime: &dyn AgentRuntime,
    config: AgentLoopConfig,
) -> Result<String> {
    let outcome = match runtime.turn_started().await {
        Ok(()) => execute_turn_inner(provider, runtime, &config).await,
        Err(error) => Err(error),
    };
    let ended = runtime.turn_finished().await;
    match (outcome, ended) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(answer), Ok(())) => Ok(answer),
    }
}

async fn execute_turn_inner(
    provider: &dyn Provider,
    runtime: &dyn AgentRuntime,
    config: &AgentLoopConfig,
) -> Result<String> {
    let mut catalog = runtime.tools().await?;
    let mut request = runtime.request(catalog.tools.clone()).await?;
    if let Some(cursor) = &request.continuation {
        let compatible = matches!(provider.continuation_capability(&request.model),
            crate::ContinuationCapability::ResponseId { provider, api }
                if provider == cursor.provider && api == cursor.api);
        if !compatible {
            request.messages = request
                .continuation
                .take()
                .expect("continuation was just checked")
                .fallback_messages;
        }
    }
    let mut final_text = String::new();
    let mut accepted_call_ids = HashSet::new();

    for step in 0..config.max_steps {
        config.cancellation.check()?;
        if step > 0 {
            catalog = runtime.tools().await?;
            request.tools.clone_from(&catalog.tools);
        }
        inject(runtime, &mut request, runtime.steering_messages().await?).await?;
        runtime.transform_context(&mut request).await?;
        runtime.before_model(&mut request).await?;
        apply_context_budget(&mut request)?;
        ensure_catalog_tools(&catalog, &request.tools)?;
        let tools = ToolCatalog::compile(ToolCatalogSnapshot {
            generation: catalog.generation,
            tools: request.tools.clone(),
        })?;

        let (response, mut calls) =
            stream_response(provider, runtime, &request, &config.cancellation).await?;
        if calls.len() > MAX_TOOL_CALLS_PER_STEP {
            bail!(
                "provider returned {} tool calls; the per-step limit is {MAX_TOOL_CALLS_PER_STEP}",
                calls.len()
            );
        }
        validate_tool_call_ids(&calls, &accepted_call_ids)?;
        for call in &mut calls {
            call.catalog_generation = tools.generation();
        }
        // A response cursor identifies the response that was current when this Turn began. Once
        // the provider has produced a new response, reusing that cursor would branch subsequent
        // tool-loop steps from stale state. The accumulated messages are the canonical fallback.
        request.continuation = None;
        let text = response
            .blocks
            .iter()
            .filter_map(|block| match block {
                AssistantResponseBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();
        if text.is_empty() && calls.is_empty() {
            bail!("provider returned an empty assistant response");
        }
        final_text.clone_from(&text);
        for block in &response.blocks {
            let content = match block {
                AssistantResponseBlock::Text { text } => {
                    Some(MessageContent::Text { text: text.clone() })
                }
                AssistantResponseBlock::ToolCall {
                    call_id,
                    tool,
                    arguments,
                    thought_signature,
                } => Some(MessageContent::ToolCall {
                    call_id: call_id.clone(),
                    tool: tool.clone(),
                    arguments: arguments.clone(),
                    thought_signature: thought_signature.clone(),
                }),
                AssistantResponseBlock::OpaqueState {
                    provider,
                    model,
                    data,
                } => Some(MessageContent::ProviderState {
                    provider: provider.clone(),
                    model: model.clone(),
                    data: data.clone(),
                }),
                AssistantResponseBlock::Reasoning { .. } => None,
            };
            if let Some(content) = content {
                request.messages.push(ModelMessage {
                    role: MessageRole::Assistant,
                    content,
                });
            }
        }
        runtime
            .event(AgentEvent::AssistantResponse {
                response,
                calls: calls.clone(),
            })
            .await?;
        accepted_call_ids.extend(calls.iter().map(|call| call.id.clone()));

        let BatchOutcome {
            mut completed,
            mut terminal_error,
        } = prepare_and_execute(runtime, calls, &tools, config).await?;
        for (call, result) in &mut completed {
            if let Err(error) = runtime.after_tool(call, result).await {
                let replay_result = match effect_uncertainty(&error) {
                    Some(uncertain) => ToolResult::uncertain(format!(
                        "after_tool outcome is uncertain: {}",
                        uncertain.reason()
                    )),
                    None => ToolResult::error(format!("after_tool failed: {error:#}")),
                };
                runtime
                    .event(AgentEvent::ToolResultRecorded {
                        call: call.clone(),
                        result: replay_result,
                    })
                    .await?;
                terminal_error.get_or_insert(error);
                continue;
            }
            runtime
                .event(AgentEvent::ToolResultRecorded {
                    call: call.clone(),
                    result: result.clone(),
                })
                .await?;
            request.messages.push(ModelMessage {
                role: MessageRole::Tool,
                content: MessageContent::ToolResult {
                    call_id: call.id.clone(),
                    tool: call.name.clone(),
                    result: result.value.clone(),
                    is_error: result.is_error,
                    uncertain: result.uncertain,
                },
            });
        }
        if let Some(error) = terminal_error {
            return Err(error);
        }

        let uncertain = completed.iter().find_map(|(_, result)| {
            result.uncertain.then(|| {
                result
                    .value
                    .as_str()
                    .unwrap_or("tool outcome is uncertain")
                    .to_owned()
            })
        });
        let after_step = runtime.after_step(&request).await;
        if let Some(reason) = uncertain {
            let error = anyhow::Error::from(EffectUncertain::new(reason));
            return match after_step {
                Ok(_) => Err(error),
                Err(after_step) => Err(error.context(format!(
                    "after_step also failed after the uncertain effect: {after_step:#}"
                ))),
            };
        }

        let step_decision = after_step?;
        if completed.iter().any(|(_, result)| result.terminate) {
            return Ok(final_text);
        }
        if step_decision == StepDecision::Stop {
            return Ok(final_text);
        }
        let update = runtime.prepare_next_step(&request).await?;
        apply_update(&mut request, update);
        if completed.is_empty() {
            let follow_ups = runtime.follow_up_messages().await?;
            if follow_ups.is_empty() {
                return Ok(final_text);
            }
            inject(runtime, &mut request, follow_ups).await?;
        }
    }
    Err(anyhow::anyhow!(
        "agent exceeded {} model steps",
        config.max_steps
    ))
    .context("model/tool loop did not finish")
}

fn validate_tool_call_ids(calls: &[ToolCall], accepted: &HashSet<String>) -> Result<()> {
    let mut ids = HashSet::with_capacity(calls.len());
    for call in calls {
        if call.id.is_empty() {
            bail!("provider returned a tool call with an empty ID");
        }
        if call.id.len() > MAX_TOOL_CALL_ID_BYTES {
            bail!("provider returned a tool-call ID longer than {MAX_TOOL_CALL_ID_BYTES} bytes");
        }
        if accepted.contains(call.id.as_str()) || !ids.insert(call.id.as_str()) {
            bail!("provider returned duplicate tool-call ID {:?}", call.id);
        }
    }
    Ok(())
}

async fn stream_response(
    provider: &dyn Provider,
    runtime: &dyn AgentRuntime,
    request: &ModelRequest,
    cancellation: &CancellationToken,
) -> Result<(AssistantResponse, Vec<ToolCall>)> {
    let mut cursor_active = request.continuation.is_some();
    let initial = start_provider_stream(provider, runtime, request.clone(), cancellation).await;
    let (mut stream, mut provider_operation) = match initial {
        Err(error)
            if request.continuation.is_some()
                && matches!(
                    error.downcast_ref::<ProviderError>(),
                    Some(ProviderError::CursorRejected(_))
                ) =>
        {
            cursor_active = false;
            start_provider_stream(provider, runtime, full_replay(request)?, cancellation).await?
        }
        result => result?,
    };
    let (default_provider, default_model) = request
        .model
        .split_once('/')
        .unwrap_or(("unknown", request.model.as_str()));
    let mut response = AssistantResponse {
        provider: default_provider.into(),
        model: default_model.into(),
        api: "unknown".into(),
        response_id: None,
        blocks: Vec::new(),
        usage: None,
        stop_reason: None,
    };
    let mut calls = Vec::new();
    let mut started = false;
    let mut provider_execution_observed = false;
    let mut completed = false;
    loop {
        let event = tokio::select! {
            _ = cancellation.cancelled() => {
                finish_provider_call(
                    runtime,
                    &mut provider_operation,
                    ProviderCallOutcome::Uncertain("provider call was cancelled after dispatch".into()),
                ).await?;
                return Err(TurnCancelled.into());
            },
            event = stream.next() => event,
        };
        let Some(event) = event else {
            if !completed {
                finish_provider_call(
                    runtime,
                    &mut provider_operation,
                    ProviderCallOutcome::Uncertain(
                        "provider stream ended before response completion".into(),
                    ),
                )
                .await?;
                bail!("provider stream ended before response completion");
            }
            break;
        };
        let event = match event {
            Ok(event) => event,
            Err(error @ ProviderError::CursorRejected(_))
                if cursor_active && !provider_execution_observed =>
            {
                finish_provider_call(
                    runtime,
                    &mut provider_operation,
                    provider_error_outcome(&error),
                )
                .await?;
                cursor_active = false;
                (stream, provider_operation) =
                    start_provider_stream(provider, runtime, full_replay(request)?, cancellation)
                        .await?;
                continue;
            }
            Err(error) => {
                finish_provider_call(
                    runtime,
                    &mut provider_operation,
                    provider_error_outcome(&error),
                )
                .await?;
                return Err(error.into());
            }
        };
        match event {
            ModelStreamEvent::Start => {
                if !started {
                    runtime.signal(AgentSignal::AssistantStarted).await?;
                    started = true;
                }
            }
            ModelStreamEvent::ResponseMetadata {
                provider,
                model,
                api,
                response_id,
            } => {
                provider_execution_observed = true;
                response.provider = provider;
                response.model = model;
                response.api = api;
                response.response_id = response_id;
            }
            ModelStreamEvent::TextDelta(delta) => {
                provider_execution_observed = true;
                if !started {
                    runtime.signal(AgentSignal::AssistantStarted).await?;
                    started = true;
                }
                if let Some(AssistantResponseBlock::Text { text }) = response.blocks.last_mut() {
                    text.push_str(&delta);
                } else {
                    response.blocks.push(AssistantResponseBlock::Text {
                        text: delta.clone(),
                    });
                }
                runtime
                    .signal(AgentSignal::AssistantTextDelta(delta))
                    .await?;
            }
            ModelStreamEvent::Reasoning { text, signature } => {
                provider_execution_observed = true;
                response
                    .blocks
                    .push(AssistantResponseBlock::Reasoning { text, signature });
            }
            ModelStreamEvent::ToolCall {
                id,
                name,
                arguments,
                thought_signature,
            } => {
                provider_execution_observed = true;
                if !started {
                    runtime.signal(AgentSignal::AssistantStarted).await?;
                    started = true;
                }
                calls.push(ToolCall {
                    id,
                    name,
                    arguments,
                    thought_signature,
                    catalog_generation: 0,
                });
                let call = calls.last().expect("tool call was just pushed");
                response.blocks.push(AssistantResponseBlock::ToolCall {
                    call_id: call.id.clone(),
                    tool: call.name.clone(),
                    arguments: call.arguments.clone(),
                    thought_signature: call.thought_signature.clone(),
                });
            }
            ModelStreamEvent::ProviderState {
                provider,
                model,
                data,
            } => {
                provider_execution_observed = true;
                response.blocks.push(AssistantResponseBlock::OpaqueState {
                    provider,
                    model,
                    data,
                });
            }
            ModelStreamEvent::ResponseCompleted { usage, stop_reason } => {
                provider_execution_observed = true;
                completed = true;
                response.usage = usage;
                response.stop_reason = stop_reason;
            }
            ModelStreamEvent::Done => {
                if !completed {
                    finish_provider_call(
                        runtime,
                        &mut provider_operation,
                        ProviderCallOutcome::Uncertain(
                            "provider stream ended before response completion".into(),
                        ),
                    )
                    .await?;
                    bail!("provider stream ended before response completion");
                }
                break;
            }
        }
    }
    finish_provider_call(
        runtime,
        &mut provider_operation,
        ProviderCallOutcome::Succeeded,
    )
    .await?;
    Ok((response, calls))
}

async fn start_provider_stream(
    provider: &dyn Provider,
    runtime: &dyn AgentRuntime,
    request: ModelRequest,
    cancellation: &CancellationToken,
) -> Result<(ModelStream, Option<OperationId>)> {
    cancellation.check()?;
    let mut operation_id = runtime.provider_call_started(&request).await?;
    let result = tokio::select! {
        _ = cancellation.cancelled() => {
            finish_provider_call(
                runtime,
                &mut operation_id,
                ProviderCallOutcome::Uncertain(
                    "provider call was cancelled after dispatch".into(),
                ),
            ).await?;
            return Err(TurnCancelled.into());
        }
        result = provider.stream(request) => result,
    };
    match result {
        Ok(stream) => Ok((stream, operation_id)),
        Err(error) => {
            finish_provider_call(runtime, &mut operation_id, provider_error_outcome(&error))
                .await?;
            Err(error.into())
        }
    }
}

async fn finish_provider_call(
    runtime: &dyn AgentRuntime,
    operation_id: &mut Option<OperationId>,
    outcome: ProviderCallOutcome,
) -> Result<()> {
    if let Some(operation_id) = operation_id.take() {
        runtime
            .provider_call_finished(operation_id, outcome)
            .await?;
    }
    Ok(())
}

fn provider_error_outcome(error: &ProviderError) -> ProviderCallOutcome {
    match error {
        ProviderError::Http(_) | ProviderError::InvalidResponse(_) | ProviderError::Cancelled => {
            ProviderCallOutcome::Uncertain(error.to_string())
        }
        _ => ProviderCallOutcome::Failed(error.to_string()),
    }
}

fn full_replay(request: &ModelRequest) -> Result<ModelRequest> {
    let mut replay = request.clone();
    if let Some(cursor) = replay.continuation.take() {
        replay.messages = cursor.fallback_messages;
    }
    apply_context_budget(&mut replay)?;
    Ok(replay)
}

enum Prepared {
    Immediate(ToolCall, ToolResult),
    Execute(ToolCall),
}

struct BatchOutcome {
    completed: Vec<(ToolCall, ToolResult)>,
    terminal_error: Option<anyhow::Error>,
}

async fn prepare_and_execute(
    runtime: &dyn AgentRuntime,
    calls: Vec<ToolCall>,
    tools: &ToolCatalog,
    config: &AgentLoopConfig,
) -> Result<BatchOutcome> {
    let mut prepared = Vec::new();
    let mut terminal_error = None;
    let mut preparation_cancelled = false;
    for call in calls {
        let result = if preparation_cancelled || config.cancellation.is_cancelled() {
            if terminal_error.is_none() {
                terminal_error = Some(anyhow::Error::from(TurnCancelled));
            }
            preparation_cancelled = true;
            Some(ToolResult::error(
                "tool call did not start because the Turn was cancelled",
            ))
        } else if terminal_error.is_some() {
            Some(ToolResult::error(
                "tool call did not start because preparation stopped",
            ))
        } else if let Err(error) = tools.validate(&call) {
            Some(ToolResult::error(error))
        } else {
            match runtime.before_tool(&call).await {
                Ok(ToolDecision::Block(reason)) => Some(ToolResult::error(reason)),
                Ok(ToolDecision::Allow) => None,
                Err(error) => {
                    let result = match effect_uncertainty(&error) {
                        Some(uncertain) => {
                            let reason = uncertain.reason().to_owned();
                            terminal_error = Some(error);
                            ToolResult::uncertain(format!(
                                "before_tool outcome is uncertain: {}",
                                reason
                            ))
                        }
                        None if crate::is_turn_cancelled(&error) => {
                            terminal_error = Some(error);
                            preparation_cancelled = true;
                            ToolResult::error("before_tool was cancelled")
                        }
                        None => ToolResult::error(format!("before_tool failed: {error:#}")),
                    };
                    Some(result)
                }
            }
        };
        prepared.push(match result {
            Some(result) => Prepared::Immediate(call, result),
            None => Prepared::Execute(call),
        });
    }

    if terminal_error.is_some() {
        prepared = prepared
            .into_iter()
            .map(|item| match item {
                Prepared::Immediate(call, result) => Prepared::Immediate(call, result),
                Prepared::Execute(call) => Prepared::Immediate(
                    call,
                    ToolResult::error(if preparation_cancelled {
                        "tool call did not start because the Turn was cancelled"
                    } else {
                        "tool call did not start because preparation stopped"
                    }),
                ),
            })
            .collect();
    }

    match config.tool_execution {
        ToolExecutionMode::Sequential => {
            let mut completed = Vec::new();
            for item in prepared {
                match execute_prepared(runtime, item, &config.cancellation).await {
                    Ok(result) => completed.push(result),
                    Err(error) => {
                        return Ok(BatchOutcome {
                            completed,
                            terminal_error: Some(error),
                        });
                    }
                }
            }
            Ok(BatchOutcome {
                completed,
                terminal_error,
            })
        }
        ToolExecutionMode::Parallel => {
            let outcomes = futures_util::stream::iter(prepared)
                .map(|item| execute_prepared(runtime, item, &config.cancellation))
                .buffered(MAX_CONCURRENT_TOOLS)
                .collect::<Vec<_>>()
                .await;
            let mut completed = Vec::new();
            let mut execution_error = terminal_error;
            for outcome in outcomes {
                match outcome {
                    Ok(result) => completed.push(result),
                    Err(error) if execution_error.is_none() => execution_error = Some(error),
                    Err(_) => {}
                }
            }
            Ok(BatchOutcome {
                completed,
                terminal_error: execution_error,
            })
        }
    }
}

fn ensure_catalog_tools(
    catalog: &ToolCatalogSnapshot,
    visible: &[mews_protocol::ToolDefinition],
) -> Result<()> {
    for tool in visible {
        let Some(source) = catalog.tools.iter().find(|source| source.name == tool.name) else {
            bail!("before_model introduced unavailable tool {:?}", tool.name);
        };
        if source != tool {
            bail!(
                "before_model changed the definition of tool {:?}",
                tool.name
            );
        }
    }
    Ok(())
}

async fn execute_prepared(
    runtime: &dyn AgentRuntime,
    prepared: Prepared,
    cancellation: &CancellationToken,
) -> Result<(ToolCall, ToolResult)> {
    let (call, result, executed) = match prepared {
        Prepared::Immediate(call, result) => (call, result, false),
        Prepared::Execute(call) => {
            let reporter = RuntimeProgress {
                runtime,
                call_id: call.id.clone(),
            };
            runtime
                .event(AgentEvent::ToolExecutionStarted(call.clone()))
                .await?;
            let result = tokio::select! {
                _ = cancellation.cancelled() => return Err(TurnCancelled.into()),
                result = runtime.execute(&call, cancellation, &reporter) => {
                    match result {
                        Ok(result) => result,
                        Err(error) => match effect_uncertainty(&error) {
                            Some(uncertain) => ToolResult::uncertain(uncertain.reason()),
                            None => ToolResult::error(error),
                        },
                    }
                }
            };
            (call, result, true)
        }
    };

    if executed {
        // Persist the raw effect outcome before batching or transformation.
        runtime
            .event(AgentEvent::ToolExecutionCompleted {
                call: call.clone(),
                result: result.clone(),
            })
            .await?;
    }
    Ok((call, result))
}

struct RuntimeProgress<'a> {
    runtime: &'a dyn AgentRuntime,
    call_id: String,
}

#[async_trait(?Send)]
impl ProgressReporter for RuntimeProgress<'_> {
    async fn report(&self, value: Value) -> Result<()> {
        self.runtime
            .signal(AgentSignal::ToolProgress(ToolProgress {
                call_id: self.call_id.clone(),
                value,
            }))
            .await
    }
}

async fn inject(
    runtime: &dyn AgentRuntime,
    request: &mut ModelRequest,
    messages: Vec<ModelMessage>,
) -> Result<()> {
    for message in messages {
        runtime
            .event(AgentEvent::MessageInjected(message.clone()))
            .await?;
        request.messages.push(message);
    }
    Ok(())
}

fn apply_update(request: &mut ModelRequest, update: NextStepUpdate) {
    if let Some(model) = update.model {
        request.model = model;
    }
    if let Some(reasoning) = update.reasoning {
        request.reasoning = reasoning;
    }
    if let Some(system) = update.system {
        request.system = system;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use crate::{EffectUncertain, ModelPart, ModelResponse, ProviderError, ToolDefinition};
    use serde_json::json;

    use super::*;

    struct TestProvider {
        turn: AtomicUsize,
        first: Vec<ModelStreamEvent>,
        later: Vec<ModelStreamEvent>,
    }

    #[async_trait::async_trait]
    impl Provider for TestProvider {
        async fn generate(
            &self,
            _request: ModelRequest,
        ) -> std::result::Result<ModelResponse, ProviderError> {
            Ok(ModelResponse {
                provider: "test".into(),
                model: "test".into(),
                api: "test".into(),
                response_id: None,
                parts: vec![ModelPart::Text {
                    text: "unused".into(),
                }],
                usage: None,
                stop_reason: None,
            })
        }

        async fn stream(
            &self,
            _request: ModelRequest,
        ) -> std::result::Result<crate::ModelStream, ProviderError> {
            let events = if self.turn.fetch_add(1, Ordering::SeqCst) == 0 {
                &self.first
            } else {
                &self.later
            };
            Ok(Box::pin(futures_util::stream::iter(
                events.clone().into_iter().map(Ok),
            )))
        }
    }

    struct Runtime {
        events: Mutex<Vec<AgentEvent>>,
        signals: Mutex<Vec<AgentSignal>>,
        provider_outcomes: Mutex<Vec<ProviderCallOutcome>>,
        executed: AtomicUsize,
        active: AtomicUsize,
        max_active: AtomicUsize,
        tool_snapshots: AtomicUsize,
        follow_up: AtomicBool,
        terminate_tools: AtomicBool,
        uncertain_tools: AtomicBool,
        cancel_after_first_tool: AtomicBool,
        fail_after_tool: AtomicBool,
        fail_before_tool: AtomicBool,
        uncertain_before_tool: AtomicBool,
        before_tool_calls: AtomicUsize,
        cancel_on_response: Mutex<Option<CancellationToken>>,
        transform_after_tool: AtomicBool,
        after_steps: AtomicUsize,
        turns_finished: AtomicUsize,
        fail_turn_start: AtomicBool,
    }

    struct StoreRuntime {
        store: mews_store::Store,
        session_id: mews_protocol::SessionId,
        turn_id: mews_protocol::TurnId,
    }

    impl StoreRuntime {
        fn new() -> Self {
            let mut store = mews_store::Store::open_in_memory().unwrap();
            let installation = store
                .initialize(
                    &mews_store::CommandContext::system(),
                    "test-host",
                    "test-public-key",
                    "test-noise-key",
                    "test-installation-key",
                )
                .unwrap();
            let (agent, _) = store
                .create_agent(
                    &mews_store::CommandContext::system(),
                    "tool-id-test",
                    "Test agent",
                    "harness = \"mews\"\ntools = [\"work\"]\n",
                    &installation.hub_host_id,
                )
                .unwrap();
            let session = store
                .create_session(
                    &agent.id,
                    &installation.hub_host_id,
                    std::path::Path::new("/tmp"),
                )
                .unwrap();
            let source = mews_protocol::MessageSource {
                kind: mews_protocol::SourceKind::Client,
                id: "tool-id-test".into(),
                channel_origin: None,
            };
            let content = mews_protocol::MessageContent::Text {
                text: "test".into(),
            };
            let (turn, _, _) = store
                .accept_turn_idempotent(
                    &session.id,
                    "tool-id-test",
                    content.clone(),
                    content,
                    Value::Null,
                    source,
                )
                .unwrap();
            Self {
                store,
                session_id: session.id,
                turn_id: turn.id,
            }
        }
    }

    #[async_trait(?Send)]
    impl AgentRuntime for StoreRuntime {
        async fn request(&self, tools: Vec<ToolDefinition>) -> Result<ModelRequest> {
            Ok(ModelRequest {
                model: "test/model".into(),
                reasoning: None,
                system: String::new(),
                messages: Vec::new(),
                tools,
                continuation: None,
            })
        }

        async fn tools(&self) -> Result<ToolCatalogSnapshot> {
            Ok(ToolCatalogSnapshot {
                generation: 1,
                tools: vec![ToolDefinition {
                    name: "work".into(),
                    description: "Work".into(),
                    schema: json!({"type":"object"}),
                    agent_id: None,
                }],
            })
        }

        async fn execute(
            &self,
            call: &ToolCall,
            _: &CancellationToken,
            _: &dyn ProgressReporter,
        ) -> Result<ToolResult> {
            Ok(ToolResult::success(json!({"id": call.id})))
        }

        async fn event(&self, event: AgentEvent) -> Result<()> {
            match event {
                AgentEvent::AssistantResponse { response, calls } => {
                    self.store.append_assistant_response_with_tool_calls(
                        &self.session_id,
                        &self.turn_id,
                        response,
                        calls.into_iter().map(stored_call).collect(),
                    )?;
                }
                AgentEvent::ToolExecutionStarted(call) => {
                    self.store.start_tool_effect(
                        &self.session_id,
                        &self.turn_id,
                        stored_call(call),
                    )?;
                }
                AgentEvent::ToolExecutionCompleted { call, result } => {
                    self.store.complete_tool_execution(
                        &self.session_id,
                        &self.turn_id,
                        stored_result(call, result),
                    )?;
                }
                AgentEvent::ToolResultRecorded { call, result } => {
                    self.store.append_tool_result(
                        &self.session_id,
                        &self.turn_id,
                        stored_result(call, result),
                    )?;
                }
                AgentEvent::MessageInjected(_) => {}
            }
            Ok(())
        }

        async fn signal(&self, _: AgentSignal) -> Result<()> {
            Ok(())
        }
    }

    fn stored_call(call: ToolCall) -> mews_protocol::ToolCall {
        mews_protocol::ToolCall {
            call_id: call.id,
            tool: call.name,
            arguments: call.arguments,
            thought_signature: call.thought_signature,
        }
    }

    fn stored_result(call: ToolCall, result: ToolResult) -> mews_protocol::ToolResult {
        mews_protocol::ToolResult {
            call_id: call.id,
            tool: call.name,
            result: result.value,
            is_error: result.is_error,
            uncertain: result.uncertain,
        }
    }

    impl Runtime {
        fn new() -> Self {
            Self {
                events: Mutex::new(Vec::new()),
                signals: Mutex::new(Vec::new()),
                provider_outcomes: Mutex::new(Vec::new()),
                executed: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                max_active: AtomicUsize::new(0),
                tool_snapshots: AtomicUsize::new(0),
                follow_up: AtomicBool::new(false),
                terminate_tools: AtomicBool::new(false),
                uncertain_tools: AtomicBool::new(false),
                cancel_after_first_tool: AtomicBool::new(false),
                fail_after_tool: AtomicBool::new(false),
                fail_before_tool: AtomicBool::new(false),
                uncertain_before_tool: AtomicBool::new(false),
                before_tool_calls: AtomicUsize::new(0),
                cancel_on_response: Mutex::new(None),
                transform_after_tool: AtomicBool::new(false),
                after_steps: AtomicUsize::new(0),
                turns_finished: AtomicUsize::new(0),
                fail_turn_start: AtomicBool::new(false),
            }
        }
    }

    #[async_trait(?Send)]
    impl AgentRuntime for Runtime {
        async fn turn_started(&self) -> Result<()> {
            if self.fail_turn_start.load(Ordering::SeqCst) {
                bail!("turn-start hook failed");
            }
            Ok(())
        }
        async fn request(&self, tools: Vec<ToolDefinition>) -> Result<ModelRequest> {
            Ok(ModelRequest {
                model: "test".into(),
                reasoning: None,
                system: String::new(),
                messages: vec![],
                tools,
                continuation: None,
            })
        }
        async fn tools(&self) -> Result<ToolCatalogSnapshot> {
            self.tool_snapshots.fetch_add(1, Ordering::SeqCst);
            Ok(ToolCatalogSnapshot {
                generation: 1,
                tools: vec![ToolDefinition {
                    name: "work".into(),
                    description: "work".into(),
                    schema: json!({"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}),
                    agent_id: None,
                }],
            })
        }
        async fn execute(
            &self,
            call: &ToolCall,
            cancellation: &CancellationToken,
            progress: &dyn ProgressReporter,
        ) -> Result<ToolResult> {
            cancellation.check()?;
            self.executed.fetch_add(1, Ordering::SeqCst);
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            progress.report(json!({"running":call.id})).await?;
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            if self.uncertain_tools.load(Ordering::SeqCst) {
                return Err(EffectUncertain::new("remote reply was lost").into());
            }
            let mut result = ToolResult::success(json!({"id":call.id}));
            result.terminate = self.terminate_tools.load(Ordering::SeqCst);
            if call.id == "a" && self.cancel_after_first_tool.load(Ordering::SeqCst) {
                cancellation.cancel();
            }
            Ok(result)
        }
        async fn event(&self, event: AgentEvent) -> Result<()> {
            if matches!(event, AgentEvent::AssistantResponse { .. })
                && let Some(cancellation) = self.cancel_on_response.lock().unwrap().take()
            {
                cancellation.cancel();
            }
            self.events.lock().unwrap().push(event);
            Ok(())
        }
        async fn signal(&self, signal: AgentSignal) -> Result<()> {
            self.signals.lock().unwrap().push(signal);
            Ok(())
        }
        async fn provider_call_started(&self, _: &ModelRequest) -> Result<Option<OperationId>> {
            Ok(Some(OperationId::new()))
        }
        async fn provider_call_finished(
            &self,
            _: OperationId,
            outcome: ProviderCallOutcome,
        ) -> Result<()> {
            self.provider_outcomes.lock().unwrap().push(outcome);
            Ok(())
        }
        async fn turn_finished(&self) -> Result<()> {
            self.turns_finished.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn follow_up_messages(&self) -> Result<Vec<ModelMessage>> {
            if self.follow_up.swap(false, Ordering::SeqCst) {
                Ok(vec![ModelMessage {
                    role: MessageRole::User,
                    content: MessageContent::Text {
                        text: "follow up".into(),
                    },
                }])
            } else {
                Ok(Vec::new())
            }
        }
        async fn after_step(&self, _: &ModelRequest) -> Result<StepDecision> {
            self.after_steps.fetch_add(1, Ordering::SeqCst);
            Ok(StepDecision::Continue)
        }
        async fn before_tool(&self, _: &ToolCall) -> Result<ToolDecision> {
            self.before_tool_calls.fetch_add(1, Ordering::SeqCst);
            if self.uncertain_before_tool.swap(false, Ordering::SeqCst) {
                return Err(EffectUncertain::new("before-tool reply was lost").into());
            }
            if self.fail_before_tool.load(Ordering::SeqCst) {
                bail!("before-tool hook failed");
            }
            Ok(ToolDecision::Allow)
        }
        async fn after_tool(&self, _: &ToolCall, result: &mut ToolResult) -> Result<()> {
            if self.fail_after_tool.load(Ordering::SeqCst) {
                bail!("after-tool hook failed");
            }
            if self.transform_after_tool.load(Ordering::SeqCst) {
                result.value = json!({"transformed":true});
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn turn_finish_runs_when_turn_start_fails() {
        let runtime = Runtime::new();
        runtime.fail_turn_start.store(true, Ordering::SeqCst);
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: text_events(),
            later: text_events(),
        };

        let error = execute_turn(&provider, &runtime, 1).await.unwrap_err();

        assert!(error.to_string().contains("turn-start hook failed"));
        assert_eq!(runtime.turns_finished.load(Ordering::SeqCst), 1);
        assert_eq!(provider.turn.load(Ordering::SeqCst), 0);
    }

    fn tool_events() -> Vec<ModelStreamEvent> {
        vec![
            ModelStreamEvent::Start,
            ModelStreamEvent::ProviderState {
                provider: "test-provider".into(),
                model: "test-model".into(),
                data: json!({"opaque":"state"}),
            },
            ModelStreamEvent::ToolCall {
                id: "a".into(),
                name: "work".into(),
                arguments: json!({"value":1}),
                thought_signature: None,
            },
            ModelStreamEvent::ToolCall {
                id: "b".into(),
                name: "work".into(),
                arguments: json!({"value":2}),
                thought_signature: None,
            },
            ModelStreamEvent::ResponseCompleted {
                usage: None,
                stop_reason: None,
            },
            ModelStreamEvent::Done,
        ]
    }

    fn text_events() -> Vec<ModelStreamEvent> {
        vec![
            ModelStreamEvent::Start,
            ModelStreamEvent::TextDelta("do".into()),
            ModelStreamEvent::TextDelta("ne".into()),
            ModelStreamEvent::ResponseCompleted {
                usage: None,
                stop_reason: None,
            },
            ModelStreamEvent::Done,
        ]
    }

    #[tokio::test]
    async fn done_before_response_completion_is_rejected_without_persisting_a_response() {
        let runtime = Runtime::new();
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: vec![
                ModelStreamEvent::Start,
                ModelStreamEvent::TextDelta("partial".into()),
                ModelStreamEvent::Done,
            ],
            later: text_events(),
        };

        let error = execute_turn(&provider, &runtime, 1).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("provider stream ended before response completion")
        );
        assert!(
            runtime
                .events
                .lock()
                .unwrap()
                .iter()
                .all(|event| !matches!(event, AgentEvent::AssistantResponse { .. }))
        );
        assert!(matches!(
            runtime.provider_outcomes.lock().unwrap().as_slice(),
            [ProviderCallOutcome::Uncertain(_)]
        ));
    }

    struct CursorProvider {
        calls: AtomicUsize,
        definitive: bool,
    }

    #[async_trait::async_trait]
    impl Provider for CursorProvider {
        fn continuation_capability(&self, _: &str) -> crate::ContinuationCapability {
            crate::ContinuationCapability::ResponseId {
                provider: "openai".into(),
                api: "responses".into(),
            }
        }
        async fn generate(
            &self,
            _: ModelRequest,
        ) -> std::result::Result<ModelResponse, ProviderError> {
            unreachable!()
        }
        async fn stream(
            &self,
            request: ModelRequest,
        ) -> std::result::Result<crate::ModelStream, ProviderError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if request.continuation.is_some() {
                return Err(if self.definitive {
                    ProviderError::CursorRejected("expired".into())
                } else {
                    ProviderError::Http("timeout".into())
                });
            }
            Ok(Box::pin(futures_util::stream::iter(
                text_events().into_iter().map(Ok),
            )))
        }
    }

    fn cursor_request() -> ModelRequest {
        ModelRequest {
            model: "openai/gpt-test".into(),
            reasoning: None,
            system: String::new(),
            messages: vec![],
            tools: vec![],
            continuation: Some(crate::ResponseContinuation {
                response_id: "resp-old".into(),
                provider: "openai".into(),
                model: "gpt-test".into(),
                api: "responses".into(),
                fallback_messages: vec![ModelMessage {
                    role: MessageRole::User,
                    content: MessageContent::Text {
                        text: "full".into(),
                    },
                }],
            }),
        }
    }

    #[tokio::test]
    async fn definitive_cursor_rejection_replays_exactly_once() {
        let provider = CursorProvider {
            calls: AtomicUsize::new(0),
            definitive: true,
        };
        let runtime = Runtime::new();
        let (_, calls) = stream_response(
            &provider,
            &runtime,
            &cursor_request(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(calls.is_empty());
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn ambiguous_cursor_failure_is_never_retried() {
        let provider = CursorProvider {
            calls: AtomicUsize::new(0),
            definitive: false,
        };
        let runtime = Runtime::new();
        assert!(
            stream_response(
                &provider,
                &runtime,
                &cursor_request(),
                &CancellationToken::new()
            )
            .await
            .is_err()
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    }

    struct RoutedCursorProvider(AtomicUsize);

    #[async_trait::async_trait]
    impl Provider for RoutedCursorProvider {
        async fn generate(
            &self,
            _: ModelRequest,
        ) -> std::result::Result<ModelResponse, ProviderError> {
            unreachable!()
        }
        async fn stream(
            &self,
            request: ModelRequest,
        ) -> std::result::Result<crate::ModelStream, ProviderError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            let events: Vec<std::result::Result<ModelStreamEvent, ProviderError>> = if request
                .continuation
                .is_some()
            {
                vec![Err(ProviderError::CursorRejected("expired".into()))]
            } else {
                assert!(request.messages.iter().any(|message| matches!(&message.content, MessageContent::Text { text } if text == "full")));
                text_events().into_iter().map(Ok).collect()
            };
            Ok(Box::pin(futures_util::stream::iter(events)))
        }
    }

    #[tokio::test]
    async fn routed_typed_cursor_rejection_also_replays_once() {
        let provider = RoutedCursorProvider(AtomicUsize::new(0));
        stream_response(
            &provider,
            &Runtime::new(),
            &cursor_request(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(provider.0.load(Ordering::SeqCst), 2);
    }

    struct CapabilityRecordingProvider {
        compatible: bool,
        requests: Mutex<Vec<ModelRequest>>,
    }

    #[async_trait::async_trait]
    impl Provider for CapabilityRecordingProvider {
        fn continuation_capability(&self, _: &str) -> crate::ContinuationCapability {
            if self.compatible {
                crate::ContinuationCapability::ResponseId {
                    provider: "openai".into(),
                    api: "responses".into(),
                }
            } else {
                crate::ContinuationCapability::None
            }
        }

        async fn generate(
            &self,
            _: ModelRequest,
        ) -> std::result::Result<ModelResponse, ProviderError> {
            unreachable!()
        }

        async fn stream(
            &self,
            request: ModelRequest,
        ) -> std::result::Result<crate::ModelStream, ProviderError> {
            self.requests.lock().unwrap().push(request);
            Ok(Box::pin(futures_util::stream::iter(
                text_events().into_iter().map(Ok),
            )))
        }
    }

    struct CursorRuntime;

    #[async_trait(?Send)]
    impl AgentRuntime for CursorRuntime {
        async fn request(&self, tools: Vec<ToolDefinition>) -> Result<ModelRequest> {
            let mut request = cursor_request();
            request.tools = tools;
            Ok(request)
        }

        async fn tools(&self) -> Result<ToolCatalogSnapshot> {
            Ok(ToolCatalogSnapshot::default())
        }

        async fn execute(
            &self,
            _: &ToolCall,
            _: &CancellationToken,
            _: &dyn ProgressReporter,
        ) -> Result<ToolResult> {
            unreachable!()
        }

        async fn event(&self, _: AgentEvent) -> Result<()> {
            Ok(())
        }
        async fn signal(&self, _: AgentSignal) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn capability_mismatch_sends_full_history_without_cursor() {
        let provider = CapabilityRecordingProvider {
            compatible: false,
            requests: Mutex::new(Vec::new()),
        };

        execute_turn(&provider, &CursorRuntime, 1).await.unwrap();

        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].continuation.is_none());
        assert!(matches!(
            &requests[0].messages[..],
            [ModelMessage {
                content: MessageContent::Text { text },
                ..
            }] if text == "full"
        ));
    }

    #[tokio::test]
    async fn compatible_provider_sends_suffix_with_cursor() {
        let provider = CapabilityRecordingProvider {
            compatible: true,
            requests: Mutex::new(Vec::new()),
        };

        execute_turn(&provider, &CursorRuntime, 1).await.unwrap();

        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].messages.is_empty());
        assert_eq!(
            requests[0]
                .continuation
                .as_ref()
                .map(|cursor| cursor.response_id.as_str()),
            Some("resp-old")
        );
    }

    struct StreamFailureProvider {
        calls: AtomicUsize,
        evidence: Option<ModelStreamEvent>,
        cursor_error: bool,
    }

    #[async_trait::async_trait]
    impl Provider for StreamFailureProvider {
        async fn generate(
            &self,
            _: ModelRequest,
        ) -> std::result::Result<ModelResponse, ProviderError> {
            unreachable!()
        }

        async fn stream(
            &self,
            request: ModelRequest,
        ) -> std::result::Result<crate::ModelStream, ProviderError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if request.continuation.is_none() {
                return Ok(Box::pin(futures_util::stream::iter(
                    text_events().into_iter().map(Ok),
                )));
            }
            let error = if self.cursor_error {
                ProviderError::CursorRejected("expired".into())
            } else {
                ProviderError::Http("stream failed".into())
            };
            let mut events = vec![Ok(ModelStreamEvent::Start)];
            if let Some(evidence) = self.evidence.clone() {
                events.push(Ok(evidence));
            }
            events.push(Err(error));
            Ok(Box::pin(futures_util::stream::iter(events)))
        }
    }

    #[tokio::test]
    async fn start_then_cursor_rejection_replays_exactly_once() {
        let provider = StreamFailureProvider {
            calls: AtomicUsize::new(0),
            evidence: None,
            cursor_error: true,
        };

        stream_response(
            &provider,
            &Runtime::new(),
            &cursor_request(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn provider_execution_evidence_prevents_cursor_replay() {
        let evidence = [
            ModelStreamEvent::ResponseMetadata {
                provider: "openai".into(),
                model: "gpt-test".into(),
                api: "responses".into(),
                response_id: Some("resp-new".into()),
            },
            ModelStreamEvent::TextDelta("partial".into()),
            ModelStreamEvent::Reasoning {
                text: "thinking".into(),
                signature: None,
            },
            ModelStreamEvent::ToolCall {
                id: "call".into(),
                name: "work".into(),
                arguments: json!({}),
                thought_signature: None,
            },
            ModelStreamEvent::ProviderState {
                provider: "openai".into(),
                model: "gpt-test".into(),
                data: json!({"state": true}),
            },
            ModelStreamEvent::ResponseCompleted {
                usage: None,
                stop_reason: None,
            },
        ];

        for event in evidence {
            let provider = StreamFailureProvider {
                calls: AtomicUsize::new(0),
                evidence: Some(event),
                cursor_error: true,
            };
            assert!(
                stream_response(
                    &provider,
                    &Runtime::new(),
                    &cursor_request(),
                    &CancellationToken::new(),
                )
                .await
                .is_err()
            );
            assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn non_cursor_stream_error_never_replays() {
        let provider = StreamFailureProvider {
            calls: AtomicUsize::new(0),
            evidence: None,
            cursor_error: false,
        };

        assert!(
            stream_response(
                &provider,
                &Runtime::new(),
                &cursor_request(),
                &CancellationToken::new(),
            )
            .await
            .is_err()
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn premature_stream_eof_is_rejected() {
        let runtime = Runtime::new();
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: vec![
                ModelStreamEvent::Start,
                ModelStreamEvent::TextDelta("partial".into()),
            ],
            later: vec![],
        };
        let error = stream_response(
            &provider,
            &runtime,
            &cursor_request(),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("before response completion"));
        assert!(matches!(
            runtime.provider_outcomes.lock().unwrap().as_slice(),
            [ProviderCallOutcome::Uncertain(_)]
        ));
    }

    #[tokio::test]
    async fn streams_deltas_progress_and_executes_parallel_batches() {
        let runtime = Runtime::new();
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: tool_events(),
            later: text_events(),
        };
        let answer = execute_turn_with_config(&provider, &runtime, AgentLoopConfig::default())
            .await
            .unwrap();
        assert_eq!(answer, "done");
        assert_eq!(runtime.executed.load(Ordering::SeqCst), 2);
        assert_eq!(runtime.max_active.load(Ordering::SeqCst), 2);
        assert_eq!(runtime.tool_snapshots.load(Ordering::SeqCst), 2);
        assert!(runtime.events.lock().unwrap().iter().any(|event| {
            matches!(event, AgentEvent::AssistantResponse { response, .. }
                if response.blocks.iter().any(|block| matches!(block,
                    AssistantResponseBlock::OpaqueState { data, .. } if data["opaque"] == "state")))
        }));
        assert!(runtime.signals.lock().unwrap().iter().any(
            |signal| matches!(signal, AgentSignal::AssistantTextDelta(delta) if delta == "do")
        ));
        assert_eq!(
            runtime
                .signals
                .lock()
                .unwrap()
                .iter()
                .filter(|signal| matches!(signal, AgentSignal::ToolProgress(_)))
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn sequential_mode_never_overlaps_tools() {
        let runtime = Runtime::new();
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: tool_events(),
            later: text_events(),
        };
        execute_turn_with_config(
            &provider,
            &runtime,
            AgentLoopConfig {
                tool_execution: ToolExecutionMode::Sequential,
                ..AgentLoopConfig::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(runtime.max_active.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn oversized_tool_batch_is_rejected_before_response_durability() {
        let runtime = Runtime::new();
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: std::iter::once(ModelStreamEvent::Start)
                .chain(
                    (0..=MAX_TOOL_CALLS_PER_STEP).map(|index| ModelStreamEvent::ToolCall {
                        id: index.to_string(),
                        name: "work".into(),
                        arguments: json!({"value": index}),
                        thought_signature: None,
                    }),
                )
                .chain([
                    ModelStreamEvent::ResponseCompleted {
                        usage: None,
                        stop_reason: None,
                    },
                    ModelStreamEvent::Done,
                ])
                .collect(),
            later: text_events(),
        };

        let error = execute_turn(&provider, &runtime, 4).await.unwrap_err();

        assert!(error.to_string().contains("per-step limit"));
        assert!(
            runtime
                .events
                .lock()
                .unwrap()
                .iter()
                .all(|event| !matches!(event, AgentEvent::AssistantResponse { .. }))
        );
    }

    #[tokio::test]
    async fn invalid_tool_call_ids_are_rejected_before_response_durability() {
        for (ids, detail) in [
            (vec![String::new()], "empty ID"),
            (
                vec!["x".repeat(MAX_TOOL_CALL_ID_BYTES + 1)],
                "longer than 256 bytes",
            ),
            (vec!["same".into(), "same".into()], "duplicate tool-call ID"),
        ] {
            let runtime = Runtime::new();
            let provider = TestProvider {
                turn: AtomicUsize::new(0),
                first: std::iter::once(ModelStreamEvent::Start)
                    .chain(ids.into_iter().map(|id| ModelStreamEvent::ToolCall {
                        id,
                        name: "work".into(),
                        arguments: json!({"value": 1}),
                        thought_signature: None,
                    }))
                    .chain([
                        ModelStreamEvent::ResponseCompleted {
                            usage: None,
                            stop_reason: Some("tool_use".into()),
                        },
                        ModelStreamEvent::Done,
                    ])
                    .collect(),
                later: text_events(),
            };

            let error = execute_turn(&provider, &runtime, 4).await.unwrap_err();

            assert!(error.to_string().contains(detail), "{error:#}");
            assert_eq!(runtime.executed.load(Ordering::SeqCst), 0);
            assert!(runtime.events.lock().unwrap().iter().all(|event| {
                !matches!(
                    event,
                    AgentEvent::AssistantResponse { .. }
                        | AgentEvent::ToolExecutionStarted(_)
                        | AgentEvent::ToolResultRecorded { .. }
                )
            }));
        }
    }

    #[tokio::test]
    async fn duplicate_tool_call_ids_leave_store_replay_unchanged() {
        let runtime = StoreRuntime::new();
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: vec![
                ModelStreamEvent::Start,
                ModelStreamEvent::ToolCall {
                    id: "same".into(),
                    name: "work".into(),
                    arguments: json!({}),
                    thought_signature: None,
                },
                ModelStreamEvent::ToolCall {
                    id: "same".into(),
                    name: "work".into(),
                    arguments: json!({}),
                    thought_signature: None,
                },
                ModelStreamEvent::ResponseCompleted {
                    usage: None,
                    stop_reason: Some("tool_use".into()),
                },
                ModelStreamEvent::Done,
            ],
            later: text_events(),
        };

        let error = execute_turn(&provider, &runtime, 4).await.unwrap_err();

        assert!(error.to_string().contains("duplicate tool-call ID"));
        let entries = runtime.store.session_entries(&runtime.session_id).unwrap();
        assert_eq!(entries.len(), 1);
        assert!(matches!(
            entries[0].payload,
            mews_protocol::SessionEntryPayload::UserMessage { .. }
        ));
    }

    #[tokio::test]
    async fn turn_scoped_tool_call_ids_keep_store_replay_paired() {
        let runtime = StoreRuntime::new();
        let call_events = || {
            vec![
                ModelStreamEvent::Start,
                ModelStreamEvent::ToolCall {
                    id: "same".into(),
                    name: "work".into(),
                    arguments: json!({}),
                    thought_signature: None,
                },
                ModelStreamEvent::ResponseCompleted {
                    usage: None,
                    stop_reason: Some("tool_use".into()),
                },
                ModelStreamEvent::Done,
            ]
        };
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: call_events(),
            later: call_events(),
        };

        let error = execute_turn(&provider, &runtime, 4).await.unwrap_err();

        assert!(error.to_string().contains("duplicate tool-call ID"));
        assert_eq!(provider.turn.load(Ordering::SeqCst), 2);
        runtime
            .store
            .finish_turn(
                &runtime.turn_id,
                mews_protocol::TurnStatus::Failed,
                Some("provider reused a tool-call ID"),
            )
            .unwrap();
        let entries = runtime.store.session_entries(&runtime.session_id).unwrap();
        assert_eq!(
            entries
                .iter()
                .filter(|entry| matches!(
                    entry.payload,
                    mews_protocol::SessionEntryPayload::AssistantResponse { .. }
                ))
                .count(),
            1
        );
        assert_eq!(
            entries
                .iter()
                .filter(|entry| matches!(
                    entry.payload,
                    mews_protocol::SessionEntryPayload::ToolStarted { .. }
                ))
                .count(),
            1
        );

        let active = runtime.store.active_entries(&runtime.session_id).unwrap();
        let replay = mews_protocol::portable_history(&active);
        let calls = replay
            .iter()
            .filter_map(|item| match &item.content {
                mews_protocol::MessageContent::ToolCall { call_id, .. } => Some(call_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let results = replay
            .iter()
            .filter_map(|item| match &item.content {
                mews_protocol::MessageContent::ToolResult { call_id, .. } => Some(call_id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(calls, ["same"]);
        assert_eq!(results, calls);
    }

    #[tokio::test]
    async fn cancellation_after_response_pairs_every_registered_call() {
        let runtime = Runtime::new();
        let cancellation = CancellationToken::new();
        *runtime.cancel_on_response.lock().unwrap() = Some(cancellation.clone());
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: tool_events(),
            later: text_events(),
        };

        let error = execute_turn_with_config(
            &provider,
            &runtime,
            AgentLoopConfig {
                cancellation,
                ..AgentLoopConfig::default()
            },
        )
        .await
        .unwrap_err();

        assert!(crate::is_turn_cancelled(&error));
        let events = runtime.events.lock().unwrap();
        let registered = events.iter().find_map(|event| match event {
            AgentEvent::AssistantResponse { calls, .. } => Some(
                calls
                    .iter()
                    .map(|call| call.id.as_str())
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        });
        let completed = events
            .iter()
            .filter_map(|event| match event {
                AgentEvent::ToolResultRecorded { call, .. } => Some(call.id.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(registered.unwrap(), completed);
    }

    #[tokio::test]
    async fn before_tool_failures_are_paired_call_errors() {
        let runtime = Runtime::new();
        runtime.fail_before_tool.store(true, Ordering::SeqCst);
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: tool_events(),
            later: text_events(),
        };

        assert_eq!(execute_turn(&provider, &runtime, 4).await.unwrap(), "done");
        let events = runtime.events.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, AgentEvent::ToolResultRecorded { result, .. } if result.is_error))
                .count(),
            2
        );
        assert_eq!(runtime.before_tool_calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn uncertain_before_tool_stops_later_preparation_without_claiming_cancellation() {
        let runtime = Runtime::new();
        runtime.uncertain_before_tool.store(true, Ordering::SeqCst);
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: tool_events(),
            later: text_events(),
        };

        let error = execute_turn(&provider, &runtime, 4).await.unwrap_err();

        assert!(effect_uncertainty(&error).is_some());
        assert_eq!(runtime.before_tool_calls.load(Ordering::SeqCst), 1);
        let events = runtime.events.lock().unwrap();
        let results = events
            .iter()
            .filter_map(|event| match event {
                AgentEvent::ToolResultRecorded { call, result } => {
                    Some((call.id.as_str(), &result.value, result.uncertain))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0],
            (
                "a",
                &json!("before_tool outcome is uncertain: before-tool reply was lost"),
                true
            )
        );
        assert_eq!(
            results[1],
            (
                "b",
                &json!("tool call did not start because preparation stopped"),
                false
            )
        );
    }

    #[tokio::test]
    async fn terminating_tool_still_completes_its_step_hook() {
        let runtime = Runtime::new();
        runtime.terminate_tools.store(true, Ordering::SeqCst);
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: tool_events(),
            later: text_events(),
        };

        execute_turn_with_config(&provider, &runtime, AgentLoopConfig::default())
            .await
            .unwrap();

        assert_eq!(runtime.after_steps.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn completed_tool_is_recorded_before_a_later_batch_cancellation() {
        let runtime = Runtime::new();
        runtime
            .cancel_after_first_tool
            .store(true, Ordering::SeqCst);
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: tool_events(),
            later: text_events(),
        };

        let error = execute_turn_with_config(
            &provider,
            &runtime,
            AgentLoopConfig {
                tool_execution: ToolExecutionMode::Sequential,
                ..AgentLoopConfig::default()
            },
        )
        .await
        .unwrap_err();

        assert!(crate::is_turn_cancelled(&error));
        assert!(
            runtime.events.lock().unwrap().iter().any(
                |event| matches!(event, AgentEvent::ToolExecutionCompleted { call, .. } if call.id == "a")
            )
        );
        assert!(runtime.events.lock().unwrap().iter().any(
            |event| matches!(event, AgentEvent::ToolResultRecorded { call, .. } if call.id == "a")
        ));
    }

    #[tokio::test]
    async fn completed_tool_is_recorded_before_after_tool_hook_runs() {
        let runtime = Runtime::new();
        runtime.fail_after_tool.store(true, Ordering::SeqCst);
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: tool_events(),
            later: text_events(),
        };

        let error = execute_turn(&provider, &runtime, 4).await.unwrap_err();

        assert!(error.to_string().contains("after-tool hook failed"));
        assert_eq!(
            runtime
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| matches!(event, AgentEvent::ToolExecutionCompleted { .. }))
                .count(),
            2
        );
        assert_eq!(
            runtime
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| matches!(event, AgentEvent::ToolResultRecorded { .. }))
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn raw_completion_is_distinct_from_transformed_replay_result() {
        let runtime = Runtime::new();
        runtime.transform_after_tool.store(true, Ordering::SeqCst);
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: tool_events(),
            later: text_events(),
        };

        execute_turn(&provider, &runtime, 4).await.unwrap();

        let events = runtime.events.lock().unwrap();
        assert!(events.iter().any(|event| matches!(
            event,
            AgentEvent::ToolExecutionCompleted { result, .. }
                if result.value.get("id").is_some()
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            AgentEvent::ToolResultRecorded { result, .. }
                if result.value == json!({"transformed":true})
        )));
    }

    #[tokio::test]
    async fn parallel_mode_caps_concurrency_and_preserves_call_order() {
        let runtime = Runtime::new();
        let calls = (0..12)
            .map(|index| ModelStreamEvent::ToolCall {
                id: index.to_string(),
                name: "work".into(),
                arguments: json!({"value": index}),
                thought_signature: None,
            })
            .collect::<Vec<_>>();
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: std::iter::once(ModelStreamEvent::Start)
                .chain(calls)
                .chain(std::iter::once(ModelStreamEvent::ResponseCompleted {
                    usage: None,
                    stop_reason: None,
                }))
                .chain(std::iter::once(ModelStreamEvent::Done))
                .collect(),
            later: text_events(),
        };

        execute_turn_with_config(&provider, &runtime, AgentLoopConfig::default())
            .await
            .unwrap();

        assert_eq!(
            runtime.max_active.load(Ordering::SeqCst),
            MAX_CONCURRENT_TOOLS
        );
        let result_ids = runtime
            .events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                AgentEvent::ToolResultRecorded { call, .. } => Some(call.id.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            result_ids,
            (0..12).map(|index| index.to_string()).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn schema_errors_are_tool_results_and_do_not_execute() {
        let runtime = Runtime::new();
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: vec![
                ModelStreamEvent::Start,
                ModelStreamEvent::ToolCall {
                    id: "bad".into(),
                    name: "work".into(),
                    arguments: json!({"value":"wrong"}),
                    thought_signature: None,
                },
                ModelStreamEvent::ResponseCompleted {
                    usage: None,
                    stop_reason: None,
                },
                ModelStreamEvent::Done,
            ],
            later: text_events(),
        };
        assert_eq!(execute_turn(&provider, &runtime, 4).await.unwrap(), "done");
        assert_eq!(runtime.executed.load(Ordering::SeqCst), 0);
        assert_eq!(runtime.before_tool_calls.load(Ordering::SeqCst), 0);
        assert!(
            runtime
                .events
                .lock()
                .unwrap()
                .iter()
                .all(|event| !matches!(event, AgentEvent::ToolExecutionStarted(_)))
        );
        assert!(runtime.events.lock().unwrap().iter().any(
            |event| matches!(event, AgentEvent::ToolResultRecorded { result, .. } if result.is_error)
        ));
    }

    #[tokio::test]
    async fn ambiguous_execution_errors_remain_uncertain_tool_results() {
        let runtime = Runtime::new();
        runtime.uncertain_tools.store(true, Ordering::SeqCst);
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: tool_events(),
            later: text_events(),
        };

        let error = execute_turn(&provider, &runtime, 4).await.unwrap_err();

        let events = runtime.events.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, AgentEvent::ToolExecutionStarted(_)))
                .count(),
            2
        );
        assert!(effect_uncertainty(&error).is_some());
        assert_eq!(runtime.after_steps.load(Ordering::SeqCst), 1);
        assert_eq!(provider.turn.load(Ordering::SeqCst), 1);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event,
                    AgentEvent::ToolResultRecorded { result, .. }
                        if result.is_error && result.uncertain
                ))
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn follow_ups_continue_after_a_text_only_turn() {
        let runtime = Runtime::new();
        runtime.follow_up.store(true, Ordering::SeqCst);
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: vec![
                ModelStreamEvent::Start,
                ModelStreamEvent::TextDelta("first".into()),
                ModelStreamEvent::ResponseCompleted {
                    usage: None,
                    stop_reason: None,
                },
                ModelStreamEvent::Done,
            ],
            later: text_events(),
        };
        assert_eq!(execute_turn(&provider, &runtime, 4).await.unwrap(), "done");
        assert!(
            runtime
                .events
                .lock()
                .unwrap()
                .iter()
                .any(|event| matches!(event, AgentEvent::MessageInjected(_)))
        );
    }

    struct PendingProvider;
    #[async_trait::async_trait]
    impl Provider for PendingProvider {
        async fn generate(
            &self,
            _request: ModelRequest,
        ) -> std::result::Result<ModelResponse, ProviderError> {
            unreachable!()
        }
        async fn stream(
            &self,
            _request: ModelRequest,
        ) -> std::result::Result<crate::ModelStream, ProviderError> {
            Ok(Box::pin(futures_util::stream::pending()))
        }
    }

    #[tokio::test]
    async fn cancellation_interrupts_a_pending_provider_stream() {
        let runtime = Runtime::new();
        let cancellation = CancellationToken::new();
        let trigger = cancellation.clone();
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            trigger.cancel();
        });
        let error = execute_turn_with_config(
            &PendingProvider,
            &runtime,
            AgentLoopConfig {
                cancellation,
                ..AgentLoopConfig::default()
            },
        )
        .await
        .unwrap_err();
        assert!(crate::is_turn_cancelled(&error));
        assert_eq!(runtime.turns_finished.load(Ordering::SeqCst), 1);
    }

    struct PendingStartupProvider {
        started: std::sync::Arc<tokio::sync::Notify>,
        dropped: std::sync::Arc<AtomicBool>,
    }

    struct StartupGuard(std::sync::Arc<AtomicBool>);

    impl Drop for StartupGuard {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl Provider for PendingStartupProvider {
        async fn generate(
            &self,
            _request: ModelRequest,
        ) -> std::result::Result<ModelResponse, ProviderError> {
            unreachable!()
        }

        async fn stream(
            &self,
            _request: ModelRequest,
        ) -> std::result::Result<crate::ModelStream, ProviderError> {
            let _guard = StartupGuard(self.dropped.clone());
            self.started.notify_one();
            futures_util::future::pending().await
        }
    }

    #[tokio::test]
    async fn cancellation_drops_pending_provider_startup() {
        let runtime = Runtime::new();
        let cancellation = CancellationToken::new();
        let started = std::sync::Arc::new(tokio::sync::Notify::new());
        let dropped = std::sync::Arc::new(AtomicBool::new(false));
        let provider = PendingStartupProvider {
            started: started.clone(),
            dropped: dropped.clone(),
        };
        let trigger = cancellation.clone();
        tokio::spawn(async move {
            started.notified().await;
            trigger.cancel();
        });

        let error = execute_turn_with_config(
            &provider,
            &runtime,
            AgentLoopConfig {
                cancellation,
                ..AgentLoopConfig::default()
            },
        )
        .await
        .unwrap_err();

        assert!(crate::is_turn_cancelled(&error));
        assert!(dropped.load(Ordering::SeqCst));
        assert!(matches!(
            runtime.provider_outcomes.lock().unwrap().as_slice(),
            [ProviderCallOutcome::Uncertain(_)]
        ));
    }

    struct RecordingProvider(Mutex<Vec<ModelRequest>>);

    #[async_trait::async_trait]
    impl Provider for RecordingProvider {
        fn continuation_capability(&self, _: &str) -> crate::ContinuationCapability {
            crate::ContinuationCapability::ResponseId {
                provider: "openai".into(),
                api: "responses".into(),
            }
        }

        async fn generate(
            &self,
            _request: ModelRequest,
        ) -> std::result::Result<ModelResponse, ProviderError> {
            unreachable!()
        }
        async fn stream(
            &self,
            request: ModelRequest,
        ) -> std::result::Result<crate::ModelStream, ProviderError> {
            self.0.lock().unwrap().push(request);
            Ok(Box::pin(futures_util::stream::iter(
                text_events().into_iter().map(Ok),
            )))
        }
    }

    struct LifecycleRuntime {
        follow_up: AtomicBool,
        turns: AtomicUsize,
    }

    #[async_trait(?Send)]
    impl AgentRuntime for LifecycleRuntime {
        async fn request(&self, tools: Vec<ToolDefinition>) -> Result<ModelRequest> {
            Ok(ModelRequest {
                model: "first".into(),
                reasoning: None,
                system: "base".into(),
                messages: vec![],
                tools,
                continuation: Some(crate::ResponseContinuation {
                    response_id: "initial-response".into(),
                    provider: "openai".into(),
                    model: "first".into(),
                    api: "responses".into(),
                    fallback_messages: vec![],
                }),
            })
        }
        async fn tools(&self) -> Result<ToolCatalogSnapshot> {
            Ok(ToolCatalogSnapshot::default())
        }
        async fn execute(
            &self,
            _: &ToolCall,
            _: &CancellationToken,
            _: &dyn ProgressReporter,
        ) -> Result<ToolResult> {
            unreachable!()
        }
        async fn event(&self, _: AgentEvent) -> Result<()> {
            Ok(())
        }
        async fn signal(&self, _: AgentSignal) -> Result<()> {
            Ok(())
        }
        async fn steering_messages(&self) -> Result<Vec<ModelMessage>> {
            if self.turns.load(Ordering::SeqCst) == 0 {
                Ok(vec![ModelMessage {
                    role: MessageRole::User,
                    content: MessageContent::Text {
                        text: "steer".into(),
                    },
                }])
            } else {
                Ok(Vec::new())
            }
        }
        async fn transform_context(&self, request: &mut ModelRequest) -> Result<()> {
            request.system.push_str("+transform");
            Ok(())
        }
        async fn prepare_next_step(&self, _: &ModelRequest) -> Result<NextStepUpdate> {
            Ok(NextStepUpdate {
                model: Some("second".into()),
                system: Some("updated".into()),
                ..Default::default()
            })
        }
        async fn after_step(&self, _: &ModelRequest) -> Result<StepDecision> {
            let turn = self.turns.fetch_add(1, Ordering::SeqCst);
            Ok(if turn == 1 {
                StepDecision::Stop
            } else {
                StepDecision::Continue
            })
        }
        async fn follow_up_messages(&self) -> Result<Vec<ModelMessage>> {
            if self.follow_up.swap(false, Ordering::SeqCst) {
                Ok(vec![ModelMessage {
                    role: MessageRole::User,
                    content: MessageContent::Text {
                        text: "again".into(),
                    },
                }])
            } else {
                Ok(Vec::new())
            }
        }
    }

    #[tokio::test]
    async fn lifecycle_can_steer_transform_update_and_stop_turns() {
        let provider = RecordingProvider(Mutex::new(Vec::new()));
        let runtime = LifecycleRuntime {
            follow_up: AtomicBool::new(true),
            turns: AtomicUsize::new(0),
        };
        assert_eq!(execute_turn(&provider, &runtime, 4).await.unwrap(), "done");
        let requests = provider.0.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].system, "base+transform");
        assert!(requests[0].messages.iter().any(
            |message| matches!(&message.content, MessageContent::Text { text } if text == "steer")
        ));
        assert_eq!(requests[1].model, "second");
        assert_eq!(requests[1].system, "updated+transform");
        assert!(requests[0].continuation.is_some());
        assert!(requests[1].continuation.is_none());
    }
}
