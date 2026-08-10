use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures_util::StreamExt;
use serde_json::Value;

use crate::{
    AgentEvent, AgentLoopConfig, AgentRuntime, CancellationToken, MessageContent, MessageRole,
    ModelMessage, ModelRequest, ModelStreamEvent, NextTurnUpdate, ProgressReporter, Provider,
    ToolCall, ToolCatalog, ToolDecision, ToolExecutionMode, ToolProgress, ToolResult, TurnDecision,
    apply_context_budget,
};
use mews_protocol::{AssistantResponse, AssistantResponseBlock};

const MAX_TOOL_CALLS_PER_TURN: usize = 64;
const MAX_CONCURRENT_TOOLS: usize = 8;

pub async fn run(
    provider: &dyn Provider,
    runtime: &dyn AgentRuntime,
    max_steps: usize,
) -> Result<String> {
    run_with_config(
        provider,
        runtime,
        AgentLoopConfig {
            max_steps,
            ..AgentLoopConfig::default()
        },
    )
    .await
}

pub async fn run_with_config(
    provider: &dyn Provider,
    runtime: &dyn AgentRuntime,
    config: AgentLoopConfig,
) -> Result<String> {
    runtime.event(AgentEvent::RunStart).await?;
    let outcome = run_inner(provider, runtime, &config).await;
    let ended = runtime.event(AgentEvent::RunEnd).await;
    match (outcome, ended) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(answer), Ok(())) => Ok(answer),
    }
}

async fn run_inner(
    provider: &dyn Provider,
    runtime: &dyn AgentRuntime,
    config: &AgentLoopConfig,
) -> Result<String> {
    let mut request = runtime.request(runtime.tools().await?).await?;
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

    for turn in 0..config.max_steps {
        config.cancellation.check()?;
        if turn > 0 {
            request.tools = runtime.tools().await?;
        }
        inject(runtime, &mut request, runtime.steering_messages().await?).await?;
        runtime.event(AgentEvent::TurnStart { index: turn }).await?;
        runtime.transform_context(&mut request).await?;
        runtime.before_model(&mut request).await?;
        apply_context_budget(&mut request)?;
        let tools = ToolCatalog::compile(request.tools.clone())?;
        runtime.event(AgentEvent::BeforeModel).await?;

        let (response, calls) =
            stream_response(provider, runtime, &request, &config.cancellation).await?;
        // A response cursor identifies the response that was current when this run began. Once
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
            .event(AgentEvent::AssistantResponse(response))
            .await?;

        let mut results = prepare_and_execute(runtime, calls, &tools, config).await?;
        for (call, result) in &mut results {
            runtime.after_tool(call, result).await?;
            request.messages.push(ModelMessage {
                role: MessageRole::Tool,
                content: MessageContent::ToolResult {
                    call_id: call.id.clone(),
                    tool: call.name.clone(),
                    result: result.value.clone(),
                    is_error: result.is_error,
                },
            });
            runtime
                .event(AgentEvent::ToolResult {
                    call: call.clone(),
                    result: result.clone(),
                })
                .await?;
        }

        runtime.event(AgentEvent::TurnEnd { index: turn }).await?;
        if results.iter().any(|(_, result)| result.terminate) {
            return Ok(final_text);
        }
        let update = runtime.prepare_next_turn(&request).await?;
        apply_update(&mut request, update);
        if runtime.after_turn(&request).await? == TurnDecision::Stop {
            return Ok(final_text);
        }
        if results.is_empty() {
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

async fn stream_response(
    provider: &dyn Provider,
    runtime: &dyn AgentRuntime,
    request: &ModelRequest,
    cancellation: &CancellationToken,
) -> Result<(AssistantResponse, Vec<ToolCall>)> {
    let mut cursor_active = request.continuation.is_some();
    let mut stream = match provider.stream(request.clone()).await {
        Err(crate::ProviderError::CursorRejected(_)) if request.continuation.is_some() => {
            cursor_active = false;
            provider.stream(full_replay(request)?).await?
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
            _ = cancellation.cancelled() => bail!("agent run cancelled"),
            event = stream.next() => event,
        };
        let Some(event) = event else {
            if !completed {
                bail!("provider stream ended before response completion");
            }
            break;
        };
        let event = match event {
            Ok(event) => event,
            Err(crate::ProviderError::CursorRejected(_))
                if cursor_active && !provider_execution_observed =>
            {
                cursor_active = false;
                stream = provider.stream(full_replay(request)?).await?;
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        match event {
            ModelStreamEvent::Start => {
                if !started {
                    runtime.event(AgentEvent::AssistantStart).await?;
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
                    runtime.event(AgentEvent::AssistantStart).await?;
                    started = true;
                }
                if let Some(AssistantResponseBlock::Text { text }) = response.blocks.last_mut() {
                    text.push_str(&delta);
                } else {
                    response.blocks.push(AssistantResponseBlock::Text {
                        text: delta.clone(),
                    });
                }
                runtime.event(AgentEvent::AssistantTextDelta(delta)).await?;
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
                    runtime.event(AgentEvent::AssistantStart).await?;
                    started = true;
                }
                calls.push(ToolCall {
                    id,
                    name,
                    arguments,
                    thought_signature,
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
                    bail!("provider stream ended before response completion");
                }
                break;
            }
        }
    }
    Ok((response, calls))
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

async fn prepare_and_execute(
    runtime: &dyn AgentRuntime,
    calls: Vec<ToolCall>,
    tools: &ToolCatalog,
    config: &AgentLoopConfig,
) -> Result<Vec<(ToolCall, ToolResult)>> {
    if calls.len() > MAX_TOOL_CALLS_PER_TURN {
        bail!(
            "provider returned {} tool calls; the per-turn limit is {MAX_TOOL_CALLS_PER_TURN}",
            calls.len()
        );
    }
    let mut prepared = Vec::new();
    for mut call in calls {
        config.cancellation.check()?;
        let decision = runtime.before_tool(&mut call).await?;
        runtime.event(AgentEvent::ToolCall(call.clone())).await?;
        let result = match decision {
            ToolDecision::Block(reason) => Some(ToolResult::error(reason)),
            ToolDecision::Allow => tools.validate(&call).err().map(ToolResult::error),
        };
        prepared.push(match result {
            Some(result) => Prepared::Immediate(call, result),
            None => Prepared::Execute(call),
        });
    }

    match config.tool_execution {
        ToolExecutionMode::Sequential => {
            let mut results = Vec::new();
            for item in prepared {
                results.push(execute_prepared(runtime, item, &config.cancellation).await?);
            }
            Ok(results)
        }
        ToolExecutionMode::Parallel => futures_util::stream::iter(prepared)
            .map(|item| execute_prepared(runtime, item, &config.cancellation))
            .buffered(MAX_CONCURRENT_TOOLS)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect(),
    }
}

async fn execute_prepared(
    runtime: &dyn AgentRuntime,
    prepared: Prepared,
    cancellation: &CancellationToken,
) -> Result<(ToolCall, ToolResult)> {
    let call = match prepared {
        Prepared::Immediate(call, result) => return Ok((call, result)),
        Prepared::Execute(call) => call,
    };
    let reporter = RuntimeProgress {
        runtime,
        call_id: call.id.clone(),
    };
    let result = tokio::select! {
        _ = cancellation.cancelled() => bail!("agent run cancelled"),
        result = runtime.execute(&call, cancellation, &reporter) => {
            match result { Ok(result) => result, Err(error) => ToolResult::error(error) }
        }
    };
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
            .event(AgentEvent::ToolProgress(ToolProgress {
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

fn apply_update(request: &mut ModelRequest, update: NextTurnUpdate) {
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

    use crate::{ModelPart, ModelResponse, ProviderError, ToolDefinition};
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
        executed: AtomicUsize,
        active: AtomicUsize,
        max_active: AtomicUsize,
        tool_snapshots: AtomicUsize,
        follow_up: AtomicBool,
        terminate_tools: AtomicBool,
        after_turns: AtomicUsize,
    }

    impl Runtime {
        fn new() -> Self {
            Self {
                events: Mutex::new(Vec::new()),
                executed: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                max_active: AtomicUsize::new(0),
                tool_snapshots: AtomicUsize::new(0),
                follow_up: AtomicBool::new(false),
                terminate_tools: AtomicBool::new(false),
                after_turns: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait(?Send)]
    impl AgentRuntime for Runtime {
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
        async fn tools(&self) -> Result<Vec<ToolDefinition>> {
            self.tool_snapshots.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ToolDefinition {
                name: "work".into(),
                description: "work".into(),
                schema: json!({"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}),
            }])
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
            let mut result = ToolResult::success(json!({"id":call.id}));
            result.terminate = self.terminate_tools.load(Ordering::SeqCst);
            Ok(result)
        }
        async fn event(&self, event: AgentEvent) -> Result<()> {
            self.events.lock().unwrap().push(event);
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
        async fn after_turn(&self, _: &ModelRequest) -> Result<TurnDecision> {
            self.after_turns.fetch_add(1, Ordering::SeqCst);
            Ok(TurnDecision::Continue)
        }
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

        let error = run(&provider, &runtime, 1).await.unwrap_err();
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
                .all(|event| !matches!(event, AgentEvent::AssistantResponse(_)))
        );
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

        async fn tools(&self) -> Result<Vec<ToolDefinition>> {
            Ok(Vec::new())
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
    }

    #[tokio::test]
    async fn capability_mismatch_sends_full_history_without_cursor() {
        let provider = CapabilityRecordingProvider {
            compatible: false,
            requests: Mutex::new(Vec::new()),
        };

        run(&provider, &CursorRuntime, 1).await.unwrap();

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

        run(&provider, &CursorRuntime, 1).await.unwrap();

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
            &Runtime::new(),
            &cursor_request(),
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("before response completion"));
    }

    #[tokio::test]
    async fn streams_deltas_progress_and_executes_parallel_batches() {
        let runtime = Runtime::new();
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: tool_events(),
            later: text_events(),
        };
        let answer = run_with_config(&provider, &runtime, AgentLoopConfig::default())
            .await
            .unwrap();
        assert_eq!(answer, "done");
        assert_eq!(runtime.executed.load(Ordering::SeqCst), 2);
        assert_eq!(runtime.max_active.load(Ordering::SeqCst), 2);
        assert_eq!(runtime.tool_snapshots.load(Ordering::SeqCst), 2);
        assert!(runtime.events.lock().unwrap().iter().any(|event| {
            matches!(event, AgentEvent::AssistantResponse(response)
                if response.blocks.iter().any(|block| matches!(block,
                    AssistantResponseBlock::OpaqueState { data, .. } if data["opaque"] == "state")))
        }));
        assert!(
            runtime.events.lock().unwrap().iter().any(
                |event| matches!(event, AgentEvent::AssistantTextDelta(delta) if delta == "do")
            )
        );
        assert_eq!(
            runtime
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| matches!(event, AgentEvent::ToolProgress(_)))
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
        run_with_config(
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
    async fn terminating_tool_skips_next_turn_hooks() {
        let runtime = Runtime::new();
        runtime.terminate_tools.store(true, Ordering::SeqCst);
        let provider = TestProvider {
            turn: AtomicUsize::new(0),
            first: tool_events(),
            later: text_events(),
        };

        run_with_config(&provider, &runtime, AgentLoopConfig::default())
            .await
            .unwrap();

        assert_eq!(runtime.after_turns.load(Ordering::SeqCst), 0);
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

        run_with_config(&provider, &runtime, AgentLoopConfig::default())
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
                AgentEvent::ToolResult { call, .. } => Some(call.id.clone()),
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
        assert_eq!(run(&provider, &runtime, 4).await.unwrap(), "done");
        assert_eq!(runtime.executed.load(Ordering::SeqCst), 0);
        assert!(runtime.events.lock().unwrap().iter().any(
            |event| matches!(event, AgentEvent::ToolResult { result, .. } if result.is_error)
        ));
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
        assert_eq!(run(&provider, &runtime, 4).await.unwrap(), "done");
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
        let error = run_with_config(
            &PendingProvider,
            &runtime,
            AgentLoopConfig {
                cancellation,
                ..AgentLoopConfig::default()
            },
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("cancelled"));
        assert!(matches!(
            runtime.events.lock().unwrap().last(),
            Some(AgentEvent::RunEnd)
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
        async fn tools(&self) -> Result<Vec<ToolDefinition>> {
            Ok(Vec::new())
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
        async fn prepare_next_turn(&self, _: &ModelRequest) -> Result<NextTurnUpdate> {
            Ok(NextTurnUpdate {
                model: Some("second".into()),
                system: Some("updated".into()),
                ..Default::default()
            })
        }
        async fn after_turn(&self, _: &ModelRequest) -> Result<TurnDecision> {
            let turn = self.turns.fetch_add(1, Ordering::SeqCst);
            Ok(if turn == 1 {
                TurnDecision::Stop
            } else {
                TurnDecision::Continue
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
        assert_eq!(run(&provider, &runtime, 4).await.unwrap(), "done");
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
