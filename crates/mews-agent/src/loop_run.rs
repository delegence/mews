use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures_util::{StreamExt, future::join_all};
use mews_protocol::ToolDefinition;
use serde_json::Value;

use crate::{
    AgentEvent, AgentLoopConfig, AgentRuntime, CancellationToken, MessageContent, MessageRole,
    ModelMessage, ModelRequest, ModelStreamEvent, NextTurnUpdate, ProgressReporter, Provider,
    ToolCall, ToolDecision, ToolExecutionMode, ToolProgress, ToolResult, TurnDecision,
};

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
    let mut final_text = String::new();

    for turn in 0..config.max_steps {
        config.cancellation.check()?;
        request.tools = runtime.tools().await?;
        inject(runtime, &mut request, runtime.steering_messages().await?).await?;
        runtime.event(AgentEvent::TurnStart { index: turn }).await?;
        runtime.transform_context(&mut request).await?;
        runtime.before_model(&mut request).await?;
        runtime.event(AgentEvent::BeforeModel).await?;

        let (text, calls, provider_states) =
            stream_response(provider, runtime, &request, &config.cancellation).await?;
        if text.is_empty() && calls.is_empty() {
            bail!("provider returned an empty assistant response");
        }
        final_text.clone_from(&text);
        for state in provider_states {
            let message = ModelMessage {
                role: MessageRole::Assistant,
                content: state,
            };
            request.messages.push(message.clone());
            runtime.event(AgentEvent::ProviderState(message)).await?;
        }
        if !text.is_empty() {
            request.messages.push(ModelMessage {
                role: MessageRole::Assistant,
                content: MessageContent::Text { text: text.clone() },
            });
            runtime.event(AgentEvent::AssistantText(text)).await?;
        }

        let mut results = prepare_and_execute(runtime, calls, config).await?;
        for (call, _) in &results {
            request.messages.push(ModelMessage {
                role: MessageRole::Assistant,
                content: MessageContent::ToolCall {
                    call_id: call.id.clone(),
                    tool: call.name.clone(),
                    arguments: call.arguments.clone(),
                    thought_signature: call.thought_signature.clone(),
                },
            });
        }
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
        let update = runtime.prepare_next_turn(&request).await?;
        apply_update(&mut request, update);
        if runtime.after_turn(&request).await? == TurnDecision::Stop {
            return Ok(final_text);
        }

        let terminated = results.iter().any(|(_, result)| result.terminate);
        if terminated {
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
) -> Result<(String, Vec<ToolCall>, Vec<MessageContent>)> {
    let mut stream = provider.stream(request.clone()).await?;
    let mut text = String::new();
    let mut calls = Vec::new();
    let mut provider_states = Vec::new();
    let mut started = false;
    loop {
        let event = tokio::select! {
            _ = cancellation.cancelled() => bail!("agent run cancelled"),
            event = stream.next() => event,
        };
        let Some(event) = event else {
            break;
        };
        match event? {
            ModelStreamEvent::Start => {
                if !started {
                    runtime.event(AgentEvent::AssistantStart).await?;
                    started = true;
                }
            }
            ModelStreamEvent::TextDelta(delta) => {
                if !started {
                    runtime.event(AgentEvent::AssistantStart).await?;
                    started = true;
                }
                text.push_str(&delta);
                runtime.event(AgentEvent::AssistantTextDelta(delta)).await?;
            }
            ModelStreamEvent::ToolCall {
                id,
                name,
                arguments,
                thought_signature,
            } => {
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
            }
            ModelStreamEvent::ProviderState {
                provider,
                model,
                data,
            } => provider_states.push(MessageContent::ProviderState {
                provider,
                model,
                data,
            }),
            ModelStreamEvent::Done => break,
        }
    }
    Ok((text, calls, provider_states))
}

enum Prepared {
    Immediate(ToolCall, ToolResult),
    Execute(ToolCall),
}

async fn prepare_and_execute(
    runtime: &dyn AgentRuntime,
    calls: Vec<ToolCall>,
    config: &AgentLoopConfig,
) -> Result<Vec<(ToolCall, ToolResult)>> {
    let tools = runtime.tools().await?;
    let mut prepared = Vec::new();
    for mut call in calls {
        config.cancellation.check()?;
        let decision = runtime.before_tool(&mut call).await?;
        runtime.event(AgentEvent::ToolCall(call.clone())).await?;
        let result = match decision {
            ToolDecision::Block(reason) => Some(ToolResult::error(reason)),
            ToolDecision::Allow => validate_call(&call, &tools).err().map(ToolResult::error),
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
        ToolExecutionMode::Parallel => Ok(join_all(
            prepared
                .into_iter()
                .map(|item| execute_prepared(runtime, item, &config.cancellation)),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?),
    }
}

fn validate_call(call: &ToolCall, tools: &[ToolDefinition]) -> Result<()> {
    let tool = tools
        .iter()
        .find(|tool| tool.name == call.name)
        .with_context(|| format!("tool {:?} is unavailable", call.name))?;
    let validator = jsonschema::validator_for(&tool.schema)
        .with_context(|| format!("tool {:?} has an invalid schema", call.name))?;
    if let Err(error) = validator.validate(&call.arguments) {
        bail!("invalid arguments for tool {:?}: {error}", call.name);
    }
    Ok(())
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

    use crate::{ModelPart, ModelResponse, ProviderError};
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
                parts: vec![ModelPart::Text {
                    text: "unused".into(),
                }],
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
        follow_up: AtomicBool,
    }

    impl Runtime {
        fn new() -> Self {
            Self {
                events: Mutex::new(Vec::new()),
                executed: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                max_active: AtomicUsize::new(0),
                follow_up: AtomicBool::new(false),
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
            })
        }
        async fn tools(&self) -> Result<Vec<ToolDefinition>> {
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
            Ok(ToolResult::success(json!({"id":call.id})))
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
            ModelStreamEvent::Done,
        ]
    }

    fn text_events() -> Vec<ModelStreamEvent> {
        vec![
            ModelStreamEvent::Start,
            ModelStreamEvent::TextDelta("do".into()),
            ModelStreamEvent::TextDelta("ne".into()),
            ModelStreamEvent::Done,
        ]
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
        assert!(runtime.events.lock().unwrap().iter().any(|event| {
            matches!(event, AgentEvent::ProviderState(ModelMessage { content: MessageContent::ProviderState { data, .. }, .. }) if data["opaque"] == "state")
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
    }
}
