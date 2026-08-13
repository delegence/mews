//! Durable orchestration that connects the generic agent brain to MEWS state.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use mews_agent::{
    AgentCapabilities, AgentEvent, AgentLoopConfig, AgentRuntime, AgentSignal, CancellationToken,
    ContextDocument, ContextSnapshot, LifecycleHook, ModelMessage, ModelRequest, NextStepUpdate,
    ProgressReporter, Provider, ProviderCallOutcome, StepDecision, ToolCall, ToolDecision,
    ToolDefinition, ToolResult,
};
use mews_protocol::{AgentConfig, EffectRequest, OperationId, ReasoningEffort, ToolExecutionMode};
use serde_json::Value;

mod prompt;
pub use prompt::{canonical_prompt, initial_session_prompt};

/// Conversation boundary required by the durable runtime. SQLite is only one implementation.
pub trait ConversationStore {
    fn begin_turn(&self) -> Result<()>;
    fn finish_turn(&self, termination: TurnTermination) -> Result<()>;
    fn history(&self, model: &str) -> Result<Vec<ModelMessage>>;
    fn append(&self, message: ModelMessage) -> Result<()>;
    fn append_response(&self, response: mews_protocol::AssistantResponse) -> Result<()>;
    fn tool_requested(&self, call: ToolCall) -> Result<()>;
    fn tool_execution_started(&self, call: ToolCall) -> Result<()>;
    fn tool_execution_completed(&self, call: ToolCall, result: ToolResult) -> Result<()>;
    fn tool_result_recorded(&self, call: ToolCall, result: ToolResult) -> Result<()>;
    fn continuation(&self, _model: &str) -> Result<Option<mews_agent::ResponseContinuation>> {
        Ok(None)
    }
    /// Publishes a transient signal without adding it to durable replay history.
    fn signal(&self, signal: AgentSignal) -> Result<()>;
    fn start_effect(&self, effect: EffectRequest) -> Result<OperationId>;
    fn finish_effect(&self, operation_id: &OperationId, outcome: EffectTermination) -> Result<()>;
}

/// The single terminal fact recorded for a native Harness turn.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TurnTermination {
    Completed,
    Cancelled,
    Failed { error: String },
}

#[derive(Clone, Debug, PartialEq)]
pub enum EffectTermination {
    Succeeded(Option<Value>),
    Failed(String),
    Uncertain(String),
}

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub agent_id: mews_protocol::AgentId,
    pub agent_slug: String,
    pub model: String,
    pub reasoning: Option<ReasoningEffort>,
    pub allowed_tools: Vec<String>,
    pub tool_execution: ToolExecutionMode,
    pub cwd: PathBuf,
    pub soul: String,
    pub cancellation: CancellationToken,
}

/// A resolved invocation of one Harness. The dispatcher deliberately receives
/// interfaces rather than a concrete database or Host implementation.
pub struct HarnessTurn<'a> {
    pub provider: &'a dyn Provider,
    pub environment: &'a dyn AgentCapabilities,
    pub store: &'a dyn ConversationStore,
    pub agent_id: mews_protocol::AgentId,
    pub agent_slug: String,
    pub agent: &'a AgentConfig,
    pub model_override: Option<String>,
    pub default_model: Option<String>,
    pub default_reasoning: Option<ReasoningEffort>,
    pub cwd: PathBuf,
    pub soul: String,
    pub cancellation: CancellationToken,
}

pub type HarnessOutcome = String;

#[async_trait(?Send)]
pub trait Harness: Send + Sync {
    async fn execute_turn(&self, input: HarnessTurn<'_>) -> Result<HarnessOutcome>;
}

/// The native MEWS Harness is the existing in-process model/tool loop.
#[derive(Clone, Copy, Debug, Default)]
pub struct MewsHarness;

#[async_trait(?Send)]
impl Harness for MewsHarness {
    async fn execute_turn(&self, input: HarnessTurn<'_>) -> Result<HarnessOutcome> {
        let options = MewsHarnessOptions::from_agent(input.agent)?;
        let uses_installation_default = input.model_override.is_none() && options.model.is_none();
        let model = input
            .model_override
            .or(options.model)
            .or(input.default_model)
            .context(
                "No model is configured for this Agent. Configure one with `mews providers login` or `mews providers set-key <provider>`, then select it with `mews providers models`.",
            )?;
        execute_mews_turn(
            input.provider,
            input.environment,
            input.store,
            RuntimeConfig {
                agent_id: input.agent_id,
                agent_slug: input.agent_slug,
                model,
                reasoning: options.reasoning.or(if uses_installation_default {
                    input.default_reasoning
                } else {
                    None
                }),
                allowed_tools: input.agent.tools.clone(),
                tool_execution: input.agent.tool_execution,
                cwd: input.cwd,
                soul: input.soul,
                cancellation: input.cancellation,
            },
        )
        .await
    }
}

/// Convenience entry point for callers running the native Harness directly.
pub async fn execute_turn<E: AgentCapabilities>(
    provider: &dyn Provider,
    environment: &E,
    store: &dyn ConversationStore,
    config: RuntimeConfig,
) -> Result<String> {
    MewsHarness
        .execute_native_turn(provider, environment, store, config)
        .await
}

impl MewsHarness {
    async fn execute_native_turn(
        &self,
        provider: &dyn Provider,
        environment: &dyn AgentCapabilities,
        store: &dyn ConversationStore,
        config: RuntimeConfig,
    ) -> Result<HarnessOutcome> {
        execute_mews_turn(provider, environment, store, config).await
    }
}

async fn execute_mews_turn(
    provider: &dyn Provider,
    environment: &dyn AgentCapabilities,
    store: &dyn ConversationStore,
    config: RuntimeConfig,
) -> Result<HarnessOutcome> {
    store.begin_turn()?;
    let outcome = async {
        let context = environment.context(&config.agent_slug, &config.cwd).await?;
        let runtime = MewsRuntime {
            environment,
            store,
            config: &config,
            context: &context,
        };
        mews_agent::execute_turn_with_config(
            provider,
            &runtime,
            AgentLoopConfig {
                tool_execution: config.tool_execution,
                cancellation: config.cancellation.clone(),
                ..AgentLoopConfig::default()
            },
        )
        .await
    }
    .await;
    let termination = match &outcome {
        Ok(_) => TurnTermination::Completed,
        Err(error) if mews_agent::is_turn_cancelled(error) => TurnTermination::Cancelled,
        Err(error) => TurnTermination::Failed {
            error: error.to_string(),
        },
    };
    let persisted = store.finish_turn(termination);
    match (outcome, persisted) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(answer), Ok(())) => Ok(answer),
    }
}

pub const MEWS_HARNESS: &str = "mews";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MewsHarnessOptions {
    pub model: Option<String>,
    pub reasoning: Option<ReasoningEffort>,
}

impl MewsHarnessOptions {
    pub fn from_agent(agent: &AgentConfig) -> Result<Self> {
        if agent.harness != MEWS_HARNESS {
            bail!("MewsHarness cannot run logical Harness {:?}", agent.harness);
        }
        let mut options = agent.harness_options.clone();
        let model = options.remove("model");
        let reasoning = options
            .remove("reasoning")
            .map(|value| parse_reasoning(&value))
            .transpose()?;
        if let Some((name, _)) = options.into_iter().next() {
            bail!("mews Harness does not support option {name:?}");
        }
        Ok(Self { model, reasoning })
    }
}

fn parse_reasoning(value: &str) -> Result<ReasoningEffort> {
    match value {
        "none" => Ok(ReasoningEffort::None),
        "minimal" => Ok(ReasoningEffort::Minimal),
        "low" => Ok(ReasoningEffort::Low),
        "medium" => Ok(ReasoningEffort::Medium),
        "high" => Ok(ReasoningEffort::High),
        "xhigh" => Ok(ReasoningEffort::XHigh),
        "max" => Ok(ReasoningEffort::Max),
        _ => bail!("invalid mews Harness reasoning option {value:?}"),
    }
}

struct MewsRuntime<'a, E: AgentCapabilities + ?Sized> {
    environment: &'a E,
    store: &'a dyn ConversationStore,
    config: &'a RuntimeConfig,
    context: &'a ContextSnapshot,
}

impl<E: AgentCapabilities + ?Sized> MewsRuntime<'_, E> {
    async fn execute_hook(&self, hook: LifecycleHook, payload: Value) -> Result<Value> {
        self.execute_hook_with_cancellation(hook, payload, &self.config.cancellation)
            .await
    }

    async fn execute_hook_with_cancellation(
        &self,
        hook: LifecycleHook,
        payload: Value,
        cancellation: &CancellationToken,
    ) -> Result<Value> {
        let operation_id = self.store.start_effect(EffectRequest::LifecycleHook {
            hook: lifecycle_hook_name(hook).into(),
        })?;
        let outcome = self
            .environment
            .hook(
                &self.config.agent_id,
                hook,
                payload,
                &self.config.cwd,
                cancellation,
            )
            .await;
        let termination = match &outcome {
            Ok(value) => EffectTermination::Succeeded(Some(value.clone())),
            Err(error)
                if mews_agent::effect_uncertainty(error).is_some()
                    || mews_agent::is_turn_cancelled(error) =>
            {
                EffectTermination::Uncertain(error.to_string())
            }
            Err(error) => EffectTermination::Failed(error.to_string()),
        };
        let persisted = self.store.finish_effect(&operation_id, termination);
        match (outcome, persisted) {
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(value), Ok(())) => Ok(value),
        }
    }
}

fn lifecycle_hook_name(hook: LifecycleHook) -> &'static str {
    match hook {
        LifecycleHook::TurnStart => "turn_start",
        LifecycleHook::BeforeModel => "before_model",
        LifecycleHook::BeforeTool => "before_tool",
        LifecycleHook::AfterTool => "after_tool",
        LifecycleHook::AfterStep => "after_step",
        LifecycleHook::TurnEnd => "turn_end",
    }
}

#[async_trait(?Send)]
impl<E: AgentCapabilities + ?Sized> AgentRuntime for MewsRuntime<'_, E> {
    async fn turn_started(&self) -> Result<()> {
        self.execute_hook(LifecycleHook::TurnStart, serde_json::json!({}))
            .await?;
        Ok(())
    }

    async fn turn_finished(&self) -> Result<()> {
        // Turn cleanup must still run after the Turn's cooperative cancellation
        // token has fired. It remains bounded by the extension process timeout.
        self.execute_hook_with_cancellation(
            LifecycleHook::TurnEnd,
            serde_json::json!({}),
            &CancellationToken::new(),
        )
        .await?;
        Ok(())
    }

    async fn request(&self, tools: Vec<ToolDefinition>) -> Result<ModelRequest> {
        let mut system = self.config.soul.clone();
        append_project_context(&mut system, &self.context.documents);
        if !self.context.skills.is_empty() {
            system.push_str("\n\n<available_skills>\n");
            for skill in &self.context.skills {
                system.push_str(&format!(
                    "<skill name={:?} description={:?} path={:?} />\n",
                    skill.name, skill.description, skill.path
                ));
            }
            system.push_str("</available_skills>");
        }
        if !self.context.prompts.is_empty() {
            system.push_str("\n\n<available_prompts>\n");
            for prompt in &self.context.prompts {
                system.push_str(&format!(
                    "<prompt name={:?} description={:?} path={:?} />\n",
                    prompt.name, prompt.description, prompt.path
                ));
            }
            system.push_str("</available_prompts>");
        }
        Ok(ModelRequest {
            model: self.config.model.clone(),
            reasoning: self.config.reasoning,
            system,
            messages: self.store.history(&self.config.model)?,
            tools,
            continuation: self.store.continuation(&self.config.model)?,
        })
    }

    async fn tools(&self) -> Result<Vec<ToolDefinition>> {
        Ok(self
            .environment
            .tools()
            .into_iter()
            .filter(|tool| {
                tool.agent_id
                    .as_ref()
                    .is_none_or(|owner| owner == &self.config.agent_id)
                    && self
                        .config
                        .allowed_tools
                        .iter()
                        .any(|pattern| tool_allowed(pattern, &tool.name))
            })
            .map(|mut tool| {
                tool.agent_id = None;
                tool
            })
            .collect())
    }

    async fn execute(
        &self,
        call: &ToolCall,
        cancellation: &CancellationToken,
        progress: &dyn ProgressReporter,
    ) -> Result<ToolResult> {
        self.environment
            .execute(
                &self.config.agent_id,
                call,
                &self.config.cwd,
                cancellation,
                progress,
            )
            .await
    }

    async fn event(&self, event: AgentEvent) -> Result<()> {
        match event {
            AgentEvent::AssistantResponse(response) => self.store.append_response(response)?,
            AgentEvent::ToolCall(call) => self.store.tool_requested(call)?,
            AgentEvent::ToolExecutionStarted(call) => self.store.tool_execution_started(call)?,
            AgentEvent::ToolExecutionCompleted { call, result } => {
                self.store.tool_execution_completed(call, result)?
            }
            AgentEvent::ToolResultRecorded { call, result } => {
                self.store.tool_result_recorded(call, result)?
            }
            AgentEvent::MessageInjected(message) => self.store.append(message)?,
        }
        Ok(())
    }

    async fn signal(&self, signal: AgentSignal) -> Result<()> {
        self.store.signal(signal)
    }

    async fn provider_call_started(&self, request: &ModelRequest) -> Result<Option<OperationId>> {
        let (provider, model) = request
            .model
            .split_once('/')
            .unwrap_or(("unknown", request.model.as_str()));
        self.store
            .start_effect(EffectRequest::ProviderCall {
                provider: provider.into(),
                model: model.into(),
            })
            .map(Some)
    }

    async fn provider_call_finished(
        &self,
        operation_id: OperationId,
        outcome: ProviderCallOutcome,
    ) -> Result<()> {
        let outcome = match outcome {
            ProviderCallOutcome::Succeeded => EffectTermination::Succeeded(None),
            ProviderCallOutcome::Failed(error) => EffectTermination::Failed(error),
            ProviderCallOutcome::Uncertain(reason) => EffectTermination::Uncertain(reason),
        };
        self.store.finish_effect(&operation_id, outcome)
    }

    async fn before_model(&self, request: &mut ModelRequest) -> Result<()> {
        let payload = self
            .execute_hook(LifecycleHook::BeforeModel, serde_json::to_value(&*request)?)
            .await?;
        if !payload.is_null() {
            *request = serde_json::from_value(payload)?;
        }
        Ok(())
    }

    async fn before_tool(&self, call: &mut ToolCall) -> Result<ToolDecision> {
        let payload = self
            .execute_hook(LifecycleHook::BeforeTool, serde_json::to_value(&*call)?)
            .await?;
        if let Some(reason) = payload.get("block").and_then(Value::as_str) {
            return Ok(ToolDecision::Block(reason.into()));
        }
        if let Some(name) = payload.get("name").and_then(Value::as_str) {
            call.name = name.into();
        }
        if let Some(arguments) = payload.get("arguments") {
            call.arguments = arguments.clone();
        }
        Ok(ToolDecision::Allow)
    }

    async fn after_tool(&self, call: &ToolCall, result: &mut ToolResult) -> Result<()> {
        let payload = self
            .execute_hook(
                LifecycleHook::AfterTool,
                serde_json::json!({
                    "call": call,
                    "result": result.value,
                    "is_error": result.is_error,
                    "terminate": result.terminate,
                }),
            )
            .await?;
        if let Some(value) = payload.get("result") {
            result.value = value.clone();
        }
        if let Some(value) = payload.get("is_error").and_then(Value::as_bool) {
            result.is_error = value;
        }
        if let Some(value) = payload.get("terminate").and_then(Value::as_bool) {
            result.terminate = value;
        }
        Ok(())
    }

    async fn prepare_next_step(&self, _: &ModelRequest) -> Result<NextStepUpdate> {
        Ok(NextStepUpdate::default())
    }
    async fn after_step(&self, _: &ModelRequest) -> Result<StepDecision> {
        self.execute_hook(LifecycleHook::AfterStep, serde_json::json!({}))
            .await?;
        Ok(StepDecision::Continue)
    }
}

fn tool_allowed(pattern: &str, name: &str) -> bool {
    pattern == "*"
        || pattern == name
        || pattern
            .strip_suffix('*')
            .is_some_and(|prefix| name.starts_with(prefix))
}

fn append_project_context(system: &mut String, documents: &[ContextDocument]) {
    for document in documents {
        system.push_str(&format!(
            "\n\n<project_instruction path={:?}>\n{}\n</project_instruction>",
            document.path, document.content
        ));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use anyhow::Result;
    use async_trait::async_trait;
    use mews_agent::{
        ContextDocument, ContextSnapshot, MessageContent, MessageRole, ModelResponse, ModelStream,
        ModelStreamEvent, ProviderError, ResourceDescriptor,
    };
    use mews_protocol::{AgentConfig, ToolExecutionMode};
    use serde_json::json;

    use super::*;

    #[test]
    fn tool_allowlist_supports_exact_prefix_and_global_patterns() {
        assert!(tool_allowed("read", "read"));
        assert!(tool_allowed("git_*", "git_status"));
        assert!(tool_allowed("*", "anything"));
        assert!(!tool_allowed("read", "write"));
    }

    #[test]
    fn project_instructions_have_explicit_file_boundaries() {
        let mut system = "soul".to_owned();
        append_project_context(
            &mut system,
            &[
                ContextDocument {
                    path: "/project/AGENTS.md".into(),
                    content: "broad".into(),
                },
                ContextDocument {
                    path: "/project/src/AGENTS.md".into(),
                    content: "specific".into(),
                },
            ],
        );
        assert!(system.contains("path=\"/project/AGENTS.md\">\nbroad\n</project_instruction>"));
        assert!(
            system.contains("path=\"/project/src/AGENTS.md\">\nspecific\n</project_instruction>")
        );
    }

    #[test]
    fn mews_harness_owns_its_option_validation() {
        let valid = AgentConfig::parse(
            "harness = \"mews\"\n[harness_options]\nmodel = \"openai/gpt-5\"\nreasoning = \"high\"\n",
        )
        .unwrap();
        let options = MewsHarnessOptions::from_agent(&valid).unwrap();
        assert_eq!(options.model.as_deref(), Some("openai/gpt-5"));
        assert_eq!(
            options.reasoning,
            Some(mews_protocol::ReasoningEffort::High)
        );

        let unknown =
            AgentConfig::parse("harness = \"mews\"\n[harness_options]\nmode = \"fast\"\n").unwrap();
        assert!(MewsHarnessOptions::from_agent(&unknown).is_err());

        let auto =
            AgentConfig::parse("harness = \"mews\"\n[harness_options]\nreasoning = \"auto\"\n")
                .unwrap();
        assert!(MewsHarnessOptions::from_agent(&auto).is_err());
    }

    struct SnapshotEnvironment {
        contexts: AtomicUsize,
        catalogs: AtomicUsize,
    }

    #[async_trait]
    impl AgentCapabilities for SnapshotEnvironment {
        async fn context(&self, _: &str, _: &std::path::Path) -> Result<ContextSnapshot> {
            self.contexts.fetch_add(1, Ordering::SeqCst);
            Ok(ContextSnapshot {
                documents: vec![ContextDocument {
                    path: "AGENTS.md".into(),
                    content: "stable".into(),
                }],
                skills: Vec::<ResourceDescriptor>::new(),
                prompts: Vec::<ResourceDescriptor>::new(),
            })
        }

        fn tools(&self) -> Vec<ToolDefinition> {
            self.catalogs.fetch_add(1, Ordering::SeqCst);
            vec![
                ToolDefinition {
                    name: "work".into(),
                    description: "work".into(),
                    schema: json!({"type":"object"}),
                    agent_id: None,
                },
                ToolDefinition {
                    name: "foreign".into(),
                    description: "another Agent's tool".into(),
                    schema: json!({"type":"object"}),
                    agent_id: Some(mews_protocol::AgentId::new()),
                },
            ]
        }

        async fn execute(
            &self,
            _: &mews_protocol::AgentId,
            _: &ToolCall,
            _: &std::path::Path,
            _: &CancellationToken,
            _: &dyn ProgressReporter,
        ) -> Result<ToolResult> {
            Ok(ToolResult::success(json!({"ok": true})))
        }

        async fn hook(
            &self,
            _: &mews_protocol::AgentId,
            _: LifecycleHook,
            payload: Value,
            _: &std::path::Path,
            cancellation: &CancellationToken,
        ) -> Result<Value> {
            cancellation.check()?;
            Ok(payload)
        }
    }

    #[derive(Default)]
    struct MemoryStore {
        messages: Mutex<Vec<ModelMessage>>,
        terminations: Mutex<Vec<TurnTermination>>,
        signals: Mutex<Vec<AgentSignal>>,
        effects_started: Mutex<Vec<(OperationId, EffectRequest)>>,
        effects_finished: Mutex<Vec<(OperationId, EffectTermination)>>,
    }

    impl ConversationStore for MemoryStore {
        fn begin_turn(&self) -> Result<()> {
            Ok(())
        }
        fn finish_turn(&self, termination: TurnTermination) -> Result<()> {
            self.terminations.lock().unwrap().push(termination);
            Ok(())
        }
        fn history(&self, _: &str) -> Result<Vec<ModelMessage>> {
            Ok(self.messages.lock().unwrap().clone())
        }
        fn append(&self, message: ModelMessage) -> Result<()> {
            self.messages.lock().unwrap().push(message);
            Ok(())
        }
        fn append_response(&self, response: mews_protocol::AssistantResponse) -> Result<()> {
            for block in response.blocks {
                let content = match block {
                    mews_protocol::AssistantResponseBlock::Text { text } => {
                        Some(MessageContent::Text { text })
                    }
                    mews_protocol::AssistantResponseBlock::ToolCall {
                        call_id,
                        tool,
                        arguments,
                        thought_signature,
                    } => Some(MessageContent::ToolCall {
                        call_id,
                        tool,
                        arguments,
                        thought_signature,
                    }),
                    mews_protocol::AssistantResponseBlock::OpaqueState {
                        provider,
                        model,
                        data,
                    } => Some(MessageContent::ProviderState {
                        provider,
                        model,
                        data,
                    }),
                    mews_protocol::AssistantResponseBlock::Reasoning { .. } => None,
                };
                if let Some(content) = content {
                    self.messages.lock().unwrap().push(ModelMessage {
                        role: MessageRole::Assistant,
                        content,
                    });
                }
            }
            Ok(())
        }

        fn tool_requested(&self, _call: ToolCall) -> Result<()> {
            Ok(())
        }
        fn tool_execution_started(&self, _call: ToolCall) -> Result<()> {
            Ok(())
        }
        fn tool_execution_completed(&self, _: ToolCall, _: ToolResult) -> Result<()> {
            Ok(())
        }
        fn tool_result_recorded(&self, call: ToolCall, result: ToolResult) -> Result<()> {
            self.messages.lock().unwrap().push(ModelMessage {
                role: MessageRole::Tool,
                content: MessageContent::ToolResult {
                    call_id: call.id,
                    tool: call.name,
                    result: result.value,
                    is_error: result.is_error,
                    uncertain: result.uncertain,
                },
            });
            Ok(())
        }
        fn signal(&self, signal: AgentSignal) -> Result<()> {
            self.signals.lock().unwrap().push(signal);
            Ok(())
        }
        fn start_effect(&self, effect: EffectRequest) -> Result<OperationId> {
            let operation_id = OperationId::new();
            self.effects_started
                .lock()
                .unwrap()
                .push((operation_id.clone(), effect));
            Ok(operation_id)
        }
        fn finish_effect(
            &self,
            operation_id: &OperationId,
            outcome: EffectTermination,
        ) -> Result<()> {
            self.effects_finished
                .lock()
                .unwrap()
                .push((operation_id.clone(), outcome));
            Ok(())
        }
    }

    struct TwoTurnProvider(AtomicUsize);

    #[async_trait]
    impl Provider for TwoTurnProvider {
        async fn generate(
            &self,
            _: ModelRequest,
        ) -> std::result::Result<ModelResponse, ProviderError> {
            unreachable!()
        }

        async fn stream(
            &self,
            request: ModelRequest,
        ) -> std::result::Result<ModelStream, ProviderError> {
            assert!(request.tools.iter().all(|tool| tool.name != "foreign"));
            assert!(request.tools.iter().all(|tool| tool.agent_id.is_none()));
            let events = if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                vec![
                    ModelStreamEvent::Start,
                    ModelStreamEvent::ToolCall {
                        id: "call-1".into(),
                        name: "work".into(),
                        arguments: json!({}),
                        thought_signature: None,
                    },
                    ModelStreamEvent::ResponseCompleted {
                        usage: None,
                        stop_reason: None,
                    },
                    ModelStreamEvent::Done,
                ]
            } else {
                vec![
                    ModelStreamEvent::Start,
                    ModelStreamEvent::TextDelta("done".into()),
                    ModelStreamEvent::ResponseCompleted {
                        usage: None,
                        stop_reason: None,
                    },
                    ModelStreamEvent::Done,
                ]
            };
            Ok(Box::pin(futures_util::stream::iter(
                events.into_iter().map(Ok),
            )))
        }
    }

    struct FailingProvider(ProviderError);

    #[async_trait]
    impl Provider for FailingProvider {
        async fn generate(
            &self,
            _: ModelRequest,
        ) -> std::result::Result<ModelResponse, ProviderError> {
            unreachable!()
        }

        async fn stream(&self, _: ModelRequest) -> std::result::Result<ModelStream, ProviderError> {
            let error = match &self.0 {
                ProviderError::Cancelled => ProviderError::Cancelled,
                ProviderError::Http(message) => ProviderError::Http(message.clone()),
                _ => unreachable!("test only uses cancellation and HTTP failures"),
            };
            Err(error)
        }
    }

    fn test_config() -> RuntimeConfig {
        RuntimeConfig {
            agent_id: mews_protocol::AgentId::new(),
            agent_slug: "coder".into(),
            model: "test/model".into(),
            reasoning: None,
            allowed_tools: vec!["*".into()],
            tool_execution: ToolExecutionMode::Parallel,
            cwd: ".".into(),
            soul: "soul".into(),
            cancellation: CancellationToken::new(),
        }
    }

    #[tokio::test]
    async fn snapshots_context_once_and_tools_once_per_turn() {
        let environment = SnapshotEnvironment {
            contexts: AtomicUsize::new(0),
            catalogs: AtomicUsize::new(0),
        };
        let store = MemoryStore::default();
        let answer = execute_turn(
            &TwoTurnProvider(AtomicUsize::new(0)),
            &environment,
            &store,
            test_config(),
        )
        .await
        .unwrap();

        assert_eq!(answer, "done");
        assert_eq!(
            *store.terminations.lock().unwrap(),
            vec![TurnTermination::Completed]
        );
        assert!(store.signals.lock().unwrap().iter().any(
            |signal| matches!(signal, AgentSignal::AssistantTextDelta(delta) if delta == "done")
        ));
        assert_eq!(environment.contexts.load(Ordering::SeqCst), 1);
        assert_eq!(environment.catalogs.load(Ordering::SeqCst), 2);
        assert_eq!(
            store
                .effects_started
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, effect)| matches!(effect, EffectRequest::ProviderCall { .. }))
                .count(),
            2
        );
        assert!(store.effects_started.lock().unwrap().iter().any(
            |(_, effect)| matches!(effect, EffectRequest::LifecycleHook { hook } if hook == "after_step")
        ));
        assert_eq!(
            store.effects_started.lock().unwrap().len(),
            store.effects_finished.lock().unwrap().len()
        );
        assert!(
            store
                .effects_finished
                .lock()
                .unwrap()
                .iter()
                .all(|(_, outcome)| !matches!(outcome, EffectTermination::Uncertain(_)))
        );
    }

    #[tokio::test]
    async fn records_typed_cancellation_instead_of_failure() {
        let environment = SnapshotEnvironment {
            contexts: AtomicUsize::new(0),
            catalogs: AtomicUsize::new(0),
        };
        let store = MemoryStore::default();

        let error = execute_turn(
            &FailingProvider(ProviderError::Cancelled),
            &environment,
            &store,
            test_config(),
        )
        .await
        .unwrap_err();

        assert!(mews_agent::is_turn_cancelled(&error));
        assert_eq!(
            *store.terminations.lock().unwrap(),
            vec![TurnTermination::Cancelled]
        );
        assert!(
            store
                .effects_finished
                .lock()
                .unwrap()
                .iter()
                .any(|(_, outcome)| matches!(outcome, EffectTermination::Uncertain(_)))
        );
        let effects = store.effects_started.lock().unwrap();
        let turn_end = effects
            .iter()
            .find(|(_, effect)| {
                matches!(effect, EffectRequest::LifecycleHook { hook } if hook == "turn_end")
            })
            .expect("turn_end is attempted after cancellation")
            .0
            .clone();
        assert!(
            store
                .effects_finished
                .lock()
                .unwrap()
                .iter()
                .any(|(operation_id, outcome)| operation_id == &turn_end
                    && matches!(outcome, EffectTermination::Succeeded(_)))
        );
    }

    #[tokio::test]
    async fn records_non_cancellation_as_failure() {
        let environment = SnapshotEnvironment {
            contexts: AtomicUsize::new(0),
            catalogs: AtomicUsize::new(0),
        };
        let store = MemoryStore::default();

        execute_turn(
            &FailingProvider(ProviderError::Http("offline".into())),
            &environment,
            &store,
            test_config(),
        )
        .await
        .unwrap_err();

        assert_eq!(
            *store.terminations.lock().unwrap(),
            vec![TurnTermination::Failed {
                error: "provider request failed: offline".into()
            }]
        );
        assert!(
            store
                .effects_finished
                .lock()
                .unwrap()
                .iter()
                .any(|(_, outcome)| matches!(outcome, EffectTermination::Uncertain(_)))
        );
    }
}
