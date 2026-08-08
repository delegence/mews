//! Durable orchestration that connects the generic agent brain to MEWS state.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use mews_agent::{
    AgentCapabilities, AgentEvent, AgentLoopConfig, AgentRuntime, CancellationToken,
    ContextDocument, ContextSnapshot, LifecycleHook, MessageContent, MessageRole, ModelMessage,
    ModelRequest, NextTurnUpdate, ProgressReporter, Provider, ToolCall, ToolDecision,
    ToolDefinition, ToolResult, TurnDecision,
};
use mews_protocol::{AgentConfig, ReasoningEffort, ToolExecutionMode};
use serde_json::Value;

mod prompt;
pub use prompt::{canonical_prompt, initial_session_prompt};

/// Persistence boundary required by the durable runtime. SQLite is only one implementation.
pub trait ConversationStore {
    fn begin_run(&self) -> Result<()>;
    fn finish_run(&self, error: Option<&str>) -> Result<()>;
    fn history(&self) -> Result<Vec<ModelMessage>>;
    fn append(&self, message: ModelMessage) -> Result<()>;
    fn assistant_delta(&self, delta: String) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
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
pub struct HarnessRun<'a> {
    pub provider: &'a dyn Provider,
    pub environment: &'a dyn AgentCapabilities,
    pub store: &'a dyn ConversationStore,
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
    async fn run(&self, input: HarnessRun<'_>) -> Result<HarnessOutcome>;
}

/// The native MEWS Harness is the existing in-process model/tool loop.
#[derive(Clone, Copy, Debug, Default)]
pub struct MewsHarness;

#[async_trait(?Send)]
impl Harness for MewsHarness {
    async fn run(&self, input: HarnessRun<'_>) -> Result<HarnessOutcome> {
        let options = MewsHarnessOptions::from_agent(input.agent)?;
        let uses_installation_default = input.model_override.is_none() && options.model.is_none();
        let model = input
            .model_override
            .or(options.model)
            .or(input.default_model)
            .context(
                "No model is configured for this Agent. Configure one with `mews providers login` or `mews providers set-key <provider>`, then select it with `mews providers models`.",
            )?;
        run_mews(
            input.provider,
            input.environment,
            input.store,
            RuntimeConfig {
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
pub async fn run<E: AgentCapabilities>(
    provider: &dyn Provider,
    environment: &E,
    store: &dyn ConversationStore,
    config: RuntimeConfig,
) -> Result<String> {
    MewsHarness
        .run_native(provider, environment, store, config)
        .await
}

impl MewsHarness {
    async fn run_native(
        &self,
        provider: &dyn Provider,
        environment: &dyn AgentCapabilities,
        store: &dyn ConversationStore,
        config: RuntimeConfig,
    ) -> Result<HarnessOutcome> {
        run_mews(provider, environment, store, config).await
    }
}

async fn run_mews(
    provider: &dyn Provider,
    environment: &dyn AgentCapabilities,
    store: &dyn ConversationStore,
    config: RuntimeConfig,
) -> Result<HarnessOutcome> {
    store.begin_run()?;
    let outcome = async {
        let context = environment.context(&config.cwd).await?;
        let runtime = MewsRuntime {
            environment,
            store,
            config: &config,
            context: &context,
        };
        mews_agent::run_with_config(
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
    let persisted = store.finish_run(
        outcome
            .as_ref()
            .err()
            .map(|error| error.to_string())
            .as_deref(),
    );
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
        "auto" => Ok(ReasoningEffort::Auto),
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

#[async_trait(?Send)]
impl<E: AgentCapabilities + ?Sized> AgentRuntime for MewsRuntime<'_, E> {
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
            messages: self.store.history()?,
            tools,
        })
    }

    async fn tools(&self) -> Result<Vec<ToolDefinition>> {
        Ok(self
            .environment
            .tools()
            .into_iter()
            .filter(|tool| {
                self.config
                    .allowed_tools
                    .iter()
                    .any(|pattern| tool_allowed(pattern, &tool.name))
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
            .execute(call, &self.config.cwd, cancellation, progress)
            .await
    }

    async fn event(&self, event: AgentEvent) -> Result<()> {
        let hook = match &event {
            AgentEvent::RunStart => Some(LifecycleHook::RunStart),
            AgentEvent::TurnEnd { .. } => Some(LifecycleHook::AfterTurn),
            AgentEvent::RunEnd => Some(LifecycleHook::RunEnd),
            _ => None,
        };
        if let Some(hook) = hook {
            self.environment
                .hook(hook, serde_json::json!({}), &self.config.cwd)
                .await?;
        }
        match event {
            AgentEvent::AssistantTextDelta(delta) => self.store.assistant_delta(delta)?,
            AgentEvent::AssistantText(text) => self.store.append(ModelMessage {
                role: MessageRole::Assistant,
                content: MessageContent::Text { text },
            })?,
            AgentEvent::ProviderState(message) => self.store.append(message)?,
            AgentEvent::ToolCall(call) => self.store.append(ModelMessage {
                role: MessageRole::Assistant,
                content: MessageContent::ToolCall {
                    call_id: call.id,
                    tool: call.name,
                    arguments: call.arguments,
                    thought_signature: call.thought_signature,
                },
            })?,
            AgentEvent::ToolResult { call, result } => self.store.append(ModelMessage {
                role: MessageRole::Tool,
                content: MessageContent::ToolResult {
                    call_id: call.id,
                    tool: call.name,
                    result: result.value,
                    is_error: result.is_error,
                },
            })?,
            AgentEvent::MessageInjected(message) => self.store.append(message)?,
            _ => {}
        }
        Ok(())
    }

    async fn before_model(&self, request: &mut ModelRequest) -> Result<()> {
        let payload = self
            .environment
            .hook(
                LifecycleHook::BeforeModel,
                serde_json::to_value(&*request)?,
                &self.config.cwd,
            )
            .await?;
        if !payload.is_null() {
            *request = serde_json::from_value(payload)?;
        }
        Ok(())
    }

    async fn before_tool(&self, call: &mut ToolCall) -> Result<ToolDecision> {
        let payload = self
            .environment
            .hook(
                LifecycleHook::BeforeTool,
                serde_json::to_value(&*call)?,
                &self.config.cwd,
            )
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
            .environment
            .hook(
                LifecycleHook::AfterTool,
                serde_json::json!({
                    "call": call,
                    "result": result.value,
                    "is_error": result.is_error,
                    "terminate": result.terminate,
                }),
                &self.config.cwd,
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

    async fn prepare_next_turn(&self, _: &ModelRequest) -> Result<NextTurnUpdate> {
        Ok(NextTurnUpdate::default())
    }
    async fn after_turn(&self, _: &ModelRequest) -> Result<TurnDecision> {
        Ok(TurnDecision::Continue)
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
        ContextDocument, ContextSnapshot, ModelResponse, ModelStream, ModelStreamEvent,
        ProviderError, ResourceDescriptor,
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
    }

    struct SnapshotEnvironment {
        contexts: AtomicUsize,
        catalogs: AtomicUsize,
    }

    #[async_trait]
    impl AgentCapabilities for SnapshotEnvironment {
        async fn context(&self, _: &std::path::Path) -> Result<ContextSnapshot> {
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
            vec![ToolDefinition {
                name: "work".into(),
                description: "work".into(),
                schema: json!({"type":"object"}),
            }]
        }

        async fn execute(
            &self,
            _: &ToolCall,
            _: &std::path::Path,
            _: &CancellationToken,
            _: &dyn ProgressReporter,
        ) -> Result<ToolResult> {
            Ok(ToolResult::success(json!({"ok": true})))
        }

        async fn hook(
            &self,
            _: LifecycleHook,
            payload: Value,
            _: &std::path::Path,
        ) -> Result<Value> {
            Ok(payload)
        }
    }

    #[derive(Default)]
    struct MemoryStore(Mutex<Vec<ModelMessage>>);

    impl ConversationStore for MemoryStore {
        fn begin_run(&self) -> Result<()> {
            Ok(())
        }
        fn finish_run(&self, _: Option<&str>) -> Result<()> {
            Ok(())
        }
        fn history(&self) -> Result<Vec<ModelMessage>> {
            Ok(self.0.lock().unwrap().clone())
        }
        fn append(&self, message: ModelMessage) -> Result<()> {
            self.0.lock().unwrap().push(message);
            Ok(())
        }
        fn assistant_delta(&self, _: String) -> Result<()> {
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

        async fn stream(&self, _: ModelRequest) -> std::result::Result<ModelStream, ProviderError> {
            let events = if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                vec![
                    ModelStreamEvent::Start,
                    ModelStreamEvent::ToolCall {
                        id: "call-1".into(),
                        name: "work".into(),
                        arguments: json!({}),
                        thought_signature: None,
                    },
                    ModelStreamEvent::Done,
                ]
            } else {
                vec![
                    ModelStreamEvent::Start,
                    ModelStreamEvent::TextDelta("done".into()),
                    ModelStreamEvent::Done,
                ]
            };
            Ok(Box::pin(futures_util::stream::iter(
                events.into_iter().map(Ok),
            )))
        }
    }

    #[tokio::test]
    async fn snapshots_context_once_and_tools_once_per_turn() {
        let environment = SnapshotEnvironment {
            contexts: AtomicUsize::new(0),
            catalogs: AtomicUsize::new(0),
        };
        let answer = run(
            &TwoTurnProvider(AtomicUsize::new(0)),
            &environment,
            &MemoryStore::default(),
            RuntimeConfig {
                model: "test/model".into(),
                reasoning: None,
                allowed_tools: vec!["*".into()],
                tool_execution: ToolExecutionMode::Parallel,
                cwd: ".".into(),
                soul: "soul".into(),
                cancellation: CancellationToken::new(),
            },
        )
        .await
        .unwrap();

        assert_eq!(answer, "done");
        assert_eq!(environment.contexts.load(Ordering::SeqCst), 1);
        assert_eq!(environment.catalogs.load(Ordering::SeqCst), 2);
    }
}
