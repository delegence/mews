use crate::{CancellationToken, ProgressReporter, ToolCall, ToolProgress, ToolResult};
use anyhow::Result;
use async_trait::async_trait;
pub use mews_protocol::ToolExecutionMode;
use mews_protocol::{ReasoningEffort, ToolDefinition};

use crate::{ModelMessage, ModelRequest};

pub const DEFAULT_MAX_STEPS: usize = 32;

#[derive(Clone, Debug)]
pub enum AgentEvent {
    RunStart,
    TurnStart { index: usize },
    BeforeModel,
    AssistantStart,
    AssistantTextDelta(String),
    AssistantText(String),
    ProviderState(ModelMessage),
    MessageInjected(ModelMessage),
    ToolCall(ToolCall),
    ToolProgress(ToolProgress),
    ToolResult { call: ToolCall, result: ToolResult },
    TurnEnd { index: usize },
    RunEnd,
}

#[derive(Clone, Debug, Default)]
pub enum ToolDecision {
    #[default]
    Allow,
    Block(String),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TurnDecision {
    #[default]
    Continue,
    Stop,
}

#[derive(Clone, Debug, Default)]
pub struct NextTurnUpdate {
    pub model: Option<String>,
    pub reasoning: Option<Option<ReasoningEffort>>,
    pub system: Option<String>,
}

#[derive(Clone, Debug)]
pub struct AgentLoopConfig {
    pub max_steps: usize,
    pub tool_execution: ToolExecutionMode,
    pub cancellation: CancellationToken,
}

impl Default for AgentLoopConfig {
    fn default() -> Self {
        Self {
            max_steps: DEFAULT_MAX_STEPS,
            tool_execution: ToolExecutionMode::Parallel,
            cancellation: CancellationToken::new(),
        }
    }
}

#[async_trait(?Send)]
pub trait AgentRuntime {
    /// Returns the initial model request, including existing conversation history.
    async fn request(&self, tools: Vec<ToolDefinition>) -> Result<ModelRequest>;
    async fn tools(&self) -> Result<Vec<ToolDefinition>>;
    async fn execute(
        &self,
        call: &ToolCall,
        cancellation: &CancellationToken,
        progress: &dyn ProgressReporter,
    ) -> Result<ToolResult>;
    async fn event(&self, event: AgentEvent) -> Result<()>;

    /// Transform context immediately before every model call.
    async fn transform_context(&self, _request: &mut ModelRequest) -> Result<()> {
        Ok(())
    }
    async fn before_model(&self, _request: &mut ModelRequest) -> Result<()> {
        Ok(())
    }
    async fn before_tool(&self, _call: &mut ToolCall) -> Result<ToolDecision> {
        Ok(ToolDecision::Allow)
    }
    async fn after_tool(&self, _call: &ToolCall, _result: &mut ToolResult) -> Result<()> {
        Ok(())
    }

    /// Messages injected before the next model call while a run is active.
    async fn steering_messages(&self) -> Result<Vec<ModelMessage>> {
        Ok(Vec::new())
    }
    /// Messages processed after the agent would otherwise stop.
    async fn follow_up_messages(&self) -> Result<Vec<ModelMessage>> {
        Ok(Vec::new())
    }
    async fn prepare_next_turn(&self, _request: &ModelRequest) -> Result<NextTurnUpdate> {
        Ok(NextTurnUpdate::default())
    }
    async fn after_turn(&self, _request: &ModelRequest) -> Result<TurnDecision> {
        Ok(TurnDecision::Continue)
    }
}
