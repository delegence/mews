use crate::{CancellationToken, ProgressReporter, ToolCall, ToolProgress, ToolResult};
use anyhow::Result;
use async_trait::async_trait;
pub use mews_protocol::ToolExecutionMode;
use mews_protocol::{AssistantResponse, OperationId, ReasoningEffort, ToolDefinition};

use crate::{ModelMessage, ModelRequest};

pub const DEFAULT_MAX_STEPS: usize = 32;

/// Facts produced by the agent loop that the embedding runtime may persist.
#[derive(Clone, Debug)]
pub enum AgentEvent {
    AssistantResponse(AssistantResponse),
    MessageInjected(ModelMessage),
    ToolCall(ToolCall),
    /// The validated, policy-approved call is about to cross the effect boundary.
    ToolExecutionStarted(ToolCall),
    /// Immutable raw outcome observed at the external tool boundary.
    ToolExecutionCompleted {
        call: ToolCall,
        result: ToolResult,
    },
    /// Replayable, potentially hook-transformed result presented to the model.
    ToolResultRecorded {
        call: ToolCall,
        result: ToolResult,
    },
}

/// Live signals that are never part of durable conversation replay.
#[derive(Clone, Debug, PartialEq)]
pub enum AgentSignal {
    AssistantStarted,
    AssistantTextDelta(String),
    ToolProgress(ToolProgress),
}

#[derive(Clone, Debug, Default)]
pub enum ToolDecision {
    #[default]
    Allow,
    Block(String),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StepDecision {
    #[default]
    Continue,
    Stop,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderCallOutcome {
    Succeeded,
    Failed(String),
    Uncertain(String),
}

#[derive(Clone, Debug, Default)]
pub struct NextStepUpdate {
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
    /// Records one replayable semantic fact.
    async fn event(&self, event: AgentEvent) -> Result<()>;
    /// Publishes live-only output. Signals must not be used to rebuild state.
    async fn signal(&self, signal: AgentSignal) -> Result<()>;

    /// Journals one provider request before it crosses the remote effect boundary.
    async fn provider_call_started(&self, _request: &ModelRequest) -> Result<Option<OperationId>> {
        Ok(None)
    }
    async fn provider_call_finished(
        &self,
        _operation_id: OperationId,
        _outcome: ProviderCallOutcome,
    ) -> Result<()> {
        Ok(())
    }

    async fn turn_started(&self) -> Result<()> {
        Ok(())
    }
    async fn turn_finished(&self) -> Result<()> {
        Ok(())
    }

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

    /// Messages injected before the next model call while a Turn is active.
    async fn steering_messages(&self) -> Result<Vec<ModelMessage>> {
        Ok(Vec::new())
    }
    /// Messages processed after the agent would otherwise stop.
    async fn follow_up_messages(&self) -> Result<Vec<ModelMessage>> {
        Ok(Vec::new())
    }
    async fn prepare_next_step(&self, _request: &ModelRequest) -> Result<NextStepUpdate> {
        Ok(NextStepUpdate::default())
    }
    async fn after_step(&self, _request: &ModelRequest) -> Result<StepDecision> {
        Ok(StepDecision::Continue)
    }
}
