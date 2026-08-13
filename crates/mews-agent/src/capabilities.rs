use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::Result;
use async_trait::async_trait;
use mews_protocol::ToolDefinition;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Notify;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thought_signature: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolResult {
    pub value: Value,
    pub is_error: bool,
    /// The effect may have happened, but no definitive result was observed.
    #[serde(default)]
    pub uncertain: bool,
    #[serde(default)]
    pub terminate: bool,
}

impl ToolResult {
    pub fn success(value: Value) -> Self {
        Self {
            value,
            is_error: false,
            uncertain: false,
            terminate: false,
        }
    }
    pub fn error(error: impl ToString) -> Self {
        Self {
            value: Value::String(error.to_string()),
            is_error: true,
            uncertain: false,
            terminate: false,
        }
    }

    pub fn uncertain(reason: impl ToString) -> Self {
        Self {
            value: Value::String(reason.to_string()),
            is_error: true,
            uncertain: true,
            terminate: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolProgress {
    pub call_id: String,
    pub value: Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextDocument {
    pub path: PathBuf,
    pub content: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSnapshot {
    pub documents: Vec<ContextDocument>,
    pub skills: Vec<ResourceDescriptor>,
    pub prompts: Vec<ResourceDescriptor>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceDescriptor {
    pub name: String,
    pub description: String,
    pub path: PathBuf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleHook {
    TurnStart,
    BeforeModel,
    BeforeTool,
    AfterTool,
    AfterStep,
    TurnEnd,
}

#[derive(Clone, Default, Debug)]
pub struct CancellationToken {
    inner: Arc<CancellationState>,
}

#[derive(Default, Debug)]
struct CancellationState {
    cancelled: AtomicBool,
    notify: Notify,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn cancel(&self) {
        if !self.inner.cancelled.swap(true, Ordering::SeqCst) {
            self.inner.notify.notify_waiters();
        }
    }
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::SeqCst)
    }
    pub async fn cancelled(&self) {
        let notified = self.inner.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.is_cancelled() {
            notified.await;
        }
    }
    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(TurnCancelled.into())
        } else {
            Ok(())
        }
    }
}

/// Typed cooperative cancellation marker. Callers should inspect the error
/// chain rather than classifying cancellation from display text.
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("agent Turn cancelled")]
pub struct TurnCancelled;

pub fn is_turn_cancelled(error: &anyhow::Error) -> bool {
    error.downcast_ref::<TurnCancelled>().is_some()
        || matches!(
            error.downcast_ref::<crate::ProviderError>(),
            Some(crate::ProviderError::Cancelled)
        )
}

/// Marks failures after a remote effect was dispatched but before its outcome
/// could be observed. Retrying such an effect automatically may duplicate it.
#[derive(Clone, Debug, thiserror::Error)]
#[error("effect outcome is uncertain: {reason}")]
pub struct EffectUncertain {
    reason: String,
}

impl EffectUncertain {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

pub fn effect_uncertainty(error: &anyhow::Error) -> Option<&EffectUncertain> {
    error.downcast_ref::<EffectUncertain>()
}

#[async_trait(?Send)]
pub trait ProgressReporter {
    async fn report(&self, progress: Value) -> Result<()>;
}

/// Everything the generic harness may observe or invoke outside the model.
#[async_trait]
pub trait AgentCapabilities: Send + Sync {
    async fn context(&self, agent_slug: &str, cwd: &Path) -> Result<ContextSnapshot>;
    async fn read_prompt(&self, _cwd: &Path, _name: &str) -> Result<Option<String>> {
        Ok(None)
    }
    fn tools(&self) -> Vec<ToolDefinition>;
    /// Agent extensions are distinct from the native MEWS tools. External
    /// Harnesses receive only the selected Agent's catalog through MCP.
    /// Returning no tools by default is intentionally least-authority.
    fn extension_tools(&self, _agent_id: &mews_protocol::AgentId) -> Vec<ToolDefinition> {
        Vec::new()
    }
    async fn execute(
        &self,
        agent_id: &mews_protocol::AgentId,
        call: &ToolCall,
        cwd: &Path,
        cancellation: &CancellationToken,
        progress: &dyn ProgressReporter,
    ) -> Result<ToolResult>;
    async fn hook(
        &self,
        agent_id: &mews_protocol::AgentId,
        hook: LifecycleHook,
        payload: Value,
        cwd: &Path,
        cancellation: &CancellationToken,
    ) -> Result<Value>;
}

pub const UNCERTAIN_EFFECT_INSTRUCTION: &str = "The external effect may have happened, but its outcome could not be observed. Do not retry automatically; ask the user before repeating the operation.";

/// Keeps errors explicit when a provider-specific protocol has no native
/// result-status field. Successful values retain their existing shape.
pub fn tool_result_for_model(value: &Value, is_error: bool, uncertain: bool) -> Value {
    if uncertain {
        serde_json::json!({
            "outcome": "uncertain",
            "is_error": is_error,
            "reason": value,
            "instruction": UNCERTAIN_EFFECT_INSTRUCTION,
        })
    } else if is_error {
        serde_json::json!({
            "outcome": "error",
            "is_error": true,
            "error": value,
        })
    } else {
        value.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn cancellation_wakes_waiters_and_is_sticky() {
        let token = CancellationToken::new();
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let waiter = token.clone();
            tasks.push(tokio::spawn(async move { waiter.cancelled().await }));
        }
        token.cancel();
        for task in tasks {
            tokio::time::timeout(std::time::Duration::from_secs(1), task)
                .await
                .expect("every cancellation waiter wakes")
                .unwrap();
        }
        tokio::time::timeout(std::time::Duration::from_secs(1), token.cancelled())
            .await
            .expect("cancellation is sticky");
        let error = token.check().unwrap_err();
        assert!(is_turn_cancelled(&error));
    }

    #[test]
    fn provider_cancellation_uses_the_same_classification() {
        let error =
            anyhow::Error::from(crate::ProviderError::Cancelled).context("provider stream failed");
        assert!(is_turn_cancelled(&error));
    }

    #[test]
    fn provider_replay_preserves_ordinary_results_and_marks_uncertainty() {
        let success = serde_json::json!({"value": 7});
        assert_eq!(tool_result_for_model(&success, false, false), success);

        let ordinary = tool_result_for_model(&serde_json::json!("denied"), true, false);
        assert_eq!(ordinary["outcome"], "error");
        assert_eq!(ordinary["error"], "denied");

        let uncertain = tool_result_for_model(&serde_json::json!("reply lost"), true, true);
        assert_eq!(uncertain["outcome"], "uncertain");
        assert_eq!(uncertain["is_error"], true);
        assert!(
            uncertain["instruction"]
                .as_str()
                .unwrap()
                .contains("Do not retry automatically")
        );
    }
}
