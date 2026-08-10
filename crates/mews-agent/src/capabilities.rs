use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Result, bail};
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
    #[serde(default)]
    pub terminate: bool,
}

impl ToolResult {
    pub fn success(value: Value) -> Self {
        Self {
            value,
            is_error: false,
            terminate: false,
        }
    }
    pub fn error(error: impl ToString) -> Self {
        Self {
            value: Value::String(error.to_string()),
            is_error: true,
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
    RunStart,
    BeforeModel,
    BeforeTool,
    AfterTool,
    AfterTurn,
    RunEnd,
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
            bail!("agent run cancelled")
        } else {
            Ok(())
        }
    }
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
    /// Host extensions are distinct from the native MEWS tools. External
    /// Harnesses receive only this catalog through the run-scoped MCP bridge.
    /// Returning no tools by default is intentionally least-authority.
    fn extension_tools(&self) -> Vec<ToolDefinition> {
        Vec::new()
    }
    async fn execute(
        &self,
        call: &ToolCall,
        cwd: &Path,
        cancellation: &CancellationToken,
        progress: &dyn ProgressReporter,
    ) -> Result<ToolResult>;
    async fn hook(
        &self,
        hook: LifecycleHook,
        payload: Value,
        cwd: &Path,
        cancellation: &CancellationToken,
    ) -> Result<Value>;
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
        assert!(token.check().is_err());
    }
}
