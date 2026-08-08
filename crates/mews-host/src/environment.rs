use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use async_trait::async_trait;
use mews_agent::{
    AgentCapabilities, CancellationToken, ContextDocument, ContextSnapshot, LifecycleHook,
    ProgressReporter, ResourceDescriptor, ToolCall, ToolDefinition, ToolResult,
};
use serde_json::Value;

use crate::{ToolRegistry, context, resources};

/// Full-authority execution environment owned by the local Host OS user.
pub struct LocalEnvironment {
    root: Option<PathBuf>,
    registry: Arc<ToolRegistry>,
}

impl LocalEnvironment {
    pub fn new(root: Option<PathBuf>, registry: Arc<ToolRegistry>) -> Self {
        Self { root, registry }
    }
    pub fn registry(&self) -> &Arc<ToolRegistry> {
        &self.registry
    }
}

#[async_trait(?Send)]
impl AgentCapabilities for LocalEnvironment {
    async fn context(&self, cwd: &Path) -> Result<ContextSnapshot> {
        let documents = context::discover_agents_md(cwd)?
            .into_iter()
            .map(|file| ContextDocument {
                path: file.path,
                content: file.content,
            })
            .collect();
        let convert = |item: resources::Resource| ResourceDescriptor {
            name: item.name,
            description: item.description,
            path: item.path,
        };
        Ok(ContextSnapshot {
            documents,
            skills: resources::discover_skills(self.root.as_deref(), cwd)?
                .into_iter()
                .map(convert)
                .collect(),
            prompts: resources::discover_prompts(self.root.as_deref(), cwd)?
                .into_iter()
                .map(convert)
                .collect(),
        })
    }

    async fn read_prompt(&self, cwd: &Path, name: &str) -> Result<Option<String>> {
        resources::read_prompt(self.root.as_deref(), cwd, name)
    }

    fn tools(&self) -> Vec<ToolDefinition> {
        self.registry
            .definitions()
            .into_iter()
            .map(|tool| ToolDefinition {
                name: tool.name,
                description: tool.description,
                schema: tool.schema,
            })
            .collect()
    }

    fn extension_tools(&self) -> Vec<ToolDefinition> {
        self.registry
            .extension_definitions()
            .into_iter()
            .map(|tool| ToolDefinition {
                name: tool.name,
                description: tool.description,
                schema: tool.schema,
            })
            .collect()
    }

    async fn execute(
        &self,
        call: &ToolCall,
        cwd: &Path,
        cancellation: &CancellationToken,
        _progress: &dyn ProgressReporter,
    ) -> Result<ToolResult> {
        cancellation.check()?;
        let value = self
            .registry
            .execute(&call.name, call.arguments.clone(), cwd)
            .await?;
        Ok(ToolResult::success(value))
    }

    async fn hook(&self, hook: LifecycleHook, payload: Value, cwd: &Path) -> Result<Value> {
        let name = match hook {
            LifecycleHook::RunStart => "run_start",
            LifecycleHook::BeforeModel => "before_model",
            LifecycleHook::BeforeTool => "before_tool",
            LifecycleHook::AfterTool => "after_tool",
            LifecycleHook::AfterTurn => "after_turn",
            LifecycleHook::RunEnd => "run_end",
        };
        self.registry.execute_hooks(name, payload, cwd).await
    }
}
