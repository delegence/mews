use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use async_trait::async_trait;
use mews_agent::{
    AgentCapabilities, CancellationToken, ContextDocument, ContextSnapshot, LifecycleHook,
    ProgressReporter, ResourceDescriptor, ToolCall, ToolResult,
};
use mews_protocol::ToolCatalogSnapshot;
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

#[async_trait]
impl AgentCapabilities for LocalEnvironment {
    async fn context(&self, agent_slug: &str, cwd: &Path) -> Result<ContextSnapshot> {
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
            skills: resources::discover_skills(self.root.as_deref(), agent_slug, cwd)?
                .into_iter()
                .map(convert)
                .collect(),
        })
    }

    fn tools(&self) -> ToolCatalogSnapshot {
        self.registry.snapshot()
    }

    fn extension_tools(&self, agent_id: &mews_protocol::AgentId) -> ToolCatalogSnapshot {
        self.registry.extension_definitions(agent_id)
    }

    async fn execute(
        &self,
        agent_id: &mews_protocol::AgentId,
        call: &ToolCall,
        cwd: &Path,
        cancellation: &CancellationToken,
        _progress: &dyn ProgressReporter,
    ) -> Result<ToolResult> {
        cancellation.check()?;
        let value = self
            .registry
            .execute_at_generation(
                agent_id,
                &call.name,
                call.arguments.clone(),
                cwd,
                cancellation,
                call.catalog_generation,
            )
            .await?;
        Ok(ToolResult::success(value))
    }

    async fn hook(
        &self,
        agent_id: &mews_protocol::AgentId,
        hook: LifecycleHook,
        payload: Value,
        cwd: &Path,
        cancellation: &CancellationToken,
        catalog_generation: Option<u64>,
    ) -> Result<Value> {
        let name = match hook {
            LifecycleHook::TurnStart => "turn_start",
            LifecycleHook::BeforeModel => "before_model",
            LifecycleHook::BeforeTool => "before_tool",
            LifecycleHook::AfterTool => "after_tool",
            LifecycleHook::AfterStep => "after_step",
            LifecycleHook::TurnEnd => "turn_end",
        };
        self.registry
            .execute_hooks_at_generation(
                agent_id,
                name,
                payload,
                cwd,
                cancellation,
                catalog_generation,
            )
            .await
    }
}
