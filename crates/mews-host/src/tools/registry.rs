use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use mews_agent::CancellationToken;
use serde_json::{Value, json};

use super::builtins::{Bash, Edit, Read, Write};
use super::extensions::{
    ExtensionManifest, ExternalHook, ExternalTool, resource_fingerprint, runtime_extensions,
};

#[async_trait]
pub trait Tool: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str;
    fn schema(&self) -> Value;
    async fn execute(
        &self,
        arguments: Value,
        cwd: &Path,
        cancellation: &CancellationToken,
    ) -> Result<Value>;
}

#[derive(Clone)]
pub struct ToolRegistry {
    inner: Arc<RegistryInner>,
}

struct RegistryInner {
    tools: RwLock<Tools>,
    catalog: tokio::sync::watch::Sender<Vec<mews_protocol::ToolDefinition>>,
    root: Option<PathBuf>,
    hooks: RwLock<Vec<ExternalHook>>,
    acp_pool: mews_acp::AcpRuntimePool,
}

/// Native tools are shared; extension tools and hooks are owned by one Agent.
#[derive(Clone, Default)]
struct Tools {
    native: BTreeMap<String, Arc<dyn Tool>>,
    extensions: BTreeMap<(String, String), Arc<dyn Tool>>,
}

impl Default for ToolRegistry {
    fn default() -> Self {
        let (catalog, _) = tokio::sync::watch::channel(Vec::new());
        Self {
            inner: Arc::new(RegistryInner {
                tools: RwLock::new(Tools::default()),
                catalog,
                root: None,
                hooks: RwLock::new(Vec::new()),
                acp_pool: mews_acp::AcpRuntimePool::default(),
            }),
        }
    }
}

impl ToolRegistry {
    pub(crate) fn acp_pool(&self) -> &mews_acp::AcpRuntimePool {
        &self.inner.acp_pool
    }

    pub fn with_defaults() -> Self {
        let registry = Self::default();
        for name in ["read", "write", "edit", "bash"] {
            registry.restore_default(name);
        }
        registry
    }

    /// Loads extensions owned by Agents under `<MEWS_HOME>/agents/*/extensions`.
    pub fn with_agent_extensions(root: &Path) -> Result<Self> {
        let mut registry = Self::with_defaults();
        Arc::get_mut(&mut registry.inner)
            .expect("new registry is unique")
            .root = Some(root.to_path_buf());
        registry.reload_extensions(root)?;
        Ok(registry)
    }

    pub fn root(&self) -> Option<&Path> {
        self.inner.root.as_deref()
    }

    pub async fn watch_agent_extensions(&self, root: PathBuf) {
        let mut fingerprint = String::new();
        loop {
            // A short-lived in-process Host owns one watcher; let it finish
            // when the Host link and every other registry handle are gone.
            if Arc::strong_count(&self.inner) == 1 {
                return;
            }
            if let Ok(next) = resource_fingerprint(&root)
                && next != fingerprint
                && self.reload_extensions(&root).is_ok()
            {
                fingerprint = next;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    fn reload_extensions(&self, root: &Path) -> Result<()> {
        let extensions = runtime_extensions(root)?;
        let mut tools = BTreeMap::new();
        for extension in &extensions {
            for tool in &extension.tools {
                let manifest = ExtensionManifest {
                    name: tool.name.clone(),
                    description: tool.description.clone(),
                    command: extension.command.clone(),
                    schema: tool.schema.clone(),
                    envelope: true,
                    agent_id: extension.agent_id.clone(),
                };
                let agent_id = extension
                    .agent_id
                    .clone()
                    .context("extension has no Agent ID")?;
                if tools
                    .insert((agent_id.to_string(), manifest.name.clone()), manifest)
                    .is_some()
                {
                    bail!("duplicate extension tool name");
                }
            }
        }
        self.apply_extensions(tools)?;
        *self.inner.hooks.write().expect("extension hooks poisoned") = extensions
            .into_iter()
            .flat_map(|extension| {
                extension.hooks.into_iter().map(move |hook| ExternalHook {
                    agent_id: extension.agent_id.clone().expect("validated Agent ID"),
                    extension: extension.name.clone(),
                    command: extension.command.clone(),
                    hook,
                })
            })
            .collect();
        Ok(())
    }

    pub async fn execute_hooks(
        &self,
        agent_id: &mews_protocol::AgentId,
        hook: &str,
        mut payload: Value,
        cwd: &Path,
        cancellation: &CancellationToken,
    ) -> Result<Value> {
        let hooks = self
            .inner
            .hooks
            .read()
            .expect("extension hooks poisoned")
            .clone();
        for extension in hooks
            .into_iter()
            .filter(|item| &item.agent_id == agent_id && item.hook == hook)
        {
            let input = json!({
                "type": "hook", "extension": extension.extension, "hook": hook, "payload": payload,
            });
            let output = super::process::execute(&extension.command, cwd, input, cancellation)
                .await
                .with_context(|| format!("extension {:?} hook {hook}", extension.extension))?;
            if !output.is_null() {
                payload = output;
            }
        }
        Ok(payload)
    }

    /// Registers an Agent-owned extension tool. Native MEWS tools are kept
    /// separate so external Harnesses receive only extension tools through MCP.
    fn register(&self, agent_id: mews_protocol::AgentId, tool: impl Tool + 'static) {
        let name = tool.name().to_owned();
        self.inner
            .tools
            .write()
            .expect("tool registry poisoned")
            .extensions
            .insert((agent_id.to_string(), name), Arc::new(tool));
        self.publish_catalog();
    }

    fn register_native(&self, tool: impl Tool + 'static) {
        let name = tool.name().to_owned();
        self.inner
            .tools
            .write()
            .expect("tool registry poisoned")
            .native
            .insert(name, Arc::new(tool));
        self.publish_catalog();
    }

    fn restore_default(&self, name: &str) -> bool {
        match name {
            "read" => self.register_native(Read),
            "write" => self.register_native(Write),
            "edit" => self.register_native(Edit),
            "bash" => self.register_native(Bash),
            _ => return false,
        }
        true
    }

    fn apply_extensions(
        &self,
        manifests: BTreeMap<(String, String), ExtensionManifest>,
    ) -> Result<()> {
        let candidate = Self::with_defaults();
        for manifest in manifests.into_values() {
            if candidate
                .inner
                .tools
                .read()
                .expect("tool registry poisoned")
                .native
                .contains_key(&manifest.name)
            {
                bail!("extension tool name conflicts with a native MEWS tool");
            }
            let agent_id = manifest
                .agent_id
                .clone()
                .context("extension tool has no Agent owner")?;
            candidate.register(agent_id, ExternalTool(manifest));
        }
        let definitions = candidate.definitions();
        mews_protocol::encode(mews_protocol::HostToHub::ToolCatalogChanged {
            tools: definitions.clone(),
        })?;
        let tools = candidate
            .inner
            .tools
            .read()
            .expect("tool registry poisoned")
            .clone();
        *self.inner.tools.write().expect("tool registry poisoned") = tools;
        self.inner.catalog.send_replace(definitions);
        Ok(())
    }

    pub fn names(&self) -> Vec<String> {
        let tools = self.inner.tools.read().expect("tool registry poisoned");
        tools
            .native
            .keys()
            .chain(tools.extensions.keys().map(|(_, name)| name))
            .cloned()
            .collect()
    }

    pub fn definitions(&self) -> Vec<mews_protocol::ToolDefinition> {
        let tools = self.inner.tools.read().expect("tool registry poisoned");
        tools
            .native
            .values()
            .map(|tool| mews_protocol::ToolDefinition {
                name: tool.name().to_owned(),
                description: tool.description().to_owned(),
                schema: tool.schema(),
                agent_id: None,
            })
            .chain(tools.extensions.iter().map(|((agent_id, _), tool)| {
                mews_protocol::ToolDefinition {
                    name: tool.name().to_owned(),
                    description: tool.description().to_owned(),
                    schema: tool.schema(),
                    agent_id: Some(agent_id.parse().expect("validated Agent ID")),
                }
            }))
            .collect()
    }

    /// Definitions for one Agent's extensions. External Harnesses receive this
    /// catalog through the Turn-scoped MCP bridge.
    pub fn extension_definitions(
        &self,
        agent_id: &mews_protocol::AgentId,
    ) -> Vec<mews_protocol::ToolDefinition> {
        self.inner
            .tools
            .read()
            .expect("tool registry poisoned")
            .extensions
            .iter()
            .filter(|((owner, _), _)| owner == agent_id.as_str())
            .map(|(_, tool)| mews_protocol::ToolDefinition {
                name: tool.name().to_owned(),
                description: tool.description().to_owned(),
                schema: tool.schema(),
                agent_id: None,
            })
            .collect()
    }

    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<Vec<mews_protocol::ToolDefinition>> {
        self.inner.catalog.subscribe()
    }

    fn publish_catalog(&self) {
        self.inner.catalog.send_replace(self.definitions());
    }

    pub async fn execute(
        &self,
        agent_id: &mews_protocol::AgentId,
        name: &str,
        arguments: Value,
        cwd: &Path,
        cancellation: &CancellationToken,
    ) -> Result<Value> {
        let tool = {
            let tools = self.inner.tools.read().expect("tool registry poisoned");
            tools
                .extensions
                .get(&(agent_id.to_string(), name.to_owned()))
                .or_else(|| tools.native.get(name))
                .with_context(|| format!("Host does not provide tool {name:?}"))?
                .clone()
        };
        tool.execute(arguments, cwd, cancellation).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::process::MAX_OUTPUT;
    use tokio::fs;

    fn agent_replica(root: &Path, slug: &str) -> mews_protocol::AgentId {
        let agent_id = mews_protocol::AgentId::new();
        let directory = root.join("agents").join(slug);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(".agent-id"), agent_id.as_str()).unwrap();
        agent_id
    }

    #[tokio::test]
    async fn edit_refuses_ambiguous_changes() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(directory.path().join("a.txt"), "same same")
            .await
            .unwrap();
        let tools = ToolRegistry::with_defaults();
        assert!(
            tools
                .execute(
                    &mews_protocol::AgentId::new(),
                    "edit",
                    json!({"path":"a.txt","old_text":"same","new_text":"x"}),
                    directory.path(),
                    &CancellationToken::new(),
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn bash_schema_is_strict_and_null_timeout_uses_the_default() {
        let tools = ToolRegistry::with_defaults();
        let bash = tools
            .definitions()
            .into_iter()
            .find(|tool| tool.name == "bash")
            .unwrap();
        assert_eq!(
            bash.schema["required"],
            json!(["command", "timeout_seconds"])
        );
        assert_eq!(
            bash.schema["properties"]["timeout_seconds"]["type"],
            json!(["integer", "null"])
        );

        let result = tools
            .execute(
                &mews_protocol::AgentId::new(),
                "bash",
                json!({"command":"printf ok","timeout_seconds":null}),
                Path::new("."),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(result["stdout"], "ok");
        assert_eq!(result["success"], true);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bash_drains_noisy_stdout_and_stderr_into_bounded_buffers() {
        let tools = ToolRegistry::with_defaults();
        let result = tools
            .execute(
                &mews_protocol::AgentId::new(),
                "bash",
                json!({
                    "command":"(yes o | head -c 70000) & (yes e | head -c 70000 >&2) & wait",
                    "timeout_seconds":5
                }),
                Path::new("."),
                &CancellationToken::new(),
            )
            .await
            .unwrap();

        assert_eq!(result["stdout"].as_str().unwrap().len(), MAX_OUTPUT);
        assert_eq!(result["stderr"].as_str().unwrap().len(), MAX_OUTPUT);
        assert_eq!(result["success"], true);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn extension_output_limit_terminates_the_process_group() {
        let root = tempfile::tempdir().unwrap();
        let agent_id = agent_replica(root.path(), "coder");
        let directory = root.path().join("agents/coder/extensions");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join("noisy.toml"),
            r#"name = "noisy"
command = ["sh", "-c", "yes x | head -c 70000; sleep 30"]

[[tools]]
name = "noisy"
description = "Exceed the output contract"
schema = { type = "object" }
"#,
        )
        .unwrap();
        let tools = ToolRegistry::with_agent_extensions(root.path()).unwrap();

        let result = tokio::time::timeout(
            Duration::from_secs(3),
            tools.execute(
                &agent_id,
                "noisy",
                json!({}),
                root.path(),
                &CancellationToken::new(),
            ),
        )
        .await
        .expect("the output limit should stop the sleeping process");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("extension tool output exceeds 64 KiB")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_stops_shell_descendants() {
        let directory = tempfile::tempdir().unwrap();
        let pid_path = directory.path().join("descendant.pid");
        let tools = ToolRegistry::with_defaults();
        let cancellation = CancellationToken::new();
        let command = format!(
            "sleep 30 & child=$!; printf %s $child > {}; wait",
            pid_path.display()
        );
        let agent_id = mews_protocol::AgentId::new();
        let execution = tools.execute(
            &agent_id,
            "bash",
            json!({"command":command,"timeout_seconds":30}),
            directory.path(),
            &cancellation,
        );
        tokio::pin!(execution);

        let descendant = loop {
            tokio::select! {
                result = &mut execution => panic!("shell exited before cancellation: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(10)) => {
                    if let Ok(pid) = std::fs::read_to_string(&pid_path)
                        && let Ok(pid) = pid.parse::<i32>()
                    {
                        break pid;
                    }
                }
            }
        };
        cancellation.cancel();
        assert!(
            execution
                .await
                .unwrap_err()
                .to_string()
                .contains("cancelled")
        );

        for _ in 0..100 {
            // SAFETY: signal 0 only queries whether the test child still exists.
            if unsafe { libc::kill(descendant, 0) } == -1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("cancelled shell descendant {descendant} is still running");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_stops_extension_hook_descendants() {
        let root = tempfile::tempdir().unwrap();
        let agent_id = agent_replica(root.path(), "coder");
        let extensions = root.path().join("agents/coder/extensions");
        std::fs::create_dir_all(&extensions).unwrap();
        let pid_path = root.path().join("hook-descendant.pid");
        let executable = root.path().join("hook.sh");
        std::fs::write(
            &executable,
            format!(
                "cat >/dev/null\nsleep 30 & child=$!\nprintf %s \"$child\" > {}\nwait\n",
                pid_path.display()
            ),
        )
        .unwrap();
        std::fs::write(
            extensions.join("hook.toml"),
            format!(
                r#"name = "hook"
command = ["sh", {:?}]
hooks = ["before_model"]
"#,
                executable.display().to_string()
            ),
        )
        .unwrap();

        let registry = ToolRegistry::with_agent_extensions(root.path()).unwrap();
        let cancellation = CancellationToken::new();
        let execution = registry.execute_hooks(
            &agent_id,
            "before_model",
            json!({}),
            root.path(),
            &cancellation,
        );
        tokio::pin!(execution);
        let descendant = loop {
            tokio::select! {
                result = &mut execution => panic!("hook exited before cancellation: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(10)) => {
                    if let Ok(pid) = std::fs::read_to_string(&pid_path)
                        && let Ok(pid) = pid.parse::<i32>()
                    {
                        break pid;
                    }
                }
            }
        };
        cancellation.cancel();
        assert!(execution.await.is_err());

        for _ in 0..100 {
            // SAFETY: signal 0 only queries whether the test child still exists.
            if unsafe { libc::kill(descendant, 0) } == -1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("cancelled extension hook descendant {descendant} is still running");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_stops_shell_descendants() {
        let directory = tempfile::tempdir().unwrap();
        let pid_path = directory.path().join("timed-out-descendant.pid");
        let command = format!(
            "sleep 30 & child=$!; printf %s $child > {}; wait",
            pid_path.display()
        );

        let error = ToolRegistry::with_defaults()
            .execute(
                &mews_protocol::AgentId::new(),
                "bash",
                json!({"command":command,"timeout_seconds":1}),
                directory.path(),
                &CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("timed out"));
        let descendant = std::fs::read_to_string(pid_path)
            .unwrap()
            .parse::<i32>()
            .unwrap();

        for _ in 0..100 {
            // SAFETY: signal 0 only queries whether the test child still exists.
            if unsafe { libc::kill(descendant, 0) } == -1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed-out shell descendant {descendant} is still running");
    }

    #[tokio::test]
    async fn read_rejects_files_over_the_streaming_limit() {
        let directory = tempfile::tempdir().unwrap();
        fs::write(
            directory.path().join("large.txt"),
            vec![b'x'; MAX_OUTPUT + 1],
        )
        .await
        .unwrap();

        let error = ToolRegistry::with_defaults()
            .execute(
                &mews_protocol::AgentId::new(),
                "read",
                json!({"path":"large.txt"}),
                directory.path(),
                &CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("file exceeds 64 KiB"));
    }

    #[tokio::test]
    async fn runtime_extension_authority_survives_an_agent_rename() {
        let root = tempfile::tempdir().unwrap();
        let agent_id = agent_replica(root.path(), "coder");
        let directory = root.path().join("agents/coder/extensions");
        std::fs::create_dir_all(&directory).unwrap();
        let executable = root.path().join("extension.sh");
        std::fs::write(
            &executable,
            r#"input=$(cat)
case "$input" in
  *\"type\":\"hook\"*) printf '{"changed":true}' ;;
  *) printf '{"tool":true}' ;;
esac
"#,
        )
        .unwrap();
        std::fs::write(
            directory.join("example.toml"),
            format!(
                r#"name = "example"
command = ["sh", {:?}]
hooks = ["before_tool"]

[[tools]]
name = "example_tool"
description = "Example tool"
schema = {{ type = "object" }}
"#,
                executable.display().to_string()
            ),
        )
        .unwrap();
        std::fs::rename(
            root.path().join("agents/coder"),
            root.path().join("agents/renamed"),
        )
        .unwrap();

        let registry = ToolRegistry::with_agent_extensions(root.path()).unwrap();
        assert!(registry.names().contains(&"example_tool".to_owned()));
        assert_eq!(
            registry
                .execute(
                    &agent_id,
                    "example_tool",
                    json!({}),
                    root.path(),
                    &CancellationToken::new(),
                )
                .await
                .unwrap(),
            json!({"tool": true})
        );
        assert_eq!(
            registry
                .execute_hooks(
                    &agent_id,
                    "before_tool",
                    json!({}),
                    root.path(),
                    &CancellationToken::new(),
                )
                .await
                .unwrap(),
            json!({"changed": true})
        );
        let other = mews_protocol::AgentId::new();
        assert!(registry.extension_definitions(&other).is_empty());
        assert!(
            registry
                .execute(
                    &other,
                    "example_tool",
                    json!({}),
                    root.path(),
                    &CancellationToken::new(),
                )
                .await
                .is_err()
        );
    }
}
