use std::path::Path;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use mews_agent::CancellationToken;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::registry::Tool;

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ExtensionManifest {
    pub name: String,
    pub description: String,
    pub command: Vec<String>,
    pub schema: Value,
    #[serde(default, skip_serializing)]
    pub envelope: bool,
    #[serde(default, skip_serializing)]
    pub agent_id: Option<mews_protocol::AgentId>,
}

pub(super) struct ExternalTool(pub ExtensionManifest);

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RuntimeExtension {
    pub name: String,
    pub command: Vec<String>,
    #[serde(default)]
    pub hooks: Vec<String>,
    #[serde(default)]
    pub tools: Vec<RuntimeTool>,
    #[serde(skip)]
    pub agent_id: Option<mews_protocol::AgentId>,
    #[serde(skip)]
    pub agent_slug: String,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RuntimeTool {
    pub name: String,
    pub description: String,
    pub schema: Value,
}

#[derive(Clone)]
pub(super) struct ExternalHook {
    pub agent_id: mews_protocol::AgentId,
    pub extension: String,
    pub command: Vec<String>,
    pub hook: String,
}

pub(super) fn runtime_extensions(root: &Path) -> Result<Vec<RuntimeExtension>> {
    let agents = root.join("agents");
    if !agents.exists() {
        return Ok(Vec::new());
    }
    let mut extensions = Vec::new();
    for entry in std::fs::read_dir(agents)? {
        let entry = entry?;
        if !is_agent_replica(&entry)? {
            continue;
        }
        let agent_slug = entry.file_name().to_string_lossy().into_owned();
        let agent_id: mews_protocol::AgentId =
            std::fs::read_to_string(entry.path().join(".agent-id"))?
                .trim()
                .parse()
                .map_err(anyhow::Error::msg)?;
        for mut extension in runtime_extensions_in(&entry.path().join("extensions"))? {
            extension.agent_id = Some(agent_id.clone());
            extension.agent_slug = agent_slug.clone();
            extensions.push(extension);
        }
    }
    extensions.sort_by(|left, right| {
        left.agent_slug
            .cmp(&right.agent_slug)
            .then_with(|| left.name.cmp(&right.name))
    });
    Ok(extensions)
}

fn runtime_extensions_in(directory: &Path) -> Result<Vec<RuntimeExtension>> {
    if !directory.exists() {
        return Ok(Vec::new());
    }
    let mut extensions = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("toml") {
            continue;
        }
        let extension: RuntimeExtension = toml::from_str(&std::fs::read_to_string(&path)?)
            .with_context(|| format!("parse extension {}", path.display()))?;
        if extension.name.is_empty() || extension.command.is_empty() {
            bail!("extension name and command are required");
        }
        for tool in &extension.tools {
            if mews_protocol::is_reserved_acp_skill_tool(&tool.name) {
                bail!(
                    "extension tool name {:?} is reserved for ACP skills",
                    tool.name
                );
            }
        }
        for hook in &extension.hooks {
            if !matches!(
                hook.as_str(),
                "turn_start"
                    | "before_model"
                    | "before_tool"
                    | "after_tool"
                    | "after_step"
                    | "turn_end"
            ) {
                bail!("unsupported extension hook {hook:?}");
            }
        }
        extensions.push((path, extension));
    }
    extensions.sort_by(|(left_path, left), (right_path, right)| {
        left.name
            .cmp(&right.name)
            .then_with(|| left_path.cmp(right_path))
    });
    if extensions
        .windows(2)
        .any(|items| items[0].1.name == items[1].1.name)
    {
        bail!("duplicate runtime extension name");
    }
    Ok(extensions
        .into_iter()
        .map(|(_, extension)| extension)
        .collect())
}

pub(super) fn resource_fingerprint(root: &Path) -> Result<String> {
    let mut entries = Vec::new();
    let agents = root.join("agents");
    if !agents.exists() {
        return Ok(String::new());
    }
    for agent in std::fs::read_dir(agents)? {
        let agent = agent?;
        if !is_agent_replica(&agent)? {
            continue;
        }
        let directory = agent.path().join("extensions");
        if !directory.exists() {
            continue;
        }
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            entries.push(format!(
                "{}:{}:{:?}",
                entry.path().display(),
                metadata.len(),
                metadata.modified()?
            ));
        }
    }
    entries.sort();
    Ok(entries.join("|"))
}

fn is_agent_replica(entry: &std::fs::DirEntry) -> Result<bool> {
    if !entry.file_type()?.is_dir() {
        return Ok(false);
    }
    let slug = entry.file_name();
    let Some(slug) = slug.to_str() else {
        return Ok(false);
    };
    Ok(slug.len() <= 64
        && !slug.is_empty()
        && slug.split('-').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        })
        && entry.path().join(".agent-id").is_file())
}

#[async_trait]
impl Tool for ExternalTool {
    fn name(&self) -> &str {
        &self.0.name
    }
    fn description(&self) -> &str {
        &self.0.description
    }
    fn schema(&self) -> Value {
        self.0.schema.clone()
    }
    async fn execute(
        &self,
        arguments: Value,
        cwd: &Path,
        cancellation: &CancellationToken,
    ) -> Result<Value> {
        let input = if self.0.envelope {
            json!({"type":"tool", "name":self.0.name, "arguments":arguments})
        } else {
            arguments
        };
        super::process::execute(&self.0.command, cwd, input, cancellation).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn documented_extension_manifest_parses() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(
            directory.path().join("policy.toml"),
            r#"name = "policy"
command = ["/opt/mews-extensions/policy"]
hooks = ["turn_start", "before_model", "before_tool", "after_tool", "after_step", "turn_end"]

[[tools]]
name = "lookup"
description = "Look up a record"
schema = { type = "object", properties = { id = { type = "string" } }, required = ["id"] }
"#,
        )
        .unwrap();

        let extensions = runtime_extensions_in(directory.path()).unwrap();
        assert_eq!(extensions.len(), 1);
        assert_eq!(extensions[0].hooks.len(), 6);
        assert_eq!(extensions[0].tools[0].name, "lookup");
    }

    #[test]
    fn runtime_extensions_are_sorted_by_name() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(
            directory.path().join("first-on-disk.toml"),
            "name = 'zebra'\ncommand = ['true']\n",
        )
        .unwrap();
        std::fs::write(
            directory.path().join("second-on-disk.toml"),
            "name = 'alpha'\ncommand = ['true']\n",
        )
        .unwrap();

        let names: Vec<_> = runtime_extensions_in(directory.path())
            .unwrap()
            .into_iter()
            .map(|extension| extension.name)
            .collect();
        assert_eq!(names, ["alpha", "zebra"]);
    }

    #[test]
    fn duplicate_runtime_extension_names_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        for file in ["one.toml", "two.toml"] {
            std::fs::write(
                directory.path().join(file),
                "name = 'duplicate'\ncommand = ['true']\n",
            )
            .unwrap();
        }

        let error = runtime_extensions_in(directory.path()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("duplicate runtime extension name")
        );
    }

    #[test]
    fn reserved_acp_skill_tool_names_are_rejected() {
        for name in mews_protocol::ACP_SKILL_TOOL_NAMES {
            let directory = tempfile::tempdir().unwrap();
            std::fs::write(
                directory.path().join("reserved.toml"),
                format!(
                    "name = 'reserved'\ncommand = ['true']\n\n[[tools]]\nname = '{name}'\ndescription = 'reserved'\nschema = {{ type = 'object' }}\n"
                ),
            )
            .unwrap();

            let error = runtime_extensions_in(directory.path()).unwrap_err();
            assert!(error.to_string().contains(name));
            assert!(error.to_string().contains("reserved for ACP skills"));
        }
    }

    #[test]
    fn discovery_ignores_staged_and_previous_agent_directories() {
        let root = tempfile::tempdir().unwrap();
        for name in [".coder.staged-1", ".coder.previous-1"] {
            let directory = root.path().join("agents").join(name).join("extensions");
            std::fs::create_dir_all(&directory).unwrap();
            std::fs::write(
                directory.join("hidden.toml"),
                "name = 'hidden'\ncommand = ['true']\n",
            )
            .unwrap();
        }

        assert!(runtime_extensions(root.path()).unwrap().is_empty());
        assert!(resource_fingerprint(root.path()).unwrap().is_empty());
    }
}
