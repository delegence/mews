use std::{collections::BTreeMap, path::Path};

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
    pub extension: String,
    pub command: Vec<String>,
    pub hook: String,
}

pub(super) fn runtime_extensions(root: &Path) -> Result<Vec<RuntimeExtension>> {
    runtime_extensions_in(&root.join("extensions"))
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
        for hook in &extension.hooks {
            if !matches!(
                hook.as_str(),
                "run_start"
                    | "before_model"
                    | "before_tool"
                    | "after_tool"
                    | "after_turn"
                    | "run_end"
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
    for directory in [root.join("tools"), root.join("extensions")] {
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

pub(super) fn extension_manifests(root: &Path) -> Result<BTreeMap<String, ExtensionManifest>> {
    let directory = root.join("tools");
    if !directory.exists() {
        return Ok(BTreeMap::new());
    }
    let mut manifests = BTreeMap::new();
    for entry in std::fs::read_dir(&directory)? {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("toml") {
            continue;
        }
        let metadata = std::fs::symlink_metadata(&path)?;
        if !metadata.file_type().is_file() {
            bail!("tool manifest must be a regular file");
        }
        if metadata.len() > 32 * 1024 {
            bail!("tool manifest exceeds 32 KiB");
        }
        let manifest: ExtensionManifest = toml::from_str(&std::fs::read_to_string(&path)?)
            .with_context(|| format!("parse tool manifest {}", path.display()))?;
        if manifest.name.is_empty()
            || !manifest
                .name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            bail!("tool name must contain lowercase letters, digits, or underscores");
        }
        if manifest.command.is_empty() {
            bail!("tool command must not be empty");
        }
        if manifest.description.is_empty()
            || manifest.description.len() > 1024
            || manifest.command.iter().map(String::len).sum::<usize>() > 8 * 1024
            || serde_json::to_vec(&manifest.schema)?.len() > 32 * 1024
        {
            bail!("tool description, command, or schema exceeds its limit");
        }
        if manifest.schema.get("type").and_then(Value::as_str) != Some("object") {
            bail!("tool schema must be a JSON object schema");
        }
        if manifests.insert(manifest.name.clone(), manifest).is_some() {
            bail!("duplicate tool name");
        }
    }
    Ok(manifests)
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
}
