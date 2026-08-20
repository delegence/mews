use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use mews_protocol::{ToolCatalogSnapshot, ToolDefinition};

use crate::ToolCall;

/// Shared allowlist semantics for every Harness and inspection surface.
pub fn tool_allowed(pattern: &str, name: &str) -> bool {
    pattern == "*"
        || pattern == name
        || pattern
            .strip_suffix('*')
            .is_some_and(|prefix| name.starts_with(prefix))
}

/// One immutable tool catalog advertised to a model turn.
///
/// Schemas are compiled once so validation and execution use the exact same
/// definitions the provider observed.
pub struct ToolCatalog {
    generation: u64,
    definitions: Vec<ToolDefinition>,
    validators: BTreeMap<String, jsonschema::Validator>,
}

impl ToolCatalog {
    pub fn compile(snapshot: ToolCatalogSnapshot) -> Result<Self> {
        let ToolCatalogSnapshot {
            generation,
            tools: definitions,
        } = snapshot;
        let mut validators = BTreeMap::new();
        for tool in &definitions {
            let validator = jsonschema::validator_for(&tool.schema)
                .with_context(|| format!("tool {:?} has an invalid schema", tool.name))?;
            if validators.insert(tool.name.clone(), validator).is_some() {
                bail!("tool {:?} appears more than once in the catalog", tool.name);
            }
        }
        Ok(Self {
            generation,
            definitions,
            validators,
        })
    }

    pub fn definitions(&self) -> &[ToolDefinition] {
        &self.definitions
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn validate(&self, call: &ToolCall) -> Result<()> {
        let validator = self
            .validators
            .get(&call.name)
            .with_context(|| format!("tool {:?} is unavailable", call.name))?;
        if let Err(error) = validator.validate(&call.arguments) {
            bail!("invalid arguments for tool {:?}: {error}", call.name);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use mews_protocol::ToolDefinition;
    use serde_json::json;

    use super::*;

    #[test]
    fn allowlist_supports_exact_prefix_and_global_patterns() {
        assert!(tool_allowed("read", "read"));
        assert!(tool_allowed("git_*", "git_status"));
        assert!(tool_allowed("*", "anything"));
        assert!(!tool_allowed("read", "write"));
    }

    #[test]
    fn compiles_once_and_validates_the_snapshotted_schema() {
        let catalog = ToolCatalog::compile(ToolCatalogSnapshot {
            generation: 7,
            tools: vec![ToolDefinition {
                name: "read".into(),
                description: "read".into(),
                agent_id: None,
                schema: json!({
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"]
                }),
            }],
        })
        .unwrap();
        let mut call = ToolCall {
            id: "call-1".into(),
            name: "read".into(),
            arguments: json!({"path": "a.txt"}),
            thought_signature: None,
            catalog_generation: 7,
        };

        catalog.validate(&call).unwrap();
        call.arguments = json!({"path": 1});
        assert!(catalog.validate(&call).is_err());
    }
}
