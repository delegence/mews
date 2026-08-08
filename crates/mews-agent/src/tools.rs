use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use mews_protocol::ToolDefinition;

use crate::ToolCall;

/// One immutable tool catalog advertised to a model turn.
///
/// Schemas are compiled once so validation and execution use the exact same
/// definitions the provider observed.
pub struct ToolCatalog {
    definitions: Vec<ToolDefinition>,
    validators: BTreeMap<String, jsonschema::Validator>,
}

impl ToolCatalog {
    pub fn compile(definitions: Vec<ToolDefinition>) -> Result<Self> {
        let mut validators = BTreeMap::new();
        for tool in &definitions {
            let validator = jsonschema::validator_for(&tool.schema)
                .with_context(|| format!("tool {:?} has an invalid schema", tool.name))?;
            if validators.insert(tool.name.clone(), validator).is_some() {
                bail!("tool {:?} appears more than once in the catalog", tool.name);
            }
        }
        Ok(Self {
            definitions,
            validators,
        })
    }

    pub fn definitions(&self) -> &[ToolDefinition] {
        &self.definitions
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
    fn compiles_once_and_validates_the_snapshotted_schema() {
        let catalog = ToolCatalog::compile(vec![ToolDefinition {
            name: "read".into(),
            description: "read".into(),
            schema: json!({
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"]
            }),
        }])
        .unwrap();
        let mut call = ToolCall {
            id: "call-1".into(),
            name: "read".into(),
            arguments: json!({"path": "a.txt"}),
            thought_signature: None,
        };

        catalog.validate(&call).unwrap();
        call.arguments = json!({"path": 1});
        assert!(catalog.validate(&call).is_err());
    }
}
