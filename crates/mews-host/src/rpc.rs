use std::path::Path;

use anyhow::{Context, Result, bail};
use mews_agent::CancellationToken;
use mews_protocol::{HostToHub, HubToHost, MAX_PROJECT_CONTEXT_BYTES};
use serde_json::Value;

use crate::{ToolRegistry, context, resources};

/// Handles Host protocol operations that belong to the execution environment.
/// Hub transfer, enrollment, and Agent replica control remain in the control plane.
pub async fn handle_execution_request(
    registry: &ToolRegistry,
    message: &HubToHost,
    cancellation: Option<&CancellationToken>,
) -> Option<HostToHub> {
    Some(match message {
        HubToHost::ReadProjectContext {
            request_id,
            agent_slug,
            canonical_cwd,
        } => match project_context(registry.root(), agent_slug, canonical_cwd) {
            Ok(context) => HostToHub::ProjectContext {
                request_id: request_id.clone(),
                context: Some(context),
                error: None,
            },
            Err(error) => HostToHub::ProjectContext {
                request_id: request_id.clone(),
                context: None,
                error: Some(error.to_string()),
            },
        },
        HubToHost::ReadPrompt {
            request_id,
            name,
            canonical_cwd,
        } => match resources::read_prompt(registry.root(), canonical_cwd, name) {
            Ok(content) => HostToHub::Prompt {
                request_id: request_id.clone(),
                content,
                error: None,
            },
            Err(error) => HostToHub::Prompt {
                request_id: request_id.clone(),
                content: None,
                error: Some(error.to_string()),
            },
        },
        HubToHost::AttestDirectory { request_id, path } => {
            let attested = path
                .canonicalize()
                .with_context(|| format!("resolve {}", path.display()))
                .and_then(|path| {
                    if path.is_dir() {
                        Ok(path)
                    } else {
                        bail!("working directory is not a directory")
                    }
                });
            let (canonical_path, error) = match attested {
                Ok(path) => (Some(path), None),
                Err(error) => (None, Some(error.to_string())),
            };
            HostToHub::DirectoryAttested {
                request_id: request_id.clone(),
                canonical_path,
                error,
            }
        }
        HubToHost::ExecuteTool {
            request_id,
            tool,
            arguments,
            canonical_cwd,
        } => {
            let result = async {
                let resolved = canonical_cwd
                    .canonicalize()
                    .with_context(|| format!("resolve {}", canonical_cwd.display()))?;
                if resolved != *canonical_cwd || !resolved.is_dir() {
                    bail!("Session working directory no longer resolves to its attested path");
                }
                let cancellation = cancellation
                    .ok_or_else(|| anyhow::anyhow!("tool request has no cancellation scope"))?;
                registry
                    .execute(tool, arguments.clone(), &resolved, cancellation)
                    .await
            }
            .await;
            let (result, error) = match result {
                Ok(result) => (result, None),
                Err(error) => (Value::Null, Some(error.to_string())),
            };
            HostToHub::ToolResult {
                request_id: request_id.clone(),
                result,
                error,
            }
        }
        HubToHost::CancelTool { .. } => return None,
        HubToHost::ExecuteHook {
            request_id,
            hook,
            payload,
            canonical_cwd,
        } => {
            let result = async {
                let resolved = canonical_cwd.canonicalize()?;
                if resolved != *canonical_cwd || !resolved.is_dir() {
                    bail!("Session working directory no longer resolves to its attested path");
                }
                let cancellation = cancellation
                    .ok_or_else(|| anyhow::anyhow!("hook request has no cancellation scope"))?;
                registry
                    .execute_hooks(hook, payload.clone(), &resolved, cancellation)
                    .await
            }
            .await;
            match result {
                Ok(payload) => HostToHub::HookResult {
                    request_id: request_id.clone(),
                    payload: Some(payload),
                    error: None,
                },
                Err(error) => HostToHub::HookResult {
                    request_id: request_id.clone(),
                    payload: None,
                    error: Some(error.to_string()),
                },
            }
        }
        _ => return None,
    })
}

fn project_context(root: Option<&Path>, agent_slug: &str, cwd: &Path) -> Result<String> {
    let resolved = cwd.canonicalize()?;
    if resolved != cwd || !resolved.is_dir() {
        bail!("Session working directory no longer resolves to its attested path");
    }
    let mut output = String::new();
    for file in context::discover_agents_md(&resolved)? {
        let section = format!("\n\n# {}\n{}", file.path.display(), file.content);
        if output.len() + section.len() > MAX_PROJECT_CONTEXT_BYTES {
            bail!("combined AGENTS.md project context exceeds 192 KiB");
        }
        output.push_str(&section);
    }
    let discovered = resources::prompt_context(root, agent_slug, &resolved)?;
    if output.len() + discovered.len() > MAX_PROJECT_CONTEXT_BYTES {
        bail!("combined project and resource context exceeds 192 KiB");
    }
    output.push_str(&discovered);
    Ok(output)
}
