use anyhow::{Context, Result, bail};
use serde::Serialize;

use crate::{MessageRole, ModelMessage, ModelRequest, ReasoningEffort, ToolDefinition};

#[derive(Serialize)]
struct FixedRequest<'a> {
    model: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reasoning: Option<ReasoningEffort>,
    system: &'a str,
    messages: &'a [ModelMessage],
    tools: &'a [ToolDefinition],
}

/// Conservative serialized-input ceilings for providers with different context
/// windows. Bytes make the policy deterministic without coupling the generic
/// agent to a provider tokenizer.
pub fn context_budget_bytes(model: &str) -> usize {
    match model.split_once('/').map(|(provider, _)| provider) {
        Some("google") => 2 * 1024 * 1024,
        Some("anthropic") => 640 * 1024,
        Some("openai" | "openai-codex") => 384 * 1024,
        _ => 256 * 1024,
    }
}

/// Retains the largest recent suffix of complete user-led turns that fits the
/// selected provider's deterministic input budget.
///
/// The same function is public so native and ACP request paths can apply one
/// policy before sending model context.
pub fn apply_context_budget(request: &mut ModelRequest) -> Result<usize> {
    let limit = context_budget_bytes(&request.model);
    let fixed_bytes = serde_json::to_vec(&FixedRequest {
        model: &request.model,
        reasoning: request.reasoning,
        system: &request.system,
        messages: &[],
        tools: &request.tools,
    })
    .context("serialize fixed model request for context budgeting")?
    .len();
    let available = limit
        .checked_sub(fixed_bytes)
        .with_context(|| format!("fixed model context exceeds the {limit}-byte input budget"))?;
    let current_bytes = message_array_bytes(&request.messages)?;
    if current_bytes <= available {
        return Ok(0);
    }

    let mut suffix_bytes = 2_usize;
    let mut retained_start = None;
    for (index, message) in request.messages.iter().enumerate().rev() {
        if suffix_bytes > 2 {
            suffix_bytes = suffix_bytes.saturating_add(1);
        }
        suffix_bytes = suffix_bytes.saturating_add(
            serde_json::to_vec(message)
                .context("serialize model message for context budgeting")?
                .len(),
        );
        if suffix_bytes > available {
            break;
        }
        if message.role == MessageRole::User {
            retained_start = Some(index);
        }
    }
    if let Some(start) = retained_start {
        request.messages.drain(..start);
        return Ok(start);
    }

    bail!("latest model turn exceeds the {available}-byte history budget")
}

fn message_array_bytes(messages: &[ModelMessage]) -> Result<usize> {
    messages.iter().try_fold(2_usize, |bytes, message| {
        let separator = usize::from(bytes > 2);
        let message = serde_json::to_vec(message)
            .context("serialize model message for context budgeting")?
            .len();
        Ok(bytes.saturating_add(separator).saturating_add(message))
    })
}

#[cfg(test)]
mod tests {
    use crate::{MessageContent, ModelMessage};

    use super::*;

    fn message(role: MessageRole, text: String) -> ModelMessage {
        ModelMessage {
            role,
            content: MessageContent::Text { text },
        }
    }

    #[test]
    fn keeps_the_largest_recent_complete_turn_suffix() {
        let mut request = ModelRequest {
            model: "unknown/model".into(),
            reasoning: None,
            system: String::new(),
            messages: vec![
                message(MessageRole::User, "old".into()),
                message(MessageRole::Assistant, "x".repeat(180 * 1024)),
                message(MessageRole::User, "recent".into()),
                message(MessageRole::Assistant, "y".repeat(100 * 1024)),
            ],
            tools: Vec::new(),
            continuation: None,
        };

        assert_eq!(apply_context_budget(&mut request).unwrap(), 2);
        assert_eq!(request.messages.len(), 2);
        assert!(matches!(
            &request.messages[0].content,
            MessageContent::Text { text } if text == "recent"
        ));
    }

    #[test]
    fn provider_defaults_are_explicit_and_deterministic() {
        assert!(context_budget_bytes("google/gemini") > context_budget_bytes("anthropic/claude"));
        assert!(context_budget_bytes("anthropic/claude") > context_budget_bytes("openai/gpt"));
        assert_eq!(context_budget_bytes("custom/model"), 256 * 1024);
    }
}
