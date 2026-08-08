use mews_agent::{MessageContent, MessageRole, ModelMessage};

pub fn canonical_prompt(history: Vec<ModelMessage>, soul: &str) -> String {
    let mut prompt =
        String::from("Continue this MEWS conversation. Reply to its latest user request.\n");
    if !soul.is_empty() {
        prompt.push_str("\n<instructions>\n");
        prompt.push_str(soul);
        prompt.push_str("\n</instructions>\n");
    }
    prompt.push_str("\n<conversation>\n");
    for message in history {
        let role = match message.role {
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::Tool => "tool",
        };
        match message.content {
            MessageContent::Text { text } => prompt.push_str(&format!("<{role}>{text}</{role}>\n")),
            MessageContent::ToolCall {
                tool, arguments, ..
            } => prompt.push_str(&format!("<{role} tool={tool:?}>{arguments}</{role}>\n")),
            MessageContent::ToolResult { result, .. } => {
                prompt.push_str(&format!("<{role}>{result}</{role}>\n"))
            }
            MessageContent::ProviderState { .. } => {}
        }
    }
    prompt.push_str("</conversation>");
    prompt
}

pub fn initial_session_prompt(soul: &str, user_prompt: &str) -> String {
    if soul.is_empty() {
        return user_prompt.to_owned();
    }
    format!(
        "<mews_identity>\n{soul}\n</mews_identity>\n\n<user_request>\n{user_prompt}\n</user_request>"
    )
}

#[cfg(test)]
mod tests {
    use mews_agent::{MessageContent, MessageRole, ModelMessage};

    use super::{canonical_prompt, initial_session_prompt};

    #[test]
    fn rehydration_uses_canonical_text_history() {
        let prompt = canonical_prompt(
            vec![ModelMessage {
                role: MessageRole::User,
                content: MessageContent::Text {
                    text: "fix it".into(),
                },
            }],
            "be concise",
        );
        assert!(prompt.contains("<instructions>\nbe concise"));
        assert!(prompt.contains("<user>fix it</user>"));
    }

    #[test]
    fn identity_is_only_wrapped_for_session_creation() {
        let prompt = initial_session_prompt("be concise", "fix it");
        assert!(prompt.contains("<mews_identity>\nbe concise"));
        assert!(prompt.ends_with("<user_request>\nfix it\n</user_request>"));
    }
}
