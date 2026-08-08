use std::collections::HashMap;

use anyhow::Result;
use serde_json::{Value, json};

use crate::session::AcpStreamEvent;

const MAX_METADATA_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, Default, PartialEq)]
struct ToolActivityState {
    title: String,
    kind: Option<String>,
    status: Option<String>,
    input: Value,
}

#[derive(Default)]
pub(crate) struct UpdateState {
    answer: String,
    assistant_boundary: bool,
    assistant_message_id: Option<String>,
    tools: HashMap<String, ToolActivityState>,
}

impl UpdateState {
    pub(crate) fn answer(self) -> String {
        self.answer
    }

    pub(crate) fn apply(
        &mut self,
        update: &Value,
        events: &mut dyn FnMut(AcpStreamEvent) -> Result<()>,
    ) -> Result<()> {
        match update_kind(update) {
            Some("agent_message_chunk") => self.apply_message(update, events)?,
            Some("agent_thought_chunk") => {
                if let Some(text) = content_text(update) {
                    events(AcpStreamEvent::ReasoningDelta {
                        delta: text.to_owned(),
                        message_id: update_message_id(update),
                    })?;
                }
            }
            Some("tool_call" | "tool_call_update") => self.apply_tool(update, events)?,
            Some("permission_request") => {
                events(AcpStreamEvent::ProviderState(bounded_json(update)))?;
            }
            _ => {}
        }
        Ok(())
    }

    fn apply_message(
        &mut self,
        update: &Value,
        events: &mut dyn FnMut(AcpStreamEvent) -> Result<()>,
    ) -> Result<()> {
        let Some(text) = update_text(update) else {
            return Ok(());
        };
        let message_id = update_message_id(update);
        let message_changed = self
            .assistant_message_id
            .as_ref()
            .zip(message_id.as_ref())
            .is_some_and(|(previous, next)| previous != next);
        if (self.assistant_boundary || message_changed) && !self.answer.is_empty() {
            self.answer.push_str("\n\n");
            events(AcpStreamEvent::AssistantDelta {
                delta: "\n\n".into(),
                message_id: message_id.clone(),
            })?;
        }
        self.assistant_boundary = false;
        if message_id.is_some() {
            self.assistant_message_id = message_id.clone();
        }
        self.answer.push_str(text);
        events(AcpStreamEvent::AssistantDelta {
            delta: text.to_owned(),
            message_id,
        })
    }

    fn apply_tool(
        &mut self,
        update: &Value,
        events: &mut dyn FnMut(AcpStreamEvent) -> Result<()>,
    ) -> Result<()> {
        self.assistant_boundary = true;
        let Some(call_id) = update
            .get("toolCallId")
            .and_then(Value::as_str)
            .filter(|call_id| !call_id.trim().is_empty())
            .map(str::to_owned)
        else {
            return events(AcpStreamEvent::ProviderState(bounded_json(update)));
        };
        let state = self.tools.entry(call_id.clone()).or_default();
        let previous = state.clone();
        if let Some(title) = non_empty_string(update, "title") {
            state.title = title.to_owned();
        }
        if let Some(kind) = non_empty_string(update, "kind") {
            state.kind = Some(kind.to_owned());
        }
        if let Some(status) = non_empty_string(update, "status") {
            state.status = Some(status.to_owned());
        }
        if let Some(input) = update.get("rawInput") {
            merge_json(&mut state.input, &bounded_json(input));
        }
        if *state == previous {
            return Ok(());
        }
        events(AcpStreamEvent::ToolActivity {
            call_id,
            title: if state.title.is_empty() {
                "Tool call".into()
            } else {
                state.title.clone()
            },
            kind: state.kind.clone(),
            status: state.status.clone(),
            input: state.input.clone(),
        })
    }
}

pub(crate) fn update_text(update: &Value) -> Option<&str> {
    if update_kind(update) != Some("agent_message_chunk") {
        return None;
    }
    content_text(update)
}

fn update_kind(update: &Value) -> Option<&str> {
    update.get("sessionUpdate").and_then(Value::as_str)
}

fn update_message_id(update: &Value) -> Option<String> {
    update
        .get("messageId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
}

fn non_empty_string<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
}

fn merge_json(current: &mut Value, update: &Value) {
    match (current, update) {
        (Value::Object(current), Value::Object(update)) => {
            for (key, value) in update {
                if value.is_null() || value.as_str().is_some_and(str::is_empty) {
                    continue;
                }
                match current.get_mut(key) {
                    Some(current) => merge_json(current, value),
                    None => {
                        current.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        (current, update) if !update.is_null() => *current = update.clone(),
        _ => {}
    }
}

pub(crate) fn bounded_json(value: &Value) -> Value {
    let encoded = value.to_string();
    if encoded.len() <= MAX_METADATA_BYTES {
        value.clone()
    } else {
        let mut end = MAX_METADATA_BYTES;
        while !encoded.is_char_boundary(end) {
            end -= 1;
        }
        json!({ "truncated": true, "preview": &encoded[..end] })
    }
}

fn content_text(update: &Value) -> Option<&str> {
    update
        .get("content")
        .and_then(|content| {
            content
                .get("text")
                .and_then(Value::as_str)
                .or_else(|| content.as_str())
        })
        .or_else(|| update.get("text").and_then(Value::as_str))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unicode_metadata_is_truncated_on_a_character_boundary() {
        let value = json!({"body": "🙂".repeat(MAX_METADATA_BYTES)});
        let bounded = bounded_json(&value);
        assert_eq!(bounded["truncated"], true);
        assert!(bounded["preview"].as_str().unwrap().len() <= MAX_METADATA_BYTES);
    }

    #[test]
    fn raw_input_is_bounded_and_duplicate_updates_are_suppressed() {
        let update = json!({
            "sessionUpdate":"tool_call", "toolCallId":"large-1",
            "title":"Large tool", "rawInput":{"body":"🙂".repeat(MAX_METADATA_BYTES)}
        });
        let mut state = UpdateState::default();
        let mut events = Vec::new();
        state
            .apply(&update, &mut |event| {
                events.push(event);
                Ok(())
            })
            .unwrap();
        state
            .apply(&update, &mut |event| {
                events.push(event);
                Ok(())
            })
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            AcpStreamEvent::ToolActivity { input, .. } if input["truncated"] == true
        ));
    }
}
