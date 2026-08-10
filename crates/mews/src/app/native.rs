use anyhow::{Context, Result};
use mews_agent::{
    AgentCapabilities, CancellationToken, MessageContent as ModelContent, MessageRole as ModelRole,
    ModelMessage, Provider,
};
use serde_json::Value;
use std::sync::{Arc, Mutex};

use crate::{
    AgentConfig, MessageContent, MessageRole, MessageSource, RunStatus, Session, SourceKind,
};
use mews_store::Store;

pub(crate) fn canonical_acp_prompt(store: &Store, session: &Session, soul: &str) -> Result<String> {
    let mut request = mews_agent::ModelRequest {
        model: "acp/default".into(),
        reasoning: None,
        system: soul.to_owned(),
        messages: provider_messages(model_messages(store.active_messages(&session.id)?)),
        tools: Vec::new(),
        continuation: None,
    };
    mews_agent::apply_context_budget(&mut request)?;
    Ok(mews_runtime::canonical_prompt(request.messages, soul))
}

pub async fn run(
    store: &Store,
    provider: &dyn Provider,
    environment: &dyn AgentCapabilities,
    session: &Session,
    soul: String,
    harness: mews_protocol::HarnessDescriptor,
) -> Result<String> {
    run_inner(
        store,
        provider,
        environment,
        session,
        soul,
        RunExecution::new(harness),
    )
    .await
}

pub struct StartedRun {
    pub id: crate::RunId,
    pub event_notify: Arc<tokio::sync::Notify>,
    pub harness: mews_protocol::HarnessDescriptor,
    pub cancellation: CancellationToken,
}

pub async fn run_started(
    store: &Store,
    provider: &dyn Provider,
    environment: &dyn AgentCapabilities,
    session: &Session,
    soul: String,
    started: StartedRun,
) -> Result<String> {
    let harness = started.harness.clone();
    run_inner(
        store,
        provider,
        environment,
        session,
        soul,
        RunExecution::started(started, harness),
    )
    .await
}

async fn run_inner(
    store: &Store,
    provider: &dyn Provider,
    environment: &dyn AgentCapabilities,
    session: &Session,
    soul: String,
    execution: RunExecution,
) -> Result<String> {
    let revision = store.agent_revision(&session.agent_id, session.agent_revision)?;
    let agent_slug = store
        .agents()?
        .into_iter()
        .find(|agent| agent.id == session.agent_id)
        .context("Session Agent no longer exists")?
        .slug;
    let config = AgentConfig::parse(&revision.config_toml)?;
    let defaults = store.provider_defaults()?;
    let scoped = SessionStore {
        store,
        session,
        run: Mutex::new(RunState {
            id: execution.started.as_ref().map(|started| started.id.clone()),
            finished: false,
        }),
        event_notify: execution
            .started
            .as_ref()
            .map(|started| Arc::clone(&started.event_notify)),
        harness: execution.harness,
    };
    if config.harness != mews_runtime::MEWS_HARNESS {
        anyhow::bail!("native runtime cannot execute Harness {}", config.harness);
    }
    mews_runtime::Harness::run(
        &mews_runtime::MewsHarness,
        mews_runtime::HarnessRun {
            provider,
            environment,
            store: &scoped,
            agent_slug,
            agent: &config,
            model_override: session.model_override.clone(),
            default_model: defaults.model,
            default_reasoning: defaults.reasoning,
            cwd: session.working_directory.clone(),
            soul,
            cancellation: execution
                .started
                .as_ref()
                .map_or_else(CancellationToken::new, |started| {
                    started.cancellation.clone()
                }),
        },
    )
    .await
}

struct RunExecution {
    started: Option<StartedRun>,
    harness: mews_protocol::HarnessDescriptor,
}

impl RunExecution {
    fn new(harness: mews_protocol::HarnessDescriptor) -> Self {
        Self {
            started: None,
            harness,
        }
    }

    fn started(started: StartedRun, harness: mews_protocol::HarnessDescriptor) -> Self {
        Self {
            started: Some(started),
            harness,
        }
    }
}

struct RunState {
    id: Option<crate::RunId>,
    finished: bool,
}

struct SessionStore<'a> {
    store: &'a Store,
    session: &'a Session,
    run: Mutex<RunState>,
    event_notify: Option<Arc<tokio::sync::Notify>>,
    harness: mews_protocol::HarnessDescriptor,
}

impl SessionStore<'_> {
    fn notify_event(&self) {
        if let Some(notify) = &self.event_notify {
            notify.notify_waiters();
        }
    }
}

impl Drop for SessionStore<'_> {
    fn drop(&mut self) {
        let state = self.run.lock().expect("Run state poisoned");
        if !state.finished
            && let Some(run_id) = &state.id
        {
            let _ = self.store.finish_run(
                run_id,
                RunStatus::Cancelled,
                Some("Run task was cancelled before completion"),
            );
        }
    }
}

impl mews_runtime::ConversationStore for SessionStore<'_> {
    fn begin_run(&self) -> Result<()> {
        let mut state = self.run.lock().expect("Run state poisoned");
        if state.id.is_none() {
            state.id = Some(self.store.start_run(&self.session.id)?.id);
        }
        let run_id = state.id.as_ref().expect("Run was just started");
        self.store.record_run_harness(
            run_id,
            &self.harness.name,
            &self.harness.definition_hash,
            self.harness.executable_version.as_deref(),
        )?;
        Ok(())
    }

    fn finish_run(&self, error: Option<&str>) -> Result<()> {
        let mut state = self.run.lock().expect("Run state poisoned");
        let run_id = state
            .id
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Run was not started"))?;
        self.store.finish_run(
            run_id,
            if error.is_some() {
                RunStatus::Failed
            } else {
                RunStatus::Completed
            },
            error,
        )?;
        state.finished = true;
        self.notify_event();
        Ok(())
    }

    fn history(&self, model: &str) -> Result<Vec<ModelMessage>> {
        let entries = self.store.active_entries(&self.session.id)?;
        if let Some(index) = continuation_anchor(&entries, model) {
            return Ok(provider_messages(model_messages(projected_messages(
                &entries[index + 1..],
            ))));
        }
        Ok(native_replay(&entries, model))
    }

    fn continuation(&self, model: &str) -> Result<Option<mews_agent::ResponseContinuation>> {
        let Some((provider, provider_model)) = model.split_once('/') else {
            return Ok(None);
        };
        if !matches!(provider, "openai" | "openai-codex") {
            return Ok(None);
        }
        let entries = self.store.active_entries(&self.session.id)?;
        Ok(entries
            .iter()
            .rev()
            .take_while(|entry| {
                !matches!(
                    entry.payload,
                    mews_protocol::SessionEntryPayload::ContextCompaction { .. }
                )
            })
            .find_map(|entry| match &entry.payload {
                mews_protocol::SessionEntryPayload::AssistantResponse { response, .. }
                    if response.provider == provider
                        && response.model == provider_model
                        && response.api == "responses" =>
                {
                    response.response_id.as_ref().map(|response_id| {
                        mews_agent::ResponseContinuation {
                            response_id: response_id.clone(),
                            provider: provider.into(),
                            model: provider_model.into(),
                            api: "responses".into(),
                            fallback_messages: native_replay(&entries, model),
                        }
                    })
                }
                mews_protocol::SessionEntryPayload::ContextCompaction { .. } => None,
                _ => None,
            }))
    }

    fn append_response(&self, response: mews_protocol::AssistantResponse) -> Result<()> {
        let run_id = self.active_run_id()?;
        self.store
            .append_assistant_response(&self.session.id, &run_id, response)?;
        self.notify_event();
        Ok(())
    }

    fn tool_started(&self, call: mews_agent::ToolCall) -> Result<()> {
        let run_id = self.active_run_id()?;
        self.store.append_tool_started(
            &self.session.id,
            &run_id,
            mews_protocol::ToolCall {
                call_id: call.id,
                tool: call.name,
                arguments: call.arguments,
                thought_signature: call.thought_signature,
            },
        )?;
        self.notify_event();
        Ok(())
    }

    fn append(&self, message: ModelMessage) -> Result<()> {
        let run_id = self.active_run_id()?;
        let role = match message.role {
            ModelRole::User => MessageRole::User,
            ModelRole::Assistant => MessageRole::Assistant,
            ModelRole::Tool => MessageRole::Tool,
        };
        let content = match message.content {
            ModelContent::Text { text } => MessageContent::Text { text },
            ModelContent::ToolCall {
                call_id,
                tool,
                arguments,
                thought_signature,
            } => MessageContent::ToolCall {
                call_id,
                tool,
                arguments,
                thought_signature,
            },
            ModelContent::ToolResult {
                call_id,
                tool,
                result,
                is_error,
            } => MessageContent::ToolResult {
                call_id,
                tool,
                result,
                is_error,
            },
            ModelContent::ProviderState {
                provider,
                model,
                data,
            } => MessageContent::ProviderState {
                provider,
                model,
                data,
            },
        };
        let source = MessageSource {
            kind: if role == MessageRole::Tool {
                SourceKind::Host
            } else {
                SourceKind::Harness
            },
            id: if role == MessageRole::Tool {
                self.session.host_id.to_string()
            } else {
                "default".into()
            },
            channel_origin: None,
        };
        match (role, content) {
            (MessageRole::User, content) => {
                self.store.append_message(
                    &self.session.id,
                    MessageRole::User,
                    content,
                    Value::Null,
                    source,
                )?;
            }
            (
                MessageRole::Tool,
                MessageContent::ToolResult {
                    call_id,
                    tool,
                    result,
                    is_error,
                },
            ) => {
                self.store.append_tool_result(
                    &self.session.id,
                    &run_id,
                    mews_protocol::ToolResult {
                        call_id,
                        tool,
                        result,
                        is_error,
                    },
                )?;
            }
            (
                MessageRole::Assistant,
                MessageContent::ToolCall {
                    call_id,
                    tool,
                    arguments,
                    ..
                },
            ) => {
                self.store.append_tool_started(
                    &self.session.id,
                    &run_id,
                    mews_protocol::ToolCall {
                        call_id,
                        tool,
                        arguments,
                        thought_signature: None,
                    },
                )?;
            }
            (
                _,
                MessageContent::ProviderState {
                    provider,
                    model,
                    data,
                },
            ) => {
                self.store.append_harness_observation(
                    &self.session.id,
                    &run_id,
                    None,
                    "provider_state",
                    serde_json::json!({"provider": provider, "model": model, "data": data}),
                    None,
                )?;
            }
            (MessageRole::Assistant, MessageContent::Text { text }) => {
                self.store.append_assistant_response(
                    &self.session.id,
                    &run_id,
                    mews_protocol::AssistantResponse {
                        provider: "mews".into(),
                        model: "injected".into(),
                        api: "runtime".into(),
                        response_id: None,
                        blocks: vec![mews_protocol::AssistantResponseBlock::Text { text }],
                        usage: None,
                        stop_reason: None,
                    },
                )?;
            }
            _ => anyhow::bail!("unsupported normalized runtime message"),
        }
        self.notify_event();
        Ok(())
    }

    fn assistant_delta(&self, delta: String) -> Result<()> {
        let run_id = self
            .run
            .lock()
            .expect("Run state poisoned")
            .id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Run was not started"))?;
        self.store.append_client_event(
            &self.session.id,
            crate::ClientEventKind::AssistantDelta {
                run_id,
                delta,
                message_id: None,
            },
        )?;
        self.notify_event();
        Ok(())
    }
}

impl SessionStore<'_> {
    fn active_run_id(&self) -> Result<crate::RunId> {
        self.run
            .lock()
            .expect("Run state poisoned")
            .id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Run was not started"))
    }
}

fn continuation_anchor(entries: &[mews_protocol::SessionEntry], model: &str) -> Option<usize> {
    let (provider, provider_model) = model.split_once('/')?;
    if !matches!(provider, "openai" | "openai-codex") {
        return None;
    }
    entries
        .iter()
        .enumerate()
        .rev()
        .take_while(|(_, entry)| {
            !matches!(
                entry.payload,
                mews_protocol::SessionEntryPayload::ContextCompaction { .. }
            )
        })
        .find_map(|(index, entry)| match &entry.payload {
            mews_protocol::SessionEntryPayload::AssistantResponse { response, .. }
                if response.provider == provider
                    && response.model == provider_model
                    && response.api == "responses"
                    && response.response_id.is_some() =>
            {
                Some(index)
            }
            _ => None,
        })
}

fn projected_messages(entries: &[mews_protocol::SessionEntry]) -> Vec<crate::Message> {
    mews_protocol::portable_history(entries)
        .into_iter()
        .enumerate()
        .map(|(index, item)| crate::Message {
            id: crate::MessageId::new(),
            session_id: entries
                .first()
                .map(|entry| entry.session_id.clone())
                .unwrap_or_default(),
            sequence: u64::try_from(index + 1).unwrap_or(u64::MAX),
            role: item.role,
            content: item.content,
            metadata: Value::Null,
            source: MessageSource {
                kind: SourceKind::Client,
                id: "cli".into(),
                channel_origin: None,
            },
            created_at: chrono::Utc::now(),
        })
        .collect()
}

fn native_replay(entries: &[mews_protocol::SessionEntry], target: &str) -> Vec<ModelMessage> {
    let (target_provider, target_model) = target.split_once('/').unwrap_or(("", target));
    let mut messages = Vec::new();
    for entry in entries {
        match &entry.payload {
            mews_protocol::SessionEntryPayload::UserMessage { content, .. } => {
                messages.push(ModelMessage {
                    role: ModelRole::User,
                    content: match content.clone() {
                        MessageContent::Text { text } => ModelContent::Text { text },
                        MessageContent::ToolCall {
                            call_id,
                            tool,
                            arguments,
                            thought_signature,
                        } => ModelContent::ToolCall {
                            call_id,
                            tool,
                            arguments,
                            thought_signature,
                        },
                        MessageContent::ToolResult {
                            call_id,
                            tool,
                            result,
                            is_error,
                        } => ModelContent::ToolResult {
                            call_id,
                            tool,
                            result,
                            is_error,
                        },
                        MessageContent::ProviderState {
                            provider,
                            model,
                            data,
                        } => ModelContent::ProviderState {
                            provider,
                            model,
                            data,
                        },
                    },
                })
            }
            mews_protocol::SessionEntryPayload::AssistantResponse { response, .. } => {
                for block in &response.blocks {
                    let content = match block {
                        mews_protocol::AssistantResponseBlock::Text { text } => {
                            Some(ModelContent::Text { text: text.clone() })
                        }
                        mews_protocol::AssistantResponseBlock::ToolCall {
                            call_id,
                            tool,
                            arguments,
                            thought_signature,
                        } => Some(ModelContent::ToolCall {
                            call_id: call_id.clone(),
                            tool: tool.clone(),
                            arguments: arguments.clone(),
                            thought_signature: (response.provider == target_provider
                                && response.model == target_model)
                                .then(|| thought_signature.clone())
                                .flatten(),
                        }),
                        mews_protocol::AssistantResponseBlock::OpaqueState {
                            provider,
                            model,
                            data,
                        } if provider == target_provider && model == target_model => {
                            Some(ModelContent::ProviderState {
                                provider: provider.clone(),
                                model: model.clone(),
                                data: data.clone(),
                            })
                        }
                        mews_protocol::AssistantResponseBlock::Reasoning { text, signature }
                            if response.provider == target_provider
                                && response.model == target_model =>
                        {
                            let data = match target_provider {
                                "anthropic" => {
                                    serde_json::json!({"type":"thinking","thinking":text,"signature":signature})
                                }
                                "google" => {
                                    serde_json::json!({"thought":true,"text":text,"thoughtSignature":signature})
                                }
                                _ => continue,
                            };
                            Some(ModelContent::ProviderState {
                                provider: target_provider.into(),
                                model: target_model.into(),
                                data,
                            })
                        }
                        _ => None,
                    };
                    if let Some(content) = content {
                        messages.push(ModelMessage {
                            role: ModelRole::Assistant,
                            content,
                        });
                    }
                }
            }
            mews_protocol::SessionEntryPayload::ToolResult { result, .. } => {
                messages.push(ModelMessage {
                    role: ModelRole::Tool,
                    content: ModelContent::ToolResult {
                        call_id: result.call_id.clone(),
                        tool: result.tool.clone(),
                        result: result.result.clone(),
                        is_error: result.is_error,
                    },
                });
            }
            mews_protocol::SessionEntryPayload::ContextCompaction { summary, .. } => {
                messages.push(ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text {
                        text: format!("[Earlier context summary]\n{summary}"),
                    },
                })
            }
            mews_protocol::SessionEntryPayload::RunStarted { .. }
            | mews_protocol::SessionEntryPayload::ToolStarted { .. }
            | mews_protocol::SessionEntryPayload::Reasoning { .. }
            | mews_protocol::SessionEntryPayload::RunCompleted { .. }
            | mews_protocol::SessionEntryPayload::RunFailed { .. }
            | mews_protocol::SessionEntryPayload::RunCancelled { .. }
            | mews_protocol::SessionEntryPayload::HarnessObservation { .. } => {}
        }
    }
    messages
}

fn model_messages(mut messages: Vec<crate::Message>) -> Vec<crate::Message> {
    for message in &mut messages {
        if message.role != MessageRole::User {
            continue;
        }
        let MessageContent::Text { text } = &mut message.content else {
            continue;
        };
        if message.metadata.is_null()
            && message.source.kind == SourceKind::Client
            && message.source.id == "cli"
        {
            continue;
        }
        let metadata = serde_json::to_string(&message.metadata).unwrap_or_else(|_| "null".into());
        *text = format!(
            "[Untrusted message context: source={:?}/{}, metadata={metadata}]\n{text}",
            message.source.kind, message.source.id
        );
    }
    messages
}

fn provider_messages(messages: Vec<crate::Message>) -> Vec<ModelMessage> {
    messages
        .into_iter()
        .map(|message| ModelMessage {
            role: match message.role {
                MessageRole::User => ModelRole::User,
                MessageRole::Assistant => ModelRole::Assistant,
                MessageRole::Tool => ModelRole::Tool,
            },
            content: match message.content {
                MessageContent::Text { text } => ModelContent::Text { text },
                MessageContent::ToolCall {
                    call_id,
                    tool,
                    arguments,
                    thought_signature,
                } => ModelContent::ToolCall {
                    call_id,
                    tool,
                    arguments,
                    thought_signature,
                },
                MessageContent::ToolResult {
                    call_id,
                    tool,
                    result,
                    is_error,
                } => ModelContent::ToolResult {
                    call_id,
                    tool,
                    result,
                    is_error,
                },
                MessageContent::ProviderState {
                    provider,
                    model,
                    data,
                } => ModelContent::ProviderState {
                    provider,
                    model,
                    data,
                },
            },
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_acp_prompt_keeps_linear_history_in_order() {
        let mut store = Store::open_in_memory().unwrap();
        let installation = store
            .initialize("laptop", "key", "noise-key", "installation-key")
            .unwrap();
        let (agent, _) = store
            .create_agent(
                "timeline-prompt",
                "Soul",
                "harness = \"mews\"\n",
                &installation.hub_host_id,
            )
            .unwrap();
        let session = store
            .create_session(
                &agent.id,
                &installation.hub_host_id,
                std::path::Path::new("/tmp"),
            )
            .unwrap();
        let source = MessageSource {
            kind: SourceKind::Client,
            id: "cli".into(),
            channel_origin: None,
        };
        store
            .append_message(
                &session.id,
                MessageRole::User,
                MessageContent::Text {
                    text: "hello".into(),
                },
                Value::Null,
                source,
            )
            .unwrap();
        store
            .append_assistant_response(
                &session.id,
                &crate::RunId::new(),
                mews_protocol::AssistantResponse {
                    provider: "test".into(),
                    model: "test".into(),
                    api: "test".into(),
                    response_id: None,
                    blocks: vec![mews_protocol::AssistantResponseBlock::Text { text: "hi".into() }],
                    usage: None,
                    stop_reason: None,
                },
            )
            .unwrap();

        let prompt = canonical_acp_prompt(&store, &session, "Soul").unwrap();
        let prompt: serde_json::Value = serde_json::from_str(&prompt).unwrap();
        assert_eq!(prompt["conversation"][0]["role"], "user");
        assert_eq!(prompt["conversation"][0]["content"]["text"], "hello");
        assert_eq!(prompt["conversation"][1]["role"], "assistant");
        assert_eq!(prompt["conversation"][1]["content"]["text"], "hi");
    }
}
