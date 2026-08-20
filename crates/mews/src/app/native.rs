use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::STANDARD_NO_PAD};
use mews_agent::{
    AgentCapabilities, AgentSignal, CancellationToken, MessageContent as ModelContent,
    MessageRole as ModelRole, ModelMessage, ModelRequest, Provider,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};

use crate::{
    AgentConfig, MessageContent, MessageRole, MessageSource, Session, SourceKind, TurnStatus,
};
use mews_store::Store;

const PREPARED_REQUEST_CHUNK_BYTES: usize = 384 * 1024;

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

pub struct StartedTurn {
    pub id: crate::TurnId,
    pub event_notify: Option<Arc<tokio::sync::Notify>>,
    pub harness: mews_protocol::HarnessDescriptor,
    pub cancellation: CancellationToken,
}

pub async fn turn_started(
    store: &Store,
    provider: &dyn Provider,
    environment: &dyn AgentCapabilities,
    session: &Session,
    revision: &crate::AgentRevision,
    started: StartedTurn,
) -> Result<String> {
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
        turn: Mutex::new(TurnState {
            id: started.id,
            finished: false,
        }),
        event_notify: started.event_notify,
        harness: started.harness,
    };
    if config.harness != mews_runtime::MEWS_HARNESS {
        anyhow::bail!("native runtime cannot execute Harness {}", config.harness);
    }
    mews_runtime::Harness::execute_turn(
        &mews_runtime::MewsHarness,
        mews_runtime::HarnessTurn {
            provider,
            environment,
            store: &scoped,
            agent_id: session.agent_id.clone(),
            agent_slug,
            agent: &config,
            model_override: session.model_override.clone(),
            default_model: defaults.model,
            default_reasoning: defaults.reasoning,
            cwd: session.working_directory.clone(),
            soul: revision.soul.clone(),
            cancellation: started.cancellation,
        },
    )
    .await
}

struct TurnState {
    id: crate::TurnId,
    finished: bool,
}

struct SessionStore<'a> {
    store: &'a Store,
    session: &'a Session,
    turn: Mutex<TurnState>,
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

fn prepared_request_observations(
    request: &ModelRequest,
    turn_id: &crate::TurnId,
) -> Result<Vec<mews_store::HarnessObservationInput>> {
    let data = serde_json::to_value(request)?;
    let inline = mews_protocol::SessionEntryPayload::HarnessObservation {
        turn_id: turn_id.clone(),
        harness_session_id: None,
        kind: "model_request_prepared".into(),
        data: data.clone(),
    };
    let request_id = mews_protocol::OperationId::new().to_string();
    if serde_json::to_vec(&inline)?.len() <= mews_protocol::MAX_SESSION_ITEM_BYTES {
        return Ok(vec![mews_store::HarnessObservationInput {
            harness_session_id: None,
            kind: "model_request_prepared".into(),
            data,
            idempotency_key: Some(format!("model_request_prepared:{request_id}")),
        }]);
    }

    let encoded = serde_json::to_vec(request)?;
    let chunks = encoded.len().div_ceil(PREPARED_REQUEST_CHUNK_BYTES);
    let checksum = format!("{:x}", Sha256::digest(&encoded));
    let mut observations = Vec::with_capacity(chunks + 1);
    observations.push(mews_store::HarnessObservationInput {
        harness_session_id: None,
        kind: "model_request_prepared".into(),
        data: serde_json::json!({
            "format": "chunked_json_v1",
            "request_id": request_id,
            "sha256": checksum,
            "bytes": encoded.len(),
            "chunks": chunks,
            "model": request.model,
        }),
        idempotency_key: Some(format!("model_request_prepared:{request_id}")),
    });
    for (index, chunk) in encoded.chunks(PREPARED_REQUEST_CHUNK_BYTES).enumerate() {
        observations.push(mews_store::HarnessObservationInput {
            harness_session_id: None,
            kind: "model_request_prepared_chunk".into(),
            data: serde_json::json!({
                "request_id": request_id,
                "index": index,
                "data": STANDARD_NO_PAD.encode(chunk),
            }),
            idempotency_key: Some(format!("model_request_prepared:{request_id}:{index}")),
        });
    }
    Ok(observations)
}

impl Drop for SessionStore<'_> {
    fn drop(&mut self) {
        let state = self.turn.lock().expect("Turn state poisoned");
        if !state.finished {
            let _ = self.store.finish_turn(
                &state.id,
                TurnStatus::Cancelled,
                Some("Turn task was cancelled before completion"),
            );
        }
    }
}

impl mews_runtime::ConversationStore for SessionStore<'_> {
    fn begin_turn(&self) -> Result<()> {
        let state = self.turn.lock().expect("Turn state poisoned");
        self.store.record_turn_harness(
            &state.id,
            &self.harness.name,
            &self.harness.definition_hash,
            self.harness.executable_version.as_deref(),
        )?;
        Ok(())
    }

    fn finish_turn(&self, termination: mews_runtime::TurnTermination) -> Result<()> {
        let mut state = self.turn.lock().expect("Turn state poisoned");
        let turn_id = state.id.clone();
        match termination {
            mews_runtime::TurnTermination::Completed => {
                self.store
                    .finish_turn(&turn_id, TurnStatus::Completed, None)?;
            }
            mews_runtime::TurnTermination::Cancelled => {
                self.store
                    .finish_turn(&turn_id, TurnStatus::Cancelled, None)?;
            }
            mews_runtime::TurnTermination::Failed { error } => {
                self.store
                    .finish_turn(&turn_id, TurnStatus::Failed, Some(&error))?;
            }
        }
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
        if provider != "openai" {
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

    fn model_request_prepared(&self, request: &ModelRequest) -> Result<()> {
        let turn_id = self.active_turn_id()?;
        self.store.append_harness_observations(
            &self.session.id,
            &turn_id,
            prepared_request_observations(request, &turn_id)?,
        )?;
        self.notify_event();
        Ok(())
    }

    fn append_response(&self, response: mews_protocol::AssistantResponse) -> Result<()> {
        let turn_id = self.active_turn_id()?;
        self.store
            .append_assistant_response(&self.session.id, &turn_id, response)?;
        self.notify_event();
        Ok(())
    }

    fn append_response_with_tool_calls(
        &self,
        response: mews_protocol::AssistantResponse,
        calls: Vec<mews_agent::ToolCall>,
    ) -> Result<()> {
        let turn_id = self.active_turn_id()?;
        let calls = calls
            .into_iter()
            .map(|call| mews_protocol::ToolCall {
                call_id: call.id,
                tool: call.name,
                arguments: call.arguments,
                thought_signature: call.thought_signature,
            })
            .collect();
        self.store.append_assistant_response_with_tool_calls(
            &self.session.id,
            &turn_id,
            response,
            calls,
        )?;
        self.notify_event();
        Ok(())
    }

    fn tool_requested(&self, call: mews_agent::ToolCall) -> Result<()> {
        let turn_id = self.active_turn_id()?;
        self.store.append_tool_requested(
            &self.session.id,
            &turn_id,
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

    fn tool_execution_started(&self, call: mews_agent::ToolCall) -> Result<()> {
        let turn_id = self.active_turn_id()?;
        self.store.start_tool_effect(
            &self.session.id,
            &turn_id,
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

    fn tool_execution_completed(
        &self,
        call: mews_agent::ToolCall,
        result: mews_agent::ToolResult,
    ) -> Result<()> {
        let turn_id = self.active_turn_id()?;
        self.store.complete_tool_execution(
            &self.session.id,
            &turn_id,
            mews_protocol::ToolResult {
                call_id: call.id,
                tool: call.name,
                result: result.value,
                is_error: result.is_error,
                uncertain: result.uncertain,
            },
        )?;
        self.notify_event();
        Ok(())
    }

    fn tool_result_recorded(
        &self,
        call: mews_agent::ToolCall,
        result: mews_agent::ToolResult,
    ) -> Result<()> {
        let turn_id = self.active_turn_id()?;
        self.store.append_tool_result(
            &self.session.id,
            &turn_id,
            mews_protocol::ToolResult {
                call_id: call.id,
                tool: call.name,
                result: result.value,
                is_error: result.is_error,
                uncertain: result.uncertain,
            },
        )?;
        self.notify_event();
        Ok(())
    }

    fn append(&self, message: ModelMessage) -> Result<()> {
        let turn_id = self.active_turn_id()?;
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
                uncertain,
            } => MessageContent::ToolResult {
                call_id,
                tool,
                result,
                is_error,
                uncertain,
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
                    uncertain,
                },
            ) => {
                self.store.append_tool_result(
                    &self.session.id,
                    &turn_id,
                    mews_protocol::ToolResult {
                        call_id,
                        tool,
                        result,
                        is_error,
                        uncertain,
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
                self.store.append_tool_requested(
                    &self.session.id,
                    &turn_id,
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
                    &turn_id,
                    None,
                    "provider_state",
                    serde_json::json!({"provider": provider, "model": model, "data": data}),
                    None,
                )?;
            }
            (MessageRole::Assistant, MessageContent::Text { text }) => {
                self.store.append_assistant_response(
                    &self.session.id,
                    &turn_id,
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

    fn signal(&self, signal: AgentSignal) -> Result<()> {
        let payload = match signal {
            AgentSignal::AssistantStarted => return Ok(()),
            AgentSignal::AssistantTextDelta(delta) => {
                mews_protocol::RuntimeSignalPayload::AssistantDelta {
                    delta,
                    message_id: None,
                }
            }
            AgentSignal::ToolProgress(progress) => {
                mews_protocol::RuntimeSignalPayload::ToolActivity {
                    activity: mews_protocol::ToolActivity {
                        call_id: progress.call_id,
                        title: "Tool progress".into(),
                        kind: Some("progress".into()),
                        status: Some("in_progress".into()),
                        input: progress.value,
                    },
                }
            }
        };
        super::turns::emit_runtime_signal(
            self.store,
            &self.session.id,
            &self.active_turn_id()?,
            payload,
        )?;
        self.notify_event();
        Ok(())
    }

    fn start_effect(
        &self,
        effect: mews_protocol::EffectRequest,
    ) -> Result<mews_protocol::OperationId> {
        let operation_id =
            self.store
                .start_effect(&self.session.id, &self.active_turn_id()?, effect)?;
        self.notify_event();
        Ok(operation_id)
    }

    fn finish_effect(
        &self,
        operation_id: &mews_protocol::OperationId,
        outcome: mews_runtime::EffectTermination,
    ) -> Result<()> {
        let outcome = match outcome {
            mews_runtime::EffectTermination::Succeeded(result) => {
                mews_store::EffectOutcome::Succeeded(result)
            }
            mews_runtime::EffectTermination::Failed(error) => {
                mews_store::EffectOutcome::Failed(error)
            }
            mews_runtime::EffectTermination::Uncertain(reason) => {
                mews_store::EffectOutcome::Uncertain(reason)
            }
        };
        self.store.finish_effect(
            &self.session.id,
            &self.active_turn_id()?,
            operation_id,
            outcome,
        )?;
        self.notify_event();
        Ok(())
    }
}

impl SessionStore<'_> {
    fn active_turn_id(&self) -> Result<crate::TurnId> {
        Ok(self.turn.lock().expect("Turn state poisoned").id.clone())
    }
}

fn continuation_anchor(entries: &[mews_protocol::SessionEntry], model: &str) -> Option<usize> {
    let (provider, provider_model) = model.split_once('/')?;
    if provider != "openai" {
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
                            uncertain,
                        } => ModelContent::ToolResult {
                            call_id,
                            tool,
                            result,
                            is_error,
                            uncertain,
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
                        uncertain: result.uncertain,
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
            mews_protocol::SessionEntryPayload::TurnStarted { .. }
            | mews_protocol::SessionEntryPayload::ToolStarted { .. }
            | mews_protocol::SessionEntryPayload::Reasoning { .. }
            | mews_protocol::SessionEntryPayload::TurnCompleted { .. }
            | mews_protocol::SessionEntryPayload::TurnFailed { .. }
            | mews_protocol::SessionEntryPayload::TurnCancelled { .. }
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
                    uncertain,
                } => ModelContent::ToolResult {
                    call_id,
                    tool,
                    result,
                    is_error,
                    uncertain,
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

    fn native_harness_descriptor() -> mews_protocol::HarnessDescriptor {
        mews_protocol::HarnessDescriptor {
            name: "mews".into(),
            protocol: mews_protocol::HarnessProtocol::Mews,
            definition_hash: "test".into(),
            availability: mews_protocol::HarnessAvailability {
                runtime: mews_protocol::HarnessReadiness::Ready,
                adapter: mews_protocol::HarnessReadiness::NotApplicable,
                authentication: mews_protocol::HarnessReadiness::NotApplicable,
                catalog: mews_protocol::HarnessReadiness::Ready,
                detail: None,
            },
            executable_version: None,
            native_tools: Vec::new(),
            modes: Vec::new(),
            supports_http_mcp: false,
            supports_continuation: false,
            models: Vec::new(),
            config_options: Vec::new(),
            probed_at: None,
        }
    }

    #[test]
    fn canonical_acp_prompt_keeps_linear_history_in_order() {
        let mut store = Store::open_in_memory().unwrap();
        let installation = store
            .initialize(
                &mews_store::CommandContext::system(),
                "laptop",
                "key",
                "noise-key",
                "installation-key",
            )
            .unwrap();
        let (agent, _) = store
            .create_agent(
                &mews_store::CommandContext::system(),
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
        let (turn, _, _) = store
            .accept_turn_idempotent(
                &session.id,
                "canonical-prompt",
                MessageContent::Text {
                    text: "hello".into(),
                },
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
                &turn.id,
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

    #[test]
    fn injected_user_message_is_durable_without_admitting_harness_turns() {
        let mut store = Store::open_in_memory().unwrap();
        let installation = store
            .initialize(
                &mews_store::CommandContext::system(),
                "laptop",
                "key",
                "noise-key",
                "installation-key",
            )
            .unwrap();
        let (agent, _) = store
            .create_agent(
                &mews_store::CommandContext::system(),
                "injected-message",
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
        let harness_source = MessageSource {
            kind: SourceKind::Harness,
            id: "default".into(),
            channel_origin: None,
        };
        let content = MessageContent::Text {
            text: "initial".into(),
        };
        assert!(
            store
                .accept_turn_idempotent(
                    &session.id,
                    "harness-turn",
                    content.clone(),
                    content.clone(),
                    Value::Null,
                    harness_source,
                )
                .is_err()
        );
        let (turn, _, _) = store
            .accept_turn_idempotent(
                &session.id,
                "client-turn",
                content.clone(),
                content,
                Value::Null,
                MessageSource {
                    kind: SourceKind::Client,
                    id: "cli".into(),
                    channel_origin: None,
                },
            )
            .unwrap();
        let scoped = SessionStore {
            store: &store,
            session: &session,
            turn: Mutex::new(TurnState {
                id: turn.id,
                finished: true,
            }),
            event_notify: None,
            harness: native_harness_descriptor(),
        };

        mews_runtime::ConversationStore::append(
            &scoped,
            ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text {
                    text: "follow up".into(),
                },
            },
        )
        .unwrap();

        let injected = store
            .session_entries(&session.id)
            .unwrap()
            .into_iter()
            .find(|entry| {
                matches!(
                    &entry.payload,
                    mews_protocol::SessionEntryPayload::UserMessage {
                        content: MessageContent::Text { text },
                        source: MessageSource { kind: SourceKind::Harness, .. },
                        ..
                    } if text == "follow up"
                )
            });
        assert!(injected.is_some());
    }

    #[test]
    fn page_safe_model_request_keeps_the_direct_observation_format() {
        let request = ModelRequest {
            model: "openai/gpt".into(),
            reasoning: None,
            system: "system".into(),
            messages: Vec::new(),
            tools: Vec::new(),
            continuation: None,
        };

        let observations = prepared_request_observations(&request, &crate::TurnId::new()).unwrap();

        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].kind, "model_request_prepared");
        assert_eq!(observations[0].data, serde_json::to_value(request).unwrap());
    }

    #[test]
    fn oversized_google_request_is_recorded_in_page_safe_chunks() {
        let mut store = Store::open_in_memory().unwrap();
        let installation = store
            .initialize(
                &mews_store::CommandContext::system(),
                "laptop",
                "key",
                "noise-key",
                "installation-key",
            )
            .unwrap();
        let (agent, _) = store
            .create_agent(
                &mews_store::CommandContext::system(),
                "large-request",
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
        let (turn, _, _) = store
            .accept_turn_idempotent(
                &session.id,
                "large-request",
                MessageContent::Text {
                    text: "hello".into(),
                },
                MessageContent::Text {
                    text: "hello".into(),
                },
                Value::Null,
                MessageSource {
                    kind: SourceKind::Client,
                    id: "cli".into(),
                    channel_origin: None,
                },
            )
            .unwrap();
        let mut request = ModelRequest {
            model: "google/gemini".into(),
            reasoning: None,
            system: "x".repeat(800 * 1024),
            messages: Vec::new(),
            tools: Vec::new(),
            continuation: None,
        };
        mews_agent::apply_context_budget(&mut request).unwrap();

        store
            .append_harness_observations(
                &session.id,
                &turn.id,
                prepared_request_observations(&request, &turn.id).unwrap(),
            )
            .unwrap();

        let entries = store.session_entries(&session.id).unwrap();
        let observations = entries
            .iter()
            .filter_map(|entry| match &entry.payload {
                mews_protocol::SessionEntryPayload::HarnessObservation { kind, data, .. } => {
                    assert!(
                        serde_json::to_vec(&entry.payload).unwrap().len()
                            <= mews_protocol::MAX_SESSION_ITEM_BYTES
                    );
                    Some((kind.as_str(), data))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        let manifest = observations
            .iter()
            .find_map(|(kind, data)| (*kind == "model_request_prepared").then_some(*data))
            .unwrap();
        assert_eq!(manifest["format"], "chunked_json_v1");
        let mut chunks = observations
            .iter()
            .filter(|(kind, _)| *kind == "model_request_prepared_chunk")
            .map(|(_, data)| {
                (
                    data["index"].as_u64().unwrap(),
                    data["data"].as_str().unwrap(),
                )
            })
            .collect::<Vec<_>>();
        chunks.sort_by_key(|(index, _)| *index);
        assert_eq!(manifest["chunks"], chunks.len());
        let encoded = chunks
            .into_iter()
            .flat_map(|(_, data)| STANDARD_NO_PAD.decode(data).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(manifest["bytes"], encoded.len());
        assert_eq!(
            manifest["sha256"],
            format!("{:x}", Sha256::digest(&encoded))
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&encoded).unwrap(),
            serde_json::to_value(request).unwrap()
        );
    }
}
