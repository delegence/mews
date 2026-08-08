use anyhow::Result;
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
        messages: provider_messages(model_messages(store.messages(&session.id)?)),
        tools: Vec::new(),
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

    fn history(&self) -> Result<Vec<ModelMessage>> {
        Ok(provider_messages(model_messages(
            self.store.messages(&self.session.id)?,
        )))
    }

    fn append(&self, message: ModelMessage) -> Result<()> {
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
        };
        self.store
            .append_message(&self.session.id, role, content, Value::Null, source)?;
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
