use mews_protocol::{ACP_CONTEXT_VERSION, AcpReplacementReason};
use serde_json::json;

use super::*;
use crate::{MessageRole, SourceKind};

const CONFIG: &str = "harness = \"mews\"\ntools = [\"read\", \"write\", \"edit\", \"bash\"]\n[harness_options]\nmodel = \"test\"\n";

fn initialized() -> (Store, Installation) {
    let mut store = Store::open_in_memory().unwrap();
    let installation = store
        .initialize(
            &CommandContext::system(),
            "laptop",
            "test-public-key",
            "test-noise-key",
            "installation-key",
        )
        .unwrap();
    (store, installation)
}

fn append_assistant(store: &Store, session_id: &SessionId, text: String) -> Message {
    let turn_id = store
        .connection
        .query_row(
            "SELECT id FROM turns WHERE session_id = ?1 AND completed_at IS NULL",
            [session_id.as_str()],
            |row| parse_id(row.get::<_, String>(0)?),
        )
        .optional()
        .unwrap()
        .unwrap_or_else(|| store.start_turn(session_id).unwrap().id);
    let entry = store
        .append_assistant_response(
            session_id,
            &turn_id,
            AssistantResponse {
                provider: "test".into(),
                model: "test".into(),
                api: "test".into(),
                response_id: None,
                blocks: vec![mews_protocol::AssistantResponseBlock::Text { text: text.clone() }],
                usage: None,
                stop_reason: None,
            },
        )
        .unwrap();
    Message {
        id: entry.id,
        session_id: session_id.clone(),
        sequence: entry.sequence,
        role: MessageRole::Assistant,
        content: MessageContent::Text { text },
        metadata: Value::Null,
        source: MessageSource {
            kind: SourceKind::Harness,
            id: "test".into(),
            channel_origin: None,
        },
        created_at: entry.created_at,
    }
}

#[test]
fn scoped_control_command_replays_its_result_and_rejects_changed_input() {
    let (mut store, installation) = initialized();
    let mut context = CommandContext::new(
        "client-request-1",
        mews_protocol::EventActor {
            kind: mews_protocol::EventActorKind::Client,
            id: Some("client-1".into()),
        },
    );
    context.correlation_id = Some("correlation-1".into());

    let first = store
        .create_agent(
            &context,
            "receipt",
            "Soul",
            CONFIG,
            &installation.hub_host_id,
        )
        .unwrap();
    let events = store.journal_entries_after(0, 1_000).unwrap();
    let event_count = events.len();
    let event = events.last().unwrap();
    assert_eq!(event.actor, context.actor);
    assert_eq!(event.correlation_id, context.correlation_id);
    let replayed = store
        .create_agent(
            &context,
            "receipt",
            "Soul",
            CONFIG,
            &installation.hub_host_id,
        )
        .unwrap();

    assert_eq!(replayed, first);
    assert_eq!(
        store.journal_entries_after(0, 1_000).unwrap().len(),
        event_count
    );
    assert!(matches!(
        store.create_agent(
            &context,
            "receipt",
            "Changed soul",
            CONFIG,
            &installation.hub_host_id
        ),
        Err(StoreError::CommandConflict { .. })
    ));
}

#[test]
fn concurrent_control_command_retries_return_one_committed_result() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("state.db");
    let mut setup = Store::open(&database).unwrap();
    let installation = setup
        .initialize(
            &CommandContext::system(),
            "laptop",
            "test-public-key",
            "test-noise-key",
            "installation-key",
        )
        .unwrap();
    drop(setup);

    let context = CommandContext::new("concurrent-create", mews_protocol::EventActor::system());
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let create = |mut store: Store| {
        let barrier = barrier.clone();
        let context = context.clone();
        let host_id = installation.hub_host_id.clone();
        std::thread::spawn(move || {
            barrier.wait();
            store
                .create_agent(&context, "one-result", "Soul", CONFIG, &host_id)
                .unwrap()
        })
    };
    let first = create(Store::open(&database).unwrap());
    let second = create(Store::open(&database).unwrap());

    assert_eq!(first.join().unwrap(), second.join().unwrap());
    let store = Store::open(&database).unwrap();
    assert_eq!(store.agents().unwrap().len(), 1);
    assert_eq!(
        store
            .journal_entries_after(0, 1_000)
            .unwrap()
            .iter()
            .filter(|entry| matches!(
                entry.payload,
                mews_protocol::JournalEvent::AgentCreated { .. }
            ))
            .count(),
        1
    );
}

#[test]
fn atomic_turn_acceptance_replays_the_original_and_rejects_changed_raw_input() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "atomic-turn",
            "Soul",
            CONFIG,
            &installation.hub_host_id,
        )
        .unwrap();
    let session = store
        .create_session(
            &agent.id,
            &installation.hub_host_id,
            &std::env::current_dir().unwrap(),
        )
        .unwrap();
    let raw = MessageContent::Text {
        text: "@agent hello".into(),
    };
    let resolved = MessageContent::Text {
        text: "hello".into(),
    };
    let source = MessageSource {
        kind: SourceKind::Client,
        id: "test".into(),
        channel_origin: None,
    };

    let (turn, message, created) = store
        .accept_turn_idempotent(
            &session.id,
            "request-42",
            raw.clone(),
            resolved,
            json!({"request": 42}),
            source.clone(),
        )
        .unwrap();
    assert!(created);
    assert_eq!(
        message.content,
        MessageContent::Text {
            text: "hello".into()
        }
    );
    let (replayed_turn, replayed_message) = store
        .replay_turn_idempotent(
            &session.id,
            "request-42",
            &raw,
            &json!({"request": 42}),
            &source,
        )
        .unwrap()
        .unwrap();
    assert_eq!(replayed_turn.id, turn.id);
    assert_eq!(replayed_message.id, message.id);
    assert!(matches!(
        store.replay_turn_idempotent(
            &session.id,
            "request-42",
            &MessageContent::Text {
                text: "changed".into()
            },
            &json!({"request": 42}),
            &source,
        ),
        Err(StoreError::CommandConflict { .. })
    ));
    store
        .record_turn_harness(&turn.id, "mews", "definition", Some("1"))
        .unwrap();
    store
        .append_assistant_response(
            &session.id,
            &turn.id,
            AssistantResponse {
                provider: "test".into(),
                model: "test".into(),
                api: "test".into(),
                response_id: None,
                blocks: vec![mews_protocol::AssistantResponseBlock::Text {
                    text: "working".into(),
                }],
                usage: None,
                stop_reason: None,
            },
        )
        .unwrap();
    let call = ToolCall {
        call_id: "call-1".into(),
        tool: "read".into(),
        arguments: json!({"path": "README.md"}),
        thought_signature: None,
    };
    store
        .append_tool_requested(&session.id, &turn.id, call.clone())
        .unwrap();
    store
        .start_tool_effect(&session.id, &turn.id, call)
        .unwrap();
    let result = ToolResult {
        call_id: "call-1".into(),
        tool: "read".into(),
        result: json!({"text": "ok"}),
        is_error: false,
        uncertain: false,
    };
    store
        .complete_tool_execution(&session.id, &turn.id, result.clone())
        .unwrap();
    store
        .append_tool_result(&session.id, &turn.id, result)
        .unwrap();
    store
        .append_acp_observation(
            &session.id,
            turn.id.clone(),
            Some("acp-1".into()),
            Some("observation-1".into()),
            AcpObservation::BindingChanged {
                transition: mews_protocol::AcpBindingTransition::New,
            },
        )
        .unwrap();
    store
        .finish_turn(&turn.id, TurnStatus::Completed, None)
        .unwrap();
    let entries_before = store.session_entries(&session.id).unwrap();
    let effect_before: (String, String) = store
        .connection
        .query_row(
            "SELECT operation_id, status FROM effects WHERE turn_id = ?1",
            [turn.id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();

    assert_eq!(store.turn(&turn.id).unwrap().status, TurnStatus::Completed);
    assert_eq!(store.session_entries(&session.id).unwrap(), entries_before);
    let effect_after: (String, String) = store
        .connection
        .query_row(
            "SELECT operation_id, status FROM effects WHERE turn_id = ?1",
            [turn.id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(effect_after, effect_before);
    let event_count = store.journal_entries_after(0, 1_000).unwrap().len();
    store
        .append_acp_observation(
            &session.id,
            turn.id,
            Some("acp-1".into()),
            Some("observation-1".into()),
            AcpObservation::BindingChanged {
                transition: mews_protocol::AcpBindingTransition::New,
            },
        )
        .unwrap();
    assert_eq!(
        store.journal_entries_after(0, 1_000).unwrap().len(),
        event_count
    );
}

#[test]
fn terminal_turn_closes_started_effect_before_the_terminal_fact() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "terminal-effect",
            "Soul",
            CONFIG,
            &installation.hub_host_id,
        )
        .unwrap();
    let session = store
        .create_session(
            &agent.id,
            &installation.hub_host_id,
            &std::env::current_dir().unwrap(),
        )
        .unwrap();
    let content = MessageContent::Text {
        text: "turn".into(),
    };
    let source = MessageSource {
        kind: SourceKind::Client,
        id: "test".into(),
        channel_origin: None,
    };
    let (turn, _, _) = store
        .accept_turn_idempotent(
            &session.id,
            "terminal-effect",
            content.clone(),
            content,
            Value::Null,
            source,
        )
        .unwrap();
    let call = ToolCall {
        call_id: "call-open".into(),
        tool: "bash".into(),
        arguments: json!({"command": "work"}),
        thought_signature: None,
    };
    store
        .append_tool_requested(&session.id, &turn.id, call.clone())
        .unwrap();
    store
        .start_tool_effect(&session.id, &turn.id, call)
        .unwrap();

    assert!(matches!(
        store.finish_turn(&turn.id, TurnStatus::Completed, None),
        Err(StoreError::InvalidData(_))
    ));
    assert_eq!(store.turn(&turn.id).unwrap().status, TurnStatus::Running);

    store
        .finish_turn(&turn.id, TurnStatus::Cancelled, None)
        .unwrap();

    let open: u64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM effects
             WHERE turn_id = ?1 AND status IN ('scheduled', 'started')",
            [turn.id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(open, 0);
    let types = store
        .journal_entries_after(0, 1_000)
        .unwrap()
        .into_iter()
        .filter(|entry| {
            entry.subject.kind == mews_protocol::JournalSubjectType::Session
                && entry.subject.id == session.id.as_str()
        })
        .map(|event| event.event_type)
        .collect::<Vec<_>>();
    let uncertain = types
        .iter()
        .position(|kind| *kind == mews_protocol::JournalEventType::EffectUncertain)
        .unwrap();
    let tool_completed = types
        .iter()
        .position(|kind| *kind == mews_protocol::JournalEventType::ToolResultRecorded)
        .unwrap();
    let cancelled = types
        .iter()
        .position(|kind| *kind == mews_protocol::JournalEventType::TurnCancelled)
        .unwrap();
    assert!(uncertain < tool_completed && tool_completed < cancelled);
    assert!(
        store
            .session_entries(&session.id)
            .unwrap()
            .iter()
            .any(|entry| matches!(
                &entry.payload,
                SessionEntryPayload::ToolResult { result, .. }
                    if result.call_id == "call-open" && result.is_error && result.uncertain
            ))
    );
}

#[test]
fn terminal_turn_fails_requested_tools_that_never_started() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "terminal-request",
            "Soul",
            CONFIG,
            &installation.hub_host_id,
        )
        .unwrap();
    let session = store
        .create_session(
            &agent.id,
            &installation.hub_host_id,
            &std::env::current_dir().unwrap(),
        )
        .unwrap();
    let content = MessageContent::Text {
        text: "turn".into(),
    };
    let (turn, _, _) = store
        .accept_turn_idempotent(
            &session.id,
            "terminal-request",
            content.clone(),
            content,
            Value::Null,
            MessageSource {
                kind: SourceKind::Client,
                id: "test".into(),
                channel_origin: None,
            },
        )
        .unwrap();
    store
        .append_tool_requested(
            &session.id,
            &turn.id,
            ToolCall {
                call_id: "not-started".into(),
                tool: "bash".into(),
                arguments: json!({"command": "work"}),
                thought_signature: None,
            },
        )
        .unwrap();

    store
        .finish_turn(&turn.id, TurnStatus::Cancelled, None)
        .unwrap();

    assert!(
        store
            .session_entries(&session.id)
            .unwrap()
            .iter()
            .any(|entry| matches!(
                &entry.payload,
                SessionEntryPayload::ToolResult { result, .. }
                    if result.call_id == "not-started" && result.is_error && !result.uncertain
            ))
    );
}

#[test]
fn rich_assistant_response_round_trips_as_one_ordered_entry() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "rich",
            "Soul",
            CONFIG,
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
    let response = AssistantResponse {
        provider: "anthropic".into(),
        model: "claude-test".into(),
        api: "messages".into(),
        response_id: Some("resp-1".into()),
        blocks: vec![
            mews_protocol::AssistantResponseBlock::Reasoning {
                text: "plan".into(),
                signature: Some("sig".into()),
            },
            mews_protocol::AssistantResponseBlock::Text {
                text: "working".into(),
            },
            mews_protocol::AssistantResponseBlock::ToolCall {
                call_id: "one".into(),
                tool: "read".into(),
                arguments: json!({"path":"a"}),
                thought_signature: Some("thought-1".into()),
            },
            mews_protocol::AssistantResponseBlock::OpaqueState {
                provider: "anthropic".into(),
                model: "claude-test".into(),
                data: json!({"type":"redacted_thinking","data":"secret"}),
            },
            mews_protocol::AssistantResponseBlock::ToolCall {
                call_id: "two".into(),
                tool: "write".into(),
                arguments: json!({"path":"b"}),
                thought_signature: Some("thought-2".into()),
            },
        ],
        usage: Some(mews_protocol::ModelUsage {
            input_tokens: 10,
            output_tokens: 20,
            cached_input_tokens: 3,
            reasoning_tokens: 4,
        }),
        stop_reason: Some("tool_use".into()),
    };
    let turn = store.start_turn(&session.id).unwrap();
    store
        .append_assistant_response(&session.id, &turn.id, response.clone())
        .unwrap();

    let entries = store.session_entries(&session.id).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].payload,
        SessionEntryPayload::AssistantResponse {
            turn_id: turn.id,
            response
        }
    );
}

#[test]
fn assistant_response_and_delivery_event_roll_back_together() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "atomic",
            "Soul",
            CONFIG,
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
    store
        .connection
        .execute_batch(
            "CREATE TEMP TRIGGER fail_assistant_event BEFORE INSERT ON client_events
             BEGIN SELECT RAISE(FAIL, 'forced event failure'); END;",
        )
        .unwrap();

    let turn = store.start_turn(&session.id).unwrap();
    let result = store.append_assistant_response(
        &session.id,
        &turn.id,
        AssistantResponse {
            provider: "test".into(),
            model: "test".into(),
            api: "test".into(),
            response_id: None,
            blocks: vec![mews_protocol::AssistantResponseBlock::Text {
                text: "answer".into(),
            }],
            usage: None,
            stop_reason: None,
        },
    );

    assert!(result.is_err());
    assert!(store.session_entries(&session.id).unwrap().is_empty());
    assert_eq!(store.session(&session.id).unwrap().leaf_entry_id, None);
    let events: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM client_events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(events, 0);
}

#[test]
fn large_client_events_are_paged_below_the_hub_frame_limit() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "event-page",
            "Soul",
            CONFIG,
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
    let consumer = ConsumerId::new();
    store
        .subscribe_session(&consumer, &session.id, ConsumerKind::Durable)
        .unwrap();
    for value in ['a', 'b'] {
        append_assistant(&store, &session.id, value.to_string().repeat(500 * 1024));
    }

    let first = store.client_events(&consumer, 100).unwrap();
    assert_eq!(first.events.len(), 1);
    mews_protocol::encode_hub_frame(&mews_protocol::Frame::with_request_id(
        mews_protocol::HubResponse::Events(first.clone()),
        mews_protocol::RequestId::new(),
    ))
    .unwrap();
    store
        .acknowledge_events(&consumer, first.checkpoint)
        .unwrap();
    let second = store.client_events(&consumer, 100).unwrap();
    assert_eq!(second.events.len(), 1);
    mews_protocol::encode_hub_frame(&mews_protocol::Frame::with_request_id(
        mews_protocol::HubResponse::Events(second.clone()),
        mews_protocol::RequestId::new(),
    ))
    .unwrap();
    assert!(second.checkpoint > first.checkpoint);
}

#[test]
fn individually_undeliverable_client_event_is_rejected() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "event-limit",
            "Soul",
            CONFIG,
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
    let result = store.append_client_event(
        &session.id,
        ClientEventKind::AssistantDelta {
            turn_id: TurnId::new(),
            delta: "x".repeat(mews_protocol::MAX_EVENT_PAGE_PAYLOAD_BYTES),
            message_id: None,
        },
    );
    assert!(matches!(result, Err(StoreError::InvalidData(_))));
    let events: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM client_events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(events, 0);
}

#[test]
fn undeliverable_turn_failure_rolls_back_turn_completion() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "turn-event-limit",
            "Soul",
            CONFIG,
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
    let turn = store.start_turn(&session.id).unwrap();
    let result = store.finish_turn(
        &turn.id,
        TurnStatus::Failed,
        Some(&"x".repeat(mews_protocol::MAX_EVENT_PAGE_PAYLOAD_BYTES)),
    );
    assert!(matches!(result, Err(StoreError::InvalidData(_))));
    assert_eq!(store.turn(&turn.id).unwrap().status, TurnStatus::Running);
}

#[test]
fn latest_compaction_changes_active_context_without_deleting_history() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "compact",
            "Soul",
            CONFIG,
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
    let old = store
        .append_message(
            &session.id,
            MessageRole::User,
            MessageContent::Text { text: "old".into() },
            Value::Null,
            source.clone(),
        )
        .unwrap();
    let kept = store
        .append_message(
            &session.id,
            MessageRole::User,
            MessageContent::Text {
                text: "kept".into(),
            },
            Value::Null,
            source,
        )
        .unwrap();
    store
        .append_context_compaction(&session.id, "summary".into(), kept.id.clone(), 100)
        .unwrap();

    assert_eq!(store.session_entries(&session.id).unwrap().len(), 3);
    let active = store.active_entries(&session.id).unwrap();
    assert_eq!(active.len(), 2);
    assert!(
        matches!(&active[0].payload, SessionEntryPayload::ContextCompaction { summary, first_kept_entry_id, .. } if summary == "summary" && first_kept_entry_id == &kept.id)
    );
    assert_eq!(active[1].id, kept.id);
    assert_ne!(active[1].id, old.id);
}

#[test]
fn turn_idempotency_keys_are_scoped_to_the_session() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "coder",
            "Soul",
            CONFIG,
            &installation.hub_host_id,
        )
        .unwrap();
    let first = store
        .create_session(
            &agent.id,
            &installation.hub_host_id,
            std::path::Path::new("/tmp"),
        )
        .unwrap();
    let second = store
        .create_session(
            &agent.id,
            &installation.hub_host_id,
            std::path::Path::new("/tmp"),
        )
        .unwrap();

    let (first_turn, _) = store
        .start_turn_idempotent(&first.id, "message-1", None)
        .unwrap();
    store
        .finish_turn(&first_turn.id, TurnStatus::Completed, None)
        .unwrap();
    let (second_turn, created) = store
        .start_turn_idempotent(&second.id, "message-1", None)
        .unwrap();

    assert!(created);
    assert_eq!(second_turn.session_id, second.id);
}

#[test]
fn turns_snapshot_the_current_agent_revision() {
    let (mut store, installation) = initialized();
    let (agent, first_revision) = store
        .create_agent(
            &CommandContext::system(),
            "evolving-agent",
            "First soul",
            CONFIG,
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
        id: "test".into(),
        channel_origin: None,
    };
    let (first_turn, _, _) = store
        .accept_turn_idempotent(
            &session.id,
            "first",
            MessageContent::Text { text: "one".into() },
            MessageContent::Text { text: "one".into() },
            Value::Null,
            source.clone(),
        )
        .unwrap();
    store
        .finish_turn(&first_turn.id, TurnStatus::Completed, None)
        .unwrap();
    let second_revision = store
        .update_agent(
            &CommandContext::system(),
            &agent.id,
            first_revision.revision,
            "Sharper soul",
            CONFIG,
            &installation.hub_host_id,
        )
        .unwrap();
    let (second_turn, _, _) = store
        .accept_turn_idempotent(
            &session.id,
            "second",
            MessageContent::Text { text: "two".into() },
            MessageContent::Text { text: "two".into() },
            Value::Null,
            source,
        )
        .unwrap();

    assert_eq!(first_turn.agent_revision, first_revision.revision);
    assert_eq!(
        store.turn(&first_turn.id).unwrap().agent_revision,
        first_revision.revision
    );
    assert_eq!(second_turn.agent_revision, second_revision.revision);
}

#[test]
fn revisions_require_the_exact_current_parent() {
    let (mut store, installation) = initialized();
    let (agent, first) = store
        .create_agent(
            &CommandContext::system(),
            "coder",
            "Be practical.",
            CONFIG,
            &installation.hub_host_id,
        )
        .unwrap();
    let second = store
        .update_agent(
            &CommandContext::system(),
            &agent.id,
            first.revision,
            "Be concise.",
            CONFIG,
            &installation.hub_host_id,
        )
        .unwrap();
    assert_eq!(second.revision, 2);
    assert!(matches!(
        store.update_agent(
            &CommandContext::system(),
            &agent.id,
            1,
            "Stale edit.",
            CONFIG,
            &installation.hub_host_id
        ),
        Err(StoreError::RevisionConflict {
            expected: 1,
            current: 2
        })
    ));
}

#[test]
fn agents_archive_and_hosts_revoke_without_destroying_history() {
    let (mut store, installation) = initialized();
    store
        .create_agent(
            &CommandContext::system(),
            "coder",
            "Soul",
            CONFIG,
            &installation.hub_host_id,
        )
        .unwrap();
    let renamed = store
        .rename_agent(&CommandContext::system(), "coder", "builder")
        .unwrap();
    assert_eq!(renamed.slug, "builder");
    store
        .archive_agent(&CommandContext::system(), "builder")
        .unwrap();
    assert!(store.agent_by_slug("builder").is_err());
    assert!(store.agents().unwrap().is_empty());

    let (invitation, secret) = store
        .create_invitation(
            &CommandContext::system(),
            Utc::now() + chrono::Duration::minutes(1),
        )
        .unwrap();
    let host = store
        .consume_invitation(
            &CommandContext::system(),
            &invitation,
            &secret,
            "mini",
            "mini-key",
            "mini-noise",
            "wss://relay.example",
        )
        .unwrap();
    store
        .revoke_host(&CommandContext::system(), &host.id)
        .unwrap();
    assert!(store.host(&host.id).is_err());
    assert!(
        store
            .revoke_host(&CommandContext::system(), &installation.hub_host_id)
            .is_err()
    );
}

#[test]
fn invitation_is_bearer_secret_and_exactly_once() {
    let (mut store, _) = initialized();
    let expiry = Utc::now() + chrono::Duration::minutes(15);
    let (id, secret) = store
        .create_invitation(&CommandContext::system(), expiry)
        .unwrap();
    assert!(
        store
            .consume_invitation(
                &CommandContext::system(),
                &id,
                "wrong",
                "mini-pc",
                "mini-key",
                "mini-noise",
                "ws://relay"
            )
            .is_err()
    );
    let host = store
        .consume_invitation(
            &CommandContext::system(),
            &id,
            &secret,
            "mini-pc",
            "mini-key",
            "mini-noise",
            "ws://relay",
        )
        .unwrap();
    assert_eq!(host.name, "mini-pc");
    assert!(
        store
            .consume_invitation(
                &CommandContext::system(),
                &id,
                &secret,
                "another",
                "another-key",
                "another-noise",
                "ws://relay"
            )
            .is_err()
    );
}

#[test]
fn session_captures_agent_host_and_directory_and_orders_messages() {
    let (mut store, installation) = initialized();
    let directory = tempfile::tempdir().unwrap();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "coder",
            "Be practical.",
            CONFIG,
            &installation.hub_host_id,
        )
        .unwrap();
    let session = store
        .create_session(&agent.id, &installation.hub_host_id, directory.path())
        .unwrap();
    assert!(session.leaf_entry_id.is_none());
    assert!(store.active_entries(&session.id).unwrap().is_empty());
    let source = MessageSource {
        kind: SourceKind::Client,
        id: "cli".into(),
        channel_origin: None,
    };
    let first = store
        .append_message(
            &session.id,
            MessageRole::User,
            MessageContent::Text {
                text: "hello".into(),
            },
            json!({"client.custom": {"answer": 42}}),
            source.clone(),
        )
        .unwrap();
    assert!(
        store.active_entries(&session.id).unwrap()[0]
            .parent_id
            .is_none()
    );
    assert_eq!(
        store.session(&session.id).unwrap().leaf_entry_id,
        Some(first.id.clone())
    );
    let second = append_assistant(&store, &session.id, "hi".into());
    let entries = store.active_entries(&session.id).unwrap();
    assert_eq!(entries[1].parent_id, Some(first.id.clone()));
    assert_eq!(
        store.session(&session.id).unwrap().leaf_entry_id,
        Some(second.id.clone())
    );
    let messages = store.messages(&session.id).unwrap();
    assert_eq!(
        messages
            .iter()
            .map(|message| message.sequence)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(session.working_directory, directory.path());
    assert_eq!(session.host_id, installation.hub_host_id);
    let switched = store
        .set_session_model(&session.id, Some("openai/test-model"))
        .unwrap();
    assert_eq!(
        switched.model_override.as_deref(),
        Some("openai/test-model")
    );
}

#[test]
fn active_history_follows_the_selected_leaf_while_all_entries_keep_siblings() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "timeline",
            "Soul",
            CONFIG,
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
        id: "test".into(),
        channel_origin: None,
    };
    let root = store
        .append_message(
            &session.id,
            MessageRole::User,
            MessageContent::Text {
                text: "root".into(),
            },
            Value::Null,
            source.clone(),
        )
        .unwrap();
    let first_child = append_assistant(&store, &session.id, "first child".into());
    store
        .set_session_leaf_checked(&session.id, Some(&first_child.id), Some(&root.id))
        .unwrap();
    let selected_child = append_assistant(&store, &session.id, "selected child".into());

    let all_entries = store.session_entries(&session.id).unwrap();
    assert_eq!(all_entries.len(), 3);
    assert_eq!(all_entries[1].parent_id, Some(root.id.clone()));
    assert_eq!(all_entries[2].parent_id, Some(root.id.clone()));
    assert_eq!(
        store.session(&session.id).unwrap().leaf_entry_id,
        Some(selected_child.id)
    );
    assert_eq!(
        store
            .messages(&session.id)
            .unwrap()
            .into_iter()
            .map(|message| match message.content {
                MessageContent::Text { text } => text,
                _ => unreachable!(),
            })
            .collect::<Vec<_>>(),
        ["root", "selected child"]
    );
}

#[test]
fn stale_leaf_append_does_not_change_timeline_or_emit_an_event() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "leaf-conflict",
            "Soul",
            CONFIG,
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
    let consumer = ConsumerId::new();
    store
        .subscribe_session(&consumer, &session.id, ConsumerKind::Durable)
        .unwrap();
    let first = store
        .append_message(
            &session.id,
            MessageRole::User,
            MessageContent::Text {
                text: "first".into(),
            },
            Value::Null,
            MessageSource {
                kind: SourceKind::Client,
                id: "test".into(),
                channel_origin: None,
            },
        )
        .unwrap();

    assert!(matches!(
        store.append_message_checked(
            &session.id,
            None,
            MessageRole::User,
            MessageContent::Text {
                text: "stale".into()
            },
            Value::Null,
            MessageSource {
                kind: SourceKind::Client,
                id: "test".into(),
                channel_origin: None,
            },
        ),
        Err(StoreError::LeafConflict { .. })
    ));
    assert_eq!(store.session_entries(&session.id).unwrap().len(), 1);
    assert_eq!(
        store.session(&session.id).unwrap().leaf_entry_id,
        Some(first.id)
    );
    assert!(
        store
            .client_events(&consumer, 10)
            .unwrap()
            .events
            .is_empty()
    );
}

#[test]
fn subscribed_consumers_replay_events_until_acknowledged() {
    let (mut store, installation) = initialized();
    let directory = tempfile::tempdir().unwrap();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "events",
            "Be practical.",
            CONFIG,
            &installation.hub_host_id,
        )
        .unwrap();
    let session = store
        .create_session(&agent.id, &installation.hub_host_id, directory.path())
        .unwrap();
    let consumer = ConsumerId::new();
    store
        .subscribe_session(&consumer, &session.id, ConsumerKind::Durable)
        .unwrap();
    let message = append_assistant(&store, &session.id, "hello".into());
    let first = store.client_events(&consumer, 100).unwrap();
    assert!(matches!(&first.events[0].kind,
            ClientEventKind::AssistantMessage { message: event, .. } if event.id == message.id));
    assert_eq!(store.client_events(&consumer, 100).unwrap().events.len(), 1);
    store
        .acknowledge_events(&consumer, first.checkpoint)
        .unwrap();
    let retained: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM client_events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(retained, 0);
    assert!(
        store
            .client_events(&consumer, 100)
            .unwrap()
            .events
            .is_empty()
    );
}

#[test]
fn client_event_rows_round_trip_immutable_channel_origins() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "event-origins",
            "Soul",
            CONFIG,
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
    let consumer = ConsumerId::new();
    store
        .subscribe_session(&consumer, &session.id, ConsumerKind::Durable)
        .unwrap();
    let origin = mews_protocol::ChannelOrigin {
        consumer_id: ConsumerId::new(),
        conversation: "account:thread".into(),
    };
    let (turn, _) = store
        .start_turn_idempotent(&session.id, "channel-turn", Some(&origin))
        .unwrap();
    store
        .append_client_event(
            &session.id,
            ClientEventKind::AssistantDelta {
                turn_id: turn.id.clone(),
                delta: "stream".into(),
                message_id: None,
            },
        )
        .unwrap();
    store
        .append_assistant_response(
            &session.id,
            &turn.id,
            AssistantResponse {
                provider: "test".into(),
                model: "test".into(),
                api: "test".into(),
                response_id: None,
                blocks: vec![mews_protocol::AssistantResponseBlock::Text {
                    text: "done".into(),
                }],
                usage: None,
                stop_reason: None,
            },
        )
        .unwrap();
    store
        .finish_turn(&turn.id, TurnStatus::Completed, None)
        .unwrap();
    let channel_events = store.client_events(&consumer, 100).unwrap();
    assert_eq!(channel_events.events.len(), 3);
    assert!(
        channel_events
            .events
            .iter()
            .all(|event| event.channel_origin.as_ref() == Some(&origin))
    );
    store
        .acknowledge_events(&consumer, channel_events.checkpoint)
        .unwrap();

    let turn = store.start_turn(&session.id).unwrap();
    store
        .append_client_event(
            &session.id,
            ClientEventKind::AssistantDelta {
                turn_id: turn.id,
                delta: "client stream".into(),
                message_id: None,
            },
        )
        .unwrap();
    let client_events = store.client_events(&consumer, 100).unwrap();
    assert_eq!(client_events.events.len(), 1);
    assert_eq!(client_events.events[0].channel_origin, None);
}

#[test]
fn durable_events_without_durable_subscribers_do_not_accumulate() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "unobserved-events",
            "Soul",
            CONFIG,
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

    append_assistant(&store, &session.id, "not subscribed".into());

    let retained: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM client_events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(retained, 0);
}

#[test]
fn ephemeral_subscriber_receives_durable_events_until_acknowledged() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "ephemeral-live",
            "Soul",
            CONFIG,
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
    let consumer = ConsumerId::new();
    store
        .subscribe_session(&consumer, &session.id, ConsumerKind::Ephemeral)
        .unwrap();

    append_assistant(&store, &session.id, "live".into());
    let batch = store.client_events(&consumer, 10).unwrap();
    assert_eq!(batch.events.len(), 1);
    store
        .acknowledge_events(&consumer, batch.checkpoint)
        .unwrap();

    let retained: u64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM client_events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(retained, 0);
}

#[test]
fn acp_deltas_are_delivery_only_and_deduplicate_replay() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "event-keys",
            "Soul",
            CONFIG,
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
    let consumer = ConsumerId::new();
    store
        .subscribe_session(&consumer, &session.id, ConsumerKind::Durable)
        .unwrap();
    let turn = store.start_turn(&session.id).unwrap();
    let append = |store: &Store, key: &str| {
        store.append_acp_observation_with_client_event(
            &session.id,
            turn.id.clone(),
            None,
            Some(key.into()),
            AcpObservation::AssistantDelta {
                delta: "ha".into(),
                message_id: None,
                raw: Value::Null,
            },
            ClientEventKind::AssistantDelta {
                turn_id: turn.id.clone(),
                delta: "ha".into(),
                message_id: None,
            },
        )
    };
    append(&store, "delta:1").unwrap();
    append(&store, "delta:2").unwrap();
    append(&store, "delta:1").unwrap();
    assert!(store.session_entries(&session.id).unwrap().is_empty());
    let events = store
        .client_events(&consumer, 10)
        .unwrap()
        .events
        .into_iter()
        .filter(|event| matches!(event.kind, ClientEventKind::AssistantDelta { .. }))
        .collect::<Vec<_>>();
    assert_eq!(events.len(), 2);
    assert!(events.iter().all(|event| matches!(event.kind, ClientEventKind::AssistantDelta { ref delta, .. } if delta == "ha")));
}

#[test]
fn concurrent_acp_delta_replays_commit_one_keyed_signal() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("state.db");
    let mut setup = Store::open(&database).unwrap();
    let installation = setup
        .initialize(
            &CommandContext::system(),
            "laptop",
            "test-public-key",
            "test-noise-key",
            "installation-key",
        )
        .unwrap();
    let (agent, _) = setup
        .create_agent(
            &CommandContext::system(),
            "concurrent-events",
            "Soul",
            CONFIG,
            &installation.hub_host_id,
        )
        .unwrap();
    let session = setup
        .create_session(
            &agent.id,
            &installation.hub_host_id,
            std::path::Path::new("/tmp"),
        )
        .unwrap();
    let turn = setup.start_turn(&session.id).unwrap();
    let consumer = ConsumerId::new();
    setup
        .subscribe_session(&consumer, &session.id, ConsumerKind::Durable)
        .unwrap();
    drop(setup);

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let append = |store: Store, delta: &'static str| {
        let barrier = barrier.clone();
        let session_id = session.id.clone();
        let turn_id = turn.id.clone();
        std::thread::spawn(move || {
            barrier.wait();
            store.append_acp_observation_with_client_event(
                &session_id,
                turn_id.clone(),
                None,
                Some("same-delta".into()),
                AcpObservation::AssistantDelta {
                    delta: delta.into(),
                    message_id: None,
                    raw: Value::Null,
                },
                ClientEventKind::AssistantDelta {
                    turn_id,
                    delta: delta.into(),
                    message_id: None,
                },
            )
        })
    };
    let first = append(Store::open(&database).unwrap(), "first");
    let second = append(Store::open(&database).unwrap(), "second");
    first.join().unwrap().unwrap();
    second.join().unwrap().unwrap();

    let store = Store::open(&database).unwrap();
    let keyed: u64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM client_events
             WHERE session_id = ?1 AND idempotency_key = 'same-delta'",
            [session.id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(keyed, 1);
    assert_eq!(store.client_events(&consumer, 10).unwrap().events.len(), 1);
}

#[test]
fn session_pages_are_byte_bounded_and_reconstruct_large_history() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "large-pages",
            "Soul",
            CONFIG,
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
    let text = "x".repeat(300 * 1024);
    for _ in 0..4 {
        store
            .append_message(
                &session.id,
                MessageRole::User,
                MessageContent::Text { text: text.clone() },
                Value::Null,
                MessageSource {
                    kind: SourceKind::Client,
                    id: "test".into(),
                    channel_origin: None,
                },
            )
            .unwrap();
    }
    let mut after = None;
    let mut restored = Vec::new();
    loop {
        let (messages, next) = store.active_messages_page(&session.id, after, 500).unwrap();
        let frame = mews_protocol::Frame::with_request_id(
            mews_protocol::HubResponse::SessionHistory(mews_protocol::SessionHistoryPage {
                messages: messages.clone(),
                next,
            }),
            mews_protocol::RequestId::new(),
        );
        assert!(mews_protocol::encode_hub_frame(&frame).is_ok());
        restored.extend(messages);
        let Some(next) = next else { break };
        after = Some(next);
    }
    assert_eq!(restored.len(), 4);
    assert!(restored.iter().all(
        |message| matches!(&message.content, MessageContent::Text { text: value } if value == &text)
    ));
}

#[test]
fn oversized_session_item_is_rejected_before_pagination() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "oversized-page",
            "Soul",
            CONFIG,
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
    let error = store
        .append_message(
            &session.id,
            MessageRole::User,
            MessageContent::Text {
                text: "x".repeat(mews_protocol::MAX_SESSION_ITEM_BYTES),
            },
            Value::Null,
            MessageSource {
                kind: SourceKind::Client,
                id: "test".into(),
                channel_origin: None,
            },
        )
        .unwrap_err();
    assert!(matches!(error, StoreError::InvalidData(_)));
}

#[test]
fn acp_observation_validates_its_complete_payload() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "outer-observation",
            "Soul",
            CONFIG,
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
    let turn = store.start_turn(&session.id).unwrap();

    let error = store
        .append_acp_observation(
            &session.id,
            turn.id,
            Some("x".repeat(mews_protocol::MAX_SESSION_ITEM_BYTES)),
            Some("outer:oversized".into()),
            AcpObservation::ProviderUpdate {
                data: json!({"state": "small"}),
            },
        )
        .unwrap_err();
    assert!(matches!(error, StoreError::InvalidData(_)));
    assert!(store.session_entries(&session.id).unwrap().is_empty());
}

#[test]
fn corrupt_oversized_entry_is_not_returned_as_a_page() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "corrupt-page",
            "Soul",
            CONFIG,
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
    let payload = json!({
        "type": "harness_observation",
        "turn_id": TurnId::new(),
        "harness_session_id": null,
        "kind": "provider_update",
        "data": {"raw": "x".repeat(mews_protocol::MAX_SESSION_PAGE_PAYLOAD_BYTES)}
    });
    store
        .connection
        .execute(
            "INSERT INTO session_entries (id, session_id, sequence, kind, contextual, payload_json, created_at)
         VALUES (?1, ?2, 1, 'harness_observation', 0, ?3, ?4)",
            params![
                MessageId::new().as_str(),
                session.id.as_str(),
                serde_json::to_string(&payload).unwrap(),
                timestamp(Utc::now()),
            ],
        )
        .unwrap();

    let error = store
        .session_entries_page(&session.id, None, 1)
        .unwrap_err();
    assert!(matches!(error, StoreError::InvalidData(_)), "{error:?}");
}

#[test]
fn entry_pages_keep_large_acp_observations_inside_hub_frames() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "large-entries",
            "Soul",
            CONFIG,
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
    let turn = store.start_turn(&session.id).unwrap();
    for ordinal in 0..4 {
        store
            .append_acp_observation(
                &session.id,
                turn.id.clone(),
                None,
                Some(format!("raw:{ordinal}")),
                AcpObservation::ProviderUpdate {
                    data: json!({"raw": "x".repeat(300 * 1024)}),
                },
            )
            .unwrap();
    }
    let mut after = None;
    let mut restored = Vec::new();
    loop {
        let (entries, next) = store.session_entries_page(&session.id, after, 500).unwrap();
        let frame = mews_protocol::Frame::with_request_id(
            mews_protocol::HubResponse::SessionEntries(mews_protocol::SessionEntriesPage {
                entries: entries.clone(),
                next,
            }),
            mews_protocol::RequestId::new(),
        );
        assert!(mews_protocol::encode_hub_frame(&frame).is_ok());
        restored.extend(entries);
        let Some(next) = next else { break };
        after = Some(next);
    }
    assert_eq!(restored.len(), 4);
}

#[test]
fn event_polling_reads_only_subscribed_sessions() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "scoped-events",
            "Soul",
            CONFIG,
            &installation.hub_host_id,
        )
        .unwrap();
    let first = store
        .create_session(
            &agent.id,
            &installation.hub_host_id,
            std::path::Path::new("/tmp/first"),
        )
        .unwrap();
    let second = store
        .create_session(
            &agent.id,
            &installation.hub_host_id,
            std::path::Path::new("/tmp/second"),
        )
        .unwrap();
    let consumer = ConsumerId::new();
    store
        .subscribe_session(&consumer, &first.id, ConsumerKind::Durable)
        .unwrap();
    for index in 0..125 {
        store
            .append_client_event(
                &second.id,
                ClientEventKind::AssistantDelta {
                    turn_id: TurnId::new(),
                    delta: index.to_string(),
                    message_id: None,
                },
            )
            .unwrap();
    }
    let message = append_assistant(&store, &first.id, "subscribed".into());

    let batch = store.client_events(&consumer, 100).unwrap();
    assert_eq!(batch.events.len(), 1);
    assert!(matches!(
        &batch.events[0].kind,
        ClientEventKind::AssistantMessage { message: event, .. } if event.id == message.id
    ));
}

#[test]
fn deleting_ephemeral_consumer_removes_it_and_its_subscriptions() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "ephemeral-events",
            "Soul",
            CONFIG,
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
    let consumer = ConsumerId::new();
    store
        .subscribe_session(&consumer, &session.id, ConsumerKind::Ephemeral)
        .unwrap();
    assert!(
        store
            .subscribe_session(&consumer, &session.id, ConsumerKind::Durable)
            .is_err()
    );
    store.delete_consumer(&consumer).unwrap();

    assert!(matches!(
        store.client_events(&consumer, 1),
        Err(StoreError::NotFound { .. })
    ));
    let subscriptions: u64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM client_subscriptions WHERE consumer_id = ?1",
            [consumer.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(subscriptions, 0);
}

#[test]
fn stale_ephemeral_consumers_expire_without_removing_durable_consumers() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "leased-events",
            "Soul",
            CONFIG,
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
    let durable = ConsumerId::new();
    let ephemeral = ConsumerId::new();
    store
        .subscribe_session(&durable, &session.id, ConsumerKind::Durable)
        .unwrap();
    store
        .subscribe_session(&ephemeral, &session.id, ConsumerKind::Ephemeral)
        .unwrap();
    store
        .connection
        .execute(
            "UPDATE client_consumers SET last_seen_at = ?2 WHERE id = ?1",
            params![
                ephemeral.as_str(),
                timestamp(Utc::now() - chrono::Duration::minutes(3))
            ],
        )
        .unwrap();

    store.prune_client_events().unwrap();

    assert!(matches!(
        store.client_events(&ephemeral, 1),
        Err(StoreError::NotFound { .. })
    ));
    assert!(store.client_events(&durable, 1).is_ok());
}

#[test]
fn ephemeral_consumers_do_not_hold_transient_events_past_durable_cursors() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "transient-events",
            "Soul",
            CONFIG,
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
    let durable = ConsumerId::new();
    let ephemeral = ConsumerId::new();
    store
        .subscribe_session(&durable, &session.id, ConsumerKind::Durable)
        .unwrap();
    store
        .subscribe_session(&ephemeral, &session.id, ConsumerKind::Ephemeral)
        .unwrap();
    store
        .append_client_event(
            &session.id,
            ClientEventKind::AssistantDelta {
                turn_id: TurnId::new(),
                delta: "hello".into(),
                message_id: None,
            },
        )
        .unwrap();
    let batch = store.client_events(&durable, 10).unwrap();
    assert_eq!(batch.events.len(), 1);
    store
        .acknowledge_events(&durable, batch.checkpoint)
        .unwrap();
    // The checkpoint remains valid after another consumer compacts the row.
    store
        .acknowledge_events(&ephemeral, batch.checkpoint)
        .unwrap();

    assert!(
        store
            .client_events(&ephemeral, 10)
            .unwrap()
            .events
            .is_empty()
    );
}

#[test]
fn cancelled_turns_emit_a_distinct_terminal_event() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "cancel-event",
            "Soul",
            CONFIG,
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
    let consumer = ConsumerId::new();
    store
        .subscribe_session(&consumer, &session.id, ConsumerKind::Durable)
        .unwrap();
    let turn = store.start_turn(&session.id).unwrap();
    store
        .finish_turn(&turn.id, TurnStatus::Cancelled, Some("user cancelled"))
        .unwrap();

    assert!(store
        .client_events(&consumer, 10)
        .unwrap()
        .events
        .iter()
        .any(|event| matches!(&event.kind, ClientEventKind::TurnCancelled { turn_id } if turn_id == &turn.id)));
}

#[test]
fn stale_development_schema_is_rejected_early() {
    let state = tempfile::tempdir().unwrap();
    let database = state.path().join("mews.db");
    let connection = Connection::open(&database).unwrap();
    connection
        .execute("CREATE TABLE hosts (id TEXT PRIMARY KEY)", [])
        .unwrap();
    drop(connection);

    let error = Store::open(&database).err().unwrap();
    assert!(error.to_string().contains("reset MEWS_HOME"));
}

#[test]
fn previous_v1_development_schema_is_rejected_early() {
    let state = tempfile::tempdir().unwrap();
    let database = state.path().join("mews.db");
    let connection = Connection::open(&database).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE hosts (id TEXT PRIMARY KEY);
             PRAGMA user_version = 1;",
        )
        .unwrap();
    drop(connection);

    let error = Store::open(&database).err().unwrap();
    assert!(error.to_string().contains("found version 1, expected 5"));
    assert!(error.to_string().contains("reset MEWS_HOME"));
}

#[test]
fn effect_completion_rejects_an_operation_owned_by_another_turn() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "effect-owner",
            "Soul",
            CONFIG,
            &installation.hub_host_id,
        )
        .unwrap();
    let first_session = store
        .create_session(
            &agent.id,
            &installation.hub_host_id,
            std::path::Path::new("/tmp/first"),
        )
        .unwrap();
    let second_session = store
        .create_session(
            &agent.id,
            &installation.hub_host_id,
            std::path::Path::new("/tmp/second"),
        )
        .unwrap();
    let first_turn = store.start_turn(&first_session.id).unwrap();
    let second_turn = store.start_turn(&second_session.id).unwrap();
    let operation = store
        .start_effect(
            &first_session.id,
            &first_turn.id,
            mews_protocol::EffectRequest::LifecycleHook {
                hook: "before_model".into(),
            },
        )
        .unwrap();

    let error = store
        .finish_effect(
            &second_session.id,
            &second_turn.id,
            &operation,
            EffectOutcome::Succeeded(None),
        )
        .unwrap_err();
    assert!(error.to_string().contains("does not belong"));
    let status: String = store
        .connection
        .query_row(
            "SELECT status FROM effects WHERE operation_id = ?1",
            [operation.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(status, "started");
}

#[test]
fn effect_can_remain_scheduled_until_external_dispatch() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "scheduled-effect",
            "Soul",
            CONFIG,
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
    let turn = store.start_turn(&session.id).unwrap();
    let operation = store
        .schedule_effect(
            &session.id,
            &turn.id,
            mews_protocol::EffectRequest::ProviderCall {
                provider: "test".into(),
                model: "model".into(),
            },
        )
        .unwrap();
    let status = |store: &Store| {
        store
            .connection
            .query_row(
                "SELECT status FROM effects WHERE operation_id = ?1",
                [operation.as_str()],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
    };
    assert_eq!(status(&store), "scheduled");
    let error = store
        .finish_effect(
            &session.id,
            &turn.id,
            &operation,
            EffectOutcome::Succeeded(None),
        )
        .unwrap_err();
    assert!(error.to_string().contains("started effect"));
    assert_eq!(status(&store), "scheduled");
    store
        .mark_effect_started(&session.id, &turn.id, &operation)
        .unwrap();
    assert_eq!(status(&store), "started");
}

#[test]
fn turn_idempotency_returns_the_original_turn() {
    let (mut store, installation) = initialized();
    let directory = tempfile::tempdir().unwrap();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "turns",
            "Soul",
            CONFIG,
            &installation.hub_host_id,
        )
        .unwrap();
    let session = store
        .create_session(&agent.id, &installation.hub_host_id, directory.path())
        .unwrap();
    let (first, created) = store
        .start_turn_idempotent(&session.id, "request-1", None)
        .unwrap();
    assert!(created);
    let (second, created) = store
        .start_turn_idempotent(&session.id, "request-1", None)
        .unwrap();
    assert!(!created);
    assert_eq!(first.id, second.id);
    let stored_key: Option<String> = store
        .connection
        .query_row(
            "SELECT idempotency_key FROM turns WHERE id = ?1",
            [first.id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored_key.as_deref(), Some("request-1"));
    let has_turn_requests: bool = store
        .connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = 'turn_requests')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!has_turn_requests);
    store
        .finish_turn(&first.id, TurnStatus::Cancelled, Some("test cancellation"))
        .unwrap();
    assert_eq!(store.turn(&first.id).unwrap().status, TurnStatus::Cancelled);
}

#[test]
fn turn_harness_provenance_is_exact_and_immutable() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "provenance",
            "Soul",
            CONFIG,
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
    let turn = store.start_turn(&session.id).unwrap();

    store
        .record_turn_harness(&turn.id, "codex", "sha256:definition", Some("1.2.3"))
        .unwrap();
    let stored = store.turn(&turn.id).unwrap();
    assert_eq!(stored.harness.as_deref(), Some("codex"));
    assert_eq!(
        stored.harness_definition_hash.as_deref(),
        Some("sha256:definition")
    );
    assert_eq!(stored.harness_version.as_deref(), Some("1.2.3"));
    let entries = store.session_entries(&session.id).unwrap();
    assert!(matches!(
        &entries[0].payload,
        SessionEntryPayload::TurnStarted { turn_id, harness }
            if turn_id == &turn.id
                && harness.name == "codex"
                && harness.definition_hash == "sha256:definition"
                && harness.version.as_deref() == Some("1.2.3")
    ));

    assert!(
        store
            .record_turn_harness(&turn.id, "claude", "sha256:other", None)
            .is_err()
    );
}

#[test]
fn terminal_turn_state_and_transcript_entry_are_committed_together() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "terminal-entry",
            "Soul",
            CONFIG,
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
    let turn = store.start_turn(&session.id).unwrap();
    store
        .record_turn_harness(&turn.id, "mews", "sha256:native", Some("1"))
        .unwrap();
    store
        .finish_turn_with_stop_reason(&turn.id, TurnStatus::Completed, None, Some("end_turn"))
        .unwrap();

    assert_eq!(store.turn(&turn.id).unwrap().status, TurnStatus::Completed);
    let entries = store.session_entries(&session.id).unwrap();
    assert!(matches!(
        &entries[1].payload,
        SessionEntryPayload::TurnCompleted { turn_id, stop_reason }
            if turn_id == &turn.id && stop_reason.as_deref() == Some("end_turn")
    ));
    assert!(store.session(&session.id).unwrap().leaf_entry_id.is_none());
}

#[test]
fn acp_semantics_normalize_into_typed_transcript_entries() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "normalized-acp",
            "Soul",
            CONFIG,
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
    let turn = store.start_turn(&session.id).unwrap();
    store
        .append_acp_observation(
            &session.id,
            turn.id.clone(),
            Some("external-session".into()),
            Some("reasoning:1".into()),
            AcpObservation::CompletedReasoning {
                text: "use the weather tool".into(),
                message_id: Some("external-message".into()),
                visibility: ReasoningVisibility::Visible,
            },
        )
        .unwrap();
    let entries = store.session_entries(&session.id).unwrap();
    assert!(matches!(
        entries[0].payload,
        SessionEntryPayload::Reasoning { .. }
    ));
    assert!(store.active_entries(&session.id).unwrap().is_empty());
}

#[test]
fn acp_session_binding_is_one_to_one_and_replacement_is_explicit() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "acp-binding",
            "Soul",
            CONFIG,
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

    let context = AcpContextSnapshot {
        version: ACP_CONTEXT_VERSION,
        agent_slug: "acp-binding".into(),
        soul: "Soul".into(),
        skills: Vec::new(),
    };
    let context_text = context.render().unwrap();
    store
        .bind_acp_session(
            &session.id,
            &session.host_id,
            "codex",
            "definition-1",
            "codex-session-1",
            &AcpBindingTransition::New,
            &context,
            &context_text,
            AcpInstructionChannel::FirstPrompt,
            true,
        )
        .unwrap();
    assert_eq!(
        store
            .acp_session_binding(&session.id)
            .unwrap()
            .unwrap()
            .acp_session_id,
        "codex-session-1"
    );
    assert!(
        store
            .bind_acp_session(
                &session.id,
                &session.host_id,
                "codex",
                "definition-1",
                "codex-session-2",
                &AcpBindingTransition::New,
                &context,
                &context_text,
                AcpInstructionChannel::FirstPrompt,
                true,
            )
            .is_err()
    );
    let replaced = store
        .bind_acp_session(
            &session.id,
            &session.host_id,
            "codex",
            "definition-1",
            "codex-session-2",
            &AcpBindingTransition::Replace {
                reason: AcpReplacementReason::ResourceNotFound,
            },
            &context,
            &context_text,
            AcpInstructionChannel::FirstPrompt,
            true,
        )
        .unwrap();
    assert_eq!(replaced.acp_session_id, "codex-session-2");
    assert!(replaced.replaced_at.is_some());
    let has_unused_audit_table: bool = store
        .connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'acp_session_replacements')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!has_unused_audit_table);
}

#[test]
fn acp_binding_and_context_acknowledgement_bundles_are_idempotent() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "acp-bundle",
            "Soul",
            CONFIG,
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
    let turn = store.start_turn(&session.id).unwrap();
    let context = AcpContextSnapshot {
        version: ACP_CONTEXT_VERSION,
        agent_slug: "acp-bundle".into(),
        soul: "Soul".into(),
        skills: Vec::new(),
    };
    let context_text = context.render().unwrap();
    let bind = |store: &Store| {
        store.bind_acp_session_with_observations(
            &session.id,
            &session.host_id,
            "codex",
            "definition-1",
            "codex-session-1",
            &AcpBindingTransition::New,
            &context,
            &context_text,
            AcpInstructionChannel::FirstPrompt,
            false,
            turn.id.clone(),
        )
    };
    bind(&store).unwrap();
    bind(&store).unwrap();
    store
        .mark_acp_context_dispatched_with_observation(
            &session.id,
            turn.id.clone(),
            "codex-session-1",
        )
        .unwrap();
    store
        .mark_acp_context_dispatched_with_observation(&session.id, turn.id, "codex-session-1")
        .unwrap();

    let events = store
        .journal_entries_for_subject(
            mews_protocol::JournalSubjectType::Session,
            session.id.as_str(),
            0,
        )
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event.payload,
                mews_protocol::JournalEvent::AcpBindingChanged { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event.payload,
                mews_protocol::JournalEvent::AcpContextDispatched { .. }
            ))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event.payload,
                mews_protocol::JournalEvent::HarnessObservationRecorded { .. }
            ))
            .count(),
        2
    );
}

#[test]
fn acp_observation_and_transient_signal_roll_back_together() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "acp-atomic",
            "Soul",
            CONFIG,
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
    let turn = store.start_turn(&session.id).unwrap();
    let error = store
        .append_acp_observation_with_client_event(
            &session.id,
            turn.id.clone(),
            None,
            Some("atomic-failure".into()),
            AcpObservation::ProviderUpdate {
                data: json!({"phase": "working"}),
            },
            ClientEventKind::AssistantDelta {
                turn_id: turn.id,
                delta: "x".repeat(mews_protocol::MAX_EVENT_PAGE_PAYLOAD_BYTES + 1),
                message_id: None,
            },
        )
        .unwrap_err();
    assert!(error.to_string().contains("event page limit"));
    assert!(store.session_entries(&session.id).unwrap().is_empty());
}

#[test]
fn acp_observations_anchor_the_leaf_without_becoming_context() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "acp-observation",
            "Soul",
            CONFIG,
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
    let user = store
        .append_message(
            &session.id,
            MessageRole::User,
            MessageContent::Text {
                text: "hello".into(),
            },
            Value::Null,
            MessageSource {
                kind: SourceKind::Client,
                id: "test".into(),
                channel_origin: None,
            },
        )
        .unwrap();
    let turn = store.start_turn(&session.id).unwrap();
    store
        .append_acp_observation(
            &session.id,
            turn.id.clone(),
            None,
            Some("provider:1".into()),
            AcpObservation::ProviderUpdate {
                data: json!({"state":"working"}),
            },
        )
        .unwrap();
    let observation = store.session_entries(&session.id).unwrap().pop().unwrap();
    assert_eq!(observation.parent_id, Some(user.id.clone()));
    assert_eq!(
        store.session(&session.id).unwrap().leaf_entry_id,
        Some(user.id.clone())
    );
    assert_eq!(store.active_messages(&session.id).unwrap().len(), 1);
    assert_eq!(store.session_entries(&session.id).unwrap().len(), 2);
    store
        .append_acp_observation(
            &session.id,
            turn.id,
            None,
            Some("provider:2".into()),
            AcpObservation::ProviderUpdate {
                data: json!({"state":"working"}),
            },
        )
        .unwrap();
    assert_eq!(store.session_entries(&session.id).unwrap().len(), 3);
    let error = store
        .set_session_leaf_checked(&session.id, Some(&user.id), Some(&observation.id))
        .unwrap_err();
    assert!(matches!(error, StoreError::InvalidData(_)));
    assert_eq!(store.active_messages(&session.id).unwrap().len(), 1);
}

#[test]
fn reopening_recovers_an_interrupted_turn_and_tool_call() {
    let state = tempfile::tempdir().unwrap();
    let database = state.path().join("mews.db");
    let lock = state.path().join("hub.lock");
    let work = tempfile::tempdir().unwrap();
    let (turn_id, session_id) = {
        let mut store = Store::open_hub(&database, &lock).unwrap();
        let installation = store
            .initialize(
                &CommandContext::system(),
                "laptop",
                "key",
                "noise-key",
                "installation-key",
            )
            .unwrap();
        let (agent, _) = store
            .create_agent(
                &CommandContext::system(),
                "coder",
                "Soul",
                CONFIG,
                &installation.hub_host_id,
            )
            .unwrap();
        let session = store
            .create_session(&agent.id, &installation.hub_host_id, work.path())
            .unwrap();
        let turn = store.start_turn(&session.id).unwrap();
        store
            .record_turn_harness(&turn.id, "mews", "definition", Some("test"))
            .unwrap();
        let call = ToolCall {
            call_id: "unfinished".into(),
            tool: "bash".into(),
            arguments: json!({"command":"side-effect"}),
            thought_signature: None,
        };
        store
            .append_tool_requested(&session.id, &turn.id, call.clone())
            .unwrap();
        store
            .start_tool_effect(&session.id, &turn.id, call)
            .unwrap();
        (turn.id, session.id)
    };

    let store = Store::open_hub(&database, &lock).unwrap();
    store.recover_interrupted_work().unwrap();
    let turn = store.turn(&turn_id).unwrap();
    assert_eq!(turn.status, TurnStatus::Failed);
    let messages = store.messages(&session_id).unwrap();
    assert!(matches!(
        messages.last().map(|message| &message.content),
        Some(MessageContent::ToolResult { is_error: true, .. })
    ));
}

#[test]
fn reopening_closes_a_tool_call_that_never_started() {
    let state = tempfile::tempdir().unwrap();
    let database = state.path().join("mews.db");
    let lock = state.path().join("hub.lock");
    let work = tempfile::tempdir().unwrap();
    let (turn_id, session_id) = {
        let mut store = Store::open_hub(&database, &lock).unwrap();
        let installation = store
            .initialize(
                &CommandContext::system(),
                "laptop",
                "key",
                "noise-key",
                "installation-key",
            )
            .unwrap();
        let (agent, _) = store
            .create_agent(
                &CommandContext::system(),
                "pre-start",
                "Soul",
                CONFIG,
                &installation.hub_host_id,
            )
            .unwrap();
        let session = store
            .create_session(&agent.id, &installation.hub_host_id, work.path())
            .unwrap();
        let turn = store.start_turn(&session.id).unwrap();
        store
            .append_tool_requested(
                &session.id,
                &turn.id,
                ToolCall {
                    call_id: "not-started".into(),
                    tool: "bash".into(),
                    arguments: json!({"command":"side-effect"}),
                    thought_signature: None,
                },
            )
            .unwrap();
        // An unrelated opaque effect must not make this tool call uncertain.
        store
            .start_effect(
                &session.id,
                &turn.id,
                mews_protocol::EffectRequest::LifecycleHook {
                    hook: "turn_end".into(),
                },
            )
            .unwrap();
        (turn.id, session.id)
    };

    let store = Store::open_hub(&database, &lock).unwrap();
    store.recover_interrupted_work().unwrap();
    assert_eq!(store.turn(&turn_id).unwrap().status, TurnStatus::Failed);
    assert!(
        store
            .session_entries(&session_id)
            .unwrap()
            .iter()
            .any(|entry| {
                matches!(
                    &entry.payload,
                    SessionEntryPayload::ToolResult { result, .. }
                        if result.call_id == "not-started" && result.is_error && !result.uncertain
                )
            })
    );
}

#[test]
fn reopening_preserves_that_raw_tool_execution_completed() {
    let state = tempfile::tempdir().unwrap();
    let database = state.path().join("mews.db");
    let lock = state.path().join("hub.lock");
    let work = tempfile::tempdir().unwrap();
    let session_id = {
        let mut store = Store::open_hub(&database, &lock).unwrap();
        let installation = store
            .initialize(
                &CommandContext::system(),
                "laptop",
                "key",
                "noise-key",
                "installation-key",
            )
            .unwrap();
        let (agent, _) = store
            .create_agent(
                &CommandContext::system(),
                "raw-complete",
                "Soul",
                CONFIG,
                &installation.hub_host_id,
            )
            .unwrap();
        let session = store
            .create_session(&agent.id, &installation.hub_host_id, work.path())
            .unwrap();
        let turn = store.start_turn(&session.id).unwrap();
        let call = ToolCall {
            call_id: "raw-complete".into(),
            tool: "read".into(),
            arguments: json!({}),
            thought_signature: None,
        };
        store
            .append_tool_requested(&session.id, &turn.id, call.clone())
            .unwrap();
        store
            .start_tool_effect(&session.id, &turn.id, call)
            .unwrap();
        store
            .complete_tool_execution(
                &session.id,
                &turn.id,
                ToolResult {
                    call_id: "raw-complete".into(),
                    tool: "read".into(),
                    result: json!({"raw": true}),
                    is_error: false,
                    uncertain: false,
                },
            )
            .unwrap();
        session.id
    };

    let store = Store::open_hub(&database, &lock).unwrap();
    store.recover_interrupted_work().unwrap();
    let result = store
        .session_entries(&session_id)
        .unwrap()
        .into_iter()
        .find_map(|entry| match entry.payload {
            SessionEntryPayload::ToolResult { result, .. } if result.call_id == "raw-complete" => {
                Some(result)
            }
            _ => None,
        })
        .unwrap();
    assert!(result.uncertain);
    assert!(
        result
            .result
            .as_str()
            .unwrap()
            .contains("result processing was interrupted")
    );
    assert!(!result.result.as_str().unwrap().contains("did not start"));
}

#[test]
fn blocked_tool_result_does_not_create_an_effect() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "blocked-effect",
            "Soul",
            CONFIG,
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
    let turn = store.start_turn(&session.id).unwrap();
    store
        .record_turn_harness(&turn.id, "mews", "definition", Some("test"))
        .unwrap();
    let call = ToolCall {
        call_id: "blocked".into(),
        tool: "write".into(),
        arguments: json!({"path":"file", "content":"value"}),
        thought_signature: None,
    };

    store
        .append_tool_requested(&session.id, &turn.id, call)
        .unwrap();
    store
        .append_tool_result(
            &session.id,
            &turn.id,
            ToolResult {
                call_id: "blocked".into(),
                tool: "write".into(),
                result: Value::String("blocked by policy".into()),
                is_error: true,
                uncertain: false,
            },
        )
        .unwrap();

    let effects: u64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM effects WHERE turn_id = ?1 AND call_id = 'blocked'",
            [turn.id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(effects, 0);
}

#[test]
fn uncertain_tool_result_sets_an_uncertain_effect_terminal() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "uncertain-effect",
            "Soul",
            CONFIG,
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
    let turn = store.start_turn(&session.id).unwrap();
    store
        .record_turn_harness(&turn.id, "mews", "definition", Some("test"))
        .unwrap();
    let call = ToolCall {
        call_id: "ambiguous".into(),
        tool: "write".into(),
        arguments: json!({"path":"file", "content":"value"}),
        thought_signature: None,
    };

    store
        .append_tool_requested(&session.id, &turn.id, call.clone())
        .unwrap();
    store
        .start_tool_effect(&session.id, &turn.id, call)
        .unwrap();
    let result = ToolResult {
        call_id: "ambiguous".into(),
        tool: "write".into(),
        result: Value::String("Host disconnected before replying".into()),
        is_error: true,
        uncertain: true,
    };
    store
        .complete_tool_execution(&session.id, &turn.id, result.clone())
        .unwrap();
    store
        .append_tool_result(&session.id, &turn.id, result)
        .unwrap();

    let (status, terminal_event): (String, String) = store
        .connection
        .query_row(
            "SELECT effects.status, journal_entries.event_type
             FROM effects JOIN journal_entries ON journal_entries.id = effects.terminal_journal_entry_id
             WHERE effects.turn_id = ?1 AND effects.call_id = 'ambiguous'",
            [turn.id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(status, "uncertain");
    assert_eq!(terminal_event, "tool_execution_completed");
    assert!(matches!(
        store
            .messages(&session.id)
            .unwrap()
            .last()
            .map(|message| &message.content),
        Some(MessageContent::ToolResult {
            uncertain: true,
            ..
        })
    ));
}

#[test]
fn raw_tool_completion_and_transformed_result_are_distinct_facts() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
            &CommandContext::system(),
            "tool-facts",
            "Soul",
            CONFIG,
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
    let turn = store.start_turn(&session.id).unwrap();
    let call = ToolCall {
        call_id: "separate".into(),
        tool: "read".into(),
        arguments: json!({}),
        thought_signature: None,
    };
    store
        .append_tool_requested(&session.id, &turn.id, call.clone())
        .unwrap();
    store
        .start_tool_effect(&session.id, &turn.id, call)
        .unwrap();
    let raw = ToolResult {
        call_id: "separate".into(),
        tool: "read".into(),
        result: json!({"secret":"raw"}),
        is_error: false,
        uncertain: false,
    };
    let transformed = ToolResult {
        result: json!({"secret":"redacted"}),
        ..raw.clone()
    };

    assert!(
        store
            .append_tool_result(&session.id, &turn.id, transformed.clone())
            .unwrap_err()
            .to_string()
            .contains("before its execution completes")
    );
    store
        .complete_tool_execution(&session.id, &turn.id, raw.clone())
        .unwrap();
    store
        .append_tool_result(&session.id, &turn.id, transformed.clone())
        .unwrap();

    let events = store.journal_entries_after(0, 1_000).unwrap();
    assert!(events.iter().any(|event| matches!(
        &event.payload,
        mews_protocol::JournalEvent::ToolExecutionCompleted { result, .. } if result == &raw
    )));
    assert!(events.iter().any(|event| matches!(
        &event.payload,
        mews_protocol::JournalEvent::ToolResultRecorded { result, .. } if result == &transformed
    )));
    assert!(matches!(
        store.session_entries(&session.id).unwrap().last().map(|entry| &entry.payload),
        Some(SessionEntryPayload::ToolResult { result, .. }) if result == &transformed
    ));
}
