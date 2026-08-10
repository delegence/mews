use mews_protocol::{ACP_CONTEXT_VERSION, AcpReplacementReason};
use serde_json::json;

use super::*;
use crate::{MessageRole, SourceKind};

const CONFIG: &str = "harness = \"mews\"\ntools = [\"read\", \"write\", \"edit\", \"bash\"]\n[harness_options]\nmodel = \"test\"\n";

fn initialized() -> (Store, Installation) {
    let mut store = Store::open_in_memory().unwrap();
    let installation = store
        .initialize(
            "laptop",
            "test-public-key",
            "test-noise-key",
            "installation-key",
        )
        .unwrap();
    (store, installation)
}

fn append_assistant(store: &Store, session_id: &SessionId, text: String) -> Message {
    let entry = store
        .append_assistant_response(
            session_id,
            &RunId::new(),
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
fn rich_assistant_response_round_trips_as_one_ordered_entry() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent("rich", "Soul", CONFIG, &installation.hub_host_id)
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
    let run = store.start_run(&session.id).unwrap();
    store
        .append_assistant_response(&session.id, &run.id, response.clone())
        .unwrap();

    let entries = store.session_entries(&session.id).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].payload,
        SessionEntryPayload::AssistantResponse {
            run_id: run.id,
            response
        }
    );
}

#[test]
fn assistant_response_and_delivery_event_roll_back_together() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent("atomic", "Soul", CONFIG, &installation.hub_host_id)
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

    let run = store.start_run(&session.id).unwrap();
    let result = store.append_assistant_response(
        &session.id,
        &run.id,
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
        .create_agent("event-page", "Soul", CONFIG, &installation.hub_host_id)
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
        .create_agent("event-limit", "Soul", CONFIG, &installation.hub_host_id)
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
            run_id: RunId::new(),
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
fn undeliverable_run_failure_rolls_back_run_completion() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent("run-event-limit", "Soul", CONFIG, &installation.hub_host_id)
        .unwrap();
    let session = store
        .create_session(
            &agent.id,
            &installation.hub_host_id,
            std::path::Path::new("/tmp"),
        )
        .unwrap();
    let run = store.start_run(&session.id).unwrap();
    let result = store.finish_run(
        &run.id,
        RunStatus::Failed,
        Some(&"x".repeat(mews_protocol::MAX_EVENT_PAGE_PAYLOAD_BYTES)),
    );
    assert!(matches!(result, Err(StoreError::InvalidData(_))));
    assert_eq!(store.run(&run.id).unwrap().status, RunStatus::Running);
}

#[test]
fn latest_compaction_changes_active_context_without_deleting_history() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent("compact", "Soul", CONFIG, &installation.hub_host_id)
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
        .create_agent("coder", "Soul", CONFIG, &installation.hub_host_id)
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

    let (first_run, _) = store
        .start_run_idempotent(&first.id, "message-1", None)
        .unwrap();
    store
        .finish_run(&first_run.id, RunStatus::Completed, None)
        .unwrap();
    let (second_run, created) = store
        .start_run_idempotent(&second.id, "message-1", None)
        .unwrap();

    assert!(created);
    assert_eq!(second_run.session_id, second.id);
}

#[test]
fn revisions_require_the_exact_current_parent() {
    let (mut store, installation) = initialized();
    let (agent, first) = store
        .create_agent("coder", "Be practical.", CONFIG, &installation.hub_host_id)
        .unwrap();
    let second = store
        .update_agent(
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
        .create_agent("coder", "Soul", CONFIG, &installation.hub_host_id)
        .unwrap();
    let renamed = store.rename_agent("coder", "builder").unwrap();
    assert_eq!(renamed.slug, "builder");
    store.archive_agent("builder").unwrap();
    assert!(store.agent_by_slug("builder").is_err());
    assert!(store.agents().unwrap().is_empty());

    let (invitation, secret) = store
        .create_invitation(Utc::now() + chrono::Duration::minutes(1))
        .unwrap();
    let host = store
        .consume_invitation(
            &invitation,
            &secret,
            "mini",
            "mini-key",
            "mini-noise",
            "wss://relay.example",
        )
        .unwrap();
    store.revoke_host(&host.id).unwrap();
    assert!(store.host(&host.id).is_err());
    assert!(store.revoke_host(&installation.hub_host_id).is_err());
}

#[test]
fn invitation_is_bearer_secret_and_exactly_once() {
    let (mut store, _) = initialized();
    let expiry = Utc::now() + chrono::Duration::minutes(15);
    let (id, secret) = store.create_invitation(expiry).unwrap();
    assert!(
        store
            .consume_invitation(
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
        .create_agent("coder", "Be practical.", CONFIG, &installation.hub_host_id)
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
        .create_agent("timeline", "Soul", CONFIG, &installation.hub_host_id)
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
        .create_agent("leaf-conflict", "Soul", CONFIG, &installation.hub_host_id)
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
        .create_agent("events", "Be practical.", CONFIG, &installation.hub_host_id)
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
        .create_agent("event-origins", "Soul", CONFIG, &installation.hub_host_id)
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
    let (run, _) = store
        .start_run_idempotent(&session.id, "channel-turn", Some(&origin))
        .unwrap();
    store
        .append_client_event(
            &session.id,
            ClientEventKind::AssistantDelta {
                run_id: run.id.clone(),
                delta: "stream".into(),
                message_id: None,
            },
        )
        .unwrap();
    store
        .append_assistant_response(
            &session.id,
            &run.id,
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
        .finish_run(&run.id, RunStatus::Completed, None)
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

    let run = store.start_run(&session.id).unwrap();
    store
        .append_client_event(
            &session.id,
            ClientEventKind::AssistantDelta {
                run_id: run.id,
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
        .create_agent("ephemeral-live", "Soul", CONFIG, &installation.hub_host_id)
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
        .create_agent("event-keys", "Soul", CONFIG, &installation.hub_host_id)
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
    let run = store.start_run(&session.id).unwrap();
    let append = |store: &Store, key: &str| {
        store.append_acp_observation_with_client_event(
            &session.id,
            run.id.clone(),
            None,
            Some(key.into()),
            AcpObservation::AssistantDelta {
                delta: "ha".into(),
                message_id: None,
                raw: Value::Null,
            },
            ClientEventKind::AssistantDelta {
                run_id: run.id.clone(),
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
fn session_pages_are_byte_bounded_and_reconstruct_large_history() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent("large-pages", "Soul", CONFIG, &installation.hub_host_id)
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
        .create_agent("oversized-page", "Soul", CONFIG, &installation.hub_host_id)
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
    let run = store.start_run(&session.id).unwrap();

    let error = store
        .append_acp_observation(
            &session.id,
            run.id,
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
        .create_agent("corrupt-page", "Soul", CONFIG, &installation.hub_host_id)
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
        "run_id": RunId::new(),
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
        .create_agent("large-entries", "Soul", CONFIG, &installation.hub_host_id)
        .unwrap();
    let session = store
        .create_session(
            &agent.id,
            &installation.hub_host_id,
            std::path::Path::new("/tmp"),
        )
        .unwrap();
    let run = store.start_run(&session.id).unwrap();
    for ordinal in 0..4 {
        store
            .append_acp_observation(
                &session.id,
                run.id.clone(),
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
        .create_agent("scoped-events", "Soul", CONFIG, &installation.hub_host_id)
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
                    run_id: RunId::new(),
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
fn ephemeral_consumers_do_not_hold_transient_events_past_durable_cursors() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent(
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
                run_id: RunId::new(),
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
fn cancelled_runs_emit_a_distinct_terminal_event() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent("cancel-event", "Soul", CONFIG, &installation.hub_host_id)
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
    let run = store.start_run(&session.id).unwrap();
    store
        .finish_run(&run.id, RunStatus::Cancelled, Some("user cancelled"))
        .unwrap();

    assert!(store
        .client_events(&consumer, 10)
        .unwrap()
        .events
        .iter()
        .any(|event| matches!(&event.kind, ClientEventKind::RunCancelled { run_id } if run_id == &run.id)));
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
fn turn_idempotency_returns_the_original_run() {
    let (mut store, installation) = initialized();
    let directory = tempfile::tempdir().unwrap();
    let (agent, _) = store
        .create_agent("turns", "Soul", CONFIG, &installation.hub_host_id)
        .unwrap();
    let session = store
        .create_session(&agent.id, &installation.hub_host_id, directory.path())
        .unwrap();
    let (first, created) = store
        .start_run_idempotent(&session.id, "request-1", None)
        .unwrap();
    assert!(created);
    let (second, created) = store
        .start_run_idempotent(&session.id, "request-1", None)
        .unwrap();
    assert!(!created);
    assert_eq!(first.id, second.id);
    let stored_key: Option<String> = store
        .connection
        .query_row(
            "SELECT idempotency_key FROM runs WHERE id = ?1",
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
        .finish_run(&first.id, RunStatus::Cancelled, Some("test cancellation"))
        .unwrap();
    assert_eq!(store.run(&first.id).unwrap().status, RunStatus::Cancelled);
}

#[test]
fn run_harness_provenance_is_exact_and_immutable() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent("provenance", "Soul", CONFIG, &installation.hub_host_id)
        .unwrap();
    let session = store
        .create_session(
            &agent.id,
            &installation.hub_host_id,
            std::path::Path::new("/tmp"),
        )
        .unwrap();
    let run = store.start_run(&session.id).unwrap();

    store
        .record_run_harness(&run.id, "codex", "sha256:definition", Some("1.2.3"))
        .unwrap();
    let stored = store.run(&run.id).unwrap();
    assert_eq!(stored.harness.as_deref(), Some("codex"));
    assert_eq!(
        stored.harness_definition_hash.as_deref(),
        Some("sha256:definition")
    );
    assert_eq!(stored.harness_version.as_deref(), Some("1.2.3"));
    let entries = store.session_entries(&session.id).unwrap();
    assert!(matches!(
        &entries[0].payload,
        SessionEntryPayload::RunStarted { run_id, harness }
            if run_id == &run.id
                && harness.name == "codex"
                && harness.definition_hash == "sha256:definition"
                && harness.version.as_deref() == Some("1.2.3")
    ));

    assert!(
        store
            .record_run_harness(&run.id, "claude", "sha256:other", None)
            .is_err()
    );
}

#[test]
fn terminal_run_state_and_transcript_entry_are_committed_together() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent("terminal-entry", "Soul", CONFIG, &installation.hub_host_id)
        .unwrap();
    let session = store
        .create_session(
            &agent.id,
            &installation.hub_host_id,
            std::path::Path::new("/tmp"),
        )
        .unwrap();
    let run = store.start_run(&session.id).unwrap();
    store
        .record_run_harness(&run.id, "mews", "sha256:native", Some("1"))
        .unwrap();
    store
        .finish_run_with_stop_reason(&run.id, RunStatus::Completed, None, Some("end_turn"))
        .unwrap();

    assert_eq!(store.run(&run.id).unwrap().status, RunStatus::Completed);
    let entries = store.session_entries(&session.id).unwrap();
    assert!(matches!(
        &entries[1].payload,
        SessionEntryPayload::RunCompleted { run_id, stop_reason }
            if run_id == &run.id && stop_reason.as_deref() == Some("end_turn")
    ));
    assert!(store.session(&session.id).unwrap().leaf_entry_id.is_none());
}

#[test]
fn acp_semantics_normalize_into_typed_transcript_entries() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent("normalized-acp", "Soul", CONFIG, &installation.hub_host_id)
        .unwrap();
    let session = store
        .create_session(
            &agent.id,
            &installation.hub_host_id,
            std::path::Path::new("/tmp"),
        )
        .unwrap();
    let run = store.start_run(&session.id).unwrap();
    store
        .append_acp_observation(
            &session.id,
            run.id.clone(),
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
        .create_agent("acp-binding", "Soul", CONFIG, &installation.hub_host_id)
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
fn acp_observations_anchor_the_leaf_without_becoming_context() {
    let (mut store, installation) = initialized();
    let (agent, _) = store
        .create_agent("acp-observation", "Soul", CONFIG, &installation.hub_host_id)
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
    let run = store.start_run(&session.id).unwrap();
    store
        .append_acp_observation(
            &session.id,
            run.id.clone(),
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
            run.id,
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
fn reopening_recovers_an_interrupted_run_and_tool_call() {
    let state = tempfile::tempdir().unwrap();
    let database = state.path().join("mews.db");
    let lock = state.path().join("hub.lock");
    let work = tempfile::tempdir().unwrap();
    let (run_id, session_id) = {
        let mut store = Store::open_hub(&database, &lock).unwrap();
        let installation = store
            .initialize("laptop", "key", "noise-key", "installation-key")
            .unwrap();
        let (agent, _) = store
            .create_agent("coder", "Soul", CONFIG, &installation.hub_host_id)
            .unwrap();
        let session = store
            .create_session(&agent.id, &installation.hub_host_id, work.path())
            .unwrap();
        let run = store.start_run(&session.id).unwrap();
        store
            .record_run_harness(&run.id, "mews", "definition", Some("test"))
            .unwrap();
        store
            .append_tool_started(
                &session.id,
                &run.id,
                ToolCall {
                    call_id: "unfinished".into(),
                    tool: "bash".into(),
                    arguments: json!({"command":"side-effect"}),
                    thought_signature: None,
                },
            )
            .unwrap();
        (run.id, session.id)
    };

    let store = Store::open_hub(&database, &lock).unwrap();
    let run = store.run(&run_id).unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    let messages = store.messages(&session_id).unwrap();
    assert!(matches!(
        messages.last().map(|message| &message.content),
        Some(MessageContent::ToolResult { is_error: true, .. })
    ));
}
