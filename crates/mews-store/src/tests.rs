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

    let (first_run, _) = store.start_run_idempotent(&first.id, "message-1").unwrap();
    store
        .finish_run(&first_run.id, RunStatus::Completed, None)
        .unwrap();
    let (second_run, created) = store.start_run_idempotent(&second.id, "message-1").unwrap();

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
    let source = MessageSource {
        kind: SourceKind::Client,
        id: "cli".into(),
    };
    store
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
    store
        .append_message(
            &session.id,
            MessageRole::Assistant,
            MessageContent::Text { text: "hi".into() },
            Value::Null,
            source,
        )
        .unwrap();
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
    store.subscribe_session(&consumer, &session.id).unwrap();
    let message = store
        .append_message(
            &session.id,
            MessageRole::Assistant,
            MessageContent::Text {
                text: "hello".into(),
            },
            Value::Null,
            MessageSource {
                kind: SourceKind::Harness,
                id: "default".into(),
            },
        )
        .unwrap();
    let first = store.client_events(&consumer, 100).unwrap();
    assert!(matches!(&first.events[0].kind,
            ClientEventKind::AssistantMessage { message: event } if event.id == message.id));
    assert_eq!(store.client_events(&consumer, 100).unwrap().events.len(), 1);
    store
        .acknowledge_events(&consumer, first.checkpoint)
        .unwrap();
    assert!(
        store
            .client_events(&consumer, 100)
            .unwrap()
            .events
            .is_empty()
    );
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
        .start_run_idempotent(&session.id, "request-1")
        .unwrap();
    assert!(created);
    let (second, created) = store
        .start_run_idempotent(&session.id, "request-1")
        .unwrap();
    assert!(!created);
    assert_eq!(first.id, second.id);
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

    assert!(
        store
            .record_run_harness(&run.id, "claude", "sha256:other", None)
            .is_err()
    );
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

    store
        .bind_acp_session(
            &session.id,
            &session.host_id,
            "codex",
            "definition-1",
            "codex-session-1",
            None,
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
                None,
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
            Some("resource_not_found"),
        )
        .unwrap();
    assert_eq!(replaced.acp_session_id, "codex-session-2");
    assert!(replaced.replaced_at.is_some());
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
            .append_message(
                &session.id,
                MessageRole::Assistant,
                MessageContent::ToolCall {
                    call_id: "unfinished".into(),
                    tool: "bash".into(),
                    arguments: json!({"command":"side-effect"}),
                    thought_signature: None,
                },
                Value::Null,
                MessageSource {
                    kind: SourceKind::Harness,
                    id: "default".into(),
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
