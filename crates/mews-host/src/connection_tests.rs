use super::handle_host_request;
use super::*;
use crate::connection::*;
use chrono::Utc;
use sha2::{Digest, Sha256};

#[test]
fn cancellation_registry_owner_cancels_all_work_on_drop() {
    let first = mews_agent::CancellationToken::new();
    let second = mews_agent::CancellationToken::new();
    let registry = Arc::new(std::sync::Mutex::new(HashMap::from([
        (RequestId::new(), first.clone()),
        (RequestId::new(), second.clone()),
    ])));
    drop(CancellationRegistryOwner::new(registry));
    assert!(first.is_cancelled());
    assert!(second.is_cancelled());
}

#[cfg(unix)]
#[tokio::test]
async fn remote_acp_runs_on_the_bound_host_working_directory() {
    use std::{fs, os::unix::fs::PermissionsExt};

    let host_root = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    let fixture = host_root.path().join("fixture-acp");
    fs::write(
        &fixture,
        r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{}}}' ;;
    *'"id":2'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"fixture"}}' ;;
    *'"id":3'*)
      printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"remote fixture reply"}}}}'
      : > acp-ran-on-bound-host
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn"}}'
      exit 0
      ;;
  esac
done
"#,
    )
    .unwrap();
    fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();
    fs::create_dir_all(host_root.path().join("harnesses")).unwrap();
    fs::write(
        host_root.path().join("harnesses/fixture.toml"),
        format!(
            "name = \"fixture\"\nprotocol = \"acp\"\ncommand = [\"{}\"]\n",
            fixture.display()
        ),
    )
    .unwrap();
    crate::HarnessCatalog::setup(host_root.path(), "fixture")
        .await
        .unwrap();
    let cwd = project.path().canonicalize().unwrap();

    let connected = ConnectedHost::in_process(
        HostId::new(),
        ToolRegistry::with_agent_extensions(host_root.path()).unwrap(),
    )
    .await
    .unwrap();

    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(crate::ACP_EVENT_CHANNEL_CAPACITY);
    let cancellation = mews_agent::CancellationToken::new();
    let turn = connected.execute_acp_turn(
        RemoteAcpTurn {
            agent_id: mews_protocol::AgentId::new(),
            harness: "fixture".into(),
            harness_options: Default::default(),
            tools: vec!["*".into()],
            cwd: cwd.clone(),
            prompt: "canonical remote prompt".into(),
            recovery_prompt: "recovery prompt".into(),
            agent_slug: "fixture-agent".into(),
            soul: "fixture soul".into(),
            mews_session_id: "session-fixture".into(),
            turn_id: "turn-fixture".into(),
            transition: mews_protocol::AcpBindingTransition::New,
            context: None,
        },
        event_tx,
        &cancellation,
    );
    let events = async {
        let bound = loop {
            let event = event_rx.recv().await.unwrap();
            if matches!(event, mews_protocol::AcpEvent::SessionBound { .. }) {
                break event;
            }
        };
        let mews_protocol::AcpEvent::SessionBound {
            acknowledgement_id,
            context,
            ..
        } = &bound
        else {
            panic!("first ACP event was not a SessionBound event");
        };
        connected
            .acknowledge_acp_session_binding(acknowledgement_id.clone())
            .await
            .unwrap();
        assert!(context.text.contains("fixture soul"));
        assert_eq!(
            context.hash,
            mews_protocol::AcpContextSnapshot::hash_rendered(&context.text)
        );
        let delta = loop {
            let event = event_rx.recv().await.unwrap();
            match event {
                mews_protocol::AcpEvent::ContextDispatched {
                    acknowledgement_id, ..
                } => connected
                    .acknowledge_acp_session_binding(acknowledgement_id)
                    .await
                    .unwrap(),
                event @ mews_protocol::AcpEvent::AssistantDelta { .. } => break event,
                _ => {}
            }
        };
        (bound, delta)
    };
    let (answer, (bound, delta)) = tokio::join!(turn, events);
    let answer = answer.unwrap();

    assert_eq!(answer.answer, "remote fixture reply");
    assert_eq!(answer.session_id, "fixture");
    assert!(matches!(
        bound,
        mews_protocol::AcpEvent::SessionBound { session_id, transition: mews_protocol::AcpBindingTransition::New, .. } if session_id == "fixture"
    ));
    assert!(matches!(
        delta,
        mews_protocol::AcpEvent::AssistantDelta { delta, .. } if delta == "remote fixture reply"
    ));
    assert!(cwd.join("acp-ran-on-bound-host").exists());
}

#[tokio::test]
async fn serialized_boundary_executes_and_rechecks_cwd() {
    let cwd = tempfile::tempdir().unwrap();
    std::fs::write(cwd.path().join("hello.txt"), "remote content").unwrap();
    let host = ConnectedHost::in_process(HostId::new(), ToolRegistry::with_defaults())
        .await
        .unwrap();
    let result = host
        .execute_tool(
            &mews_protocol::AgentId::new(),
            "read",
            serde_json::json!({"path":"hello.txt"}),
            &cwd.path().canonicalize().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result["content"], "remote content");
}

#[tokio::test]
async fn disconnect_after_tool_dispatch_is_an_uncertain_effect() {
    let (hub_to_host, mut requests) = tokio::sync::mpsc::channel(1);
    let (host_to_hub, replies) = tokio::sync::mpsc::channel(1);
    let host = ConnectedHost::from_channels(HostId::new(), Vec::new(), hub_to_host, replies)
        .await
        .unwrap();
    let remote = tokio::spawn(async move {
        let request = requests.recv().await.expect("tool request");
        assert!(matches!(request, HubToHost::ExecuteTool { .. }));
        drop(host_to_hub);
    });

    let error = host
        .execute_tool(
            &mews_protocol::AgentId::new(),
            "effect",
            serde_json::json!({}),
            std::path::Path::new("/"),
        )
        .await
        .unwrap_err();
    remote.await.unwrap();

    assert!(mews_agent::effect_uncertainty(&error).is_some());
}

#[tokio::test]
async fn disconnect_after_hook_dispatch_is_an_uncertain_effect() {
    let (hub_to_host, mut requests) = tokio::sync::mpsc::channel(1);
    let (host_to_hub, replies) = tokio::sync::mpsc::channel(1);
    let host = ConnectedHost::from_channels(HostId::new(), Vec::new(), hub_to_host, replies)
        .await
        .unwrap();
    let remote = tokio::spawn(async move {
        let request = requests.recv().await.expect("hook request");
        assert!(matches!(request, HubToHost::ExecuteHook { .. }));
        drop(host_to_hub);
    });

    let error = host
        .execute_hook(
            &mews_protocol::AgentId::new(),
            "before_model",
            serde_json::json!({}),
            std::path::Path::new("/"),
            &mews_agent::CancellationToken::new(),
        )
        .await
        .unwrap_err();
    remote.await.unwrap();

    assert!(mews_agent::effect_uncertainty(&error).is_some());
}

#[tokio::test]
async fn failure_before_tool_dispatch_is_not_uncertain() {
    let (hub_to_host, requests) = tokio::sync::mpsc::channel(1);
    let (_host_to_hub, replies) = tokio::sync::mpsc::channel(1);
    drop(requests);
    let host = ConnectedHost::from_channels(HostId::new(), Vec::new(), hub_to_host, replies)
        .await
        .unwrap();

    let error = host
        .execute_tool(
            &mews_protocol::AgentId::new(),
            "effect",
            serde_json::json!({}),
            std::path::Path::new("/"),
        )
        .await
        .unwrap_err();

    assert!(mews_agent::effect_uncertainty(&error).is_none());
}

#[tokio::test]
async fn remote_tool_error_reply_is_definitive() {
    let (hub_to_host, mut requests) = tokio::sync::mpsc::channel(1);
    let (host_to_hub, replies) = tokio::sync::mpsc::channel(1);
    let host = ConnectedHost::from_channels(HostId::new(), Vec::new(), hub_to_host, replies)
        .await
        .unwrap();
    let remote = tokio::spawn(async move {
        let HubToHost::ExecuteTool { request_id, .. } =
            requests.recv().await.expect("tool request")
        else {
            panic!("expected tool request");
        };
        host_to_hub
            .send(HostToHub::ToolResult {
                request_id,
                result: serde_json::Value::Null,
                error: Some("tool rejected the input".into()),
            })
            .await
            .unwrap();
    });

    let error = host
        .execute_tool(
            &mews_protocol::AgentId::new(),
            "effect",
            serde_json::json!({}),
            std::path::Path::new("/"),
        )
        .await
        .unwrap_err();
    remote.await.unwrap();

    assert!(mews_agent::effect_uncertainty(&error).is_none());
    assert_eq!(error.to_string(), "tool rejected the input");
}

#[tokio::test]
async fn disconnect_after_acp_dispatch_is_an_uncertain_effect() {
    let (hub_to_host, mut requests) = tokio::sync::mpsc::channel(1);
    let (host_to_hub, replies) = tokio::sync::mpsc::channel(1);
    let host = ConnectedHost::from_channels(HostId::new(), Vec::new(), hub_to_host, replies)
        .await
        .unwrap();
    let remote = tokio::spawn(async move {
        let request = requests.recv().await.expect("ACP request");
        assert!(matches!(request, HubToHost::ExecuteAcpTurn { .. }));
        drop(host_to_hub);
    });
    let (events, _event_receiver) = tokio::sync::mpsc::channel(1);

    let error = host
        .execute_acp_turn(
            RemoteAcpTurn {
                agent_id: mews_protocol::AgentId::new(),
                harness: "fixture".into(),
                harness_options: Default::default(),
                tools: Vec::new(),
                cwd: std::path::PathBuf::from("/"),
                prompt: "prompt".into(),
                recovery_prompt: "recovery".into(),
                agent_slug: "agent".into(),
                soul: "soul".into(),
                mews_session_id: "session".into(),
                turn_id: "turn".into(),
                transition: mews_protocol::AcpBindingTransition::New,
                context: None,
            },
            events,
            &mews_agent::CancellationToken::new(),
        )
        .await
        .unwrap_err();
    remote.await.unwrap();

    assert!(mews_agent::effect_uncertainty(&error).is_some());
}

#[cfg(unix)]
#[tokio::test]
async fn dropping_remote_tool_request_stops_shell_descendants() {
    let directory = tempfile::tempdir().unwrap();
    let pid_path = directory.path().join("remote-descendant.pid");
    let host = ConnectedHost::in_process(HostId::new(), ToolRegistry::with_defaults())
        .await
        .unwrap();
    let cwd = directory.path().canonicalize().unwrap();
    let command = format!(
        "sleep 30 & child=$!; printf %s $child > {}; wait",
        pid_path.display()
    );
    let execution = tokio::spawn(async move {
        host.execute_tool(
            &mews_protocol::AgentId::new(),
            "bash",
            serde_json::json!({"command": command, "timeout_seconds": 30}),
            &cwd,
        )
        .await
    });
    let descendant = loop {
        if let Ok(pid) = std::fs::read_to_string(&pid_path)
            && let Ok(pid) = pid.parse::<i32>()
        {
            break pid;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };

    execution.abort();
    let _ = execution.await;
    for _ in 0..100 {
        // SAFETY: signal 0 only queries whether the test child still exists.
        if unsafe { libc::kill(descendant, 0) } == -1 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("cancelled remote shell descendant {descendant} is still running");
}

#[cfg(unix)]
#[tokio::test]
async fn cancelling_remote_capability_execution_stops_shell_descendants() {
    struct NoProgress;
    #[async_trait::async_trait(?Send)]
    impl mews_agent::ProgressReporter for NoProgress {
        async fn report(&self, _progress: serde_json::Value) -> anyhow::Result<()> {
            Ok(())
        }
    }

    let directory = tempfile::tempdir().unwrap();
    let pid_path = directory.path().join("cancelled-descendant.pid");
    let host = ConnectedHost::in_process(HostId::new(), ToolRegistry::with_defaults())
        .await
        .unwrap();
    let cancellation = mews_agent::CancellationToken::new();
    let trigger = cancellation.clone();
    let command = format!(
        "sleep 30 & child=$!; printf %s $child > {}; wait",
        pid_path.display()
    );
    let call = mews_agent::ToolCall {
        id: "remote-cancel".into(),
        name: "bash".into(),
        arguments: serde_json::json!({"command": command, "timeout_seconds": 30}),
        thought_signature: None,
    };
    let cwd = directory.path().canonicalize().unwrap();
    let agent_id = mews_protocol::AgentId::new();
    let execution = mews_agent::AgentCapabilities::execute(
        &host,
        &agent_id,
        &call,
        &cwd,
        &cancellation,
        &NoProgress,
    );
    tokio::pin!(execution);
    let descendant = loop {
        tokio::select! {
            result = &mut execution => panic!("shell exited before cancellation: {result:?}"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {
                if let Ok(pid) = std::fs::read_to_string(&pid_path)
                    && let Ok(pid) = pid.parse::<i32>()
                {
                    break pid;
                }
            }
        }
    };
    trigger.cancel();
    assert!(execution.await.is_err());
    for _ in 0..100 {
        // SAFETY: signal 0 only queries whether the test child still exists.
        if unsafe { libc::kill(descendant, 0) } == -1 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("cancelled remote shell descendant {descendant} is still running");
}

#[tokio::test]
async fn connected_host_publishes_and_refreshes_its_harness_catalog() {
    let host = ConnectedHost::in_process(HostId::new(), ToolRegistry::with_defaults())
        .await
        .unwrap();
    assert!(
        host.harness_catalog()
            .iter()
            .any(|descriptor| descriptor.name == "mews" && descriptor.availability.ready())
    );

    let refreshed = host.refresh_harness_catalog().await.unwrap();
    assert!(
        refreshed
            .iter()
            .any(|descriptor| descriptor.name == "codex")
    );
    assert_eq!(host.harness_catalog(), refreshed);
}

#[tokio::test]
async fn host_reads_context_and_refuses_to_overwrite_a_changed_replica() {
    let root = tempfile::tempdir().unwrap();
    let project = tempfile::tempdir().unwrap();
    std::fs::write(project.path().join("AGENTS.md"), "Host-only context").unwrap();
    let cwd = project.path().canonicalize().unwrap();
    let context = handle_host_request(
        &ToolRegistry::with_defaults(),
        Some(root.path()),
        HubToHost::ReadProjectContext {
            request_id: RequestId::new(),
            agent_slug: "coder".into(),
            canonical_cwd: cwd,
        },
    )
    .await;
    assert!(
        matches!(context, HostToHub::ProjectContext { context: Some(value), .. } if value.contains("Host-only context"))
    );

    let agent = Agent {
        id: mews_protocol::AgentId::new(),
        slug: "coder".into(),
        current_revision: 1,
        archived: false,
        created_at: Utc::now(),
    };
    let revision = AgentRevision {
        agent_id: agent.id.clone(),
        revision: 1,
        soul: "canonical".into(),
        config_toml: "harness = \"mews\"\ntools = [\"*\"]\n".into(),
        content_hash: "unused".into(),
        author_host_id: HostId::new(),
        created_at: Utc::now(),
    };
    materialize_agent(root.path(), &agent, &revision, None, None).unwrap();
    let observed = read_agent(root.path(), "coder").unwrap().unwrap();
    std::fs::write(root.path().join("agents/coder/SOUL.md"), "late edit").unwrap();
    assert!(materialize_agent(root.path(), &agent, &revision, Some(&observed), None).is_err());
    assert_eq!(
        std::fs::read_to_string(root.path().join("agents/coder/SOUL.md")).unwrap(),
        "late edit"
    );
}

#[test]
fn host_renames_a_clean_agent_replica_and_retains_one_backup() {
    let root = tempfile::tempdir().unwrap();
    let agent = Agent {
        id: mews_protocol::AgentId::new(),
        slug: "coder".into(),
        current_revision: 1,
        archived: false,
        created_at: Utc::now(),
    };
    let revision = AgentRevision {
        agent_id: agent.id.clone(),
        revision: 1,
        soul: "canonical".into(),
        config_toml: "harness = \"mews\"\ntools = [\"*\"]\n".into(),
        content_hash: "unused".into(),
        author_host_id: HostId::new(),
        created_at: Utc::now(),
    };
    materialize_agent(root.path(), &agent, &revision, None, None).unwrap();
    std::fs::create_dir_all(root.path().join("agents/coder/skills/review")).unwrap();
    std::fs::write(
        root.path().join("agents/coder/skills/review/SKILL.md"),
        "skill",
    )
    .unwrap();
    std::fs::create_dir_all(root.path().join("agents/coder/extensions")).unwrap();
    std::fs::write(
        root.path().join("agents/coder/extensions/policy.toml"),
        "name = 'policy'\ncommand = ['true']\n",
    )
    .unwrap();
    let observed = read_agent(root.path(), "coder").unwrap().unwrap();
    let renamed = Agent {
        slug: "builder".into(),
        ..agent
    };

    materialize_agent(
        root.path(),
        &renamed,
        &revision,
        Some(&observed),
        Some("coder"),
    )
    .unwrap();

    assert!(!root.path().join("agents/coder").exists());
    assert_eq!(
        read_agent(root.path(), "builder").unwrap().unwrap(),
        observed
    );
    assert_eq!(
        std::fs::read_to_string(root.path().join("agents/builder/skills/review/SKILL.md")).unwrap(),
        "skill"
    );
    assert_eq!(
        std::fs::read_to_string(root.path().join("agents/builder/extensions/policy.toml")).unwrap(),
        "name = 'policy'\ncommand = ['true']\n"
    );
    let backups = std::fs::read_dir(root.path().join("agents"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".coder.previous-")
        })
        .count();
    assert_eq!(backups, 1);
}

#[test]
fn host_rejects_rename_when_the_old_replica_changed_after_preflight() {
    let root = tempfile::tempdir().unwrap();
    let agent = Agent {
        id: mews_protocol::AgentId::new(),
        slug: "coder".into(),
        current_revision: 1,
        archived: false,
        created_at: Utc::now(),
    };
    let revision = AgentRevision {
        agent_id: agent.id.clone(),
        revision: 1,
        soul: "canonical".into(),
        config_toml: "harness = \"mews\"\ntools = [\"*\"]\n".into(),
        content_hash: "unused".into(),
        author_host_id: HostId::new(),
        created_at: Utc::now(),
    };
    materialize_agent(root.path(), &agent, &revision, None, None).unwrap();
    let observed = read_agent(root.path(), "coder").unwrap().unwrap();
    std::fs::write(root.path().join("agents/coder/SOUL.md"), "late edit").unwrap();
    let renamed = Agent {
        slug: "builder".into(),
        ..agent
    };

    let error = materialize_agent(
        root.path(),
        &renamed,
        &revision,
        Some(&observed),
        Some("coder"),
    )
    .unwrap_err()
    .to_string();

    assert!(error.contains("replica changed"));
    assert!(!root.path().join("agents/builder").exists());
    assert_eq!(
        std::fs::read_to_string(root.path().join("agents/coder/SOUL.md")).unwrap(),
        "late edit"
    );
}

#[test]
fn prepared_hub_requires_the_source_activation_nonce() {
    let hub_root = tempfile::tempdir().unwrap();
    let target_root = tempfile::tempdir().unwrap();
    let mut state = mews::app::Mews::setup(hub_root.path(), "laptop").unwrap();
    let mut mews = state.commands(mews_store::CommandContext::system());
    std::fs::write(hub_root.path().join("auth.json"), "{}").unwrap();
    let offer = mews.create_invitation(Some("ws://127.0.0.1:9000")).unwrap();
    let identity =
        mews_transport::HostIdentity::load_or_create(&target_root.path().join("secrets/host.key"))
            .unwrap();
    let noise = mews_transport::NoiseIdentity::load_or_create(
        &target_root.path().join("secrets/host-noise.key"),
    )
    .unwrap();
    let request = mews::enrollment::JoinRequest::create(
        &offer,
        "mini-pc".into(),
        &identity,
        &noise,
        "ws://mini-pc.local:8787".into(),
    );
    let target = mews.enroll_host(&offer, &request).unwrap();
    let snapshot = mews.begin_hub_move(&target.id).unwrap();
    let database = std::fs::read(&snapshot.database_path).unwrap();
    assert_eq!(snapshot.database_size, database.len() as u64);
    assert_eq!(
        snapshot.database_sha256,
        format!("{:x}", Sha256::digest(&database))
    );
    let database_sha256 = format!("{:x}", Sha256::digest(&database));
    let credentials_sha256 = format!("{:x}", Sha256::digest(&snapshot.credentials));
    begin_hub_transfer(
        target_root.path(),
        HubTransferStart {
            move_nonce: snapshot.move_nonce.clone(),
            installation_id: snapshot.installation_id,
            generation: snapshot.generation,
            target_host_id: snapshot.target_hub,
            database_size: snapshot.database_size,
            database_sha256,
            installation_key: snapshot.installation_key,
            hub_noise_key: snapshot.hub_noise_key,
            credentials: snapshot.credentials,
            credentials_sha256,
        },
    )
    .unwrap();
    let mut offset = 0;
    for chunk in database.chunks(96 * 1024) {
        offset = write_hub_transfer(target_root.path(), offset, chunk).unwrap();
    }
    commit_hub_transfer(target_root.path()).unwrap();

    assert!(activate_hub_transfer(target_root.path()).is_err());
    assert!(arm_hub_transfer(target_root.path(), "wrong nonce").is_err());
    arm_hub_transfer(target_root.path(), &snapshot.move_nonce).unwrap();
    // Simulate a crash after one credential and the database have already
    // reached their active names. Retrying must accept both completed steps.
    std::fs::rename(
        target_root.path().join("secrets/installation.key.prepared"),
        target_root.path().join("secrets/installation.key"),
    )
    .unwrap();
    std::fs::rename(
        target_root.path().join("mews.db.prepared"),
        target_root.path().join("mews.db"),
    )
    .unwrap();
    activate_hub_transfer(target_root.path()).unwrap();
    // Activation is replay-safe when the acknowledgement was lost after the
    // prepared database became active.
    activate_hub_transfer(target_root.path()).unwrap();
    let installation = Store::open(target_root.path().join("mews.db"))
        .unwrap()
        .installation()
        .unwrap()
        .unwrap();
    assert_eq!(installation.generation, 2);
}

fn prepared_hub_with_previous_database() -> (tempfile::TempDir, String) {
    let hub_root = tempfile::tempdir().unwrap();
    let target_root = tempfile::tempdir().unwrap();
    let mut state = mews::app::Mews::setup(hub_root.path(), "laptop").unwrap();
    let mut mews = state.commands(mews_store::CommandContext::system());
    std::fs::write(hub_root.path().join("auth.json"), "{}").unwrap();
    let offer = mews.create_invitation(Some("ws://127.0.0.1:9000")).unwrap();
    let identity =
        mews_transport::HostIdentity::load_or_create(&target_root.path().join("secrets/host.key"))
            .unwrap();
    let noise = mews_transport::NoiseIdentity::load_or_create(
        &target_root.path().join("secrets/host-noise.key"),
    )
    .unwrap();
    let request = mews::enrollment::JoinRequest::create(
        &offer,
        "mini-pc".into(),
        &identity,
        &noise,
        "ws://mini-pc.local:8787".into(),
    );
    let target = mews.enroll_host(&offer, &request).unwrap();
    // A former Hub activation has a valid but older canonical database to retain.
    Store::open(hub_root.path().join("mews.db"))
        .unwrap()
        .backup_to(&target_root.path().join("mews.db"))
        .unwrap();
    let snapshot = mews.begin_hub_move(&target.id).unwrap();
    let database = std::fs::read(&snapshot.database_path).unwrap();
    begin_hub_transfer(
        target_root.path(),
        HubTransferStart {
            move_nonce: snapshot.move_nonce.clone(),
            installation_id: snapshot.installation_id.clone(),
            generation: snapshot.generation,
            target_host_id: snapshot.target_hub.clone(),
            database_size: snapshot.database_size,
            database_sha256: snapshot.database_sha256.clone(),
            installation_key: snapshot.installation_key.clone(),
            hub_noise_key: snapshot.hub_noise_key.clone(),
            credentials: snapshot.credentials.clone(),
            credentials_sha256: format!("{:x}", Sha256::digest(&snapshot.credentials)),
        },
    )
    .unwrap();
    let mut offset = 0;
    for chunk in database.chunks(96 * 1024) {
        offset = write_hub_transfer(target_root.path(), offset, chunk).unwrap();
    }
    commit_hub_transfer(target_root.path()).unwrap();
    arm_hub_transfer(target_root.path(), &snapshot.move_nonce).unwrap();
    (target_root, snapshot.move_nonce)
}

#[test]
fn hub_activation_replays_after_every_durable_transition() {
    // Each stage simulates a crash immediately after one more durable action
    // from activate_hub_transfer's ordered sequence.
    for stage in 0..=11 {
        let (target_root, _) = prepared_hub_with_previous_database();
        let root = target_root.path();
        if stage >= 1 {
            write_private(root.join("hub-activate"), b"activate").unwrap();
        }
        for (credential_stage, (prepared, active)) in [
            (
                "secrets/installation.key.prepared",
                "secrets/installation.key",
            ),
            ("secrets/hub-noise.key.prepared", "secrets/hub-noise.key"),
            ("auth.json.prepared", "auth.json"),
        ]
        .into_iter()
        .enumerate()
        {
            if stage >= credential_stage + 2 {
                std::fs::rename(root.join(prepared), root.join(active)).unwrap();
            }
        }
        if stage >= 5 {
            retain_previous_file(root, "mews.db").unwrap();
        }
        if stage >= 6 {
            std::fs::rename(root.join("mews.db.prepared"), root.join("mews.db")).unwrap();
        }
        if stage >= 7 {
            write_private(root.join("hub-promote"), b"ready").unwrap();
        }
        if stage >= 8 {
            write_private(
                root.join("hub-transfer.activated.json"),
                &std::fs::read(root.join("hub-transfer.json")).unwrap(),
            )
            .unwrap();
        }
        if stage >= 9 {
            std::fs::remove_file(root.join("hub-activate")).unwrap();
        }
        if stage >= 10 {
            std::fs::remove_file(root.join("hub-activation-token")).unwrap();
        }
        if stage >= 11 {
            std::fs::remove_file(root.join("hub-transfer.json")).unwrap();
        }

        activate_hub_transfer(root).unwrap_or_else(|error| {
            panic!("activation replay failed after transition {stage}: {error:#}")
        });
        activate_hub_transfer(root).unwrap();
        let installation = Store::open(root.join("mews.db"))
            .unwrap()
            .installation()
            .unwrap()
            .unwrap();
        assert_eq!(installation.generation, 2, "transition {stage}");
        assert!(
            !root.join("hub-transfer.json").exists(),
            "transition {stage}"
        );
        assert!(root.join("hub-transfer.activated.json").exists());
        let backups = std::fs::read_dir(root)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("mews.db.previous-")
            })
            .count();
        assert_eq!(backups, 1, "transition {stage}");
    }
}

#[test]
fn previous_file_retention_keeps_only_the_latest_predecessor() {
    let root = tempfile::tempdir().unwrap();
    for contents in [b"first".as_slice(), b"second".as_slice()] {
        std::fs::write(root.path().join("mews.db"), contents).unwrap();
        retain_previous_file(root.path(), "mews.db").unwrap();
    }
    let backups = std::fs::read_dir(root.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("mews.db.previous-")
        })
        .collect::<Vec<_>>();
    assert_eq!(backups.len(), 1);
    assert_eq!(std::fs::read(backups[0].path()).unwrap(), b"second");
}

#[test]
fn activation_replaces_and_retains_a_stale_credential() {
    let (target_root, _) = prepared_hub_with_previous_database();
    let root = target_root.path();
    std::fs::write(root.join("auth.json"), br#"{"stale":true}"#).unwrap();

    activate_hub_transfer(root).unwrap();

    assert_eq!(std::fs::read(root.join("auth.json")).unwrap(), b"{}");
    let backups = std::fs::read_dir(root)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("auth.json.previous-")
        })
        .collect::<Vec<_>>();
    assert_eq!(backups.len(), 1);
    assert_eq!(
        std::fs::read(backups[0].path()).unwrap(),
        br#"{"stale":true}"#
    );
}
