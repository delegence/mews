use super::*;
use crate::host::connection::*;
use crate::host::lifecycle::handle_host_request;
use chrono::Utc;
use sha2::{Digest, Sha256};

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
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05"}}' ;;
    *'"id":2'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"fixture"}}' ;;
    *'"id":3'*)
      printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"remote fixture reply"}}}}'
      : > acp-ran-on-bound-host
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{}}'
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
    mews_host::HarnessCatalog::setup(host_root.path(), "fixture")
        .await
        .unwrap();
    let cwd = project.path().canonicalize().unwrap();

    let (requests, mut host_requests) = tokio::sync::mpsc::channel(1);
    let (responses, response_stream) = tokio::sync::mpsc::channel(1);
    let connected =
        ConnectedHost::from_channels(HostId::new(), Vec::new(), requests, response_stream)
            .await
            .unwrap();
    let registry = ToolRegistry::with_defaults();
    let host_root = host_root.path().to_owned();
    tokio::spawn(async move {
        let request = host_requests.recv().await.unwrap();
        let (events, mut event_stream) = tokio::sync::mpsc::unbounded_channel();
        let response = crate::host::handle_host_request_streaming(
            &registry,
            Some(&host_root),
            request,
            Some(events),
            None,
            None,
        )
        .await;
        while let Ok(event) = event_stream.try_recv() {
            responses.send(event).await.unwrap();
        }
        responses.send(response).await.unwrap();
    });

    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let answer = connected
        .run_acp(
            RemoteAcpRun {
                harness: "fixture".into(),
                harness_options: Default::default(),
                tools: vec!["*".into()],
                cwd: cwd.clone(),
                prompt: "canonical remote prompt".into(),
                recovery_prompt: "recovery prompt".into(),
                session_id: None,
            },
            event_tx,
        )
        .await
        .unwrap();

    assert_eq!(answer.answer, "remote fixture reply");
    assert_eq!(answer.session_id, "fixture");
    assert!(matches!(
        event_rx.recv().await,
        Some(mews_protocol::AcpEvent::SessionBound { session_id, replaced: false, .. }) if session_id == "fixture"
    ));
    assert!(matches!(
        event_rx.recv().await,
        Some(mews_protocol::AcpEvent::AssistantDelta { delta, .. }) if delta == "remote fixture reply"
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
            "read",
            serde_json::json!({"path":"hello.txt"}),
            &cwd.path().canonicalize().unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result["content"], "remote content");
}

#[tokio::test]
async fn connected_host_observes_runtime_catalog_changes() {
    let registry = ToolRegistry::with_defaults();
    let host = ConnectedHost::in_process(HostId::new(), registry.clone())
        .await
        .unwrap();
    assert!(host.tool_catalog().iter().any(|tool| tool.name == "edit"));
    registry.remove("edit");
    for _ in 0..20 {
        if !host.tool_catalog().iter().any(|tool| tool.name == "edit") {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("runtime tool catalog update was not propagated");
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
            canonical_cwd: cwd,
        },
    )
    .await;
    assert!(
        matches!(context, HostToHub::ProjectContext { context: Some(value), .. } if value.contains("Host-only context"))
    );

    let agent = Agent {
        id: crate::AgentId::new(),
        slug: "coder".into(),
        current_revision: 1,
        archived: false,
        created_at: Utc::now(),
    };
    let revision = AgentRevision {
        agent_id: agent.id.clone(),
        revision: 1,
        soul: "canonical".into(),
        config_toml: crate::service::DEFAULT_CONFIG.into(),
        content_hash: "unused".into(),
        author_host_id: HostId::new(),
        created_at: Utc::now(),
    };
    materialize_agent(root.path(), &agent, &revision, None).unwrap();
    let observed = read_agent(root.path(), "coder").unwrap().unwrap();
    std::fs::write(root.path().join("agents/coder/SOUL.md"), "late edit").unwrap();
    assert!(materialize_agent(root.path(), &agent, &revision, Some(&observed)).is_err());
    assert_eq!(
        std::fs::read_to_string(root.path().join("agents/coder/SOUL.md")).unwrap(),
        "late edit"
    );
}

#[test]
fn prepared_hub_requires_the_source_activation_nonce() {
    let hub_root = tempfile::tempdir().unwrap();
    let target_root = tempfile::tempdir().unwrap();
    let mut mews = crate::service::Mews::setup(hub_root.path(), "laptop").unwrap();
    std::fs::write(hub_root.path().join("auth.json"), "{}").unwrap();
    let offer = mews.create_invitation(Some("ws://127.0.0.1:9000")).unwrap();
    let identity =
        crate::identity::HostIdentity::load_or_create(&target_root.path().join("secrets/host.key"))
            .unwrap();
    let noise = crate::identity::NoiseIdentity::load_or_create(
        &target_root.path().join("secrets/host-noise.key"),
    )
    .unwrap();
    let request = crate::enrollment::JoinRequest::create(
        &offer,
        "mini-pc".into(),
        &identity,
        &noise,
        "ws://mini-pc.local:8787".into(),
    );
    let target = mews.enroll_host(&offer, &request).unwrap();
    let snapshot = mews.begin_hub_move(&target.id).unwrap();
    let database_sha256 = format!("{:x}", Sha256::digest(&snapshot.database));
    let credentials_sha256 = format!("{:x}", Sha256::digest(&snapshot.credentials));
    begin_hub_transfer(
        target_root.path(),
        HubTransferStart {
            move_nonce: snapshot.move_nonce.clone(),
            installation_id: snapshot.installation_id,
            generation: snapshot.generation,
            target_host_id: snapshot.target_hub,
            database_size: snapshot.database.len() as u64,
            database_sha256,
            installation_key: snapshot.installation_key,
            hub_noise_key: snapshot.hub_noise_key,
            credentials: snapshot.credentials,
            credentials_sha256,
        },
    )
    .unwrap();
    let mut offset = 0;
    for chunk in snapshot.database.chunks(96 * 1024) {
        offset = write_hub_transfer(target_root.path(), offset, chunk).unwrap();
    }
    commit_hub_transfer(target_root.path()).unwrap();

    assert!(activate_hub_transfer(target_root.path()).is_err());
    assert!(arm_hub_transfer(target_root.path(), "wrong nonce").is_err());
    arm_hub_transfer(target_root.path(), &snapshot.move_nonce).unwrap();
    activate_hub_transfer(target_root.path()).unwrap();
    let installation = Store::open(target_root.path().join("mews.db"))
        .unwrap()
        .installation()
        .unwrap()
        .unwrap();
    assert_eq!(installation.generation, 2);
}
