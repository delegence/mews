use std::{
    fs,
    process::{Child, Command, Stdio},
    time::Duration,
};

use anyhow::Context;

#[test]
fn hub_exits_when_its_router_stops() {
    let state = tempfile::tempdir().unwrap();
    mews::app::Mews::setup(state.path(), "laptop").unwrap();
    let binary = env!("CARGO_BIN_EXE_mews");
    let mut hub = Command::new(binary)
        .arg("--root")
        .arg(state.path())
        .args(["hub", "serve"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let router = mews_router::RouterClient::new(state.path());
    runtime.block_on(async {
        for _ in 0..400 {
            if router.ready().await {
                router.shutdown().await.unwrap();
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("router did not become ready");
    });
    for _ in 0..400 {
        if let Some(status) = hub.try_wait().unwrap() {
            assert!(!status.success());
            return;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    let _ = hub.kill();
    panic!("Hub kept running after its router stopped");
}

#[test]
fn locationless_channel_turns_deliver_through_durable_events() {
    let state = tempfile::tempdir().unwrap();
    fs::write(state.path().join(".test-provider"), []).unwrap();
    let project = tempfile::tempdir().unwrap();
    let binary = env!("CARGO_BIN_EXE_mews");
    run(
        binary,
        state.path(),
        project.path(),
        &[
            "setup",
            "--name",
            "laptop",
            "--relay",
            "wss://relay.invalid",
            "--relay-listen",
            "127.0.0.1:0",
            "--no-daemon",
        ],
    );
    let _hub = HubGuard {
        binary,
        state: state.path(),
        cwd: project.path(),
    };
    configure_test_model(state.path());
    run(
        binary,
        state.path(),
        project.path(),
        &["agents", "new", "coder"],
    );

    let channel_state = state.path().join("test-channel.db");
    let second_channel_state = state.path().join("second-test-channel.db");
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::unbounded_channel();
    let (acknowledged_tx, _acknowledged_rx) = tokio::sync::mpsc::unbounded_channel();
    let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    let (second_inbound_tx, second_inbound_rx) = tokio::sync::mpsc::unbounded_channel();
    let (second_ack_tx, _second_ack_rx) = tokio::sync::mpsc::unbounded_channel();
    let (second_outbound_tx, mut second_outbound_rx) = tokio::sync::mpsc::unbounded_channel();
    inbound_tx
        .send(mews_channel::InboundMessage {
            external_id: "message-1".into(),
            conversation: "conversation-1".into(),
            text: "hello".into(),
            metadata: serde_json::Value::Null,
        })
        .unwrap();
    let channel = TestChannel {
        inbound: inbound_rx,
        acknowledged: acknowledged_tx,
        outbound: TestChannelOutboundBehavior::Immediate(outbound_tx),
    };
    let second_channel = TestChannel {
        inbound: second_inbound_rx,
        acknowledged: second_ack_tx,
        outbound: TestChannelOutboundBehavior::Immediate(second_outbound_tx),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(tokio::task::LocalSet::new().run_until(async {
        let mut client = mews_client::MewsClient::connect(state.path())
            .await
            .unwrap();
        let session = client.start_session("coder", None).await.unwrap();
        let mut channel = mews_channel::ChannelRuntime::open(
            channel,
            state.path(),
            &channel_state,
            mews_channel::ChannelConfig::new("coder"),
        )
        .await
        .unwrap();
        channel
            .attach("conversation-1", session.id.clone())
            .await
            .unwrap();
        let mut second_channel = mews_channel::ChannelRuntime::open(
            second_channel,
            state.path(),
            &second_channel_state,
            mews_channel::ChannelConfig::new("coder"),
        )
        .await
        .unwrap();
        second_channel
            .attach("conversation-2", session.id)
            .await
            .unwrap();
        let broadcast = second_channel.handle();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let first_task = tokio::task::spawn_local(channel.run_until(shutdown_rx.clone()));
        let second_task = tokio::task::spawn_local(second_channel.run_until(shutdown_rx));
        let delivered = tokio::time::timeout(Duration::from_secs(5), outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(delivered.ends_with("hello [test]"));
        assert!(
            tokio::time::timeout(Duration::from_millis(200), second_outbound_rx.recv())
                .await
                .is_err(),
            "a second Channel attached to the Session must not receive the normal reply"
        );
        second_inbound_tx
            .send(mews_channel::InboundMessage {
                external_id: "message-1".into(),
                conversation: "conversation-2".into(),
                text: "different second input".into(),
                metadata: serde_json::Value::Null,
            })
            .unwrap();
        let second_reply = tokio::time::timeout(Duration::from_secs(5), second_outbound_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(second_reply.ends_with("different second input [test]"));
        assert!(
            tokio::time::timeout(Duration::from_millis(200), outbound_rx.recv())
                .await
                .is_err(),
            "the first Channel must not receive the second identity's reply"
        );
        broadcast
            .broadcast(
                ["conversation-2".into()],
                mews_channel::BroadcastMessage {
                    idempotency_key: "broadcast-1".into(),
                    event: mews_channel::OutboundEvent::CompletedMessage {
                        text: "explicit broadcast".into(),
                    },
                },
            )
            .await
            .unwrap();
        assert_eq!(
            second_outbound_rx.recv().await.unwrap(),
            "explicit broadcast"
        );
        shutdown_tx.send(true).unwrap();
        first_task.await.unwrap().unwrap();
        second_task.await.unwrap().unwrap();
    }));

    let database = rusqlite::Connection::open(state.path().join("mews.db")).unwrap();
    let cwd: String = database
        .query_row(
            "SELECT working_directory FROM sessions ORDER BY created_at DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        std::path::PathBuf::from(cwd),
        std::path::PathBuf::from(std::env::var_os("HOME").unwrap())
            .canonicalize()
            .unwrap()
    );
}

#[test]
fn stalled_channel_delivery_does_not_stop_inbound_intake() {
    let state = tempfile::tempdir().unwrap();
    fs::write(state.path().join(".test-provider"), []).unwrap();
    let project = tempfile::tempdir().unwrap();
    let binary = env!("CARGO_BIN_EXE_mews");
    run(
        binary,
        state.path(),
        project.path(),
        &[
            "setup",
            "--name",
            "laptop",
            "--relay",
            "wss://relay.invalid",
            "--relay-listen",
            "127.0.0.1:0",
            "--no-daemon",
        ],
    );
    let _hub = HubGuard {
        binary,
        state: state.path(),
        cwd: project.path(),
    };
    configure_test_model(state.path());
    run(
        binary,
        state.path(),
        project.path(),
        &["agents", "new", "coder"],
    );

    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::unbounded_channel();
    let (acknowledged_tx, mut acknowledged_rx) = tokio::sync::mpsc::unbounded_channel();
    let (stalled_tx, mut stalled_rx) = tokio::sync::mpsc::unbounded_channel();
    inbound_tx
        .send(mews_channel::InboundMessage {
            external_id: "message-1".into(),
            conversation: "conversation-1".into(),
            text: "hello".into(),
            metadata: serde_json::Value::Null,
        })
        .unwrap();
    let channel = TestChannel {
        inbound: inbound_rx,
        acknowledged: acknowledged_tx,
        outbound: TestChannelOutboundBehavior::Stalled(stalled_tx),
    };
    let channel_state = state.path().join("stalled-channel.db");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(tokio::task::LocalSet::new().run_until(async {
        let channel = mews_channel::ChannelRuntime::open(
            channel,
            state.path(),
            &channel_state,
            mews_channel::ChannelConfig::new("coder"),
        )
        .await
        .unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let task = tokio::task::spawn_local(channel.run_until(shutdown_rx));
        tokio::time::timeout(Duration::from_secs(5), stalled_rx.recv())
            .await
            .unwrap()
            .unwrap();

        // Channel delivery is intentionally ack-before-send, even while the platform call stalls.
        let channel_db = rusqlite::Connection::open(&channel_state).unwrap();
        let consumer: String = channel_db
            .query_row(
                "SELECT value FROM metadata WHERE key = 'consumer_id'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let hub_db = rusqlite::Connection::open(state.path().join("mews.db")).unwrap();
        let cursor: u64 = hub_db
            .query_row(
                "SELECT cursor FROM client_consumers WHERE id = ?1",
                [consumer],
                |row| row.get(0),
            )
            .unwrap();
        assert!(cursor > 0);

        inbound_tx
            .send(mews_channel::InboundMessage {
                external_id: "message-2".into(),
                conversation: "conversation-1".into(),
                text: "second message".into(),
                metadata: serde_json::Value::Null,
            })
            .unwrap();
        loop {
            let acknowledged = tokio::time::timeout(Duration::from_secs(1), acknowledged_rx.recv())
                .await
                .unwrap()
                .unwrap();
            if acknowledged == "message-2" {
                break;
            }
        }
        shutdown_tx.send(true).unwrap();
        task.await.unwrap().unwrap();
    }));
}

#[test]
fn setup_agent_and_cwd_bound_tool_turn_work_end_to_end() {
    let state = tempfile::tempdir().unwrap();
    fs::write(state.path().join(".test-provider"), []).unwrap();
    let project = tempfile::tempdir().unwrap();
    fs::write(
        project.path().join("note.txt"),
        "from the selected directory\n",
    )
    .unwrap();
    fs::create_dir_all(project.path().join(".agents/prompts")).unwrap();
    fs::write(
        project.path().join(".agents/prompts/greet.md"),
        "---\ndescription: Greet someone\n---\nhello $1",
    )
    .unwrap();
    let hook_marker = project.path().join("hook-ran");
    let binary = env!("CARGO_BIN_EXE_mews");
    let port = free_port();
    let relay_address = format!("127.0.0.1:{port}");
    let relay_url = format!("ws://{relay_address}");
    let relay = Command::new(binary)
        .args(["relay", "serve", "--listen", &relay_address])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let _relay = ChildGuard(Some(relay));
    for _ in 0..100 {
        if std::net::TcpStream::connect(&relay_address).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    run(
        binary,
        state.path(),
        project.path(),
        &[
            "setup",
            "--name",
            "laptop",
            "--relay",
            "wss://relay.invalid",
            "--relay-listen",
            "127.0.0.1:0",
            "--no-daemon",
        ],
    );
    let _hub = HubGuard {
        binary,
        state: state.path(),
        cwd: project.path(),
    };
    configure_test_model(state.path());
    run(
        binary,
        state.path(),
        project.path(),
        &["agents", "new", "coder"],
    );
    let extensions = state.path().join("agents/coder/extensions");
    fs::create_dir(&extensions).unwrap();
    fs::write(
        extensions.join("observe.toml"),
        format!(
            "name = \"observe\"\ncommand = [\"sh\", \"-c\", {:?}]\nhooks = [\"turn_start\"]\n",
            format!("cat >/dev/null; : > {}; printf null", hook_marker.display())
        ),
    )
    .unwrap();
    // Restarting performs a synchronous initial extension scan, avoiding a
    // timing assumption about the live catalog watcher's polling interval.
    run(binary, state.path(), project.path(), &["hub", "stop"]);
    for _ in 0..100 {
        if !state.path().join("hub.sock").exists() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let restarted_hub = Command::new(binary)
        .arg("--root")
        .arg(state.path())
        .args(["hub", "serve"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let _restarted_hub = ChildGuard(Some(restarted_hub));
    wait_for_status(binary, state.path(), project.path());
    let prompted = run(
        binary,
        state.path(),
        project.path(),
        &["agents", "coder", "-p", "/greet", "world"],
    );
    assert!(prompted.contains("hello world [test]"));
    assert!(hook_marker.exists());
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        use mews_client::{ClientEventKind, ConsumerId, MessageSource, MewsClient, SourceKind};
        let mut requests = MewsClient::connect(state.path()).await.unwrap();
        let mut events = MewsClient::connect(state.path()).await.unwrap();
        let session = requests
            .start_session("coder", Some(project.path().to_path_buf()))
            .await
            .unwrap();
        let consumer = ConsumerId::new();
        events
            .subscribe(consumer.clone(), session.id.clone())
            .await
            .unwrap();
        requests
            .start_turn(
                session.id,
                "async hello".into(),
                serde_json::Value::Null,
                MessageSource {
                    kind: SourceKind::Client,
                    id: "e2e".into(),
                    channel_origin: None,
                },
            )
            .await
            .unwrap();
        let mut saw_message = false;
        let mut saw_completion = false;
        for _ in 0..10 {
            let batch = events.poll_events(consumer.clone(), 2_000).await.unwrap();
            for event in &batch.events {
                saw_message |= matches!(event.kind, ClientEventKind::AssistantMessage { .. });
                saw_completion |= matches!(event.kind, ClientEventKind::TurnCompleted { .. });
            }
            if batch.advanced {
                events
                    .acknowledge(consumer.clone(), batch.checkpoint)
                    .await
                    .unwrap();
            }
            if saw_message && saw_completion {
                break;
            }
        }
        assert!(
            saw_message && saw_completion,
            "async turn events were not delivered"
        );
    });
    let invitation = run(
        binary,
        state.path(),
        project.path(),
        &["hosts", "invite", "--relay", &relay_url],
    );
    let offer = mews::enrollment::JoinOffer::decode(invitation.trim()).unwrap();
    assert_eq!(offer.relay_url, relay_url);
    let joined_state = tempfile::tempdir().unwrap();
    fs::write(joined_state.path().join(".test-provider"), []).unwrap();
    let joined_relay_url = format!("ws://127.0.0.1:{}", free_port());
    run(
        binary,
        joined_state.path(),
        project.path(),
        &[
            "setup",
            "--name",
            "mini-pc",
            "--join",
            invitation.trim(),
            "--relay",
            &joined_relay_url,
            "--no-daemon",
        ],
    );
    let _joined_host = HubGuard {
        binary,
        state: joined_state.path(),
        cwd: project.path(),
    };
    let hosts = run(binary, state.path(), project.path(), &["hosts", "list"]);
    assert!(hosts.contains("mini-pc"));

    run(binary, state.path(), project.path(), &["hub", "stop"]);
    for _ in 0..100 {
        if !state.path().join("hub.sock").exists() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let restarted_hub = Command::new(binary)
        .arg("--root")
        .arg(state.path())
        .args(["hub", "serve"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let _restarted_hub = ChildGuard(Some(restarted_hub));
    let mut reconnected = false;
    for _ in 0..200 {
        let status = Command::new(binary)
            .arg("--root")
            .arg(joined_state.path())
            .arg("status")
            .current_dir(project.path())
            .output()
            .unwrap();
        if status.status.success() {
            reconnected = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(
        reconnected,
        "joined Host did not reconnect after Hub restart"
    );

    let remote_output = run(
        binary,
        joined_state.path(),
        project.path(),
        &["agents", "coder", "-p", "test:read", "note.txt"],
    );
    assert!(remote_output.contains("from the selected directory"));
    let database = rusqlite::Connection::open(state.path().join("mews.db")).unwrap();
    let remote_session: String = database
        .query_row(
            "SELECT id FROM sessions ORDER BY created_at DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let resumed_from_hub = run(
        binary,
        state.path(),
        project.path(),
        &["sessions", &remote_session, "-p", "test:read", "note.txt"],
    );
    assert!(resumed_from_hub.contains("from the selected directory"));
    assert_eq!(
        fs::read_to_string(joined_state.path().join("agents/coder/SOUL.md")).unwrap(),
        mews::app::DEFAULT_SOUL
    );
    assert!(
        joined_state
            .path()
            .join("agents/coder/agent.toml")
            .is_file()
    );
    fs::write(
        joined_state.path().join("agents/coder/SOUL.md"),
        "You are the remotely edited coder.",
    )
    .unwrap();
    let second_remote = run(
        binary,
        joined_state.path(),
        project.path(),
        &["agents", "coder", "-p", "test:echo", "synced"],
    );
    assert!(second_remote.contains("synced"));
    let output = run(
        binary,
        state.path(),
        project.path(),
        &["agents", "coder", "-p", "test:read", "note.txt"],
    );
    assert!(output.contains("from the selected directory"));

    // Keep one client connection across Hub movement to exercise automatic
    // reconnect through the old Hub's transition back to an ordinary Host.
    let reconnect_runtime = tokio::runtime::Runtime::new().unwrap();
    let mut persistent_client = reconnect_runtime
        .block_on(mews_client::MewsClient::connect(state.path()))
        .unwrap();

    run(
        binary,
        state.path(),
        project.path(),
        &["hub", "move", "mini-pc"],
    );
    let mut moved = false;
    let mut last_status = String::new();
    for _ in 0..300 {
        let status = Command::new(binary)
            .arg("--root")
            .arg(joined_state.path())
            .arg("status")
            .current_dir(project.path())
            .output()
            .unwrap();
        if status.status.success()
            && String::from_utf8_lossy(&status.stdout).contains("generation: 2")
        {
            moved = true;
            break;
        }
        last_status = format!(
            "stdout={} stderr={}",
            String::from_utf8_lossy(&status.stdout),
            String::from_utf8_lossy(&status.stderr)
        );
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(
        moved,
        "target Host did not become Hub generation 2: {last_status}; log={}",
        fs::read_to_string(joined_state.path().join("logs/host.log")).unwrap_or_default()
    );
    assert!(joined_state.path().join("auth.json").is_file());
    let mut demoted_connected = false;
    let mut demoted_error = String::new();
    for _ in 0..300 {
        let status = Command::new(binary)
            .arg("--root")
            .arg(state.path())
            .arg("status")
            .current_dir(project.path())
            .output()
            .unwrap();
        if status.status.success() {
            demoted_connected = true;
            break;
        }
        demoted_error = String::from_utf8_lossy(&status.stderr).into_owned();
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(
        demoted_connected,
        "old Hub did not reconnect as a Host: error={demoted_error} source={} target={} state={}",
        fs::read_to_string(state.path().join("logs/hub.log")).unwrap_or_default(),
        fs::read_to_string(joined_state.path().join("logs/host.log")).unwrap_or_default(),
        fs::read_to_string(state.path().join("hub.json")).unwrap_or_default()
    );
    reconnect_runtime
        .block_on(persistent_client.status())
        .expect("existing client did not reconnect after Hub movement");
    assert!(!state.path().join("auth.json").exists());
    let demoted_output = run(
        binary,
        state.path(),
        project.path(),
        &["agents", "coder", "-p", "test:read", "note.txt"],
    );
    assert!(demoted_output.contains("from the selected directory"));

    run(
        binary,
        joined_state.path(),
        project.path(),
        &["hub", "move", "laptop"],
    );
    wait_for_generation(binary, state.path(), project.path(), 3);
    wait_for_status(binary, joined_state.path(), project.path());
    let twice_demoted_output = run(
        binary,
        joined_state.path(),
        project.path(),
        &["agents", "coder", "-p", "test:read", "note.txt"],
    );
    assert!(twice_demoted_output.contains("from the selected directory"));

    let sessions = run(binary, state.path(), project.path(), &["sessions", "list"]);
    assert!(sessions.contains(project.path().to_str().unwrap()));

    let database = rusqlite::Connection::open(state.path().join("mews.db")).unwrap();
    let contextual_entries: u64 = database
        .query_row(
            "SELECT COUNT(*) FROM session_entries WHERE contextual = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let completed: u64 = database
        .query_row(
            "SELECT COUNT(*) FROM turns WHERE status_json = '\"completed\"' AND completed_at IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let canonical_soul: String = database
        .query_row(
            "SELECT soul FROM agent_revisions WHERE revision = 2",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(canonical_soul, "You are the remotely edited coder.");
    assert_eq!(
        contextual_entries, 26,
        "prompt and asynchronous turns remain durable"
    );
    assert_eq!(completed, 8);

    let detached = run(
        binary,
        state.path(),
        project.path(),
        &["agents", "coder", "-p", "test:echo", "detached", "--detach"],
    );
    assert!(detached.contains("session: ses_"));
    assert!(detached.contains("turn: turn_"));
    for _ in 0..100 {
        let completed: u64 = database
            .query_row(
                "SELECT COUNT(*) FROM turns WHERE status_json = '\"completed\"' AND completed_at IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        if completed == 9 {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("detached Turn did not complete in the daemon");
}

struct HubGuard<'a> {
    binary: &'a str,
    state: &'a std::path::Path,
    cwd: &'a std::path::Path,
}

impl Drop for HubGuard<'_> {
    fn drop(&mut self) {
        let _ = Command::new(self.binary)
            .arg("--root")
            .arg(self.state)
            .args(["hub", "stop"])
            .current_dir(self.cwd)
            .output();
    }
}

struct ChildGuard(Option<Child>);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

struct TestChannel {
    inbound: tokio::sync::mpsc::UnboundedReceiver<mews_channel::InboundMessage>,
    acknowledged: tokio::sync::mpsc::UnboundedSender<String>,
    outbound: TestChannelOutboundBehavior,
}

struct TestChannelInbound {
    messages: tokio::sync::mpsc::UnboundedReceiver<mews_channel::InboundMessage>,
    acknowledged: tokio::sync::mpsc::UnboundedSender<String>,
}

enum TestChannelOutboundBehavior {
    Immediate(tokio::sync::mpsc::UnboundedSender<String>),
    Stalled(tokio::sync::mpsc::UnboundedSender<()>),
}

struct TestChannelOutbound(TestChannelOutboundBehavior);

impl mews_channel::Channel for TestChannel {
    type Inbound = TestChannelInbound;
    type Outbound = TestChannelOutbound;

    fn name(&self) -> &str {
        "test"
    }

    fn split(self) -> (Self::Inbound, Self::Outbound) {
        (
            TestChannelInbound {
                messages: self.inbound,
                acknowledged: self.acknowledged,
            },
            TestChannelOutbound(self.outbound),
        )
    }
}

#[async_trait::async_trait]
impl mews_channel::ChannelInbound for TestChannelInbound {
    async fn receive(&mut self) -> anyhow::Result<mews_channel::InboundMessage> {
        self.messages
            .recv()
            .await
            .context("test channel inbound sender closed")
    }

    async fn acknowledge_inbound(&mut self, external_id: &str) -> anyhow::Result<()> {
        self.acknowledged.send(external_id.to_owned())?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl mews_channel::ChannelOutbound for TestChannelOutbound {
    async fn send(
        &self,
        _conversation: &str,
        message: mews_channel::OutboundMessage,
    ) -> mews_channel::DeliveryOutcome {
        match &self.0 {
            TestChannelOutboundBehavior::Immediate(outbound) => {
                let mews_channel::OutboundEvent::CompletedMessage { text } = message.event else {
                    return mews_channel::DeliveryOutcome::Dropped {
                        reason: "unsupported test event".into(),
                    };
                };
                if let Err(error) = outbound.send(text) {
                    return mews_channel::DeliveryOutcome::Dropped {
                        reason: error.to_string(),
                    };
                }
            }
            TestChannelOutboundBehavior::Stalled(started) => {
                if let Err(error) = started.send(()) {
                    return mews_channel::DeliveryOutcome::Dropped {
                        reason: error.to_string(),
                    };
                }
                std::future::pending().await
            }
        }
        mews_channel::DeliveryOutcome::Delivered(mews_channel::DeliveryReceipt {
            external_id: Some("delivered-1".into()),
        })
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn wait_for_status(binary: &str, state: &std::path::Path, cwd: &std::path::Path) {
    for _ in 0..300 {
        let output = Command::new(binary)
            .arg("--root")
            .arg(state)
            .arg("status")
            .current_dir(cwd)
            .output()
            .unwrap();
        if output.status.success() {
            return;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("MEWS endpoint did not become ready");
}

fn wait_for_generation(
    binary: &str,
    state: &std::path::Path,
    cwd: &std::path::Path,
    generation: u64,
) {
    for _ in 0..300 {
        let output = Command::new(binary)
            .arg("--root")
            .arg(state)
            .arg("status")
            .current_dir(cwd)
            .output()
            .unwrap();
        if output.status.success()
            && String::from_utf8_lossy(&output.stdout)
                .contains(&format!("generation: {generation}"))
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("Hub generation {generation} did not become ready");
}

fn run(binary: &str, state: &std::path::Path, cwd: &std::path::Path, args: &[&str]) -> String {
    let output = Command::new(binary)
        .arg("--root")
        .arg(state)
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "command {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn configure_test_model(state: &std::path::Path) {
    fs::write(state.join(".test-provider"), []).unwrap();
    let database = rusqlite::Connection::open(state.join("mews.db")).unwrap();
    database
        .execute(
            "INSERT INTO settings (key, value) VALUES ('default_model', 'test') ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [],
        )
        .unwrap();
}
