use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::identity::{HostIdentity, NoiseIdentity};
use mews_host::ToolRegistry;
use mews_protocol::{
    Agent, AgentReplica, AgentRevision, HarnessDescriptor, HostId, HostToHub, HubToHost,
    HubTransferStart, RequestId,
};
use mews_store::Store;

pub(crate) type AcpPermissionWaiters =
    Arc<Mutex<HashMap<String, tokio::sync::oneshot::Sender<Option<String>>>>>;
pub(crate) type AcpBindingWaiters = Arc<Mutex<HashMap<String, std::sync::mpsc::Sender<()>>>>;

struct HostPermissionHandler {
    request_id: RequestId,
    events: tokio::sync::mpsc::UnboundedSender<HostToHub>,
    waiters: AcpPermissionWaiters,
}

#[async_trait]
impl mews_acp::AcpPermissionHandler for HostPermissionHandler {
    async fn request_permission(
        &self,
        request: &mews_acp::AcpPermissionRequest,
        cancellation: &mews_agent::CancellationToken,
    ) -> Result<mews_acp::AcpPermissionDecision> {
        let permission_id = uuid::Uuid::now_v7().to_string();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.waiters
            .lock()
            .expect("ACP permission waiters poisoned")
            .insert(permission_id.clone(), sender);
        let request = mews_protocol::PermissionRequest {
            id: permission_id.clone(),
            tool_call: request.tool_call.clone(),
            options: request
                .options
                .iter()
                .map(|option| mews_protocol::PermissionOption {
                    id: option.option_id.clone(),
                    name: option.name.clone(),
                    kind: match option.kind {
                        mews_acp::AcpPermissionOptionKind::AllowOnce => "allow_once",
                        mews_acp::AcpPermissionOptionKind::AllowAlways => "allow_always",
                        mews_acp::AcpPermissionOptionKind::RejectOnce => "reject_once",
                        mews_acp::AcpPermissionOptionKind::RejectAlways => "reject_always",
                    }
                    .into(),
                })
                .collect(),
        };
        self.events
            .send(HostToHub::AcpPermissionRequested {
                request_id: self.request_id.clone(),
                request,
            })
            .map_err(|_| anyhow::anyhow!("Hub disconnected during ACP permission request"))?;
        let selected = tokio::select! {
            _ = cancellation.cancelled() => None,
            result = receiver => result.ok().flatten(),
        };
        self.waiters
            .lock()
            .expect("ACP permission waiters poisoned")
            .remove(&permission_id);
        Ok(selected.map_or(
            mews_acp::AcpPermissionDecision::Cancelled,
            mews_acp::AcpPermissionDecision::Selected,
        ))
    }
}

#[cfg(test)]
pub(crate) async fn handle_host_request(
    registry: &ToolRegistry,
    agent_root: Option<&Path>,
    message: HubToHost,
) -> HostToHub {
    handle_host_request_streaming(registry, agent_root, message, None, None, None).await
}

pub(crate) async fn handle_host_request_streaming(
    registry: &ToolRegistry,
    agent_root: Option<&Path>,
    message: HubToHost,
    events: Option<tokio::sync::mpsc::UnboundedSender<HostToHub>>,
    permission_waiters: Option<AcpPermissionWaiters>,
    binding_waiters: Option<AcpBindingWaiters>,
) -> HostToHub {
    if let Some(response) = mews_host::handle_execution_request(registry, &message).await {
        return response;
    }
    match message {
        HubToHost::AcknowledgeAcpSessionBinding { acknowledgement_id } => {
            if let Some(waiters) = binding_waiters
                && let Some(waiter) = waiters
                    .lock()
                    .expect("ACP binding waiters poisoned")
                    .remove(&acknowledgement_id)
            {
                let _ = waiter.send(());
            }
            HostToHub::ConfigurationResult {
                request_id: RequestId::new(),
                error: None,
            }
        }
        HubToHost::ResolveAcpPermission {
            permission_id,
            option_id,
        } => {
            if let Some(waiters) = permission_waiters
                && let Some(waiter) = waiters
                    .lock()
                    .expect("ACP permission waiters poisoned")
                    .remove(&permission_id)
            {
                let _ = waiter.send(option_id);
            }
            HostToHub::ConfigurationResult {
                request_id: RequestId::new(),
                error: None,
            }
        }
        HubToHost::ConfigureRelay {
            request_id,
            active,
            stop_at,
        } => configuration_response(
            request_id,
            configure_relay(agent_root, active, stop_at).await,
        ),
        HubToHost::UpdateRelayCandidates {
            request_id,
            relay_urls,
        } => configuration_response(
            request_id,
            agent_root
                .context("relay candidates require a Host root")
                .and_then(|root| {
                    let path = root.join("hub.json");
                    let mut state: crate::enrollment::relay::JoinedHostState =
                        serde_json::from_slice(&std::fs::read(&path)?)?;
                    state.relay_urls = relay_urls;
                    std::fs::write(path, serde_json::to_vec_pretty(&state)?)?;
                    Ok(())
                }),
        ),
        HubToHost::BeginHubTransfer {
            request_id,
            transfer,
        } => transfer_response(
            request_id,
            agent_root
                .context("Hub transfer requires a remote Host root")
                .and_then(|root| begin_hub_transfer(root, transfer))
                .map(|_| None),
        ),
        HubToHost::WriteHubTransfer {
            request_id,
            offset,
            data,
        } => transfer_response(
            request_id,
            agent_root
                .context("Hub transfer requires a remote Host root")
                .and_then(|root| write_hub_transfer(root, offset, &data))
                .map(Some),
        ),
        HubToHost::CommitHubTransfer { request_id } => transfer_response(
            request_id,
            agent_root
                .context("Hub transfer requires a remote Host root")
                .and_then(commit_hub_transfer)
                .map(|_| None),
        ),
        HubToHost::ArmHubTransfer {
            request_id,
            move_nonce,
        } => transfer_response(
            request_id,
            agent_root
                .context("Hub transfer requires a remote Host root")
                .and_then(|root| arm_hub_transfer(root, &move_nonce))
                .map(|_| None),
        ),
        HubToHost::ActivateHubTransfer { request_id } => transfer_response(
            request_id,
            agent_root
                .context("Hub transfer requires a remote Host root")
                .and_then(activate_hub_transfer)
                .map(|_| None),
        ),
        HubToHost::ReadProjectContext { .. } | HubToHost::ReadPrompt { .. } => {
            unreachable!("execution requests are handled by mews-host")
        }
        HubToHost::RefreshHarnessCatalog { request_id } => catalog_response(
            request_id,
            match agent_root {
                Some(root) => mews_host::HarnessCatalog::refresh(root)
                    .await
                    .map(|catalog| catalog.descriptors()),
                None => {
                    mews_host::HarnessCatalog::discover(None).map(|catalog| catalog.descriptors())
                }
            },
        ),
        HubToHost::ReadAgentReplica { request_id, slug } => {
            let replica = agent_root.map_or(Ok(None), |root| read_agent(root, &slug));
            match replica {
                Ok(replica) => HostToHub::AgentReplica {
                    request_id,
                    replica,
                    error: None,
                },
                Err(error) => HostToHub::AgentReplica {
                    request_id,
                    replica: None,
                    error: Some(error.to_string()),
                },
            }
        }
        HubToHost::RunAcp {
            request_id,
            harness,
            harness_options,
            tools,
            canonical_cwd,
            prompt,
            recovery_prompt,
            acp_session_id,
        } => {
            let result = async {
                let root = agent_root.context("external Harness execution requires a Host root")?;
                let resolved = canonical_cwd
                    .canonicalize()
                    .with_context(|| format!("resolve {}", canonical_cwd.display()))?;
                if resolved != canonical_cwd || !resolved.is_dir() {
                    bail!("Session working directory no longer resolves to its attested path");
                }
                let launch = {
                    let catalog = mews_host::HarnessCatalog::discover(Some(root))?;
                    let descriptor = catalog
                        .descriptors()
                        .into_iter()
                        .find(|descriptor| descriptor.name == harness)
                        .with_context(|| {
                            format!("Harness {harness:?} is not published by this Host")
                        })?;
                    if !descriptor.availability.ready() {
                        bail!(
                            "Harness {harness:?} is not ready on this bound Host: {}",
                            descriptor
                                .availability
                                .detail
                                .unwrap_or_else(|| "setup or refresh is required".into())
                        );
                    }
                    catalog.launch(root, &harness)?
                };
                let mut config = mews_acp::AcpHarnessConfig::new(launch.command)?;
                config.environment = launch.environment;
                if let (Some(events), Some(waiters)) = (events.clone(), permission_waiters.clone())
                {
                    config.permission_handler = Arc::new(HostPermissionHandler {
                        request_id: request_id.clone(),
                        events,
                        waiters,
                    });
                }
                // Host request handling itself is spawned on a multithreaded
                // runtime. Run the capability bridge on its own current-thread
                // runtime so Host extension implementations may retain their
                // existing non-Send async boundary without widening authority.
                let (sender, receiver) = tokio::sync::oneshot::channel();
                let registry = registry.clone();
                let host_root = root.to_path_buf();
                let event_sender = events.clone();
                let event_request_id = request_id.clone();
                let failed_harness = harness.clone();
                let binding_waiters = binding_waiters.clone();
                std::thread::Builder::new()
                    .name("mews-acp-run".into())
                    .spawn(move || {
                        let result = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .context("create ACP extension runtime")
                            .and_then(|runtime| {
                                let environment = mews_host::LocalEnvironment::new(
                                    Some(host_root),
                                    Arc::new(registry),
                                );
                                runtime.block_on(
                                    mews_acp::run_acp_session_with_extensions_and_events(
                                        config,
                                        canonical_cwd,
                                        harness_options,
                                        mews_acp::AcpSessionRequest {
                                            prompt,
                                            recovery_prompt,
                                            session_id: acp_session_id,
                                        },
                                        &environment,
                                        &tools,
                                        &mut |event| {
                                            if let mews_acp::AcpStreamEvent::SessionBound {
                                                session_id,
                                                replaced,
                                            } = event
                                            {
                                                let acknowledgement_id = uuid::Uuid::now_v7().to_string();
                                                let (acknowledge, acknowledged) = std::sync::mpsc::channel();
                                                if let Some(waiters) = &binding_waiters {
                                                    waiters.lock().expect("ACP binding waiters poisoned").insert(acknowledgement_id.clone(), acknowledge);
                                                }
                                                if let Some(sender) = &event_sender {
                                                    sender.send(HostToHub::AcpEvent {
                                                        request_id: event_request_id.clone(),
                                                        event: mews_protocol::AcpEvent::SessionBound {
                                                            acknowledgement_id: acknowledgement_id.clone(),
                                                            session_id,
                                                            replaced,
                                                        },
                                                    }).map_err(|_| anyhow::anyhow!("Hub disconnected during ACP run"))?;
                                                }
                                                if binding_waiters.is_some() {
                                                    let acknowledgement = acknowledged.recv_timeout(
                                                        std::time::Duration::from_secs(30),
                                                    );
                                                    if let Some(waiters) = &binding_waiters {
                                                        waiters.lock().expect("ACP binding waiters poisoned").remove(&acknowledgement_id);
                                                    }
                                                    acknowledgement.map_err(|_| anyhow::anyhow!("Hub did not durably acknowledge the ACP Session binding"))?;
                                                }
                                                return Ok(());
                                            }
                                            let events = match event {
                                                mews_acp::AcpStreamEvent::AssistantDelta {
                                                    delta,
                                                    message_id,
                                                } => split_stream_delta(&delta)
                                                    .into_iter()
                                                    .map(|delta| {
                                                        mews_protocol::AcpEvent::AssistantDelta {
                                                            delta,
                                                            message_id: message_id.clone(),
                                                        }
                                                    })
                                                    .collect(),
                                                mews_acp::AcpStreamEvent::ProviderState(
                                                    data,
                                                ) => vec![mews_protocol::AcpEvent::ProviderState {
                                                    data,
                                                }],
                                                mews_acp::AcpStreamEvent::ReasoningDelta {
                                                    delta,
                                                    message_id,
                                                } => {
                                                    vec![mews_protocol::AcpEvent::ReasoningDelta {
                                                        delta,
                                                        message_id,
                                                    }]
                                                }
                                                mews_acp::AcpStreamEvent::ToolActivity {
                                                    call_id,
                                                    title,
                                                    kind,
                                                    status,
                                                    input,
                                                } => vec![mews_protocol::AcpEvent::ToolActivity {
                                                    activity: mews_protocol::ToolActivity {
                                                        call_id,
                                                        title,
                                                        kind,
                                                        status,
                                                        input,
                                                    },
                                                }],
                                                mews_acp::AcpStreamEvent::SessionBound { .. } => unreachable!(),
                                            };
                                            for event in events {
                                                if let Some(sender) = &event_sender {
                                                    sender
                                                        .send(HostToHub::AcpEvent {
                                                            request_id: event_request_id.clone(),
                                                            event,
                                                        })
                                                        .map_err(|_| {
                                                            anyhow::anyhow!(
                                                                "Hub disconnected during ACP run"
                                                            )
                                                        })?;
                                                }
                                            }
                                            Ok(())
                                        },
                                    ),
                                )
                            });
                        let _ = sender.send(result);
                    })
                    .context("start ACP extension thread")?;
                let outcome = receiver
                    .await
                    .context("ACP extension thread ended before replying")?;
                if let Err(error) = &outcome
                    && is_authentication_error(error)
                {
                    let _ =
                        mews_host::HarnessCatalog::invalidate_authentication(root, &failed_harness);
                }
                outcome
            }
            .await;
            match result {
                Ok(outcome) => HostToHub::AcpResult {
                    request_id,
                    answer: Some(outcome.answer),
                    acp_session_id: Some(outcome.session_id),
                    session_replaced: outcome.session_replaced,
                    error: None,
                },
                Err(error) => HostToHub::AcpResult {
                    request_id,
                    answer: None,
                    acp_session_id: None,
                    session_replaced: false,
                    error: Some(error.to_string()),
                },
            }
        }
        HubToHost::AttestDirectory { .. }
        | HubToHost::ExecuteTool { .. }
        | HubToHost::ExecuteHook { .. } => {
            unreachable!("execution requests are handled by mews-host")
        }
        HubToHost::SynchronizeAgent {
            request_id,
            agent,
            revision,
            expected_replica,
        } => {
            let result = agent_root.map_or(Ok(()), |root| {
                materialize_agent(root, &agent, &revision, expected_replica.as_ref())
            });
            HostToHub::AgentSynchronized {
                request_id,
                error: result.err().map(|error| error.to_string()),
            }
        }
        HubToHost::Ping { nonce } => HostToHub::Pong { nonce },
    }
}

fn is_authentication_error(error: &anyhow::Error) -> bool {
    let text = format!("{error:#}").to_ascii_lowercase();
    [
        "auth",
        "login",
        "log in",
        "credential",
        "unauthorized",
        "401",
    ]
    .iter()
    .any(|needle| text.contains(needle))
}

fn split_stream_delta(text: &str) -> Vec<String> {
    const MAX_DELTA_BYTES: usize = 32 * 1024;
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < text.len() {
        let mut end = (start + MAX_DELTA_BYTES).min(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        chunks.push(text[start..end].to_owned());
        start = end;
    }
    chunks
}

#[derive(serde::Serialize, serde::Deserialize)]
struct HubTransferManifest {
    move_nonce: String,
    installation_id: crate::InstallationId,
    generation: u64,
    target_host_id: HostId,
    database_size: u64,
    database_sha256: String,
    credentials_sha256: String,
}

fn transfer_response(request_id: RequestId, result: Result<Option<u64>>) -> HostToHub {
    match result {
        Ok(next_offset) => HostToHub::HubTransferResult {
            request_id,
            next_offset,
            error: None,
        },
        Err(error) => HostToHub::HubTransferResult {
            request_id,
            next_offset: None,
            error: Some(error.to_string()),
        },
    }
}

fn configuration_response(request_id: RequestId, result: Result<()>) -> HostToHub {
    HostToHub::ConfigurationResult {
        request_id,
        error: result.err().map(|error| error.to_string()),
    }
}

fn catalog_response(request_id: RequestId, result: Result<Vec<HarnessDescriptor>>) -> HostToHub {
    match result {
        Ok(harnesses) => HostToHub::HarnessCatalog {
            request_id,
            harnesses,
            error: None,
        },
        Err(error) => HostToHub::HarnessCatalog {
            request_id,
            harnesses: Vec::new(),
            error: Some(error.to_string()),
        },
    }
}

async fn configure_relay(
    root: Option<&Path>,
    active: bool,
    stop_at: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<()> {
    let root = root.context("configure relay requires a Host root")?;
    let mut config = crate::relay_supervisor::read(root)?;
    config.role = if active {
        crate::relay_supervisor::RelayRole::Active
    } else if let Some(stop_at) = stop_at {
        crate::relay_supervisor::RelayRole::Retiring { stop_at }
    } else {
        crate::relay_supervisor::RelayRole::Disabled
    };
    crate::relay_supervisor::write(root, &config)?;
    if active {
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(config.listen).await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        bail!("relay did not become ready on {}", config.listen);
    }
    Ok(())
}

fn begin_hub_transfer(root: &Path, transfer: HubTransferStart) -> Result<()> {
    if transfer.database_size > 1024 * 1024 * 1024 {
        bail!("Hub database transfer exceeds 1 GiB");
    }
    for name in [
        "mews.db.incoming",
        "mews.db.prepared",
        "secrets/installation.key.incoming",
        "secrets/installation.key.prepared",
        "secrets/hub-noise.key.incoming",
        "secrets/hub-noise.key.prepared",
        "auth.json.incoming",
        "auth.json.prepared",
        "hub-transfer.json",
        "hub-activation-token",
    ] {
        let path = root.join(name);
        if path.exists() {
            std::fs::remove_file(path)?;
        }
    }
    write_private(
        root.join("secrets/installation.key.incoming"),
        &transfer.installation_key,
    )?;
    write_private(
        root.join("secrets/hub-noise.key.incoming"),
        &transfer.hub_noise_key,
    )?;
    write_private(root.join("auth.json.incoming"), &transfer.credentials)?;
    write_private(root.join("mews.db.incoming"), &[])?;
    write_private(
        root.join("hub-transfer.json"),
        &serde_json::to_vec(&HubTransferManifest {
            move_nonce: transfer.move_nonce,
            installation_id: transfer.installation_id,
            generation: transfer.generation,
            target_host_id: transfer.target_host_id,
            database_size: transfer.database_size,
            database_sha256: transfer.database_sha256,
            credentials_sha256: transfer.credentials_sha256,
        })?,
    )?;
    Ok(())
}

fn write_hub_transfer(root: &Path, offset: u64, data: &[u8]) -> Result<u64> {
    if data.len() > 128 * 1024 {
        bail!("Hub transfer chunk exceeds 128 KiB");
    }
    use std::io::Write;
    let path = root.join("mews.db.incoming");
    let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
    let current = file.metadata()?.len();
    if current != offset {
        bail!("Hub transfer offset mismatch");
    }
    file.write_all(data)?;
    file.sync_data()?;
    Ok(current + data.len() as u64)
}

fn commit_hub_transfer(root: &Path) -> Result<()> {
    let manifest: HubTransferManifest =
        serde_json::from_slice(&std::fs::read(root.join("hub-transfer.json"))?)?;
    let database = std::fs::read(root.join("mews.db.incoming"))?;
    if database.len() as u64 != manifest.database_size
        || format!("{:x}", Sha256::digest(&database)) != manifest.database_sha256
    {
        bail!("Hub database transfer failed integrity verification");
    }
    let credentials = std::fs::read(root.join("auth.json.incoming"))?;
    if format!("{:x}", Sha256::digest(&credentials)) != manifest.credentials_sha256 {
        bail!("Hub credential transfer failed integrity verification");
    }
    let store = Store::open(root.join("mews.db.incoming"))?;
    let installation = store
        .installation()?
        .context("transferred Hub database has no installation")?;
    if installation.id != manifest.installation_id
        || installation.generation != manifest.generation
        || installation.hub_host_id != manifest.target_host_id
    {
        bail!("transferred Hub database does not match the authorized handoff");
    }
    let authority = HostIdentity::load(&root.join("secrets/installation.key.incoming"))?;
    if authority.public_key() != installation.public_key {
        bail!("transferred installation key does not match Hub database");
    }
    NoiseIdentity::load(&root.join("secrets/hub-noise.key.incoming"))?;
    serde_json::from_slice::<serde_json::Map<String, serde_json::Value>>(&std::fs::read(
        root.join("auth.json.incoming"),
    )?)?;
    let local = HostIdentity::load(&root.join("secrets/host.key"))?;
    if store.host(&manifest.target_host_id)?.public_key != local.public_key() {
        bail!("transferred Hub targets a different physical Host");
    }
    drop(store);
    std::fs::rename(
        root.join("secrets/installation.key.incoming"),
        root.join("secrets/installation.key.prepared"),
    )?;
    std::fs::rename(
        root.join("secrets/hub-noise.key.incoming"),
        root.join("secrets/hub-noise.key.prepared"),
    )?;
    std::fs::rename(
        root.join("auth.json.incoming"),
        root.join("auth.json.prepared"),
    )?;
    std::fs::rename(root.join("mews.db.incoming"), root.join("mews.db.prepared"))?;
    sync_directory(root)?;
    Ok(())
}

fn arm_hub_transfer(root: &Path, move_nonce: &str) -> Result<()> {
    let manifest: HubTransferManifest =
        serde_json::from_slice(&std::fs::read(root.join("hub-transfer.json"))?)?;
    if manifest.move_nonce != move_nonce {
        bail!("Hub activation nonce does not match prepared transfer");
    }
    if !root.join("hub-activation-token").exists() {
        write_private(root.join("hub-activation-token"), move_nonce.as_bytes())?;
    }
    Ok(())
}

#[doc(hidden)]
pub fn activate_hub_transfer(root: &Path) -> Result<()> {
    let manifest: HubTransferManifest =
        serde_json::from_slice(&std::fs::read(root.join("hub-transfer.json"))?)?;
    let token = std::fs::read_to_string(root.join("hub-activation-token"))?;
    if token != manifest.move_nonce {
        bail!("prepared Hub has no valid source-issued activation token");
    }
    if !root.join("hub-activate").exists() {
        write_private(root.join("hub-activate"), b"activate")?;
    }
    for (prepared, active) in [
        (
            "secrets/installation.key.prepared",
            "secrets/installation.key",
        ),
        ("secrets/hub-noise.key.prepared", "secrets/hub-noise.key"),
        ("auth.json.prepared", "auth.json"),
    ] {
        if !root.join(active).exists() {
            std::fs::rename(root.join(prepared), root.join(active))?;
        } else {
            if std::fs::read(root.join(prepared))? != std::fs::read(root.join(active))? {
                bail!("prepared Hub credential differs from the installation credential");
            }
            std::fs::remove_file(root.join(prepared))?;
        }
    }
    if root.join("mews.db").exists() {
        std::fs::rename(
            root.join("mews.db"),
            root.join(format!("mews.db.previous-{}", uuid::Uuid::now_v7())),
        )?;
    }
    std::fs::rename(root.join("mews.db.prepared"), root.join("mews.db"))?;
    if !root.join("hub-promote").exists() {
        write_private(root.join("hub-promote"), b"ready")?;
    }
    let _ = std::fs::remove_file(root.join("hub-activate"));
    let _ = std::fs::remove_file(root.join("hub-activation-token"));
    let _ = std::fs::remove_file(root.join("hub-transfer.json"));
    sync_directory(root)?;
    Ok(())
}

fn write_private(path: std::path::PathBuf, contents: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .context("private file has no parent")?
        .to_path_buf();
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    use std::io::Write;
    let mut file = options.open(path)?;
    file.write_all(contents)?;
    file.sync_all()?;
    sync_directory(&parent)?;
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}

fn read_agent(root: &Path, slug: &str) -> Result<Option<AgentReplica>> {
    let directory = root.join("agents").join(slug);
    if !directory.exists() {
        return Ok(None);
    }
    let revision = std::fs::read_to_string(directory.join(".revision"))?
        .trim()
        .parse()
        .context("invalid agent replica revision")?;
    Ok(Some(AgentReplica {
        revision,
        soul: std::fs::read_to_string(directory.join("SOUL.md"))?,
        config_toml: std::fs::read_to_string(directory.join("agent.toml"))?,
    }))
}

fn materialize_agent(
    root: &Path,
    agent: &Agent,
    revision: &AgentRevision,
    expected: Option<&AgentReplica>,
) -> Result<()> {
    if revision.agent_id != agent.id || revision.revision != agent.current_revision {
        bail!("agent revision does not match agent");
    }
    let directory = root.join("agents").join(&agent.slug);
    if read_agent(root, &agent.slug)?.as_ref() != expected {
        bail!("agent replica changed while synchronizing; local files were left untouched");
    }
    if expected.is_some_and(|replica| {
        replica.revision == revision.revision
            && replica.soul == revision.soul
            && replica.config_toml == revision.config_toml
    }) {
        return Ok(());
    }
    let agents = root.join("agents");
    std::fs::create_dir_all(&agents)?;
    let unique = uuid::Uuid::now_v7();
    let staged = agents.join(format!(".{}.sync-{unique}", agent.slug));
    std::fs::create_dir(&staged)?;
    std::fs::write(staged.join("SOUL.md"), &revision.soul)?;
    std::fs::write(staged.join("agent.toml"), &revision.config_toml)?;
    std::fs::write(staged.join(".revision"), revision.revision.to_string())?;
    let backup = if directory.exists() {
        let backup = agents.join(format!(".{}.previous-{unique}", agent.slug));
        std::fs::rename(&directory, &backup)?;
        Some(backup)
    } else {
        None
    };
    if let Err(error) = std::fs::rename(&staged, &directory) {
        if let Some(backup) = &backup {
            let _ = std::fs::rename(backup, &directory);
        }
        return Err(error.into());
    }
    // Previous directories are intentionally retained: an editor with an open
    // file can still finish writing there after the atomic directory swap.
    Ok(())
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
