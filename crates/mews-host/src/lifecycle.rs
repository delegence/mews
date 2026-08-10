use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

use crate::ToolRegistry;
use mews_protocol::{
    Agent, AgentReplica, AgentRevision, HarnessDescriptor, HostId, HostToHub, HubToHost,
    HubTransferStart, RequestId,
};
use mews_store::Store;
use mews_transport::{HostIdentity, NoiseIdentity};

const MAX_CONCURRENT_ACP_RUNS: usize = 8;

fn acp_capacity() -> Arc<tokio::sync::Semaphore> {
    static CAPACITY: std::sync::OnceLock<Arc<tokio::sync::Semaphore>> = std::sync::OnceLock::new();
    Arc::clone(
        CAPACITY.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_ACP_RUNS))),
    )
}

pub type AcpBindingWaiters = Arc<Mutex<HashMap<String, std::sync::mpsc::Sender<()>>>>;

#[cfg(test)]
pub(crate) async fn handle_host_request(
    registry: &ToolRegistry,
    agent_root: Option<&Path>,
    message: HubToHost,
) -> HostToHub {
    let cancellation = matches!(
        &message,
        HubToHost::ExecuteTool { .. } | HubToHost::ExecuteHook { .. }
    )
    .then(mews_agent::CancellationToken::new);
    handle_host_request_streaming(registry, agent_root, message, None, None, cancellation).await
}

pub async fn handle_host_request_streaming(
    registry: &ToolRegistry,
    agent_root: Option<&Path>,
    message: HubToHost,
    events: Option<tokio::sync::mpsc::Sender<HostToHub>>,
    binding_waiters: Option<AcpBindingWaiters>,
    cancellation: Option<mews_agent::CancellationToken>,
) -> HostToHub {
    if let Some(response) =
        crate::handle_execution_request(registry, &message, cancellation.as_ref()).await
    {
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
                    let mut state: serde_json::Value =
                        serde_json::from_slice(&std::fs::read(&path)?)?;
                    state["relay_urls"] = serde_json::to_value(relay_urls)?;
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
                Some(root) => crate::HarnessCatalog::refresh(root)
                    .await
                    .map(|catalog| catalog.descriptors()),
                None => crate::HarnessCatalog::discover(None).map(|catalog| catalog.descriptors()),
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
                    error: Some(format!("{error:#}")),
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
            agent_slug,
            soul,
            mews_session_id,
            run_id,
            transition,
            context,
        } => {
            let cancellation = cancellation.unwrap_or_default();
            let result = async {
                let root = agent_root.context("external Harness execution requires a Host root")?;
                let resolved = canonical_cwd
                    .canonicalize()
                    .with_context(|| format!("resolve {}", canonical_cwd.display()))?;
                if resolved != canonical_cwd || !resolved.is_dir() {
                    bail!("Session working directory no longer resolves to its attested path");
                }
                let launch = {
                    let catalog = crate::HarnessCatalog::discover(Some(root))?;
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
                let skills = crate::resources::snapshot_agent_skills(root, &agent_slug)?;
                match (transition.clone(), context) {
                    (mews_protocol::AcpBindingTransition::Resume { .. }, Some(_)) => {}
                    (mews_protocol::AcpBindingTransition::Resume { .. }, None) => bail!("compatible ACP Resume requires its stored context"),
                    (mews_protocol::AcpBindingTransition::New | mews_protocol::AcpBindingTransition::Replace { .. }, Some(_)) => bail!("new or replacement ACP Session must not receive stored resume context"),
                    (_, None) => {}
                }
                // This is intentionally rendered even for Resume: it is held
                // unused on success, but is the fresh boundary for typed
                // resource_not_found replacement.
                let context = mews_protocol::AcpBindingContext::from_snapshot(
                    &mews_protocol::AcpContextSnapshot {
                        version: mews_protocol::ACP_CONTEXT_VERSION,
                        agent_slug,
                        soul,
                        skills: skills.iter().map(|skill| mews_protocol::AcpSkillInventoryItem {
                            name: skill.name.clone(), description: skill.description.clone(), hash: skill.hash.clone(),
                        }).collect(),
                    },
                    launch.instruction_channel,
                ).map_err(anyhow::Error::msg)?;
                let skills = skills.into_iter().map(|skill| mews_acp::AcpSkill {
                    name: skill.name, description: skill.description, hash: skill.hash, content: skill.content,
                }).collect::<Vec<_>>();
                let mut config = mews_acp::AcpHarnessConfig::new(launch.command)?;
                config.environment = launch.environment;
                // Host request handling itself is spawned on a multithreaded
                // runtime. Run the capability bridge on its own current-thread
                // runtime so Host extension implementations may retain their
                // existing non-Send async boundary without widening authority.
                // Each run needs a dedicated current-thread runtime (and may need
                // an event-forwarding thread), so bound that OS resource explicitly.
                let acp_permit = acp_capacity()
                    .acquire_owned()
                    .await
                    .context("ACP execution capacity closed")?;
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
                        let _acp_permit = acp_permit;
                        // ACP's stream callback is synchronous. A bounded sync
                        // handoff preserves backpressure without blocking the
                        // current-thread ACP runtime on Tokio's async sender.
                        let (callback_events, forwarder) = event_sender
                            .map(|event_sender| {
                                let (callback_events, events) = std::sync::mpsc::sync_channel(
                                    super::ACP_EVENT_CHANNEL_CAPACITY,
                                );
                                let forwarder = std::thread::Builder::new()
                                    .name("mews-acp-events".into())
                                    .spawn(move || {
                                        while let Ok(event) = events.recv() {
                                            if event_sender.blocking_send(event).is_err() {
                                                break;
                                            }
                                        }
                                    });
                                (callback_events, forwarder)
                            })
                            .unzip();
                        let result = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .context("create ACP extension runtime")
                            .and_then(|runtime| {
                                let environment = crate::LocalEnvironment::new(
                                    Some(host_root),
                                    Arc::new(registry),
                                );
                                runtime.block_on(
                                    mews_acp::run_acp_session(mews_acp::AcpRunRequest {
                                        config,
                                        cwd: canonical_cwd,
                                        harness_options,
                                        session: mews_acp::AcpSessionRequest {
                                            transition,
                                            prompt,
                                            recovery_prompt,
                                            context_text: context.text.clone(),
                                            instruction_channel: context.channel,
                                            skills,
                                            hook_metadata: Some(mews_acp::AcpHookMetadata {
                                                mews_session_id,
                                                run_id,
                                                harness: harness.clone(),
                                                context_hash: context.hash.clone(),
                                                context_channel: context.channel,
                                                invoke_run_start: true,
                                            }),
                                        },
                                        environment: &environment,
                                        allowed_tools: &tools,
                                        cancellation,
                                        events: &mut |event| {
                                            if let mews_acp::AcpStreamEvent::SessionBound {
                                                event_key,
                                                session_id,
                                                transition,
                                            } = event
                                            {
                                                let acknowledgement_id = uuid::Uuid::now_v7().to_string();
                                                let (acknowledge, acknowledged) = std::sync::mpsc::channel();
                                                if let Some(waiters) = &binding_waiters {
                                                    waiters.lock().expect("ACP binding waiters poisoned").insert(acknowledgement_id.clone(), acknowledge);
                                                }
                                                if let Some(sender) = &callback_events {
                                                    sender.send(HostToHub::AcpEvent {
                                                        request_id: event_request_id.clone(),
                                                        event: mews_protocol::AcpEvent::SessionBound {
                                                            event_key,
                                                            acknowledgement_id: acknowledgement_id.clone(),
                                                            session_id,
                                                            transition,
                                                            context: context.clone(),
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
                                            if let mews_acp::AcpStreamEvent::ContextDispatched {
                                                event_key,
                                                session_id,
                                            } = event
                                            {
                                                let acknowledgement_id = uuid::Uuid::now_v7().to_string();
                                                let (acknowledge, acknowledged) = std::sync::mpsc::channel();
                                                if let Some(waiters) = &binding_waiters {
                                                    waiters.lock().expect("ACP binding waiters poisoned").insert(acknowledgement_id.clone(), acknowledge);
                                                }
                                                if let Some(sender) = &callback_events {
                                                    sender.send(HostToHub::AcpEvent {
                                                        request_id: event_request_id.clone(),
                                                        event: mews_protocol::AcpEvent::ContextDispatched {
                                                            event_key,
                                                            acknowledgement_id: acknowledgement_id.clone(),
                                                            session_id,
                                                        },
                                                    }).map_err(|_| anyhow::anyhow!("Hub disconnected during ACP run"))?;
                                                }
                                                if binding_waiters.is_some() {
                                                    let acknowledgement = acknowledged.recv_timeout(std::time::Duration::from_secs(30));
                                                    if let Some(waiters) = &binding_waiters {
                                                        waiters.lock().expect("ACP binding waiters poisoned").remove(&acknowledgement_id);
                                                    }
                                                    acknowledgement.map_err(|_| anyhow::anyhow!("Hub did not durably acknowledge the ACP context dispatch"))?;
                                                }
                                                return Ok(());
                                            }
                                            let events = match event {
                                                mews_acp::AcpStreamEvent::AssistantDelta {
                                                    event_key,
                                                    delta,
                                                    message_id,
                                                    raw,
                                                } => split_stream_delta(&delta)
                                                    .into_iter()
                                                    .enumerate()
                                                    .map(|(index, delta)| {
                                                        mews_protocol::AcpEvent::AssistantDelta {
                                                            event_key: format!("{event_key}:{index}"),
                                                            delta,
                                                            message_id: message_id.clone(),
                                                            raw: raw.clone(),
                                                        }
                                                    })
                                                    .collect(),
                                                mews_acp::AcpStreamEvent::ProviderState { event_key, data } => vec![mews_protocol::AcpEvent::ProviderState {
                                                    event_key,
                                                    data,
                                                }],
                                                mews_acp::AcpStreamEvent::ReasoningDelta {
                                                    event_key,
                                                    delta,
                                                    message_id,
                                                    raw,
                                                } => {
                                                    vec![mews_protocol::AcpEvent::ReasoningDelta {
                                                        event_key,
                                                        delta,
                                                        message_id,
                                                        raw,
                                                    }]
                                                }
                                                mews_acp::AcpStreamEvent::ToolActivity {
                                                    event_key,
                                                    call_id,
                                                    title,
                                                    kind,
                                                    status,
                                                    input,
                                                } => vec![mews_protocol::AcpEvent::ToolActivity {
                                                    event_key,
                                                    activity: mews_protocol::ToolActivity {
                                                        call_id,
                                                        title,
                                                        kind,
                                                        status,
                                                        input,
                                                    },
                                                }],
                                                mews_acp::AcpStreamEvent::HookOutcome { event_key, hook, ok, detail, tool, call_id } => vec![mews_protocol::AcpEvent::HookOutcome { event_key, hook, ok, detail, tool, call_id }],
                                                mews_acp::AcpStreamEvent::ContextDispatched { .. } => unreachable!(),
                                                mews_acp::AcpStreamEvent::SessionBound { .. } => unreachable!(),
                                            };
                                            for event in events {
                                                if let Some(sender) = &callback_events {
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
                                    }),
                                )
                            });
                        drop(callback_events);
                        if let Some(forwarder) = forwarder {
                            match forwarder {
                                Ok(forwarder) => {
                                    let _ = forwarder.join();
                                }
                                Err(error) => {
                                    let _ = sender.send(Err(error.into()));
                                    return;
                                }
                            }
                        }
                        let _ = sender.send(result);
                    })
                    .context("start ACP extension thread")?;
                let outcome = receiver
                    .await
                    .context("ACP extension thread ended before replying")?;
                if let Err(error) = &outcome
                    && mews_acp::classify_error(error)
                        == Some(mews_acp::AcpErrorKind::AuthenticationRequired)
                {
                    let _ =
                        crate::HarnessCatalog::invalidate_authentication(root, &failed_harness);
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
                    timings: Some(outcome.timings),
                    stop_reason: Some(outcome.stop_reason),
                    error: None,
                },
                Err(error) => HostToHub::AcpResult {
                    request_id,
                    answer: None,
                    acp_session_id: None,
                    session_replaced: false,
                    timings: None,
                    stop_reason: None,
                    error: Some(error.to_string()),
                },
            }
        }
        HubToHost::AttestDirectory { .. }
        | HubToHost::ExecuteTool { .. }
        | HubToHost::ExecuteHook { .. }
        | HubToHost::CancelAcp { .. } => {
            unreachable!("execution requests are handled by mews-host")
        }
        HubToHost::SynchronizeAgent {
            request_id,
            agent,
            revision,
            expected_replica,
            previous_slug,
        } => {
            let result = agent_root.map_or(Ok(()), |root| {
                materialize_agent(
                    root,
                    &agent,
                    &revision,
                    expected_replica.as_ref(),
                    previous_slug.as_deref(),
                )
            });
            HostToHub::AgentSynchronized {
                request_id,
                error: result.err().map(|error| error.to_string()),
            }
        }
        HubToHost::Ping { nonce } => HostToHub::Pong { nonce },
        HubToHost::CancelTool { request_id } => HostToHub::ConfigurationResult {
            request_id,
            error: Some("tool cancellation reached the execution handler".into()),
        },
    }
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
    installation_id: mews_protocol::InstallationId,
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
        "hub-transfer.activated.json",
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
    let mut database = std::fs::File::open(root.join("mews.db.incoming"))?;
    let mut database_hash = Sha256::new();
    let mut chunk = [0_u8; 96 * 1024];
    let mut database_size = 0_u64;
    loop {
        let read = std::io::Read::read(&mut database, &mut chunk)?;
        if read == 0 {
            break;
        }
        database_size += read as u64;
        sha2::Digest::update(&mut database_hash, &chunk[..read]);
    }
    if database_size != manifest.database_size
        || format!("{:x}", database_hash.finalize()) != manifest.database_sha256
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
    let manifest_path = root.join("hub-transfer.json");
    let receipt_path = root.join("hub-transfer.activated.json");
    if !manifest_path.exists() {
        let receipt: HubTransferManifest = serde_json::from_slice(&std::fs::read(&receipt_path)?)?;
        if database_matches_transfer(&root.join("mews.db"), &receipt)? {
            return Ok(());
        }
        bail!("completed Hub activation receipt does not match the active database");
    }
    let manifest: HubTransferManifest = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
    if receipt_path.exists() {
        let receipt: HubTransferManifest = serde_json::from_slice(&std::fs::read(&receipt_path)?)?;
        if serde_json::to_vec(&receipt)? != serde_json::to_vec(&manifest)?
            || !database_matches_transfer(&root.join("mews.db"), &manifest)?
        {
            bail!("completed Hub activation receipt does not match the prepared transfer");
        }
        let _ = std::fs::remove_file(root.join("hub-activate"));
        let _ = std::fs::remove_file(root.join("hub-activation-token"));
        let _ = std::fs::remove_file(manifest_path);
        sync_directory(root)?;
        return Ok(());
    }
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
        if root.join(prepared).exists() && !root.join(active).exists() {
            std::fs::rename(root.join(prepared), root.join(active))?;
        } else if root.join(prepared).exists() {
            if std::fs::read(root.join(prepared))? == std::fs::read(root.join(active))? {
                std::fs::remove_file(root.join(prepared))?;
            } else {
                let active_path = root.join(active);
                retain_previous_file(
                    active_path
                        .parent()
                        .context("Hub credential has no parent")?,
                    active_path
                        .file_name()
                        .context("Hub credential has no file name")?
                        .to_str()
                        .context("Hub credential name is not UTF-8")?,
                )?;
                std::fs::rename(root.join(prepared), active_path)?;
            }
        } else if !root.join(active).exists() {
            bail!("prepared Hub credential is missing: {prepared}");
        }
    }

    let active_database = root.join("mews.db");
    let prepared_database = root.join("mews.db.prepared");
    if database_matches_transfer(&active_database, &manifest)? {
        if prepared_database.exists() {
            if !database_matches_transfer(&prepared_database, &manifest)? {
                bail!("prepared Hub database does not match the authorized handoff");
            }
            std::fs::remove_file(&prepared_database)?;
        }
    } else {
        if !database_matches_transfer(&prepared_database, &manifest)? {
            bail!("prepared Hub database is missing or does not match the authorized handoff");
        }
        if active_database.exists() {
            retain_previous_file(root, "mews.db")?;
        }
        std::fs::rename(&prepared_database, &active_database)?;
    }
    if !root.join("hub-promote").exists() {
        write_private(root.join("hub-promote"), b"ready")?;
    }
    let receipt = serde_json::to_vec(&manifest)?;
    if receipt_path.exists() {
        if std::fs::read(&receipt_path)? != receipt {
            bail!("existing Hub activation receipt belongs to another transfer");
        }
    } else {
        write_private(receipt_path, &receipt)?;
    }
    let _ = std::fs::remove_file(root.join("hub-activate"));
    let _ = std::fs::remove_file(root.join("hub-activation-token"));
    let _ = std::fs::remove_file(manifest_path);
    sync_directory(root)?;
    Ok(())
}

fn database_matches_transfer(path: &Path, manifest: &HubTransferManifest) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let store = Store::open(path)?;
    let Some(installation) = store.installation()? else {
        return Ok(false);
    };
    Ok(installation.id == manifest.installation_id
        && installation.generation == manifest.generation
        && installation.hub_host_id == manifest.target_host_id)
}

fn retain_previous_file(root: &Path, name: &str) -> Result<()> {
    let prefix = format!("{name}.previous-");
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_name().to_string_lossy().starts_with(&prefix) {
            std::fs::remove_file(entry.path())?;
        }
    }
    std::fs::rename(
        root.join(name),
        root.join(format!("{prefix}{}", uuid::Uuid::now_v7())),
    )?;
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
    previous_slug: Option<&str>,
) -> Result<()> {
    if revision.agent_id != agent.id || revision.revision != agent.current_revision {
        bail!("agent revision does not match agent");
    }
    let directory = root.join("agents").join(&agent.slug);
    let observed_slug = previous_slug.unwrap_or(&agent.slug);
    if read_agent(root, observed_slug)?.as_ref() != expected {
        bail!("agent replica changed while synchronizing; local files were left untouched");
    }
    if previous_slug.is_some() && directory.exists() {
        bail!("renamed agent destination already exists; local files were left untouched");
    }
    if expected.is_some_and(|replica| {
        replica.revision == revision.revision
            && replica.soul == revision.soul
            && replica.config_toml == revision.config_toml
    }) && previous_slug.is_none()
    {
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
    let previous_directory = root.join("agents").join(observed_slug);
    let backup = if previous_directory.exists() {
        let backup = agents.join(format!(".{observed_slug}.previous-{unique}"));
        std::fs::rename(&previous_directory, &backup)?;
        if backup.join("skills").exists()
            && let Err(error) = std::fs::rename(backup.join("skills"), staged.join("skills"))
        {
            let _ = std::fs::rename(&backup, &previous_directory);
            return Err(error.into());
        }
        Some(backup)
    } else {
        None
    };
    if let Err(error) = std::fs::rename(&staged, &directory) {
        if let Some(backup) = &backup {
            if staged.join("skills").exists() {
                let _ = std::fs::rename(staged.join("skills"), backup.join("skills"));
            }
            let _ = std::fs::rename(backup, &previous_directory);
        }
        return Err(error.into());
    }
    retain_previous_agent_directories(&agents, observed_slug)?;
    Ok(())
}

fn retain_previous_agent_directories(agents: &Path, slug: &str) -> Result<()> {
    let prefix = format!(".{slug}.previous-");
    let mut previous = std::fs::read_dir(agents)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with(&prefix))
        .collect::<Vec<_>>();
    previous.sort_by_key(|entry| entry.file_name());
    for stale in previous.into_iter().rev().skip(1) {
        std::fs::remove_dir_all(stale.path())?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "connection_tests.rs"]
mod tests;
