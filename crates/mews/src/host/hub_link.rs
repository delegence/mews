#[cfg(not(unix))]
compile_error!("the MEWS Host daemon requires Unix sockets");

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use mews_relay::{NetworkRelay, RelayIdentity};
use tokio::sync::mpsc;

use crate::{
    enrollment::relay::EnrollmentAccepted,
    host::{ConnectedHost, HostControl},
    identity::{HostIdentity, NoiseIdentity},
    service::Mews,
    transport::{PeerAuthentication, connect_responder, run_peer_bridge},
};
use mews_protocol::{HubRequest, HubResponse, PeerEnvelope, ProtocolError};

pub(crate) async fn serve_hub_host(
    root: PathBuf,
    relay_urls: Vec<String>,
    accepted: EnrollmentAccepted,
    remote_hosts: crate::hub::RemoteHosts,
    control: crate::hub::HubControl,
) -> Result<()> {
    loop {
        if let Err(error) =
            serve_hub_host_once(&root, &relay_urls, &accepted, &remote_hosts, &control).await
        {
            eprintln!("remote Host {} disconnected: {error:#}", accepted.host.id);
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

async fn serve_hub_host_once(
    root: &Path,
    relay_urls: &[String],
    accepted: &EnrollmentAccepted,
    remote_hosts: &crate::hub::RemoteHosts,
    control: &crate::hub::HubControl,
) -> Result<()> {
    Mews::open_connection(root.to_path_buf())?.ensure_host(&accepted.host.id)?;
    let authority = HostIdentity::load(&root.join("secrets/installation.key"))?;
    let hub_noise = NoiseIdentity::load(&root.join("secrets/hub-noise.key"))?;
    let hub_identity = RelayIdentity {
        installation_id: accepted.relay_admission.installation_id.clone(),
        peer_id: accepted.hub_relay_admission.peer_id.clone(),
        public_key: authority.public_key(),
        authority_public_key: authority.public_key(),
        admission: Some(accepted.hub_relay_admission.clone()),
    };
    let host_peer = accepted.relay_admission.peer_id.clone();
    let stream = accepted.host.id.to_string();
    let mut peer = None;
    for relay_url in relay_urls {
        let Ok(Ok(relay)) = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            NetworkRelay::connect(relay_url, hub_identity.clone(), &authority),
        )
        .await
        else {
            continue;
        };
        if let Ok(Ok(connected)) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            connect_responder(
                relay,
                PeerAuthentication {
                    installation_id: accepted.relay_admission.installation_id.clone(),
                    local_peer: hub_identity.peer_id.clone(),
                    remote_peer: host_peer.clone(),
                    local_signer: &authority,
                    trusted_remote_signing_key: &accepted.host.public_key,
                    local_noise: &hub_noise,
                    expected_remote_noise_key: Some(&accepted.host.noise_public_key),
                    stream_id: &stream,
                },
            ),
        )
        .await
        {
            peer = Some(connected);
            break;
        }
    }
    let peer = peer.context("no relay candidate paired with the remote Host")?;

    let (peer_out, peer_out_rx) = mpsc::channel(64);
    let (peer_in_tx, mut peer_in) = mpsc::channel(64);
    tokio::task::spawn_local(async move {
        let _ = run_peer_bridge(peer, peer_out_rx, peer_in_tx).await;
    });
    let (tool_tx, mut tool_rx) = mpsc::channel(32);
    let (tool_response_tx, tool_response_rx) = mpsc::channel(32);
    let connected = Arc::new(
        ConnectedHost::from_channels(
            accepted.host.id.clone(),
            Vec::new(),
            tool_tx,
            tool_response_rx,
        )
        .await?,
    );
    remote_hosts
        .lock()
        .await
        .insert(accepted.host.id.clone(), Arc::clone(&connected));
    let tool_peer = peer_out.clone();
    tokio::task::spawn_local(async move {
        while let Some(body) = tool_rx.recv().await {
            if tool_peer
                .send(PeerEnvelope::ToolRequest { body })
                .await
                .is_err()
            {
                return;
            }
        }
    });
    let (client_tx, mut client_rx) = mpsc::channel(32);
    tokio::task::spawn_local(async move {
        while let Some(message) = peer_in.recv().await {
            match message {
                PeerEnvelope::ToolResponse { body } => {
                    let _ = tool_response_tx.send(body).await;
                }
                PeerEnvelope::ClientRequest { request_id, body } => {
                    let _ = client_tx.send((request_id, body)).await;
                }
                _ => return,
            }
        }
    });
    while let Some((request_id, request)) = client_rx.recv().await {
        let response = match dispatch_remote(root, &connected, remote_hosts, control, request).await
        {
            Ok(response) => response,
            Err(error) => HubResponse::Error(ProtocolError::internal(format!("{error:#}"))),
        };
        peer_out
            .send(PeerEnvelope::ClientResponse {
                request_id,
                body: response,
            })
            .await
            .context("remote Host disconnected")?;
    }
    Ok(())
}

async fn dispatch_remote(
    root: &Path,
    host: &ConnectedHost,
    remote_hosts: &crate::hub::RemoteHosts,
    control: &crate::hub::HubControl,
    request: HubRequest,
) -> Result<HubResponse> {
    let _operation = control.handoff_gate.read().await;
    if control.moving.load(std::sync::atomic::Ordering::Acquire)
        && !matches!(request, HubRequest::Status)
    {
        bail!("Hub is moving; try again after handoff");
    }
    if let HubRequest::ResolvePermission {
        request_id,
        option_id,
    } = request
    {
        if let Some(waiter) = control.permission_waiters.lock().await.remove(&request_id) {
            let _ = waiter.send(option_id);
        }
        return Ok(HubResponse::Ack);
    }
    if let HubRequest::PollEvents {
        consumer_id,
        limit,
        wait_ms,
    } = request
    {
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_millis(u64::from(wait_ms.min(30_000)));
        loop {
            let notified = control.event_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let events = Mews::open_connection(root.to_path_buf())?
                .client_events(&consumer_id, limit.clamp(1, 500))?;
            if events.advanced || tokio::time::Instant::now() >= deadline {
                return Ok(HubResponse::Events(events));
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let _ = tokio::time::timeout(remaining, notified).await;
        }
    }
    if let HubRequest::StartTurn {
        idempotency_key,
        session_id,
        prompt,
        metadata,
        source,
    } = request
    {
        let mews = Mews::open_connection(root.to_path_buf())?;
        let (run, created) = mews.start_run_idempotent(&session_id, &idempotency_key)?;
        if !created {
            return Ok(HubResponse::Run(run));
        }
        control.event_notify.notify_waiters();
        let session = mews.session(&session_id)?;
        let installation = mews.installation()?;
        let source = source.unwrap_or(crate::MessageSource {
            kind: crate::SourceKind::Client,
            id: "client".into(),
        });
        let root = root.to_path_buf();
        let remote_hosts = Arc::clone(remote_hosts);
        let locks = Arc::clone(&control.session_locks);
        let run_task = run.clone();
        let tasks = Arc::clone(&control.run_tasks);
        let notify = Arc::clone(&control.event_notify);
        let permission_handler = crate::hub::runs::permission_handler(
            &root,
            session.id.clone(),
            run.id.clone(),
            control.clone(),
        );
        let task = tokio::task::spawn_local(async move {
            let lock = {
                let mut locks = locks.lock().await;
                Arc::clone(
                    locks
                        .entry(session.id.clone())
                        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
                )
            };
            let _guard = lock.lock().await;
            let result = async {
                let mut turn = Mews::open_connection(&root)?;
                if session.host_id == installation.hub_host_id {
                    turn.send_from_started(
                        &session,
                        &prompt,
                        metadata,
                        source,
                        crate::service::StartedRun {
                            id: run_task.id.clone(),
                            event_notify: Arc::clone(&notify),
                            permission_handler: Arc::clone(&permission_handler),
                        },
                    )
                    .await?;
                } else {
                    let target = remote_hosts
                        .lock()
                        .await
                        .get(&session.host_id)
                        .cloned()
                        .with_context(|| format!("Session Host {} is offline", session.host_id))?;
                    turn.send_on_from_started(
                        &session,
                        &prompt,
                        metadata,
                        target.as_ref(),
                        source,
                        crate::service::StartedRun {
                            id: run_task.id.clone(),
                            event_notify: Arc::clone(&notify),
                            permission_handler: Arc::clone(&permission_handler),
                        },
                    )
                    .await?;
                }
                Ok::<_, anyhow::Error>(())
            }
            .await;
            if let Err(error) = result {
                if let Ok(mews) = Mews::open_connection(&root) {
                    let _ = mews.fail_run(&run_task.id, &format!("{error:#}"));
                }
            }
            tasks.lock().await.remove(&run_task.id);
            notify.notify_waiters();
        });
        control
            .run_tasks
            .lock()
            .await
            .insert(run.id.clone(), task.abort_handle());
        return Ok(HubResponse::Run(run));
    }
    let mut mews = Mews::open_connection(root.to_path_buf())?;
    mews.ensure_host(host.host_id())?;
    Ok(match request {
        HubRequest::Status => HubResponse::Status(mews.installation()?),
        HubRequest::ResolvePermission { .. } => unreachable!("handled before opening the store"),
        HubRequest::ListAgents => HubResponse::Agents(mews.agents()?),
        HubRequest::CreateAgent {
            slug,
            harness,
            harness_options,
        } => HubResponse::Agent(mews.create_agent_with_harness(
            &slug,
            harness.as_deref().unwrap_or(mews_runtime::MEWS_HARNESS),
            harness_options,
        )?),
        HubRequest::RenameAgent { slug, new_slug } => {
            HubResponse::Agent(mews.rename_agent(&slug, &new_slug)?)
        }
        HubRequest::ArchiveAgent { slug } => {
            mews.archive_agent(&slug)?;
            HubResponse::Ack
        }
        HubRequest::SetApiKey { provider, key } => {
            mews.set_api_key(&provider, key).await?;
            HubResponse::Ack
        }
        HubRequest::SetAuth {
            provider,
            credential,
        } => {
            mews.set_auth(&provider, &credential).await?;
            HubResponse::Ack
        }
        HubRequest::RemoveAuth { provider } => {
            mews.remove_auth(&provider).await?;
            HubResponse::Ack
        }
        HubRequest::ListAuth => HubResponse::Auth(mews.auth_statuses().await?),
        HubRequest::ListModels => HubResponse::Models(mews.models().await?),
        HubRequest::RefreshModels => HubResponse::Models(mews.refresh_models().await?),
        HubRequest::GetProviderDefaults => HubResponse::ProviderDefaults(mews.provider_defaults()?),
        HubRequest::SetDefaultModel { model } => {
            mews.set_default_model(&model).await?;
            HubResponse::Ack
        }
        HubRequest::SetDefaultReasoning { reasoning } => {
            mews.set_default_reasoning(reasoning).await?;
            HubResponse::Ack
        }
        HubRequest::SubscribeSession {
            consumer_id,
            session_id,
        } => {
            mews.subscribe_session(&consumer_id, &session_id)?;
            HubResponse::Ack
        }
        HubRequest::UnsubscribeSession {
            consumer_id,
            session_id,
        } => {
            mews.unsubscribe_session(&consumer_id, &session_id)?;
            HubResponse::Ack
        }
        HubRequest::AcknowledgeEvents {
            consumer_id,
            checkpoint,
        } => {
            mews.acknowledge_events(&consumer_id, checkpoint)?;
            HubResponse::Ack
        }
        HubRequest::GetRun { id } => HubResponse::Run(mews.run(&id)?),
        HubRequest::CancelRun { id } => {
            if let Some(task) = control.run_tasks.lock().await.remove(&id) {
                task.abort();
            }
            mews.cancel_run(&id)?;
            control.event_notify.notify_waiters();
            HubResponse::Ack
        }
        HubRequest::ListSessions => HubResponse::Sessions(mews.sessions()?),
        HubRequest::GetSession { id } => HubResponse::Session(mews.session(&id)?),
        HubRequest::GetSessionModelConfig { id } => {
            let session = mews.session(&id)?;
            HubResponse::SessionModelConfig(mews.session_model_config(&session)?)
        }
        HubRequest::SetSessionModel { id, model } => {
            HubResponse::Session(mews.set_session_model(&id, model.as_deref())?)
        }
        HubRequest::StartSession {
            slug,
            working_directory,
        } => HubResponse::Session(match working_directory {
            Some(directory) => mews.start_session_on(&slug, &directory, host).await?,
            None => {
                let home = std::env::var_os("HOME")
                    .map(PathBuf::from)
                    .context("HOME is unavailable for a locationless client")?
                    .canonicalize()?;
                mews.start_session(&slug, &home).await?
            }
        }),
        HubRequest::StartSessionOn {
            slug,
            host_id,
            working_directory,
        } => {
            let installation = mews.installation()?;
            if host_id == installation.hub_host_id {
                HubResponse::Session(mews.start_session(&slug, &working_directory).await?)
            } else if host_id == *host.host_id() {
                HubResponse::Session(
                    mews.start_session_on(&slug, &working_directory, host)
                        .await?,
                )
            } else {
                let target = remote_hosts
                    .lock()
                    .await
                    .get(&host_id)
                    .cloned()
                    .with_context(|| format!("target Host {host_id} is offline"))?;
                HubResponse::Session(
                    mews.start_session_on(&slug, &working_directory, target.as_ref())
                        .await?,
                )
            }
        }
        HubRequest::StartTurn { .. } | HubRequest::PollEvents { .. } => {
            unreachable!("handled above")
        }
        HubRequest::ListHosts => {
            let installation = mews.installation()?;
            let connected = remote_hosts.lock().await;
            HubResponse::Hosts(
                mews.hosts()?
                    .into_iter()
                    .map(|candidate| crate::HostStatus {
                        connected: candidate.id == installation.hub_host_id
                            || candidate.id == *host.host_id()
                            || connected.contains_key(&candidate.id),
                        host: candidate,
                    })
                    .collect(),
            )
        }
        HubRequest::ListHarnesses => {
            let installation = mews.installation()?;
            let hosts = mews.hosts()?;
            let hub_host = hosts
                .iter()
                .find(|candidate| candidate.id == installation.hub_host_id)
                .context("Hub Host is missing from installation")?
                .clone();
            let mut catalog = mews_host::HarnessCatalog::discover(Some(root))?
                .descriptors()
                .into_iter()
                .map(|descriptor| crate::HostHarnessStatus {
                    host: hub_host.clone(),
                    descriptor,
                })
                .collect::<Vec<_>>();
            let connected = remote_hosts.lock().await;
            for candidate in hosts
                .into_iter()
                .filter(|candidate| candidate.id != installation.hub_host_id)
            {
                let connection = if candidate.id == *host.host_id() {
                    Some(host)
                } else {
                    connected.get(&candidate.id).map(AsRef::as_ref)
                };
                if let Some(connection) = connection {
                    catalog.extend(connection.harness_catalog().into_iter().map(|descriptor| {
                        crate::HostHarnessStatus {
                            host: candidate.clone(),
                            descriptor,
                        }
                    }));
                }
            }
            catalog.sort_by(|left, right| {
                left.descriptor
                    .name
                    .cmp(&right.descriptor.name)
                    .then_with(|| left.host.name.cmp(&right.host.name))
            });
            HubResponse::Harnesses(catalog)
        }
        HubRequest::RefreshHarnesses => {
            let installation = mews.installation()?;
            let hosts = mews.hosts()?;
            let hub_host = hosts
                .iter()
                .find(|candidate| candidate.id == installation.hub_host_id)
                .context("Hub Host is missing from installation")?
                .clone();
            let mut catalog = mews_host::HarnessCatalog::discover(Some(root))?
                .descriptors()
                .into_iter()
                .map(|descriptor| crate::HostHarnessStatus {
                    host: hub_host.clone(),
                    descriptor,
                })
                .collect::<Vec<_>>();
            let connected = remote_hosts.lock().await;
            for candidate in hosts
                .into_iter()
                .filter(|candidate| candidate.id != installation.hub_host_id)
            {
                let connection = if candidate.id == *host.host_id() {
                    host
                } else {
                    connected
                        .get(&candidate.id)
                        .map(AsRef::as_ref)
                        .with_context(|| format!("Host {} is offline", candidate.name))?
                };
                let descriptors = connection.refresh_harness_catalog().await?;
                catalog.extend(descriptors.into_iter().map(|descriptor| {
                    crate::HostHarnessStatus {
                        host: candidate.clone(),
                        descriptor,
                    }
                }));
            }
            catalog.sort_by(|left, right| {
                left.descriptor
                    .name
                    .cmp(&right.descriptor.name)
                    .then_with(|| left.host.name.cmp(&right.host.name))
            });
            HubResponse::Harnesses(catalog)
        }
        HubRequest::RemoveHost { id } => {
            mews.remove_host(&id)?;
            remote_hosts.lock().await.remove(&id);
            HubResponse::Ack
        }
        HubRequest::CreateHostInvitation { .. } => HubResponse::Error(ProtocolError::internal(
            "create invitations on the Hub machine",
        )),
        HubRequest::MoveHub { .. } => {
            HubResponse::Error(ProtocolError::internal("move Hub from the Hub machine"))
        }
        HubRequest::Shutdown => HubResponse::Error(ProtocolError::internal(
            "cannot stop Hub from a Host client",
        )),
    })
}
