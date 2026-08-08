#[cfg(not(unix))]
compile_error!("the MEWS Host daemon requires Unix sockets");

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use mews_host::ToolRegistry;
use mews_protocol::{HostToHub, HubResponse, PeerEnvelope, ProtocolError, RequestId};
use mews_relay::{NetworkRelay, RelayIdentity};
use tokio::sync::{mpsc, oneshot};

use super::activate_hub_transfer;
use crate::{
    enrollment::relay::JoinedHostState,
    identity::{HostIdentity, NoiseIdentity},
    transport::{PeerAuthentication, connect_initiator, run_peer_bridge},
};

pub async fn serve_joined_host(root: PathBuf) -> Result<()> {
    loop {
        if root.join("hub-activate").exists() {
            activate_hub_transfer(&root)?;
        }
        if root.join("hub-promote").exists() {
            return Box::pin(crate::hub::serve(root)).await;
        }
        match serve_joined_host_once(&root).await {
            Ok(true) => {
                return Box::pin(crate::hub::serve(root)).await;
            }
            Ok(false) => return Ok(()),
            Err(error) => eprintln!("Host connection lost: {error:#}"),
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

async fn serve_joined_host_once(root: &Path) -> Result<bool> {
    let root = root.to_path_buf();
    let state: JoinedHostState = serde_json::from_slice(&fs::read(root.join("hub.json"))?)?;
    let identity = Arc::new(HostIdentity::load(&root.join("secrets/host.key"))?);
    let noise = NoiseIdentity::load(&root.join("secrets/host-noise.key"))?;
    let relay_identity = RelayIdentity {
        installation_id: state.installation_id.clone(),
        peer_id: state.accepted.relay_admission.peer_id.clone(),
        public_key: identity.public_key(),
        authority_public_key: state.installation_public_key.clone(),
        admission: Some(state.accepted.relay_admission.clone()),
    };
    let stream = state.accepted.host.id.to_string();
    let mut peer = None;
    for relay_url in &state.relay_urls {
        let Ok(Ok(relay)) = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            NetworkRelay::connect(relay_url, relay_identity.clone(), identity.as_ref()),
        )
        .await
        else {
            continue;
        };
        if let Ok(Ok(connected)) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            connect_initiator(
                relay,
                PeerAuthentication {
                    installation_id: state.installation_id.clone(),
                    local_peer: relay_identity.peer_id.clone(),
                    remote_peer: state.accepted.hub_relay_admission.peer_id.clone(),
                    local_signer: &identity,
                    trusted_remote_signing_key: &state.installation_public_key,
                    local_noise: &noise,
                    expected_remote_noise_key: Some(&state.hub_noise_public_key),
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
    let peer = peer.context("no relay candidate paired with Hub")?;
    let (peer_out, peer_out_rx) = mpsc::channel(64);
    let (peer_in_tx, mut peer_in) = mpsc::channel(64);
    let bridge = tokio::spawn(run_peer_bridge(peer, peer_out_rx, peer_in_tx));
    let registry = Arc::new(ToolRegistry::with_host_extensions(&root)?);
    let harnesses = mews_host::HarnessCatalog::discover(Some(&root))?.descriptors();
    tokio::spawn({
        let registry = registry.as_ref().clone();
        let root = root.to_path_buf();
        async move { registry.watch_host_extensions(root).await }
    });
    peer_out
        .send(PeerEnvelope::ToolResponse {
            body: HostToHub::Ready {
                tools: registry.definitions(),
                harnesses,
            },
        })
        .await?;
    let mut catalog = registry.subscribe();
    let catalog_peer = peer_out.clone();
    tokio::spawn(async move {
        while catalog.changed().await.is_ok() {
            let tools = catalog.borrow().clone();
            if catalog_peer
                .send(PeerEnvelope::ToolResponse {
                    body: HostToHub::ToolCatalogChanged { tools },
                })
                .await
                .is_err()
            {
                return;
            }
        }
    });
    let pending: Arc<Mutex<HashMap<RequestId, oneshot::Sender<HubResponse>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let dispatch_pending = Arc::clone(&pending);
    let dispatch_peer = peer_out.clone();
    let dispatch_registry = Arc::clone(&registry);
    let agent_root = root.clone();
    let permission_waiters: crate::host::lifecycle::AcpPermissionWaiters =
        Arc::new(std::sync::Mutex::new(HashMap::new()));
    let binding_waiters: crate::host::AcpBindingWaiters =
        Arc::new(std::sync::Mutex::new(HashMap::new()));
    let acp_cancellations = Arc::new(std::sync::Mutex::new(HashMap::<
        RequestId,
        mews_agent::CancellationToken,
    >::new()));
    let (promotion_tx, mut promotion_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        while let Some(message) = peer_in.recv().await {
            match message {
                PeerEnvelope::ToolRequest { body } => {
                    if let mews_protocol::HubToHost::ResolveAcpPermission {
                        permission_id,
                        option_id,
                    } = &body
                    {
                        if let Some(waiter) = permission_waiters
                            .lock()
                            .expect("ACP permission waiters poisoned")
                            .remove(permission_id)
                        {
                            let _ = waiter.send(option_id.clone());
                        }
                        continue;
                    }
                    if let mews_protocol::HubToHost::AcknowledgeAcpSessionBinding {
                        acknowledgement_id,
                    } = &body
                    {
                        if let Some(waiter) = binding_waiters
                            .lock()
                            .expect("ACP binding waiters poisoned")
                            .remove(acknowledgement_id)
                        {
                            let _ = waiter.send(());
                        }
                        continue;
                    }
                    if let mews_protocol::HubToHost::CancelAcp { request_id } = &body {
                        if let Some(cancellation) = acp_cancellations
                            .lock()
                            .expect("ACP cancellations poisoned")
                            .remove(request_id)
                        {
                            cancellation.cancel();
                        }
                        continue;
                    }
                    let promotes =
                        matches!(&body, mews_protocol::HubToHost::ActivateHubTransfer { .. });
                    if matches!(&body, mews_protocol::HubToHost::RunAcp { .. }) {
                        let request_id = match &body {
                            mews_protocol::HubToHost::RunAcp { request_id, .. } => {
                                request_id.clone()
                            }
                            _ => unreachable!(),
                        };
                        let cancellation = mews_agent::CancellationToken::new();
                        acp_cancellations
                            .lock()
                            .expect("ACP cancellations poisoned")
                            .insert(request_id.clone(), cancellation.clone());
                        let registry = Arc::clone(&dispatch_registry);
                        let root = agent_root.clone();
                        let peer = dispatch_peer.clone();
                        let waiters = Arc::clone(&permission_waiters);
                        let binding_waiters = Arc::clone(&binding_waiters);
                        let cancellations = Arc::clone(&acp_cancellations);
                        tokio::spawn(async move {
                            let (event_tx, mut event_rx) =
                                tokio::sync::mpsc::channel(super::ACP_EVENT_CHANNEL_CAPACITY);
                            let response = crate::host::handle_host_request_streaming(
                                &registry,
                                Some(&root),
                                body,
                                Some(event_tx),
                                Some(waiters),
                                Some(binding_waiters),
                                Some(cancellation),
                            );
                            tokio::pin!(response);
                            let response = loop {
                                tokio::select! {
                                    response = &mut response => break response,
                                    event = event_rx.recv() => if let Some(body) = event {
                                        let _ = peer.send(PeerEnvelope::ToolResponse { body }).await;
                                    }
                                }
                            };
                            while let Ok(body) = event_rx.try_recv() {
                                let _ = peer.send(PeerEnvelope::ToolResponse { body }).await;
                            }
                            let _ = peer
                                .send(PeerEnvelope::ToolResponse { body: response })
                                .await;
                            cancellations
                                .lock()
                                .expect("ACP cancellations poisoned")
                                .remove(&request_id);
                        });
                        continue;
                    }
                    let (event_tx, mut event_rx) =
                        tokio::sync::mpsc::channel(super::ACP_EVENT_CHANNEL_CAPACITY);
                    let response = crate::host::handle_host_request_streaming(
                        &dispatch_registry,
                        Some(&agent_root),
                        body,
                        Some(event_tx),
                        Some(Arc::clone(&permission_waiters)),
                        Some(Arc::clone(&binding_waiters)),
                        None,
                    );
                    tokio::pin!(response);
                    let response = loop {
                        tokio::select! {
                            response = &mut response => break response,
                            event = event_rx.recv() => {
                                if let Some(body) = event {
                                    let _ = dispatch_peer.send(PeerEnvelope::ToolResponse { body }).await;
                                }
                            }
                        }
                    };
                    while let Ok(body) = event_rx.try_recv() {
                        let _ = dispatch_peer
                            .send(PeerEnvelope::ToolResponse { body })
                            .await;
                    }
                    let _ = dispatch_peer
                        .send(PeerEnvelope::ToolResponse { body: response })
                        .await;
                    if promotes && agent_root.join("mews.db").exists() {
                        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                        let _ = promotion_tx.send(true);
                        return;
                    }
                }
                PeerEnvelope::ClientResponse { request_id, body } => {
                    if let Some(reply) = dispatch_pending
                        .lock()
                        .expect("pending clients poisoned")
                        .remove(&request_id)
                    {
                        let _ = reply.send(body);
                    }
                }
                _ => return,
            }
        }
    });
    let waiting = Arc::clone(&pending);
    let result = tokio::select! {
        result = super::client_proxy::serve_local_clients(&root, peer_out, pending) => result.map(|_| false),
        result = bridge => result.context("Host peer task stopped")?.map(|_| false),
        changed = promotion_rx.changed() => {
            changed.context("promotion signal closed")?;
            Ok(*promotion_rx.borrow())
        },
    };
    for (_, reply) in waiting.lock().expect("pending clients poisoned").drain() {
        let _ = reply.send(HubResponse::Error(ProtocolError::unavailable(
            "Hub connection closed",
        )));
    }
    result
}
