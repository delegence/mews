#[cfg(not(unix))]
compile_error!("the MEWS Host daemon requires Unix sockets");

use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use mews_relay::{NetworkRelay, RelayIdentity};
use tokio::sync::mpsc;

use crate::{
    app::Mews,
    enrollment::join::EnrollmentAccepted,
    host::ConnectedHost,
    identity::{HostIdentity, NoiseIdentity},
    server::{HubRuntime, RequestOrigin, dispatch, protocol_error, resolve_request_location},
    transport::{PeerAuthentication, connect_responder, run_peer_bridge},
};
use mews_protocol::{HubRequest, HubResponse, PeerEnvelope, ProtocolError};

pub(crate) async fn serve_hub_host(
    root: PathBuf,
    relay_urls: Vec<String>,
    accepted: EnrollmentAccepted,
    remote_hosts: crate::server::RemoteHosts,
    control: crate::server::HubControl,
    local_host: Arc<ConnectedHost>,
) -> Result<()> {
    loop {
        if let Err(error) = serve_hub_host_once(
            &root,
            &relay_urls,
            &accepted,
            &remote_hosts,
            &control,
            &local_host,
        )
        .await
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
    remote_hosts: &crate::server::RemoteHosts,
    control: &crate::server::HubControl,
    local_host: &Arc<ConnectedHost>,
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
    let runtime = Arc::new(HubRuntime {
        remote_hosts: Arc::clone(remote_hosts),
        local_host: Arc::clone(local_host),
        control: control.clone(),
    });
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
    let (client_tx, client_rx) = mpsc::channel(32);
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
    let dispatch_root = root.to_path_buf();
    let dispatch_host = Arc::clone(&connected);
    let dispatch_runtime = Arc::clone(&runtime);
    serve_client_requests(client_rx, peer_out, move |request| {
        let root = dispatch_root.clone();
        let host = Arc::clone(&dispatch_host);
        let runtime = Arc::clone(&dispatch_runtime);
        async move { dispatch_host_request(&runtime, &root, &host, request).await }
    })
    .await?;
    let mut hosts = remote_hosts.lock().await;
    if hosts
        .get(&accepted.host.id)
        .is_some_and(|current| Arc::ptr_eq(current, &connected))
    {
        hosts.remove(&accepted.host.id);
    }
    Ok(())
}

async fn serve_client_requests<F, Fut>(
    mut incoming: mpsc::Receiver<(mews_protocol::RequestId, HubRequest)>,
    responses: mpsc::Sender<PeerEnvelope>,
    dispatch: F,
) -> Result<()>
where
    F: Fn(HubRequest) -> Fut + Clone + 'static,
    Fut: Future<Output = HubResponse> + 'static,
{
    const MAX_CONCURRENT_REQUESTS: usize = 16;
    let mut requests = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            biased;
            result = requests.join_next(), if !requests.is_empty() => {
                result.context("remote request task disappeared")?
                    .context("remote request task panicked")??;
            }
            request = incoming.recv(), if requests.len() < MAX_CONCURRENT_REQUESTS => {
                let Some((request_id, request)) = request else {
                    break;
                };
                let responses = responses.clone();
                let dispatch = dispatch.clone();
                requests.spawn_local(async move {
                    responses
                        .send(PeerEnvelope::ClientResponse {
                            request_id,
                            body: dispatch(request).await,
                        })
                        .await
                });
            }
        }
    }
    while let Some(result) = requests.join_next().await {
        result.context("remote request task panicked")??;
    }
    Ok(())
}

async fn dispatch_host_request(
    runtime: &HubRuntime,
    root: &Path,
    host: &ConnectedHost,
    request: HubRequest,
) -> HubResponse {
    if let Some(error) = remote_origin_error(&request) {
        return HubResponse::Error(error);
    }
    let request = match resolve_request_location(RequestOrigin::Host(host), request) {
        Ok(request) => request,
        Err(error) => return HubResponse::Error(protocol_error(&error)),
    };
    match dispatch(runtime, root, RequestOrigin::Host(host), request).await {
        Ok((response, false)) => response,
        Ok((_, true)) => unreachable!("Host-origin requests cannot stop the Hub"),
        Err(error) => HubResponse::Error(protocol_error(&error)),
    }
}

fn remote_origin_error(request: &HubRequest) -> Option<ProtocolError> {
    use mews_protocol::ProtocolErrorCode;

    let message = match request {
        HubRequest::CreateHostInvitation { .. } => "create invitations on the Hub machine",
        HubRequest::MoveHub { .. } => "move the Hub from the Hub machine",
        HubRequest::Shutdown => "cannot stop the Hub from a Host client",
        _ => return None,
    };
    Some(ProtocolError {
        code: ProtocolErrorCode::InvalidRequest,
        message: message.into(),
        retryable: false,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use mews_protocol::ProtocolErrorCode;

    use super::*;

    #[test]
    fn host_origin_restrictions_are_typed_invalid_requests() {
        let error = remote_origin_error(&HubRequest::Shutdown).unwrap();
        assert_eq!(error.code, ProtocolErrorCode::InvalidRequest);
        assert!(!error.retryable);
        assert!(remote_origin_error(&HubRequest::Status).is_none());
    }

    #[tokio::test]
    async fn remote_request_scheduler_is_concurrent_and_bounded() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let (request_tx, request_rx) = mpsc::channel(64);
                let (response_tx, mut response_rx) = mpsc::channel(64);
                let slow = mews_protocol::RequestId::new();
                request_tx
                    .send((
                        slow.clone(),
                        HubRequest::PollEvents {
                            consumer_id: crate::ConsumerId::new(),
                            limit: 1,
                            wait_ms: 1,
                        },
                    ))
                    .await
                    .unwrap();
                for _ in 0..31 {
                    request_tx
                        .send((mews_protocol::RequestId::new(), HubRequest::Status))
                        .await
                        .unwrap();
                }
                drop(request_tx);
                let active = Arc::new(AtomicUsize::new(0));
                let peak = Arc::new(AtomicUsize::new(0));
                let scheduler = serve_client_requests(request_rx, response_tx, {
                    let active = Arc::clone(&active);
                    let peak = Arc::clone(&peak);
                    move |request| {
                        let active = Arc::clone(&active);
                        let peak = Arc::clone(&peak);
                        async move {
                            let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                            peak.fetch_max(current, Ordering::SeqCst);
                            let delay = if matches!(request, HubRequest::PollEvents { .. }) {
                                75
                            } else {
                                5
                            };
                            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                            active.fetch_sub(1, Ordering::SeqCst);
                            HubResponse::Ack
                        }
                    }
                });
                let collect = async move {
                    let mut ids = Vec::new();
                    while let Some(PeerEnvelope::ClientResponse { request_id, .. }) =
                        response_rx.recv().await
                    {
                        ids.push(request_id);
                    }
                    ids
                };

                let (result, responses) = tokio::join!(scheduler, collect);

                result.unwrap();
                assert_eq!(responses.len(), 32);
                assert_ne!(responses.first(), Some(&slow));
                assert!(peak.load(Ordering::SeqCst) > 1);
                assert!(peak.load(Ordering::SeqCst) <= 16);
            })
            .await;
    }
}
