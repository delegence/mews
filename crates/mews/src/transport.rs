use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use mews_relay::{RelayFrame, RelayLink, RelayPeerId};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    crypto::{IdentityBinding, MAX_ENCRYPTED_RECORD_PLAINTEXT, NoiseHandshake, NoiseTransport},
    identity::{HostIdentity, NoiseIdentity},
};
use mews_host::ToolRegistry;
use mews_protocol::{HostToHub, HubToHost, InstallationId, PeerEnvelope, RequestId};

const CHUNK_HEADER: usize = 20;
const MAX_MESSAGE_BYTES: usize = 256 * 1024;

pub struct PeerAuthentication<'a> {
    pub installation_id: InstallationId,
    pub local_peer: RelayPeerId,
    pub remote_peer: RelayPeerId,
    pub local_signer: &'a HostIdentity,
    pub trusted_remote_signing_key: &'a str,
    pub local_noise: &'a NoiseIdentity,
    pub expected_remote_noise_key: Option<&'a str>,
    pub stream_id: &'a str,
}

pub async fn connect_initiator<L: RelayLink + 'static>(
    mut relay: L,
    authentication: PeerAuthentication<'_>,
) -> Result<EncryptedRelayPeer> {
    let prologue = channel_prologue(
        &authentication.installation_id,
        &authentication.local_peer,
        &authentication.remote_peer,
        authentication.stream_id,
    );
    let mut noise = NoiseHandshake::initiator(authentication.local_noise, &prologue)?;
    let mut sequence = 0;
    send_relay(&relay, &authentication, sequence, noise.write(b"")?).await?;
    sequence += 1;
    let message = receive_relay(&mut relay, &authentication, 0).await?;
    noise.read(&message)?;
    send_relay(&relay, &authentication, sequence, noise.write(b"")?).await?;
    sequence += 1;
    finish_authentication(relay, authentication, noise, sequence, 1).await
}

pub async fn connect_responder<L: RelayLink + 'static>(
    mut relay: L,
    authentication: PeerAuthentication<'_>,
) -> Result<EncryptedRelayPeer> {
    let prologue = channel_prologue(
        &authentication.installation_id,
        &authentication.remote_peer,
        &authentication.local_peer,
        authentication.stream_id,
    );
    let mut noise = NoiseHandshake::responder(authentication.local_noise, &prologue)?;
    let message = receive_relay(&mut relay, &authentication, 0).await?;
    noise.read(&message)?;
    send_relay(&relay, &authentication, 0, noise.write(b"")?).await?;
    let message = receive_relay(&mut relay, &authentication, 1).await?;
    noise.read(&message)?;
    finish_authentication(relay, authentication, noise, 1, 2).await
}

fn channel_prologue(
    installation_id: &InstallationId,
    initiator: &RelayPeerId,
    responder: &RelayPeerId,
    stream_id: &str,
) -> Vec<u8> {
    format!("mews-host-rpc-v1\0{installation_id}\0{initiator}\0{responder}\0{stream_id}")
        .into_bytes()
}

async fn finish_authentication<L: RelayLink + 'static>(
    mut relay: L,
    authentication: PeerAuthentication<'_>,
    noise: NoiseHandshake,
    mut send_sequence: u64,
    mut receive_sequence: u64,
) -> Result<EncryptedRelayPeer> {
    let remote_noise = noise.remote_static()?.to_vec();
    if let Some(expected) = authentication.expected_remote_noise_key {
        let expected_remote = URL_SAFE_NO_PAD
            .decode(expected)
            .context("invalid expected remote Noise key")?;
        if remote_noise != expected_remote {
            bail!("remote Noise key is not the enrolled key");
        }
    }
    let handshake_hash = noise.handshake_hash().to_vec();
    let binding = IdentityBinding::create(
        authentication.local_signer,
        authentication.installation_id.clone(),
        authentication.local_peer.to_string(),
        authentication.local_noise.public_bytes(),
        &remote_noise,
        &handshake_hash,
    );
    let mut transport = noise.into_transport()?;
    let encoded = serde_json::to_vec(&binding)?;
    let encrypted = transport.encrypt(&encoded)?;
    send_relay(&relay, &authentication, send_sequence, encrypted).await?;
    send_sequence += 1;

    let encrypted = receive_relay(&mut relay, &authentication, receive_sequence).await?;
    receive_sequence += 1;
    let encoded = transport.decrypt(&encrypted)?;
    let remote_binding: IdentityBinding = serde_json::from_slice(&encoded)?;
    remote_binding.verify(
        authentication.trusted_remote_signing_key,
        &authentication.installation_id,
        authentication.remote_peer.as_str(),
        &remote_noise,
        authentication.local_noise.public_bytes(),
        &handshake_hash,
    )?;
    Ok(EncryptedRelayPeer {
        relay: Box::new(relay),
        transport,
        installation_id: authentication.installation_id,
        local_peer: authentication.local_peer,
        remote_peer: authentication.remote_peer,
        stream_id: authentication.stream_id.to_owned(),
        send_sequence,
        receive_sequence,
    })
}

pub struct EncryptedRelayPeer {
    relay: Box<dyn RelayLink>,
    transport: NoiseTransport,
    installation_id: InstallationId,
    local_peer: RelayPeerId,
    remote_peer: RelayPeerId,
    stream_id: String,
    send_sequence: u64,
    receive_sequence: u64,
}

impl EncryptedRelayPeer {
    pub async fn send<T: Serialize>(&mut self, value: &T) -> Result<()> {
        let message = serde_json::to_vec(value)?;
        self.send_bytes(&message).await
    }

    async fn send_bytes(&mut self, message: &[u8]) -> Result<()> {
        if message.len() > MAX_MESSAGE_BYTES {
            bail!("encrypted message exceeds 256 KiB");
        }
        let message_id = uuid::Uuid::now_v7().into_bytes();
        let chunk_size = MAX_ENCRYPTED_RECORD_PLAINTEXT - CHUNK_HEADER;
        let total = message.len().div_ceil(chunk_size).max(1);
        for (index, data) in message.chunks(chunk_size).enumerate() {
            let mut plaintext = Vec::with_capacity(CHUNK_HEADER + data.len());
            plaintext.extend_from_slice(&message_id);
            plaintext.extend_from_slice(&(index as u16).to_be_bytes());
            plaintext.extend_from_slice(&(total as u16).to_be_bytes());
            plaintext.extend_from_slice(data);
            let ciphertext = self.transport.encrypt(&plaintext)?;
            let frame = RelayFrame::new(
                self.installation_id.clone(),
                self.local_peer.clone(),
                self.remote_peer.clone(),
                self.stream_id.clone(),
                self.send_sequence,
                ciphertext,
            )?;
            self.relay.send_frame(frame).await?;
            self.send_sequence += 1;
        }
        Ok(())
    }

    pub async fn receive<T: DeserializeOwned>(&mut self) -> Result<T> {
        let assembled = self.receive_bytes().await?;
        Ok(serde_json::from_slice(&assembled)?)
    }

    async fn receive_bytes(&mut self) -> Result<Vec<u8>> {
        let mut assembled = Vec::new();
        let mut expected_id = None;
        let mut expected_total = None;
        loop {
            let frame = self
                .relay
                .receive_frame()
                .await
                .context("relay connection closed")?;
            if frame.installation_id != self.installation_id
                || frame.source_id != self.remote_peer
                || frame.destination_id != self.local_peer
                || frame.stream_id != self.stream_id
                || frame.sequence != self.receive_sequence
            {
                bail!("relay frame violates authenticated stream ordering");
            }
            self.receive_sequence += 1;
            let plaintext = self.transport.decrypt(&frame.ciphertext)?;
            if plaintext.len() < CHUNK_HEADER {
                bail!("encrypted chunk is truncated");
            }
            let id: [u8; 16] = plaintext[..16].try_into().expect("checked length");
            let index = u16::from_be_bytes(plaintext[16..18].try_into().expect("checked length"));
            let total = u16::from_be_bytes(plaintext[18..20].try_into().expect("checked length"));
            if total == 0 || index >= total || index as usize > 5 {
                bail!("invalid encrypted chunk sequence");
            }
            if expected_id.get_or_insert(id) != &id
                || expected_total.get_or_insert(total) != &total
                || index as usize
                    != assembled
                        .len()
                        .div_ceil(MAX_ENCRYPTED_RECORD_PLAINTEXT - CHUNK_HEADER)
            {
                bail!("interleaved or out-of-order encrypted chunks");
            }
            assembled.extend_from_slice(&plaintext[CHUNK_HEADER..]);
            if assembled.len() > MAX_MESSAGE_BYTES {
                bail!("reassembled message is too large");
            }
            if index + 1 == total {
                return Ok(assembled);
            }
        }
    }
}

/// Runs the Hub half of the serialized Host RPC over an authenticated relay.
pub async fn run_hub_host_link(
    mut peer: EncryptedRelayPeer,
    mut outbound: tokio::sync::mpsc::Receiver<HubToHost>,
    inbound: tokio::sync::mpsc::Sender<HostToHub>,
) -> Result<()> {
    loop {
        tokio::select! {
            request = outbound.recv() => {
                let Some(request) = request else { return Ok(()); };
                peer.send_bytes(&mews_protocol::encode(request)?).await?;
            }
            response = peer.receive_bytes() => {
                let response = mews_protocol::decode(&response?)?;
                inbound.send(response).await.context("Hub Host link closed")?;
            }
        }
    }
}

/// Runs the Host half. Tool code and cwd resolution never cross into Hub.
pub async fn run_host_rpc(mut peer: EncryptedRelayPeer, registry: ToolRegistry) -> Result<()> {
    let harnesses = mews_host::HarnessCatalog::discover(None)?.descriptors();
    peer.send_bytes(&mews_protocol::encode(HostToHub::Ready {
        tools: registry.definitions(),
        harnesses,
    })?)
    .await?;
    let permission_waiters: crate::host::AcpPermissionWaiters =
        std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    let binding_waiters: crate::host::AcpBindingWaiters =
        std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));
    let acp_cancellations =
        std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::<
            RequestId,
            mews_agent::CancellationToken,
        >::new()));
    let (output_tx, mut output_rx) =
        tokio::sync::mpsc::channel(crate::host::ACP_EVENT_CHANNEL_CAPACITY);
    loop {
        tokio::select! {
            bytes = peer.receive_bytes() => {
                let request: HubToHost = mews_protocol::decode(&bytes?)?;
                if let HubToHost::ResolveAcpPermission { permission_id, option_id } = request {
                    if let Some(waiter) = permission_waiters.lock().expect("ACP permission waiters poisoned").remove(&permission_id) {
                        let _ = waiter.send(option_id);
                    }
                    continue;
                }
                if let HubToHost::AcknowledgeAcpSessionBinding { acknowledgement_id } = request {
                    if let Some(waiter) = binding_waiters.lock().expect("ACP binding waiters poisoned").remove(&acknowledgement_id) {
                        let _ = waiter.send(());
                    }
                    continue;
                }
                if let HubToHost::CancelAcp { request_id } = request {
                    if let Some(cancellation) = acp_cancellations.lock().expect("ACP cancellations poisoned").remove(&request_id) {
                        cancellation.cancel();
                    }
                    continue;
                }
                let acp_request_id = match &request {
                    HubToHost::RunAcp { request_id, .. } => Some(request_id.clone()),
                    _ => None,
                };
                let cancellation = acp_request_id.as_ref().map(|request_id| {
                    let cancellation = mews_agent::CancellationToken::new();
                    acp_cancellations.lock().expect("ACP cancellations poisoned").insert(request_id.clone(), cancellation.clone());
                    cancellation
                });
                let registry = registry.clone();
                let output = output_tx.clone();
                let waiters = std::sync::Arc::clone(&permission_waiters);
                let binding_waiters = std::sync::Arc::clone(&binding_waiters);
                let cancellations = std::sync::Arc::clone(&acp_cancellations);
                tokio::spawn(async move {
                    let (event_tx, mut event_rx) =
                        tokio::sync::mpsc::channel(crate::host::ACP_EVENT_CHANNEL_CAPACITY);
                    let response = crate::host::handle_host_request_streaming(
                        &registry, None, request, Some(event_tx), Some(waiters), Some(binding_waiters), cancellation,
                    );
                    tokio::pin!(response);
                    let response = loop {
                        tokio::select! {
                            response = &mut response => break response,
                            event = event_rx.recv() => if let Some(event) = event {
                                let _ = output.send(event).await;
                            }
                        }
                    };
                    while let Ok(event) = event_rx.try_recv() {
                        let _ = output.send(event).await;
                    }
                    let _ = output.send(response).await;
                    if let Some(request_id) = acp_request_id {
                        cancellations.lock().expect("ACP cancellations poisoned").remove(&request_id);
                    }
                });
            }
            Some(response) = output_rx.recv() => {
                peer.send_bytes(&mews_protocol::encode(response)?).await?;
            }
        }
    }
}

/// Full-duplex encrypted protocol pump used by persistent Hub/Host daemons.
pub async fn run_peer_bridge(
    mut peer: EncryptedRelayPeer,
    mut outbound: tokio::sync::mpsc::Receiver<PeerEnvelope>,
    inbound: tokio::sync::mpsc::Sender<PeerEnvelope>,
) -> Result<()> {
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                peer.send_bytes(&mews_protocol::encode(PeerEnvelope::Heartbeat { nonce: 0 })?).await?;
            }
            message = outbound.recv() => {
                let Some(message) = message else { return Ok(()); };
                peer.send_bytes(&mews_protocol::encode(message)?).await?;
            }
            message = peer.receive_bytes() => {
                let message = mews_protocol::decode(&message?)?;
                if !matches!(message, PeerEnvelope::Heartbeat { .. }) {
                    inbound.send(message).await.context("peer protocol receiver closed")?;
                }
            }
        }
    }
}

async fn send_relay(
    relay: &impl RelayLink,
    authentication: &PeerAuthentication<'_>,
    sequence: u64,
    ciphertext: Vec<u8>,
) -> Result<()> {
    relay
        .send_frame(RelayFrame::new(
            authentication.installation_id.clone(),
            authentication.local_peer.clone(),
            authentication.remote_peer.clone(),
            authentication.stream_id,
            sequence,
            ciphertext,
        )?)
        .await?;
    Ok(())
}

async fn receive_relay(
    relay: &mut impl RelayLink,
    authentication: &PeerAuthentication<'_>,
    sequence: u64,
) -> Result<Vec<u8>> {
    let frame = relay
        .receive_frame()
        .await
        .context("relay connection closed")?;
    if frame.installation_id != authentication.installation_id
        || frame.source_id != authentication.remote_peer
        || frame.destination_id != authentication.local_peer
        || frame.stream_id != authentication.stream_id
        || frame.sequence != sequence
    {
        bail!("invalid relay handshake frame");
    }
    Ok(frame.ciphertext)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use super::*;
    use mews_relay::{
        RelayAuthenticator, RelayConnection, RelayIdentity, RelayRouter, registration_message,
    };

    struct Enrolled(HashSet<(InstallationId, RelayPeerId, String)>);
    impl RelayAuthenticator for Enrolled {
        fn is_enrolled(&self, identity: &RelayIdentity) -> bool {
            self.0.contains(&(
                identity.installation_id.clone(),
                identity.peer_id.clone(),
                identity.public_key.clone(),
            ))
        }
    }

    fn register(
        router: &RelayRouter,
        identity: RelayIdentity,
        signer: &HostIdentity,
    ) -> RelayConnection {
        let registration = router.challenge(identity.clone()).unwrap();
        let signature = signer.sign(&registration_message(&identity, registration.challenge()));
        registration.complete(&signature).unwrap()
    }

    #[tokio::test]
    async fn authenticated_noise_carries_fragmented_messages_through_relay() {
        let root = tempfile::tempdir().unwrap();
        let installation = InstallationId::new();
        let authority = HostIdentity::load_or_create(&root.path().join("authority.key")).unwrap();
        let host_signing =
            HostIdentity::load_or_create(&root.path().join("secrets/host.key")).unwrap();
        let hub_noise = NoiseIdentity::load_or_create(&root.path().join("hub.noise")).unwrap();
        let host_noise = NoiseIdentity::load_or_create(&root.path().join("host.noise")).unwrap();
        let hub_identity = RelayIdentity {
            installation_id: installation.clone(),
            peer_id: RelayPeerId::new("hub").unwrap(),
            public_key: authority.public_key(),
            authority_public_key: authority.public_key(),
            admission: Some(mews_relay::RelayAdmission::create(
                &authority,
                installation.clone(),
                RelayPeerId::new("hub").unwrap(),
                authority.public_key(),
                chrono::Utc::now() + chrono::Duration::minutes(5),
            )),
        };
        let host_identity = RelayIdentity {
            installation_id: installation.clone(),
            peer_id: RelayPeerId::new("host:test").unwrap(),
            public_key: host_signing.public_key(),
            authority_public_key: authority.public_key(),
            admission: None,
        };
        let enrolled = [hub_identity.clone(), host_identity.clone()]
            .into_iter()
            .map(|identity| {
                (
                    identity.installation_id,
                    identity.peer_id,
                    identity.public_key,
                )
            })
            .collect();
        let router = RelayRouter::new(Arc::new(Enrolled(enrolled)), 32).unwrap();
        let hub_relay = register(&router, hub_identity.clone(), &authority);
        let host_relay = register(&router, host_identity.clone(), &host_signing);
        let stream = "test-stream";
        let authority_public = authority.public_key();
        let host_public = host_signing.public_key();
        let hub_noise_public = hub_noise.public_key();
        let host_noise_public = host_noise.public_key();
        let (host, hub) = tokio::join!(
            connect_initiator(
                host_relay,
                PeerAuthentication {
                    installation_id: installation.clone(),
                    local_peer: host_identity.peer_id.clone(),
                    remote_peer: hub_identity.peer_id.clone(),
                    local_signer: &host_signing,
                    trusted_remote_signing_key: &authority_public,
                    local_noise: &host_noise,
                    expected_remote_noise_key: Some(&hub_noise_public),
                    stream_id: stream,
                },
            ),
            connect_responder(
                hub_relay,
                PeerAuthentication {
                    installation_id: installation,
                    local_peer: hub_identity.peer_id,
                    remote_peer: host_identity.peer_id,
                    local_signer: &authority,
                    trusted_remote_signing_key: &host_public,
                    local_noise: &hub_noise,
                    expected_remote_noise_key: Some(&host_noise_public),
                    stream_id: stream,
                },
            )
        );
        let mut host = host.unwrap();
        let mut hub = hub.unwrap();
        let payload = "private".repeat(20_000);
        host.send(&payload).await.unwrap();
        let received: String = hub.receive().await.unwrap();
        assert_eq!(received, payload);

        let remote_cwd = tempfile::tempdir().unwrap();
        std::fs::write(
            remote_cwd.path().join("only-remote.txt"),
            "from remote host",
        )
        .unwrap();
        let host_id = crate::HostId::new();
        let tools = ToolRegistry::with_defaults();
        let (request_sender, request_receiver) = tokio::sync::mpsc::channel(32);
        let (response_sender, response_receiver) = tokio::sync::mpsc::channel(32);
        let connected = crate::host::ConnectedHost::from_channels(
            host_id,
            Vec::new(),
            request_sender,
            response_receiver,
        )
        .await
        .unwrap();
        tokio::spawn(run_hub_host_link(hub, request_receiver, response_sender));
        tokio::spawn(run_host_rpc(host, tools));
        use crate::host::HostControl;
        let cwd = connected.attest_directory(remote_cwd.path()).await.unwrap();
        let result = connected
            .execute_tool("read", serde_json::json!({"path":"only-remote.txt"}), &cwd)
            .await
            .unwrap();
        assert_eq!(result["content"], "from remote host");
    }

    #[tokio::test]
    async fn durable_session_executes_on_enrolled_remote_host_without_fallback() {
        let hub_root = tempfile::tempdir().unwrap();
        std::fs::write(hub_root.path().join(".test-provider"), []).unwrap();
        let host_root = tempfile::tempdir().unwrap();
        let remote_cwd = tempfile::tempdir().unwrap();
        std::fs::write(
            remote_cwd.path().join("remote.txt"),
            "remote durable result",
        )
        .unwrap();
        let mut mews = crate::service::Mews::setup(hub_root.path(), "laptop").unwrap();
        let router = tokio::spawn(mews_router::serve(hub_root.path().to_path_buf()));
        let router_client = mews_router::RouterClient::new(hub_root.path());
        while !router_client.ready().await {
            tokio::task::yield_now().await;
        }
        mews.set_default_model("test").await.unwrap();
        mews.create_agent("coder").unwrap();
        let offer = mews.create_invitation(Some("ws://127.0.0.1:9000")).unwrap();
        let host_signing =
            HostIdentity::load_or_create(&host_root.path().join("secrets/host.key")).unwrap();
        let host_noise =
            NoiseIdentity::load_or_create(&host_root.path().join("secrets/host-noise.key"))
                .unwrap();
        let join = crate::enrollment::JoinRequest::create(
            &offer,
            "mini-pc".into(),
            &host_signing,
            &host_noise,
            "ws://mini-pc.local:8787".into(),
        );
        let enrolled = mews.enroll_host(&offer, &join).unwrap();
        let installation = mews.installation().unwrap();
        let authority =
            HostIdentity::load(&hub_root.path().join("secrets/installation.key")).unwrap();
        let hub_noise =
            NoiseIdentity::load(&hub_root.path().join("secrets/hub-noise.key")).unwrap();
        let hub_identity = RelayIdentity {
            installation_id: installation.id.clone(),
            peer_id: RelayPeerId::new("hub").unwrap(),
            public_key: authority.public_key(),
            authority_public_key: authority.public_key(),
            admission: Some(mews_relay::RelayAdmission::create(
                &authority,
                installation.id.clone(),
                RelayPeerId::new("hub").unwrap(),
                authority.public_key(),
                chrono::Utc::now() + chrono::Duration::minutes(5),
            )),
        };
        let host_identity = RelayIdentity {
            installation_id: installation.id.clone(),
            peer_id: RelayPeerId::new(format!("host:{}", enrolled.id)).unwrap(),
            public_key: host_signing.public_key(),
            authority_public_key: authority.public_key(),
            admission: Some(mews_relay::RelayAdmission::create(
                &authority,
                installation.id.clone(),
                RelayPeerId::new(format!("host:{}", enrolled.id)).unwrap(),
                host_signing.public_key(),
                chrono::Utc::now() + chrono::Duration::minutes(5),
            )),
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let relay_url = format!("ws://{}", listener.local_addr().unwrap());
        tokio::spawn(mews_relay::serve_listener(listener));
        let (hub_relay, host_relay) = tokio::join!(
            mews_relay::NetworkRelay::connect(&relay_url, hub_identity.clone(), &authority),
            mews_relay::NetworkRelay::connect(&relay_url, host_identity.clone(), &host_signing),
        );
        let hub_relay = hub_relay.unwrap();
        let host_relay = host_relay.unwrap();
        let authority_public = authority.public_key();
        let host_public = host_signing.public_key();
        let hub_noise_public = hub_noise.public_key();
        let host_noise_public = host_noise.public_key();
        let (host_peer, hub_peer) = tokio::join!(
            connect_initiator(
                host_relay,
                PeerAuthentication {
                    installation_id: installation.id.clone(),
                    local_peer: host_identity.peer_id.clone(),
                    remote_peer: hub_identity.peer_id.clone(),
                    local_signer: &host_signing,
                    trusted_remote_signing_key: &authority_public,
                    local_noise: &host_noise,
                    expected_remote_noise_key: Some(&hub_noise_public),
                    stream_id: "durable",
                }
            ),
            connect_responder(
                hub_relay,
                PeerAuthentication {
                    installation_id: installation.id,
                    local_peer: hub_identity.peer_id,
                    remote_peer: host_identity.peer_id,
                    local_signer: &authority,
                    trusted_remote_signing_key: &host_public,
                    local_noise: &hub_noise,
                    expected_remote_noise_key: Some(&host_noise_public),
                    stream_id: "durable",
                }
            )
        );
        let tools = ToolRegistry::with_defaults();
        let (request_sender, request_receiver) = tokio::sync::mpsc::channel(32);
        let (response_sender, response_receiver) = tokio::sync::mpsc::channel(32);
        let connected = crate::host::ConnectedHost::from_channels(
            enrolled.id.clone(),
            Vec::new(),
            request_sender,
            response_receiver,
        )
        .await
        .unwrap();
        tokio::spawn(run_hub_host_link(
            hub_peer.unwrap(),
            request_receiver,
            response_sender,
        ));
        tokio::spawn(run_host_rpc(host_peer.unwrap(), tools));
        let session = mews
            .start_session_on("coder", remote_cwd.path(), &connected)
            .await
            .unwrap();
        let answer = mews
            .send_on(
                &session,
                "test:read remote.txt",
                serde_json::Value::Null,
                &connected,
            )
            .await
            .unwrap();
        assert_eq!(session.host_id, enrolled.id);
        assert!(answer.contains("remote durable result"));
        router.abort();
    }
}
