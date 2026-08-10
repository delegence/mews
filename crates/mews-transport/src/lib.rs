use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use mews_relay::{RelayFrame, RelayLink, RelayPeerId};
use serde::{Serialize, de::DeserializeOwned};

use mews_protocol::{HostToHub, HubToHost, InstallationId, PeerEnvelope};

mod crypto;
mod identity;

pub use crypto::{IdentityBinding, MAX_ENCRYPTED_RECORD_PLAINTEXT};
use crypto::{NoiseHandshake, NoiseTransport};
pub use identity::{HostIdentity, NoiseIdentity};

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

    pub async fn send_bytes(&mut self, message: &[u8]) -> Result<()> {
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

    pub async fn receive_bytes(&mut self) -> Result<Vec<u8>> {
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
