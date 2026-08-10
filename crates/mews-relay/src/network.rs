use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, sync::mpsc};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, protocol::WebSocketConfig},
};

use crate::{
    MAX_CIPHERTEXT_BYTES, RelayConnection, RelayError, RelayFrame, RelayIdentity, RelayLink,
    RelayRouter, RelaySigner, SignedAdmissionAuthenticator, registration_message,
};

/// A max-size relay ciphertext expands by at most 4/3 when base64 encoded;
/// this leaves ample room for the JSON frame envelope and registration stays
/// separately bounded below.
const MAX_NETWORK_MESSAGE: usize = MAX_CIPHERTEXT_BYTES * 2;

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RegistrationMessage {
    Hello { identity: RelayIdentity },
    Challenge { challenge: [u8; 32] },
    Proof { signature: String },
    Accepted,
}

pub async fn serve(address: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(address).await?;
    serve_listener(listener).await
}

pub async fn serve_listener(listener: TcpListener) -> Result<()> {
    let router =
        RelayRouter::new(Arc::new(SignedAdmissionAuthenticator), 64).map_err(anyhow::Error::msg)?;
    let permits = Arc::new(tokio::sync::Semaphore::new(1024));
    loop {
        let (stream, _) = listener.accept().await?;
        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
            drop(stream);
            continue;
        };
        let router = router.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let _ = serve_connection(stream, router).await;
        });
    }
}

async fn serve_connection(stream: tokio::net::TcpStream, router: RelayRouter) -> Result<()> {
    let (mut socket, mut connection) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        register_connection(stream, router),
    )
    .await
    .context("relay registration timed out")??;
    loop {
        tokio::select! {
            incoming = socket.next() => {
                let Some(incoming) = incoming else { return Ok(()); };
                let message = incoming?;
                if message.is_close() { return Ok(()); }
                if let Message::Ping(payload) = message {
                    socket.send(Message::Pong(payload)).await?;
                    continue;
                }
                let bytes = match message { Message::Binary(bytes) => bytes, _ => bail!("relay data must be binary") };
                if bytes.len() > MAX_NETWORK_MESSAGE { bail!("relay network frame is too large"); }
                connection.send(serde_json::from_slice(&bytes)?)?;
            }
            frame = connection.receive() => {
                let Some(frame) = frame else { return Ok(()); };
                let encoded = serde_json::to_vec(&frame)?;
                if encoded.len() > MAX_NETWORK_MESSAGE { bail!("relay network frame is too large"); }
                socket.send(Message::Binary(encoded.into())).await?;
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
                bail!("relay connection was idle for 120 seconds");
            }
        }
    }
}

async fn register_connection(
    stream: tokio::net::TcpStream,
    router: RelayRouter,
) -> Result<(WebSocketStream<tokio::net::TcpStream>, RelayConnection)> {
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_NETWORK_MESSAGE))
        .max_frame_size(Some(MAX_NETWORK_MESSAGE));
    let mut socket = tokio_tungstenite::accept_async_with_config(stream, Some(config)).await?;
    let identity = match receive_registration(&mut socket).await? {
        RegistrationMessage::Hello { identity } => identity,
        _ => bail!("relay expected registration hello"),
    };
    let registration = router.challenge(identity.clone())?;
    send_registration(
        &mut socket,
        &RegistrationMessage::Challenge {
            challenge: *registration.challenge(),
        },
    )
    .await?;
    let signature = match receive_registration(&mut socket).await? {
        RegistrationMessage::Proof { signature } => signature,
        _ => bail!("relay expected registration proof"),
    };
    let connection = registration.complete(&signature)?;
    send_registration(&mut socket, &RegistrationMessage::Accepted).await?;
    Ok((socket, connection))
}

pub struct NetworkRelay {
    sender: mpsc::Sender<RelayFrame>,
    receiver: mpsc::Receiver<RelayFrame>,
}

impl NetworkRelay {
    pub async fn connect(
        url: &str,
        identity: RelayIdentity,
        signer: &impl RelaySigner,
    ) -> Result<Self> {
        let (mut socket, _) = tokio_tungstenite::connect_async(url).await?;
        send_registration(
            &mut socket,
            &RegistrationMessage::Hello {
                identity: identity.clone(),
            },
        )
        .await?;
        let challenge = match receive_registration(&mut socket).await? {
            RegistrationMessage::Challenge { challenge } => challenge,
            _ => bail!("relay returned an invalid challenge"),
        };
        let signature = signer.sign(&registration_message(&identity, &challenge));
        send_registration(&mut socket, &RegistrationMessage::Proof { signature }).await?;
        if !matches!(
            receive_registration(&mut socket).await?,
            RegistrationMessage::Accepted
        ) {
            bail!("relay rejected registration");
        }
        let (sender, mut outgoing) = mpsc::channel::<RelayFrame>(64);
        let (incoming, receiver) = mpsc::channel::<RelayFrame>(64);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    frame = outgoing.recv() => {
                        let Some(frame) = frame else { return; };
                        let Ok(encoded) = serde_json::to_vec(&frame) else { return; };
                        if encoded.len() > MAX_NETWORK_MESSAGE { return; }
                        if socket.send(Message::Binary(encoded.into())).await.is_err() { return; }
                    }
                    message = socket.next() => {
                        let Some(Ok(Message::Binary(bytes))) = message else { return; };
                        if bytes.len() > MAX_NETWORK_MESSAGE { return; }
                        let Ok(frame) = serde_json::from_slice::<RelayFrame>(&bytes) else { return; };
                        if frame.validate().is_err() { return; }
                        if incoming.send(frame).await.is_err() { return; }
                    }
                }
            }
        });
        Ok(Self { sender, receiver })
    }
}

#[async_trait]
impl RelayLink for NetworkRelay {
    async fn send_frame(&self, frame: RelayFrame) -> Result<(), RelayError> {
        frame.validate()?;
        self.sender
            .send(frame)
            .await
            .map_err(|_| RelayError::DestinationOffline)
    }

    async fn receive_frame(&mut self) -> Option<RelayFrame> {
        self.receiver.recv().await
    }
}

async fn send_registration<S>(
    socket: &mut WebSocketStream<S>,
    message: &RegistrationMessage,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    socket
        .send(Message::Text(serde_json::to_string(message)?.into()))
        .await?;
    Ok(())
}

async fn receive_registration<S>(socket: &mut WebSocketStream<S>) -> Result<RegistrationMessage>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let message = socket
        .next()
        .await
        .context("relay closed during registration")??;
    let text = message.into_text()?;
    if text.len() > 64 * 1024 {
        bail!("relay registration is too large");
    }
    Ok(serde_json::from_str(&text)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RelayAdmission, RelayPeerId, TestSigner};
    use chrono::{Duration, Utc};
    use mews_protocol::InstallationId;

    #[tokio::test]
    async fn websocket_relay_forwards_a_maximum_sized_frame() {
        let authority = TestSigner::new();
        let alice_key = TestSigner::new();
        let bob_key = TestSigner::new();
        let installation = InstallationId::new();
        let identity = |peer: &str, key: &TestSigner| {
            let peer_id = RelayPeerId::new(peer).unwrap();
            let admission = RelayAdmission::create(
                &authority,
                installation.clone(),
                peer_id.clone(),
                key.public_key(),
                Utc::now() + Duration::minutes(5),
            );
            RelayIdentity {
                installation_id: installation.clone(),
                peer_id,
                public_key: key.public_key(),
                authority_public_key: authority.public_key(),
                admission: Some(admission),
            }
        };
        let alice_identity = identity("host:alice", &alice_key);
        let bob_identity = identity("host:bob", &bob_key);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(serve_listener(listener));
        let url = format!("ws://{address}");
        let (alice, bob) = tokio::join!(
            NetworkRelay::connect(&url, alice_identity.clone(), &alice_key),
            NetworkRelay::connect(&url, bob_identity.clone(), &bob_key)
        );
        let alice = alice.unwrap();
        let mut bob = bob.unwrap();
        let frame = RelayFrame::new(
            installation,
            alice_identity.peer_id,
            bob_identity.peer_id,
            "network-test",
            0,
            vec![u8::MAX; MAX_CIPHERTEXT_BYTES],
        )
        .unwrap();
        alice.send_frame(frame.clone()).await.unwrap();
        assert_eq!(bob.receive_frame().await, Some(frame));
    }

    #[test]
    fn relay_ciphertext_uses_compact_base64_json() {
        let frame = RelayFrame::new(
            InstallationId::new(),
            RelayPeerId::new("host:alice").unwrap(),
            RelayPeerId::new("host:bob").unwrap(),
            "stream",
            1,
            vec![0, 1, 2, 255],
        )
        .unwrap();
        let encoded = serde_json::to_string(&frame).unwrap();
        assert!(encoded.contains(r#""ciphertext":"AAEC/w==""#));
        assert_eq!(serde_json::from_str::<RelayFrame>(&encoded).unwrap(), frame);
    }

    #[test]
    fn maximum_relay_frame_fits_the_network_message_limit() {
        let frame = RelayFrame::new(
            InstallationId::new(),
            RelayPeerId::new("host:alice").unwrap(),
            RelayPeerId::new("host:bob").unwrap(),
            "stream",
            1,
            vec![u8::MAX; MAX_CIPHERTEXT_BYTES],
        )
        .unwrap();
        let encoded = serde_json::to_vec(&frame).unwrap();
        assert!(encoded.len() <= MAX_NETWORK_MESSAGE);
        assert_eq!(
            serde_json::from_slice::<RelayFrame>(&encoded).unwrap(),
            frame
        );
    }
}
