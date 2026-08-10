//! Stateless routing primitives for the MEWS relay.
//!
//! The relay authenticates enrolled peer identities and forwards opaque frames.
//! It deliberately knows nothing about the encrypted application protocol.

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex, Weak},
};

use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc;

use mews_protocol::InstallationId;

mod network;

pub use network::{NetworkRelay, serve, serve_listener};

/// Signing capability required to create admissions and prove relay identity.
pub trait RelaySigner {
    fn public_key(&self) -> String;
    fn sign(&self, message: &[u8]) -> String;
}

#[cfg(test)]
pub(crate) struct TestSigner(ed25519_dalek::SigningKey);

#[cfg(test)]
impl TestSigner {
    pub(crate) fn new() -> Self {
        Self(ed25519_dalek::SigningKey::generate(&mut rand_core::OsRng))
    }
}

#[cfg(test)]
impl RelaySigner for TestSigner {
    fn public_key(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.0.verifying_key().as_bytes())
    }

    fn sign(&self, message: &[u8]) -> String {
        use ed25519_dalek::Signer;
        URL_SAFE_NO_PAD.encode(self.0.sign(message).to_bytes())
    }
}

fn verify_signature(public_key: &str, message: &[u8], signature: &str) -> bool {
    fn decode<const N: usize>(value: &str) -> Option<[u8; N]> {
        URL_SAFE_NO_PAD.decode(value).ok()?.try_into().ok()
    }

    let Some(key) = decode(public_key) else {
        return false;
    };
    let Some(signature) = decode(signature) else {
        return false;
    };
    VerifyingKey::from_bytes(&key)
        .and_then(|key| key.verify(message, &Signature::from_bytes(&signature)))
        .is_ok()
}

/// Relay frames carry one encrypted MEWS transport record. The current Noise
/// records are about 48 KiB at most; this leaves fixed headroom without letting
/// a relay buffer an application-sized message.
pub const MAX_CIPHERTEXT_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RelayPeerId(String);

impl RelayPeerId {
    pub fn new(value: impl Into<String>) -> Result<Self, RelayError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
            })
        {
            return Err(RelayError::InvalidPeerId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RelayPeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelayIdentity {
    pub installation_id: InstallationId,
    pub peer_id: RelayPeerId,
    /// Encoded public identity key. The relay does not interpret its format.
    pub public_key: String,
    /// Installation authority key scopes routing and validates admission.
    pub authority_public_key: String,
    pub admission: Option<RelayAdmission>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelayAdmission {
    pub installation_id: InstallationId,
    pub peer_id: RelayPeerId,
    pub peer_public_key: String,
    pub authority_public_key: String,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub signature: String,
}

impl RelayAdmission {
    pub fn create(
        authority: &impl RelaySigner,
        installation_id: InstallationId,
        peer_id: RelayPeerId,
        peer_public_key: String,
        expires_at: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        let mut admission = Self {
            installation_id,
            peer_id,
            peer_public_key,
            authority_public_key: authority.public_key(),
            expires_at,
            signature: String::new(),
        };
        admission.signature = authority.sign(&admission.signing_bytes());
        admission
    }

    pub fn verify(&self) -> bool {
        self.expires_at > chrono::Utc::now()
            && verify_signature(
                &self.authority_public_key,
                &self.signing_bytes(),
                &self.signature,
            )
    }

    fn signing_bytes(&self) -> Vec<u8> {
        format!(
            "mews-relay-admission-v1\0{}\0{}\0{}\0{}\0{}",
            self.installation_id,
            self.peer_id,
            self.peer_public_key,
            self.authority_public_key,
            self.expires_at.to_rfc3339()
        )
        .into_bytes()
    }
}

pub struct SignedAdmissionAuthenticator;

impl RelayAuthenticator for SignedAdmissionAuthenticator {
    fn is_enrolled(&self, identity: &RelayIdentity) -> bool {
        identity.admission.as_ref().is_some_and(|admission| {
            admission.verify()
                && admission.installation_id == identity.installation_id
                && admission.peer_id == identity.peer_id
                && admission.peer_public_key == identity.public_key
                && admission.authority_public_key == identity.authority_public_key
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayFrame {
    pub installation_id: InstallationId,
    pub source_id: RelayPeerId,
    pub destination_id: RelayPeerId,
    pub stream_id: String,
    pub sequence: u64,
    #[serde(with = "base64_bytes")]
    pub ciphertext: Vec<u8>,
}

mod base64_bytes {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        STANDARD.decode(encoded).map_err(serde::de::Error::custom)
    }
}

impl RelayFrame {
    pub fn new(
        installation_id: InstallationId,
        source_id: RelayPeerId,
        destination_id: RelayPeerId,
        stream_id: impl Into<String>,
        sequence: u64,
        ciphertext: Vec<u8>,
    ) -> Result<Self, RelayError> {
        let stream_id = stream_id.into();
        if stream_id.is_empty() || stream_id.len() > 128 {
            return Err(RelayError::InvalidStreamId);
        }
        if ciphertext.len() > MAX_CIPHERTEXT_BYTES {
            return Err(RelayError::FrameTooLarge {
                actual: ciphertext.len(),
                maximum: MAX_CIPHERTEXT_BYTES,
            });
        }
        Ok(Self {
            installation_id,
            source_id,
            destination_id,
            stream_id,
            sequence,
            ciphertext,
        })
    }

    pub(crate) fn validate(&self) -> Result<(), RelayError> {
        if self.stream_id.is_empty() || self.stream_id.len() > 128 {
            return Err(RelayError::InvalidStreamId);
        }
        if self.ciphertext.len() > MAX_CIPHERTEXT_BYTES {
            return Err(RelayError::FrameTooLarge {
                actual: self.ciphertext.len(),
                maximum: MAX_CIPHERTEXT_BYTES,
            });
        }
        Ok(())
    }
}

/// Supplies the relay with the already-enrolled public identities it may accept.
pub trait RelayAuthenticator: Send + Sync + 'static {
    fn is_enrolled(&self, identity: &RelayIdentity) -> bool;
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RelayError {
    #[error("peer identity is not enrolled")]
    Unauthorized,
    #[error("registration signature does not prove possession of the enrolled key")]
    InvalidSignature,
    #[error("invalid relay peer ID")]
    InvalidPeerId,
    #[error("invalid relay stream ID")]
    InvalidStreamId,
    #[error("relay frame is {actual} bytes; maximum is {maximum}")]
    FrameTooLarge { actual: usize, maximum: usize },
    #[error("frame source does not match the authenticated connection")]
    SourceMismatch,
    #[error("peer connection was replaced")]
    ConnectionReplaced,
    #[error("destination is not connected")]
    DestinationOffline,
    #[error("destination queue is full")]
    DestinationBusy,
}

#[derive(Clone)]
pub struct RelayRouter {
    inner: Arc<RouterInner>,
}

struct RouterInner {
    authenticator: Arc<dyn RelayAuthenticator>,
    queue_capacity: usize,
    state: Mutex<RouterState>,
}

#[derive(Default)]
struct RouterState {
    next_generation: u64,
    routes: HashMap<RouteKey, Route>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RouteKey {
    installation_id: InstallationId,
    authority_public_key: String,
    peer_id: RelayPeerId,
}

struct Route {
    generation: u64,
    sender: mpsc::Sender<RelayFrame>,
}

impl RelayRouter {
    pub fn new(
        authenticator: Arc<dyn RelayAuthenticator>,
        queue_capacity: usize,
    ) -> Result<Self, &'static str> {
        if queue_capacity == 0 {
            return Err("relay queue capacity must be greater than zero");
        }
        Ok(Self {
            inner: Arc::new(RouterInner {
                authenticator,
                queue_capacity,
                state: Mutex::new(RouterState::default()),
            }),
        })
    }

    /// Registers a route after checking the peer against the enrollment source.
    /// A new connection atomically replaces an older connection for the same peer.
    /// Issues a single-use challenge only for an enrolled public identity.
    pub fn challenge(&self, identity: RelayIdentity) -> Result<RelayRegistration, RelayError> {
        if !self.inner.authenticator.is_enrolled(&identity) {
            return Err(RelayError::Unauthorized);
        }
        let mut challenge = [0; 32];
        OsRng.fill_bytes(&mut challenge);
        Ok(RelayRegistration {
            router: self.clone(),
            identity,
            challenge,
        })
    }

    fn register(
        &self,
        identity: RelayIdentity,
        challenge: &[u8; 32],
        signature: &str,
    ) -> Result<RelayConnection, RelayError> {
        if !self.inner.authenticator.is_enrolled(&identity) {
            return Err(RelayError::Unauthorized);
        }
        let key = RouteKey {
            installation_id: identity.installation_id.clone(),
            authority_public_key: identity.authority_public_key.clone(),
            peer_id: identity.peer_id.clone(),
        };
        if !verify_signature(
            &identity.public_key,
            &registration_message(&identity, challenge),
            signature,
        ) {
            return Err(RelayError::InvalidSignature);
        }
        let (sender, receiver) = mpsc::channel(self.inner.queue_capacity);
        let mut state = self.inner.state.lock().expect("relay state poisoned");
        state.next_generation = state.next_generation.wrapping_add(1);
        let generation = state.next_generation;
        state
            .routes
            .insert(key.clone(), Route { generation, sender });
        Ok(RelayConnection {
            identity,
            key,
            generation,
            router: Arc::downgrade(&self.inner),
            receiver,
        })
    }
}

/// An attempt-scoped challenge. The router stores no pending challenge state,
/// so another caller cannot overwrite it or consume it.
pub struct RelayRegistration {
    router: RelayRouter,
    identity: RelayIdentity,
    challenge: [u8; 32],
}

impl RelayRegistration {
    pub fn challenge(&self) -> &[u8; 32] {
        &self.challenge
    }

    pub fn complete(self, signature: &str) -> Result<RelayConnection, RelayError> {
        self.router
            .register(self.identity, &self.challenge, signature)
    }
}

/// A live, authenticated peer route. Dropping it unregisters the route.
pub struct RelayConnection {
    identity: RelayIdentity,
    key: RouteKey,
    generation: u64,
    router: Weak<RouterInner>,
    receiver: mpsc::Receiver<RelayFrame>,
}

impl RelayConnection {
    pub fn identity(&self) -> &RelayIdentity {
        &self.identity
    }

    pub fn send(&self, frame: RelayFrame) -> Result<(), RelayError> {
        frame.validate()?;
        if frame.installation_id != self.identity.installation_id
            || frame.source_id != self.identity.peer_id
        {
            return Err(RelayError::SourceMismatch);
        }

        let router = self
            .router
            .upgrade()
            .ok_or(RelayError::DestinationOffline)?;
        let key = RouteKey {
            installation_id: frame.installation_id.clone(),
            authority_public_key: self.identity.authority_public_key.clone(),
            peer_id: frame.destination_id.clone(),
        };
        let state = router.state.lock().expect("relay state poisoned");
        if state
            .routes
            .get(&self.key)
            .is_none_or(|route| route.generation != self.generation)
        {
            return Err(RelayError::ConnectionReplaced);
        }
        let route = state
            .routes
            .get(&key)
            .ok_or(RelayError::DestinationOffline)?;
        route.sender.try_send(frame).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => RelayError::DestinationBusy,
            mpsc::error::TrySendError::Closed(_) => RelayError::DestinationOffline,
        })
    }

    pub async fn receive(&mut self) -> Option<RelayFrame> {
        let router = self.router.upgrade()?;
        let is_current = || {
            router
                .state
                .lock()
                .expect("relay state poisoned")
                .routes
                .get(&self.key)
                .is_some_and(|route| route.generation == self.generation)
        };
        if !is_current() {
            return None;
        }
        let frame = self.receiver.recv().await;
        is_current().then_some(frame).flatten()
    }
}

pub fn registration_message(identity: &RelayIdentity, challenge: &[u8; 32]) -> Vec<u8> {
    let mut message = b"mews-relay-registration-v1\0".to_vec();
    message.extend_from_slice(identity.installation_id.as_str().as_bytes());
    message.push(0);
    message.extend_from_slice(identity.peer_id.as_str().as_bytes());
    message.push(0);
    message.extend_from_slice(identity.authority_public_key.as_bytes());
    message.push(0);
    message.extend_from_slice(challenge);
    message
}

impl Drop for RelayConnection {
    fn drop(&mut self) {
        let Some(router) = self.router.upgrade() else {
            return;
        };
        let mut state = router.state.lock().expect("relay state poisoned");
        if state
            .routes
            .get(&self.key)
            .is_some_and(|route| route.generation == self.generation)
        {
            state.routes.remove(&self.key);
        }
    }
}

#[async_trait]
pub trait RelayLink: Send {
    async fn send_frame(&self, frame: RelayFrame) -> Result<(), RelayError>;
    async fn receive_frame(&mut self) -> Option<RelayFrame>;
}

#[async_trait]
impl RelayLink for RelayConnection {
    async fn send_frame(&self, frame: RelayFrame) -> Result<(), RelayError> {
        self.send(frame)
    }

    async fn receive_frame(&mut self) -> Option<RelayFrame> {
        self.receive().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

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

    fn identity(installation_id: &InstallationId, peer: &str) -> (RelayIdentity, TestSigner) {
        let key = TestSigner::new();
        let identity = RelayIdentity {
            installation_id: installation_id.clone(),
            peer_id: RelayPeerId::new(peer).unwrap(),
            public_key: key.public_key(),
            authority_public_key: "test-authority".into(),
            admission: None,
        };
        (identity, key)
    }

    fn connect(router: &RelayRouter, identity: RelayIdentity, key: &TestSigner) -> RelayConnection {
        let registration = router.challenge(identity.clone()).unwrap();
        let signature = key.sign(&registration_message(&identity, registration.challenge()));
        registration.complete(&signature).unwrap()
    }

    fn router(identities: &[RelayIdentity], capacity: usize) -> RelayRouter {
        let enrolled = identities
            .iter()
            .map(|identity| {
                (
                    identity.installation_id.clone(),
                    identity.peer_id.clone(),
                    identity.public_key.clone(),
                )
            })
            .collect();
        RelayRouter::new(Arc::new(Enrolled(enrolled)), capacity).unwrap()
    }

    #[tokio::test]
    async fn forwards_only_authenticated_non_spoofed_frames() {
        let installation = InstallationId::new();
        let (alice, alice_key) = identity(&installation, "host:alice");
        let (bob, bob_key) = identity(&installation, "host:bob");
        let router = router(&[alice.clone(), bob.clone()], 4);

        let alice_connection = connect(&router, alice.clone(), &alice_key);
        let mut bob_connection = connect(&router, bob.clone(), &bob_key);
        let frame = RelayFrame::new(
            installation.clone(),
            alice.peer_id.clone(),
            bob.peer_id.clone(),
            "stream-1",
            0,
            b"opaque".to_vec(),
        )
        .unwrap();
        alice_connection.send(frame.clone()).unwrap();
        assert_eq!(bob_connection.receive().await, Some(frame));

        let spoofed = RelayFrame::new(
            installation,
            bob.peer_id,
            alice.peer_id.clone(),
            "stream-1",
            1,
            vec![],
        )
        .unwrap();
        assert_eq!(
            alice_connection.send(spoofed),
            Err(RelayError::SourceMismatch)
        );

        let impostor = RelayIdentity {
            public_key: "wrong-key".into(),
            ..alice
        };
        assert!(matches!(
            router.challenge(impostor),
            Err(RelayError::Unauthorized)
        ));
    }

    #[tokio::test]
    async fn replaces_routes_and_bounds_each_destination_queue() {
        let installation = InstallationId::new();
        let (alice, alice_key) = identity(&installation, "host:alice");
        let (bob, bob_key) = identity(&installation, "host:bob");
        let router = router(&[alice.clone(), bob.clone()], 1);
        let alice_connection = connect(&router, alice.clone(), &alice_key);
        let mut old_bob = connect(&router, bob.clone(), &bob_key);

        let make_frame = |sequence| {
            RelayFrame::new(
                installation.clone(),
                alice.peer_id.clone(),
                bob.peer_id.clone(),
                "stream",
                sequence,
                vec![sequence as u8],
            )
            .unwrap()
        };
        alice_connection.send(make_frame(0)).unwrap();
        let mut new_bob = connect(&router, bob.clone(), &bob_key);
        assert!(old_bob.receive().await.is_none());
        alice_connection.send(make_frame(1)).unwrap();
        assert_eq!(
            alice_connection.send(make_frame(2)),
            Err(RelayError::DestinationBusy)
        );
        assert_eq!(new_bob.receive().await.unwrap().sequence, 1);
        let old_bob_frame = RelayFrame::new(
            installation.clone(),
            bob.peer_id.clone(),
            alice.peer_id.clone(),
            "stream",
            1,
            vec![],
        )
        .unwrap();
        assert_eq!(
            old_bob.send(old_bob_frame),
            Err(RelayError::ConnectionReplaced)
        );

        drop(new_bob);
        assert_eq!(
            alice_connection.send(make_frame(3)),
            Err(RelayError::DestinationOffline)
        );
    }

    #[test]
    fn rejects_oversized_frames() {
        let installation = InstallationId::new();
        let error = RelayFrame::new(
            installation,
            RelayPeerId::new("a").unwrap(),
            RelayPeerId::new("b").unwrap(),
            "stream",
            0,
            vec![0; MAX_CIPHERTEXT_BYTES + 1],
        )
        .unwrap_err();
        assert_eq!(
            error,
            RelayError::FrameTooLarge {
                actual: MAX_CIPHERTEXT_BYTES + 1,
                maximum: MAX_CIPHERTEXT_BYTES,
            }
        );
    }
}
