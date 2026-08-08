//! Noise transport plus explicit MEWS identity binding.
//!
//! A Noise handshake is not considered authenticated until the remote
//! `IdentityBinding` has been verified against an independently trusted
//! installation authority or enrolled Host signing key.

use anyhow::{Result, bail};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use crate::{
    InstallationId,
    identity::{HostIdentity, NoiseIdentity},
};

const PATTERN: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2s";
pub const MAX_ENCRYPTED_RECORD_PLAINTEXT: usize = 48 * 1024;

pub(crate) struct NoiseHandshake(snow::HandshakeState);

impl NoiseHandshake {
    pub(crate) fn initiator(identity: &NoiseIdentity, prologue: &[u8]) -> Result<Self> {
        Ok(Self(
            builder()?
                .prologue(prologue)?
                .local_private_key(identity.private_key())?
                .build_initiator()?,
        ))
    }

    pub(crate) fn responder(identity: &NoiseIdentity, prologue: &[u8]) -> Result<Self> {
        Ok(Self(
            builder()?
                .prologue(prologue)?
                .local_private_key(identity.private_key())?
                .build_responder()?,
        ))
    }

    pub(crate) fn write(&mut self, payload: &[u8]) -> Result<Vec<u8>> {
        let mut output = vec![0; 65_535];
        let size = self.0.write_message(payload, &mut output)?;
        output.truncate(size);
        Ok(output)
    }

    pub(crate) fn read(&mut self, message: &[u8]) -> Result<Vec<u8>> {
        if message.len() > 65_535 {
            bail!("Noise handshake frame is too large");
        }
        let mut output = vec![0; 65_535];
        let size = self.0.read_message(message, &mut output)?;
        output.truncate(size);
        Ok(output)
    }

    pub(crate) fn handshake_hash(&self) -> &[u8] {
        self.0.get_handshake_hash()
    }

    pub(crate) fn remote_static(&self) -> Result<&[u8]> {
        self.0
            .get_remote_static()
            .ok_or_else(|| anyhow::anyhow!("remote Noise static key is not available"))
    }

    pub(crate) fn into_transport(self) -> Result<NoiseTransport> {
        if !self.0.is_handshake_finished() {
            bail!("Noise handshake is incomplete");
        }
        Ok(NoiseTransport(self.0.into_transport_mode()?))
    }
}

pub(crate) struct NoiseTransport(snow::TransportState);

impl NoiseTransport {
    pub(crate) fn encrypt(&mut self, plaintext: &[u8]) -> Result<Vec<u8>> {
        if plaintext.len() > MAX_ENCRYPTED_RECORD_PLAINTEXT {
            bail!("encrypted record is too large");
        }
        let mut output = vec![0; plaintext.len() + 16];
        let size = self.0.write_message(plaintext, &mut output)?;
        output.truncate(size);
        Ok(output)
    }

    pub(crate) fn decrypt(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        if ciphertext.len() < 16 || ciphertext.len() > MAX_ENCRYPTED_RECORD_PLAINTEXT + 16 {
            bail!("encrypted record has invalid size");
        }
        let mut output = vec![0; ciphertext.len()];
        let size = self.0.read_message(ciphertext, &mut output)?;
        output.truncate(size);
        Ok(output)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityBinding {
    pub installation_id: InstallationId,
    pub peer_id: String,
    pub sender_noise_key: String,
    pub receiver_noise_key: String,
    pub handshake_hash: String,
    pub signature: String,
}

impl IdentityBinding {
    pub fn create(
        signer: &HostIdentity,
        installation_id: InstallationId,
        peer_id: String,
        sender_noise_key: &[u8],
        receiver_noise_key: &[u8],
        handshake_hash: &[u8],
    ) -> Self {
        let mut binding = Self {
            installation_id,
            peer_id,
            sender_noise_key: URL_SAFE_NO_PAD.encode(sender_noise_key),
            receiver_noise_key: URL_SAFE_NO_PAD.encode(receiver_noise_key),
            handshake_hash: URL_SAFE_NO_PAD.encode(handshake_hash),
            signature: String::new(),
        };
        binding.signature = signer.sign(&binding.signing_bytes());
        binding
    }

    pub fn verify(
        &self,
        trusted_signing_key: &str,
        installation_id: &InstallationId,
        expected_peer_id: &str,
        expected_sender_noise: &[u8],
        expected_receiver_noise: &[u8],
        expected_handshake_hash: &[u8],
    ) -> Result<()> {
        if &self.installation_id != installation_id
            || self.peer_id != expected_peer_id
            || self.sender_noise_key != URL_SAFE_NO_PAD.encode(expected_sender_noise)
            || self.receiver_noise_key != URL_SAFE_NO_PAD.encode(expected_receiver_noise)
            || self.handshake_hash != URL_SAFE_NO_PAD.encode(expected_handshake_hash)
        {
            bail!("identity binding does not match the encrypted channel");
        }
        HostIdentity::verify(trusted_signing_key, &self.signing_bytes(), &self.signature)
    }

    fn signing_bytes(&self) -> Vec<u8> {
        format!(
            "mews-noise-binding-v1\0{}\0{}\0{}\0{}\0{}",
            self.installation_id,
            self.peer_id,
            self.sender_noise_key,
            self.receiver_noise_key,
            self.handshake_hash
        )
        .into_bytes()
    }
}

fn builder() -> Result<snow::Builder<'static>> {
    Ok(snow::Builder::new(PATTERN.parse()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noise_channel_is_bound_to_trusted_mews_identities() {
        let root = tempfile::tempdir().unwrap();
        let hub_signing =
            HostIdentity::load_or_create(&root.path().join("secrets/installation.key")).unwrap();
        let host_signing =
            HostIdentity::load_or_create(&root.path().join("secrets/host.key")).unwrap();
        let hub_noise = NoiseIdentity::load_or_create(&root.path().join("hub.noise")).unwrap();
        let host_noise = NoiseIdentity::load_or_create(&root.path().join("host.noise")).unwrap();
        let mut host = NoiseHandshake::initiator(&host_noise, b"test-prologue").unwrap();
        let mut hub = NoiseHandshake::responder(&hub_noise, b"test-prologue").unwrap();
        let one = host.write(b"").unwrap();
        hub.read(&one).unwrap();
        let two = hub.write(b"").unwrap();
        host.read(&two).unwrap();
        let three = host.write(b"").unwrap();
        hub.read(&three).unwrap();
        let installation = InstallationId::new();
        let hub_binding = IdentityBinding::create(
            &hub_signing,
            installation.clone(),
            "hub".into(),
            hub_noise.public_bytes(),
            host_noise.public_bytes(),
            host.handshake_hash(),
        );
        hub_binding
            .verify(
                &hub_signing.public_key(),
                &installation,
                "hub",
                host.remote_static().unwrap(),
                hub.remote_static().unwrap(),
                host.handshake_hash(),
            )
            .unwrap();
        let host_binding = IdentityBinding::create(
            &host_signing,
            installation.clone(),
            "host:test".into(),
            host_noise.public_bytes(),
            hub_noise.public_bytes(),
            hub.handshake_hash(),
        );
        host_binding
            .verify(
                &host_signing.public_key(),
                &installation,
                "host:test",
                hub.remote_static().unwrap(),
                host.remote_static().unwrap(),
                hub.handshake_hash(),
            )
            .unwrap();
        let mut host = host.into_transport().unwrap();
        let mut hub = hub.into_transport().unwrap();
        let ciphertext = host.encrypt(b"private rpc").unwrap();
        assert_eq!(hub.decrypt(&ciphertext).unwrap(), b"private rpc");
        assert!(hub.decrypt(&ciphertext).is_err());
    }
}
