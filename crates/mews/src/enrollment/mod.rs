pub mod join;

use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use mews_relay::{RelayAdmission, RelayIdentity, RelayPeerId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    HostId, InstallationId, InvitationId,
    identity::{HostIdentity, NoiseIdentity},
};

pub const OFFER_VERSION: u32 = 1;

pub struct JoinOfferConnection {
    pub relay_url: String,
    pub hub_noise_public_key: String,
    pub secret: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinOffer {
    pub version: u32,
    pub installation_id: InstallationId,
    pub invitation_id: InvitationId,
    pub hub_host_id: HostId,
    pub installation_public_key: String,
    pub relay_url: String,
    pub secret: String,
    pub expires_at: DateTime<Utc>,
    pub signature: String,
    pub relay_admission: RelayAdmission,
    pub hub_relay_admission: RelayAdmission,
    pub hub_noise_public_key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinRequest {
    pub installation_id: InstallationId,
    pub invitation_id: InvitationId,
    pub host_name: String,
    pub host_public_key: String,
    pub host_noise_public_key: String,
    pub relay_url: String,
    pub signature: String,
}

impl JoinRequest {
    pub fn create(
        offer: &JoinOffer,
        host_name: String,
        host_identity: &HostIdentity,
        noise_identity: &NoiseIdentity,
        relay_url: String,
    ) -> Self {
        let mut request = Self {
            installation_id: offer.installation_id.clone(),
            invitation_id: offer.invitation_id.clone(),
            host_name,
            host_public_key: host_identity.public_key(),
            host_noise_public_key: noise_identity.public_key(),
            relay_url,
            signature: String::new(),
        };
        request.signature = host_identity.sign(&request.signing_bytes());
        request
    }

    pub fn verify(&self, offer: &JoinOffer) -> Result<()> {
        if self.installation_id != offer.installation_id
            || self.invitation_id != offer.invitation_id
        {
            bail!("join request does not match the invitation");
        }
        NoiseIdentity::validate_public_key(&self.host_noise_public_key)?;
        validate_relay_url(&self.relay_url)?;
        HostIdentity::verify(
            &self.host_public_key,
            &self.signing_bytes(),
            &self.signature,
        )
    }

    fn signing_bytes(&self) -> Vec<u8> {
        format!(
            "mews-join-request-v2\0{}\0{}\0{}\0{}\0{}\0{}",
            self.installation_id,
            self.invitation_id,
            self.host_name,
            self.host_public_key,
            self.host_noise_public_key,
            self.relay_url
        )
        .into_bytes()
    }
}

#[derive(Serialize)]
struct UnsignedOffer<'a> {
    version: u32,
    installation_id: &'a InstallationId,
    invitation_id: &'a InvitationId,
    hub_host_id: &'a HostId,
    installation_public_key: &'a str,
    relay_url: &'a str,
    secret: &'a str,
    expires_at: DateTime<Utc>,
    relay_admission: &'a RelayAdmission,
    hub_relay_admission: &'a RelayAdmission,
    hub_noise_public_key: &'a str,
}

impl JoinOffer {
    pub fn create(
        installation_id: InstallationId,
        invitation_id: InvitationId,
        hub_host_id: HostId,
        hub_identity: &HostIdentity,
        connection: JoinOfferConnection,
    ) -> Result<Self> {
        let JoinOfferConnection {
            relay_url,
            hub_noise_public_key,
            secret,
            expires_at,
        } = connection;
        validate_relay_url(&relay_url)?;
        let provisional = provisional_signer(&secret);
        let peer_id = RelayPeerId::new(format!("invite:{invitation_id}"))?;
        let relay_admission = RelayAdmission::create(
            hub_identity,
            installation_id.clone(),
            peer_id,
            provisional.public_key(),
            expires_at,
        );
        NoiseIdentity::validate_public_key(&hub_noise_public_key)?;
        let hub_relay_admission = RelayAdmission::create(
            hub_identity,
            installation_id.clone(),
            RelayPeerId::new(format!("hub-invite:{invitation_id}"))?,
            hub_identity.public_key(),
            expires_at,
        );
        let mut offer = Self {
            version: OFFER_VERSION,
            installation_id,
            invitation_id,
            hub_host_id,
            installation_public_key: hub_identity.public_key(),
            relay_url,
            secret,
            expires_at,
            signature: String::new(),
            relay_admission,
            hub_relay_admission,
            hub_noise_public_key,
        };
        offer.signature = hub_identity.sign(&offer.signing_bytes()?);
        Ok(offer)
    }

    pub fn encode(&self) -> Result<String> {
        Ok(URL_SAFE_NO_PAD.encode(serde_json::to_vec(self)?))
    }

    pub fn decode(encoded: &str) -> Result<Self> {
        let bytes = URL_SAFE_NO_PAD
            .decode(encoded)
            .context("invalid invitation encoding")?;
        if bytes.len() > 16 * 1024 {
            bail!("invitation is too large");
        }
        let offer: Self = serde_json::from_slice(&bytes).context("invalid invitation")?;
        offer.verify()?;
        Ok(offer)
    }

    pub fn verify(&self) -> Result<()> {
        if self.version != OFFER_VERSION {
            bail!("unsupported invitation version {}", self.version);
        }
        validate_relay_url(&self.relay_url)?;
        if self.expires_at <= Utc::now() {
            bail!("invitation has expired");
        }
        HostIdentity::verify(
            &self.installation_public_key,
            &self.signing_bytes()?,
            &self.signature,
        )?;
        let signer = provisional_signer(&self.secret);
        if !self.relay_admission.verify()
            || self.relay_admission.installation_id != self.installation_id
            || self.relay_admission.authority_public_key != self.installation_public_key
            || self.relay_admission.peer_public_key != signer.public_key()
        {
            bail!("invitation relay admission is invalid");
        }
        if !self.hub_relay_admission.verify()
            || self.hub_relay_admission.installation_id != self.installation_id
            || self.hub_relay_admission.authority_public_key != self.installation_public_key
            || self.hub_relay_admission.peer_public_key != self.installation_public_key
        {
            bail!("invitation Hub relay admission is invalid");
        }
        NoiseIdentity::validate_public_key(&self.hub_noise_public_key)?;
        Ok(())
    }

    pub fn relay_identity(&self) -> Result<(RelayIdentity, HostIdentity)> {
        self.verify()?;
        let signer = provisional_signer(&self.secret);
        Ok((
            RelayIdentity {
                installation_id: self.installation_id.clone(),
                peer_id: self.relay_admission.peer_id.clone(),
                public_key: signer.public_key(),
                authority_public_key: self.installation_public_key.clone(),
                admission: Some(self.relay_admission.clone()),
            },
            signer,
        ))
    }

    fn signing_bytes(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&UnsignedOffer {
            version: self.version,
            installation_id: &self.installation_id,
            invitation_id: &self.invitation_id,
            hub_host_id: &self.hub_host_id,
            installation_public_key: &self.installation_public_key,
            relay_url: &self.relay_url,
            secret: &self.secret,
            expires_at: self.expires_at,
            relay_admission: &self.relay_admission,
            hub_relay_admission: &self.hub_relay_admission,
            hub_noise_public_key: &self.hub_noise_public_key,
        })?)
    }
}

fn provisional_signer(secret: &str) -> HostIdentity {
    let mut hash = Sha256::new();
    hash.update(b"mews-invitation-relay-v1\0");
    hash.update(secret.as_bytes());
    HostIdentity::from_seed(hash.finalize().into())
}

fn validate_relay_url(url: &str) -> Result<()> {
    if url.len() > 2048 {
        bail!("relay URL is too long");
    }
    let parsed = reqwest::Url::parse(url).context("invalid relay URL")?;
    if !matches!(parsed.scheme(), "ws" | "wss") || parsed.host_str().is_none() {
        bail!("relay URL must use ws:// or wss:// and include a host");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_offer_round_trips_and_rejects_tampering() {
        let root = tempfile::tempdir().unwrap();
        let identity = HostIdentity::load_or_create(&root.path().join("secrets/host.key")).unwrap();
        let hub_noise = NoiseIdentity::load_or_create(&root.path().join("hub.noise")).unwrap();
        let offer = JoinOffer::create(
            InstallationId::new(),
            InvitationId::new(),
            HostId::new(),
            &identity,
            JoinOfferConnection {
                relay_url: "ws://127.0.0.1:9000".into(),
                hub_noise_public_key: hub_noise.public_key(),
                secret: "secret".into(),
                expires_at: Utc::now() + chrono::Duration::minutes(15),
            },
        )
        .unwrap();
        let mut decoded = JoinOffer::decode(&offer.encode().unwrap()).unwrap();
        decoded.relay_url = "wss://evil.example".into();
        assert!(decoded.verify().is_err());

        let host = HostIdentity::load_or_create(&root.path().join("joining.key")).unwrap();
        let noise = NoiseIdentity::load_or_create(&root.path().join("joining.noise")).unwrap();
        let mut request = JoinRequest::create(
            &offer,
            "mini-pc".into(),
            &host,
            &noise,
            "ws://mini-pc.local:8787".into(),
        );
        request.verify(&offer).unwrap();
        request.host_name = "attacker".into();
        assert!(request.verify(&offer).is_err());
    }
}
