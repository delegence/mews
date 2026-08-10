use std::path::Path;

use anyhow::{Result, bail};
use mews_relay::{NetworkRelay, RelayAdmission, RelayIdentity, RelayPeerId};
use serde::{Deserialize, Serialize};

use crate::{
    Host,
    enrollment::{JoinOffer, JoinRequest},
    host::HostControl,
    identity::{HostIdentity, NoiseIdentity},
    service::Mews,
    transport::{PeerAuthentication, connect_initiator, connect_responder},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnrollmentAccepted {
    pub host: Host,
    pub relay_urls: Vec<String>,
    pub relay_admission: RelayAdmission,
    pub hub_relay_admission: RelayAdmission,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinedHostState {
    pub installation_id: crate::InstallationId,
    pub installation_public_key: String,
    pub hub_noise_public_key: String,
    pub accepted: EnrollmentAccepted,
    pub relay_urls: Vec<String>,
}

pub async fn accept_join(root: &Path, offer: JoinOffer) -> Result<()> {
    let mews = Mews::open_connection(root.to_path_buf())?;
    let local_host = std::sync::Arc::new(
        crate::host::ConnectedHost::in_process(
            mews.installation()?.hub_host_id,
            mews_host::ToolRegistry::with_host_extensions(root)?,
        )
        .await?,
    );
    accept_join_inner(
        root,
        offer,
        None,
        std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        crate::hub::HubControl {
            moving: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            handoff_gate: std::sync::Arc::new(tokio::sync::RwLock::new(())),
            session_locks: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            run_tasks: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            event_notify: std::sync::Arc::new(tokio::sync::Notify::new()),
        },
        local_host,
    )
    .await
}

pub(crate) async fn accept_join_ready(
    root: &Path,
    offer: JoinOffer,
    ready: tokio::sync::oneshot::Sender<Result<()>>,
    remote_hosts: crate::hub::RemoteHosts,
    control: crate::hub::HubControl,
    local_host: std::sync::Arc<crate::host::ConnectedHost>,
) -> Result<()> {
    accept_join_inner(root, offer, Some(ready), remote_hosts, control, local_host).await
}

async fn accept_join_inner(
    root: &Path,
    offer: JoinOffer,
    ready: Option<tokio::sync::oneshot::Sender<Result<()>>>,
    remote_hosts: crate::hub::RemoteHosts,
    control: crate::hub::HubControl,
    local_host: std::sync::Arc<crate::host::ConnectedHost>,
) -> Result<()> {
    let authority = HostIdentity::load(&root.join("secrets/installation.key"))?;
    let hub_noise = NoiseIdentity::load(&root.join("secrets/hub-noise.key"))?;
    let hub_identity = RelayIdentity {
        installation_id: offer.installation_id.clone(),
        peer_id: offer.hub_relay_admission.peer_id.clone(),
        public_key: authority.public_key(),
        authority_public_key: offer.installation_public_key.clone(),
        admission: Some(offer.hub_relay_admission.clone()),
    };
    let (joining_identity, _) = offer.relay_identity()?;
    let relay =
        match NetworkRelay::connect(&offer.relay_url, hub_identity.clone(), &authority).await {
            Ok(relay) => {
                if let Some(ready) = ready {
                    let _ = ready.send(Ok(()));
                }
                relay
            }
            Err(error) => {
                if let Some(ready) = ready {
                    let _ = ready.send(Err(anyhow::anyhow!("{error:#}")));
                }
                return Err(error);
            }
        };
    let stream = offer.invitation_id.to_string();
    let mut peer = connect_responder(
        relay,
        PeerAuthentication {
            installation_id: offer.installation_id.clone(),
            local_peer: hub_identity.peer_id,
            remote_peer: joining_identity.peer_id,
            local_signer: &authority,
            trusted_remote_signing_key: &joining_identity.public_key,
            local_noise: &hub_noise,
            expected_remote_noise_key: None,
            stream_id: &stream,
        },
    )
    .await?;
    let request: JoinRequest = peer.receive().await?;
    let _enrollment_guard = control.handoff_gate.read().await;
    if control.moving.load(std::sync::atomic::Ordering::Acquire) {
        bail!("Hub is moving; request a new invitation after handoff");
    }
    let mut mews = Mews::open_connection(root.to_path_buf())?;
    let host = mews.enroll_host(&offer, &request)?;
    let relay_admission = mews.relay_admission_for_host(&host)?;
    let hub_relay_admission = RelayAdmission::create(
        &authority,
        offer.installation_id.clone(),
        RelayPeerId::new(format!("hub-host:{}", host.id))?,
        authority.public_key(),
        chrono::Utc::now() + chrono::Duration::days(36_500),
    );
    let accepted = EnrollmentAccepted {
        host,
        relay_urls: mews.relay_candidates()?,
        relay_admission,
        hub_relay_admission,
    };
    let connected_hosts: Vec<_> = remote_hosts.lock().await.values().cloned().collect();
    for connected in connected_hosts {
        let _ = connected
            .update_relay_candidates(accepted.relay_urls.clone())
            .await;
    }
    peer.send(&accepted).await?;
    drop(peer);
    drop(_enrollment_guard);
    crate::host::serve_hub_host(
        root.to_path_buf(),
        accepted.relay_urls.clone(),
        accepted,
        remote_hosts,
        control,
        local_host,
    )
    .await
}

pub async fn join_host(
    offer: &JoinOffer,
    name: &str,
    host_identity: &HostIdentity,
    host_noise: &NoiseIdentity,
    relay_url: &str,
) -> Result<EnrollmentAccepted> {
    offer.verify()?;
    let (relay_identity, provisional_signer) = offer.relay_identity()?;
    let relay = NetworkRelay::connect(
        &offer.relay_url,
        relay_identity.clone(),
        &provisional_signer,
    )
    .await?;
    let stream = offer.invitation_id.to_string();
    let mut peer = connect_initiator(
        relay,
        PeerAuthentication {
            installation_id: offer.installation_id.clone(),
            local_peer: relay_identity.peer_id,
            remote_peer: offer.hub_relay_admission.peer_id.clone(),
            local_signer: &provisional_signer,
            trusted_remote_signing_key: &offer.installation_public_key,
            local_noise: host_noise,
            expected_remote_noise_key: Some(&offer.hub_noise_public_key),
            stream_id: &stream,
        },
    )
    .await?;
    peer.send(&JoinRequest::create(
        offer,
        name.to_owned(),
        host_identity,
        host_noise,
        relay_url.to_owned(),
    ))
    .await?;
    let accepted: EnrollmentAccepted = peer.receive().await?;
    let expected_host_peer = RelayPeerId::new(format!("host:{}", accepted.host.id))?;
    let expected_hub_peer = RelayPeerId::new(format!("hub-host:{}", accepted.host.id))?;
    if accepted.host.public_key != host_identity.public_key()
        || accepted.host.noise_public_key != host_noise.public_key()
        || !accepted.relay_admission.verify()
        || accepted.relay_admission.peer_public_key != host_identity.public_key()
        || accepted.relay_admission.installation_id != offer.installation_id
        || accepted.relay_admission.authority_public_key != offer.installation_public_key
        || accepted.relay_admission.peer_id != expected_host_peer
        || !accepted.hub_relay_admission.verify()
        || accepted.hub_relay_admission.authority_public_key != offer.installation_public_key
        || accepted.hub_relay_admission.installation_id != offer.installation_id
        || accepted.hub_relay_admission.peer_public_key != offer.installation_public_key
        || accepted.hub_relay_admission.peer_id != expected_hub_peer
    {
        bail!("Hub returned an invalid enrollment acceptance");
    }
    Ok(accepted)
}
