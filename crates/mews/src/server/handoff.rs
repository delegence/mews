use std::{fs, os::unix::fs::PermissionsExt, path::Path};

use anyhow::{Result, bail};

use crate::host::{ConnectedHost, HostControl};

use super::HubMoveRecovery;

pub(super) fn write_demoted_state(
    root: &Path,
    state: &crate::enrollment::join::JoinedHostState,
) -> Result<()> {
    let path = root.join("hub.json");
    fs::write(&path, serde_json::to_vec_pretty(state)?)?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    fs::OpenOptions::new().write(true).open(&path)?.sync_all()?;
    fs::File::open(root)?.sync_all()?;
    Ok(())
}

pub(super) fn write_move_phase(root: &Path, phase: &str) -> Result<()> {
    let temporary = root.join("hub-move.phase.tmp");
    fs::write(&temporary, phase)?;
    fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))?;
    fs::rename(temporary, root.join("hub-move.phase"))?;
    std::fs::File::open(root)?.sync_all()?;
    Ok(())
}

pub(super) fn write_move_recovery(root: &Path, snapshot: &crate::app::HubSnapshot) -> Result<()> {
    let path = root.join("hub-move-recovery.json");
    fs::write(
        &path,
        serde_json::to_vec(&HubMoveRecovery {
            target_host_id: snapshot.target_hub.clone(),
            move_nonce: snapshot.move_nonce.clone(),
        })?,
    )?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    fs::OpenOptions::new().write(true).open(&path)?.sync_all()?;
    fs::File::open(root)?.sync_all()?;
    Ok(())
}

pub(super) async fn transfer_hub_snapshot(
    host: &ConnectedHost,
    snapshot: &crate::app::HubSnapshot,
) -> Result<()> {
    use sha2::{Digest, Sha256};
    host.begin_hub_transfer(mews_protocol::HubTransferStart {
        move_nonce: snapshot.move_nonce.clone(),
        installation_id: snapshot.installation_id.clone(),
        generation: snapshot.generation,
        target_host_id: snapshot.target_hub.clone(),
        database_size: snapshot.database_size,
        database_sha256: snapshot.database_sha256.clone(),
        installation_key: snapshot.installation_key.clone(),
        hub_noise_key: snapshot.hub_noise_key.clone(),
        credentials: snapshot.credentials.clone(),
        credentials_sha256: format!("{:x}", Sha256::digest(&snapshot.credentials)),
    })
    .await?;
    let mut database = tokio::fs::File::open(&snapshot.database_path).await?;
    let mut chunk = vec![0_u8; 96 * 1024];
    let mut offset = 0_u64;
    loop {
        let read = tokio::io::AsyncReadExt::read(&mut database, &mut chunk).await?;
        if read == 0 {
            break;
        }
        offset = host
            .write_hub_transfer(offset, chunk[..read].to_vec())
            .await?;
    }
    if offset != snapshot.database_size {
        bail!("target Host acknowledged the wrong snapshot length");
    }
    host.commit_hub_transfer().await
}
