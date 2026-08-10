use std::{
    fs::OpenOptions,
    net::SocketAddr,
    path::Path,
    process::{Command, Stdio},
    time::Duration,
};

use crate::{
    app::Mews,
    enrollment::JoinOffer,
    enrollment::join::{JoinedHostState, join_host},
    identity::{HostIdentity, NoiseIdentity},
    relay_supervisor::{RelayConfig, RelayRole},
};
use anyhow::{Context, Result, bail};
use mews_client::MewsClient;

pub fn validate_invitation(encoded: &str) -> Result<()> {
    JoinOffer::decode(encoded).map(|_| ())
}

pub async fn create(
    root: &Path,
    name: &str,
    relay_url: String,
    listen: SocketAddr,
    no_daemon: bool,
) -> Result<()> {
    let config = RelayConfig {
        listen,
        url: relay_url,
        role: RelayRole::Active,
    };
    let mews = Mews::setup(root, name)?;
    mews.set_relay_url(&config.url)?;
    crate::relay_supervisor::write(root, &config)?;
    drop(mews);
    if no_daemon {
        spawn_and_wait(root, "hub.log", &["hub", "serve"], "Hub").await
    } else {
        super::daemon::install(root, &std::env::current_exe()?)?;
        wait_until_ready(root).await
    }
}

pub async fn join(
    root: &Path,
    name: &str,
    encoded_offer: &str,
    relay_url: String,
    listen: SocketAddr,
    no_daemon: bool,
) -> Result<mews_protocol::Host> {
    let offer = JoinOffer::decode(encoded_offer)?;
    ensure_available(root)?;
    std::fs::create_dir_all(root)?;
    secure(root, 0o700)?;
    crate::paths::ensure_directories(root)?;
    let identity = HostIdentity::load_or_create(&root.join("secrets/host.key"))?;
    let noise = NoiseIdentity::load_or_create(&root.join("secrets/host-noise.key"))?;
    crate::relay_supervisor::write(
        root,
        &RelayConfig {
            url: relay_url.clone(),
            listen,
            role: RelayRole::Disabled,
        },
    )?;
    let accepted = join_host(&offer, name, &identity, &noise, &relay_url).await?;
    let state_path = root.join("hub.json");
    std::fs::write(
        &state_path,
        serde_json::to_vec_pretty(&JoinedHostState {
            installation_id: offer.installation_id,
            installation_public_key: offer.installation_public_key,
            hub_noise_public_key: offer.hub_noise_public_key,
            relay_urls: accepted.relay_urls.clone(),
            accepted: accepted.clone(),
        })?,
    )?;
    secure(&state_path, 0o600)?;
    if no_daemon {
        spawn_and_wait(root, "host.log", &["daemon"], "Host").await?;
    } else {
        super::daemon::install(root, &std::env::current_exe()?)?;
        wait_until_ready(root).await?;
    }
    Ok(accepted.host)
}

pub fn ensure_available(root: &Path) -> Result<()> {
    if root.join("mews.db").exists() || root.join("hub.json").exists() {
        bail!("MEWS state already exists at {}", root.display());
    }
    Ok(())
}

#[cfg(unix)]
fn secure(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
    Ok(())
}

#[cfg(not(unix))]
fn secure(_: &Path, _: u32) -> Result<()> {
    Ok(())
}

async fn spawn_and_wait(root: &Path, log_name: &str, args: &[&str], name: &str) -> Result<()> {
    crate::paths::ensure_directories(root)?;
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(crate::paths::log(root, log_name))?;
    let mut child = Command::new(std::env::current_exe()?)
        .arg("--root")
        .arg(root)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .spawn()
        .with_context(|| format!("start {name}"))?;
    if let Err(error) = wait_until_ready(root).await {
        let _ = child.kill();
        let _ = child.wait();
        return Err(error.context(format!(
            "{name} did not become ready; inspect {}",
            crate::paths::log(root, log_name).display()
        )));
    }
    Ok(())
}

async fn wait_until_ready(root: &Path) -> Result<()> {
    for _ in 0..200 {
        if let Ok(mut client) = MewsClient::connect(root).await
            && client.status().await.is_ok()
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    bail!("MEWS daemon did not become ready")
}
