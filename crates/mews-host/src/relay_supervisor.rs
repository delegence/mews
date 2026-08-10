use std::{net::SocketAddr, path::Path};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelayConfig {
    pub url: String,
    pub listen: SocketAddr,
    pub role: RelayRole,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RelayRole {
    Disabled,
    Active,
    Retiring { stop_at: DateTime<Utc> },
}

impl RelayConfig {
    pub fn should_serve(&self) -> bool {
        match self.role {
            RelayRole::Disabled => false,
            RelayRole::Active => true,
            RelayRole::Retiring { stop_at } => stop_at > Utc::now(),
        }
    }
}

pub fn path(root: &Path) -> std::path::PathBuf {
    root.join("relay.json")
}

pub fn read(root: &Path) -> Result<RelayConfig> {
    serde_json::from_slice(&std::fs::read(path(root))?).context("read relay configuration")
}

pub fn write(root: &Path, config: &RelayConfig) -> Result<()> {
    let path = path(root);
    std::fs::write(&path, serde_json::to_vec_pretty(config)?)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

pub async fn supervise(root: std::path::PathBuf) {
    loop {
        let Ok(config) = read(&root) else {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            continue;
        };
        if !config.should_serve() {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            continue;
        }
        let serving = mews_relay::serve(config.listen);
        tokio::pin!(serving);
        loop {
            tokio::select! {
                result = &mut serving => {
                    if let Err(error) = result {
                        eprintln!("Relay stopped: {error:#}");
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    break;
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                    let Ok(next) = read(&root) else { break; };
                    if !next.should_serve() || next.listen != config.listen {
                        break;
                    }
                }
            }
        }
    }
}
