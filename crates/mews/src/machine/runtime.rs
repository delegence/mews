use std::{env, fs::OpenOptions, path::PathBuf, process::Stdio, time::Duration};

use anyhow::{Context, Result, bail};

pub struct RestartFailureContext {
    pub socket: PathBuf,
    pub log: PathBuf,
    pub last_error: String,
}

pub fn restart_failure_context(root: &std::path::Path, fallback: &str) -> RestartFailureContext {
    let socket = crate::server::socket_path(root);
    let log = crate::paths::log(root, "daemon.log");
    let last_error = std::fs::read_to_string(&log)
        .ok()
        .and_then(|contents| {
            contents
                .lines()
                .rev()
                .find(|line| !line.trim().is_empty())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| fallback.to_owned());
    RestartFailureContext {
        socket,
        log,
        last_error,
    }
}

pub async fn serve_machine(root: PathBuf, allow_host: bool) -> Result<()> {
    crate::paths::ensure_directories(&root)?;
    let relay = tokio::spawn(crate::relay_supervisor::supervise(root.clone()));
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(crate::paths::log(&root, "router.log"))?;
    let mut router = tokio::process::Command::new(env::current_exe()?)
        .arg("--root")
        .arg(&root)
        .arg("router")
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .spawn()
        .context("start mews-router")?;
    let client = mews_router::RouterClient::new(&root);
    for _ in 0..100 {
        if client.ready().await {
            let service = async {
                if root.join("mews.db").exists() {
                    crate::server::serve(root).await
                } else if allow_host {
                    crate::host::serve_joined_host(root).await
                } else {
                    bail!("this machine does not own Hub state")
                }
            };
            tokio::pin!(service);
            return tokio::select! {
                result = &mut service => { let _ = router.kill().await; relay.abort(); result }
                status = router.wait() => { relay.abort(); bail!("mews-router exited unexpectedly: {}", status?) }
            };
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let _ = router.kill().await;
    relay.abort();
    bail!("mews-router did not become ready")
}

pub async fn recover_hub(root: PathBuf) -> Result<()> {
    crate::host::activate_hub_transfer(&root)?;
    crate::server::serve(root).await
}

pub async fn serve_relay(listen: std::net::SocketAddr) -> Result<()> {
    mews_relay::serve(listen).await
}

pub async fn serve_router(root: PathBuf) -> Result<()> {
    mews_router::serve(root).await
}
