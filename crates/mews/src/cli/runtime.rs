use std::{env, fs::OpenOptions, path::PathBuf, process::Stdio, time::Duration};

use anyhow::{Context, Result, bail};

pub async fn serve_machine(root: PathBuf, allow_host: bool) -> Result<()> {
    mews::paths::ensure_directories(&root)?;
    let relay = tokio::spawn(mews::relay_supervisor::supervise(root.clone()));
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(mews::paths::log(&root, "router.log"))?;
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
                    mews::hub::serve(root).await
                } else if allow_host {
                    mews::host::serve_joined_host(root).await
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
