use std::{path::Path, process::Stdio, time::Duration};

use anyhow::{Context, Result, bail};
use serde_json::Value;
use tokio::{io::AsyncWriteExt, process::Command, time::timeout};

const MAX_OUTPUT: usize = 64 * 1024;

pub(super) async fn execute(command: &[String], cwd: &Path, input: Value) -> Result<Value> {
    let mut process = Command::new(&command[0]);
    process
        .args(&command[1..])
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = process.spawn().context("start extension tool")?;
    child
        .stdin
        .take()
        .context("open extension stdin")?
        .write_all(&serde_json::to_vec(&input)?)
        .await?;
    let output = timeout(Duration::from_secs(120), child.wait_with_output())
        .await
        .context("extension tool timed out")??;
    if output.stdout.len() > MAX_OUTPUT || output.stderr.len() > MAX_OUTPUT {
        bail!("extension tool output exceeds 64 KiB");
    }
    if !output.status.success() {
        bail!(
            "extension tool failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    serde_json::from_slice(&output.stdout).context("extension must print one JSON value")
}
