use std::{path::Path, process::Stdio, time::Duration};

use anyhow::{Context, Result, bail};
use mews_agent::CancellationToken;
use serde_json::Value;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{Child, Command},
    time::timeout,
};

pub(super) const MAX_OUTPUT: usize = 64 * 1024;

pub(super) struct CapturedOutput {
    pub status: std::process::ExitStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Clone, Copy)]
pub(super) enum OutputLimit {
    Truncate,
    Reject,
}

/// A process group is the unit of ownership: dropping an in-flight tool must
/// also stop descendants started by its shell or extension process.
struct ChildGuard(Child);

impl ChildGuard {
    fn kill_group(&mut self) {
        #[cfg(unix)]
        if let Some(process_id) = self.0.id() {
            // SAFETY: `process_id` came from the child spawned in its own group.
            unsafe {
                libc::kill(-(process_id as i32), libc::SIGKILL);
            }
        }
        let _ = self.0.start_kill();
    }

    async fn terminate_and_reap(&mut self) {
        self.kill_group();
        let _ = timeout(Duration::from_secs(5), self.0.wait()).await;
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.kill_group();
    }
}

pub(super) fn isolate_process_group(process: &mut Command) {
    process.kill_on_drop(true);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        process.as_std_mut().process_group(0);
    }
}

async fn read_output(mut reader: impl AsyncRead + Unpin, policy: OutputLimit) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(8 * 1024);
    let mut chunk = [0; 8 * 1024];
    loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            return Ok(output);
        }
        let remaining = MAX_OUTPUT.saturating_sub(output.len());
        output.extend_from_slice(&chunk[..read.min(remaining)]);
        if matches!(policy, OutputLimit::Reject) && read > remaining {
            bail!("extension tool output exceeds 64 KiB");
        }
    }
}

pub(super) async fn capture(
    mut process: Command,
    input: Option<Vec<u8>>,
    duration: Duration,
    cancellation: &CancellationToken,
    policy: OutputLimit,
) -> Result<CapturedOutput> {
    isolate_process_group(&mut process);
    let mut child = ChildGuard(process.spawn().context("start tool process")?);
    let stdout = child.0.stdout.take().context("open tool stdout")?;
    let stderr = child.0.stderr.take().context("open tool stderr")?;
    let mut stdin = child.0.stdin.take();

    let operation = async {
        let write_input = async move {
            if let (Some(mut stdin), Some(input)) = (stdin.take(), input) {
                stdin.write_all(&input).await?;
            }
            Result::<()>::Ok(())
        };
        let wait = async { Ok::<_, anyhow::Error>(child.0.wait().await?) };
        let (status, stdout, stderr, ()) = tokio::try_join!(
            wait,
            read_output(stdout, policy),
            read_output(stderr, policy),
            write_input,
        )?;
        Ok::<_, anyhow::Error>(CapturedOutput {
            status,
            stdout,
            stderr,
        })
    };

    let result = tokio::select! {
        _ = cancellation.cancelled() => Err(anyhow::anyhow!("agent Turn cancelled")),
        result = timeout(duration, operation) => match result {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!("tool process timed out")),
        },
    };
    if result.is_err() {
        child.terminate_and_reap().await;
    }
    result
}

pub(super) async fn execute(
    command: &[String],
    cwd: &Path,
    input: Value,
    cancellation: &CancellationToken,
) -> Result<Value> {
    let mut process = Command::new(&command[0]);
    process
        .args(&command[1..])
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let output = capture(
        process,
        Some(serde_json::to_vec(&input)?),
        Duration::from_secs(120),
        cancellation,
        OutputLimit::Reject,
    )
    .await?;
    if !output.status.success() {
        bail!(
            "extension tool failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    serde_json::from_slice(&output.stdout).context("extension must print one JSON value")
}
