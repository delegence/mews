use std::{
    collections::BTreeMap, env, ffi::OsString, fmt, path::PathBuf, sync::Arc, time::Duration,
};

use anyhow::{Context, Result, bail};
use tokio::process::{Child, Command};

use crate::permissions::{AcpPermissionHandler, AllowPermissions};

/// Trusted, Host-owned ACP launch information. It is intentionally separate
/// from Agent configuration: Agents select a logical Harness, never an executable.
#[derive(Clone)]
pub struct AcpHarnessConfig {
    pub command: Vec<OsString>,
    pub environment: BTreeMap<OsString, OsString>,
    pub request_timeout: Duration,
    pub(crate) permission_handler: Arc<dyn AcpPermissionHandler>,
}

impl fmt::Debug for AcpHarnessConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AcpHarnessConfig")
            .field("command", &self.command)
            .field("environment", &self.environment)
            .field("request_timeout", &self.request_timeout)
            .field("permission_handler", &"<handler>")
            .finish()
    }
}

impl AcpHarnessConfig {
    pub fn new(command: impl IntoIterator<Item = impl Into<OsString>>) -> Result<Self> {
        let command = command.into_iter().map(Into::into).collect::<Vec<_>>();
        if command.is_empty() {
            bail!("ACP Harness command must not be empty");
        }
        Ok(Self {
            command,
            environment: BTreeMap::new(),
            request_timeout: Duration::from_secs(120),
            permission_handler: Arc::new(AllowPermissions),
        })
    }
}

pub(crate) struct AcpProcess {
    pub(crate) config: AcpHarnessConfig,
}

/// Keeps process-tree ownership even if the async Turn task itself is aborted.
pub(crate) struct ProcessTreeGuard {
    process_id: Option<u32>,
}

impl ProcessTreeGuard {
    pub(crate) fn new(child: &Child) -> Self {
        Self {
            process_id: child.id(),
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.process_id = None;
    }
}

impl Drop for ProcessTreeGuard {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(process_id) = self.process_id {
            // SAFETY: ACP children are spawned into a group whose id is their pid.
            unsafe {
                libc::kill(-(process_id as i32), libc::SIGKILL);
            }
        }
    }
}

impl AcpProcess {
    pub(crate) fn new(config: AcpHarnessConfig) -> Self {
        Self { config }
    }

    pub(crate) fn spawn(&self, cwd: &PathBuf) -> Result<Child> {
        let (program, arguments) = self
            .config
            .command
            .split_first()
            .expect("validated ACP Harness command");
        let mut command = Command::new(program);
        command
            .args(arguments)
            .current_dir(cwd)
            // ACP Harnesses never inherit a developer's personal profile.
            // Recipes selectively add their managed profile and credentials.
            .env_clear()
            .envs(&self.config.environment)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            // The Host daemon owns stderr, so adapter startup failures remain
            // diagnosable without contaminating the ACP stdout transport.
            .stderr(std::process::Stdio::inherit());
        command.kill_on_drop(true);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.as_std_mut().process_group(0);
        }
        if let Some(path) = env::var_os("PATH") {
            command.env("PATH", path);
        }
        command
            .spawn()
            .with_context(|| format!("start ACP Harness {:?}", program))
    }
}

pub(crate) async fn terminate_process_tree(child: &mut Child) {
    #[cfg(unix)]
    if let Some(process_id) = child.id() {
        // The ACP child starts a fresh process group, so one signal covers any
        // descendants it started for this Turn.
        unsafe {
            libc::kill(-(process_id as i32), libc::SIGKILL);
        }
    }
    let _ = child.start_kill();
    let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
}
