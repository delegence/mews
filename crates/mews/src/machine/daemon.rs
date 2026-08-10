use std::{
    fs,
    path::Path,
    process::{Command, Stdio},
};

use anyhow::{Context, Result};

const LABEL: &str = "sh.mews.host";

#[cfg(target_os = "macos")]
pub fn restart() -> Result<()> {
    let domain = format!("gui/{}/{}", unsafe { libc::getuid() }, LABEL);
    let status = Command::new("launchctl")
        .args(["kickstart", "-k", &domain])
        .status()
        .context("restart MEWS daemon with launchctl")?;
    if !status.success() {
        anyhow::bail!("launchctl could not restart the MEWS daemon; run `mews setup` first");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub fn restart() -> Result<()> {
    let status = Command::new("systemctl")
        .args(["--user", "restart", "mews.service"])
        .status()
        .context("restart MEWS daemon with systemd")?;
    if !status.success() {
        anyhow::bail!("systemd could not restart the MEWS daemon; run `mews setup` first");
    }
    Ok(())
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn restart() -> Result<()> {
    anyhow::bail!("automatic daemon restart supports macOS and Linux")
}

#[cfg(target_os = "macos")]
pub fn install(root: &Path, executable: &Path) -> Result<()> {
    install_launchd(root, executable)
}

#[cfg(target_os = "linux")]
pub fn install(root: &Path, executable: &Path) -> Result<()> {
    install_systemd(root, executable)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn install(_root: &Path, _executable: &Path) -> Result<()> {
    anyhow::bail!("automatic daemon installation supports macOS and Linux")
}

#[cfg(target_os = "macos")]
fn install_launchd(root: &Path, executable: &Path) -> Result<()> {
    crate::paths::ensure_directories(root)?;
    let home = std::env::var_os("HOME").context("HOME is unavailable")?;
    let directory = Path::new(&home).join("Library/LaunchAgents");
    fs::create_dir_all(&directory)?;
    let domain = format!("gui/{}", unsafe { libc::getuid() });

    let plist = directory.join(format!("{LABEL}.plist"));
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>\n<key>Label</key><string>{LABEL}</string>\n<key>ProgramArguments</key><array><string>{}</string><string>--root</string><string>{}</string><string>daemon</string></array>\n<key>RunAtLoad</key><true/><key>KeepAlive</key><true/>\n<key>StandardOutPath</key><string>{}</string><key>StandardErrorPath</key><string>{}</string>\n</dict></plist>\n",
        xml(executable),
        xml(root),
        xml(&crate::paths::log(root, "daemon.log")),
        xml(&crate::paths::log(root, "daemon.log")),
    );
    fs::write(&plist, xml)?;
    bootout(&domain, &plist);
    let status = Command::new("launchctl")
        .args(["bootstrap", &domain])
        .arg(&plist)
        .status()
        .context("run launchctl")?;
    if !status.success() {
        anyhow::bail!("launchctl could not start the MEWS daemon");
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn bootout(domain: &str, plist: &Path) {
    // A missing service is expected on first setup, so keep launchctl's error quiet.
    let _ = Command::new("launchctl")
        .args(["bootout", domain])
        .arg(plist)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

#[cfg(target_os = "macos")]
fn xml(path: &Path) -> String {
    path.to_string_lossy()
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(target_os = "linux")]
fn install_systemd(root: &Path, executable: &Path) -> Result<()> {
    crate::paths::ensure_directories(root)?;
    let config = std::env::var_os("XDG_CONFIG_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| Path::new(&home).join(".config")))
        .context("HOME and XDG_CONFIG_HOME are unavailable")?;
    let directory = config.join("systemd/user");
    fs::create_dir_all(&directory)?;
    fs::write(
        directory.join("mews.service"),
        format!(
            "[Unit]\nDescription=MEWS durable agent daemon\nAfter=network-online.target\n\n[Service]\nExecStart={} --root {} daemon\nRestart=always\nRestartSec=2\n\n[Install]\nWantedBy=default.target\n",
            systemd_arg(executable),
            systemd_arg(root),
        ),
    )?;
    let status = Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status()
        .context("run systemctl --user")?;
    if !status.success() {
        anyhow::bail!("systemctl --user daemon-reload failed");
    }
    let status = Command::new("systemctl")
        .args(["--user", "enable", "--now", "mews.service"])
        .status()?;
    if !status.success() {
        anyhow::bail!("systemctl could not enable the MEWS daemon");
    }
    let status = Command::new("loginctl")
        .arg("enable-linger")
        .status()
        .context("run loginctl enable-linger")?;
    if !status.success() {
        anyhow::bail!(
            "MEWS is running, but loginctl could not enable boot-before-login; enable lingering for this user"
        );
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn systemd_arg(path: &Path) -> String {
    format!(
        "\"{}\"",
        path.to_string_lossy()
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
    )
}
