use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result, bail};

use super::{
    HarnessLaunch,
    catalog::{ensure_directory_is_not_a_symlink, restrict_directory_permissions},
};

mod claude;
mod codex;

pub(super) const NAMES: [&str; 2] = ["codex", "claude"];

pub(super) struct Recipe {
    pub(super) name: &'static str,
    pub(super) runtime: &'static str,
    pub(super) package: &'static str,
    pub(super) version: &'static str,
    pub(super) binary: &'static str,
    profile_variable: &'static str,
    auth_args: &'static [&'static str],
}

pub(super) fn recipe(name: &str) -> Option<&'static Recipe> {
    match name {
        "codex" => Some(&codex::RECIPE),
        "claude" => Some(&claude::RECIPE),
        _ => None,
    }
}

pub(super) fn apply_profile_environment(
    name: &str,
    profile: PathBuf,
    environment: &mut BTreeMap<std::ffi::OsString, std::ffi::OsString>,
) {
    if let Some(recipe) = recipe(name) {
        environment.insert(recipe.profile_variable.into(), profile.into_os_string());
    }
}

pub(super) fn recipe_runtime(root: &Path, recipe: &Recipe) -> PathBuf {
    root.join("harnesses")
        .join(recipe.name)
        .join("runtime")
        .join(recipe.version)
}

pub(super) fn recipe_binary(root: &Path, recipe: &Recipe) -> PathBuf {
    recipe_runtime(root, recipe)
        .join("node_modules")
        .join(".bin")
        .join(recipe.binary)
}

pub(super) fn recipe_node_path(root: &Path, recipe: &Recipe) -> PathBuf {
    recipe_runtime(root, recipe).join("node-path")
}

pub(super) fn recipe_command(root: &Path, recipe: &Recipe) -> Option<Vec<std::ffi::OsString>> {
    let binary = recipe_binary(root, recipe);
    let node = fs::read_to_string(recipe_node_path(root, recipe)).ok()?;
    let node = PathBuf::from(node.trim());
    (binary.is_file() && node.is_file())
        .then(|| vec![node.into_os_string(), binary.into_os_string()])
}

pub(super) fn install_recipe(root: &Path, recipe: &Recipe) -> Result<()> {
    let runtime = recipe_runtime(root, recipe);
    fs::create_dir_all(&runtime)
        .with_context(|| format!("create managed runtime {}", runtime.display()))?;
    ensure_directory_is_not_a_symlink(&runtime)?;
    restrict_directory_permissions(&runtime)?;

    if !recipe_binary(root, recipe).is_file() {
        let package = format!("{}@{}", recipe.package, recipe.version);
        let output = Command::new("npm")
            .args([
                "install",
                "--prefix",
                runtime
                    .to_str()
                    .context("managed Harness runtime path is not UTF-8")?,
                "--ignore-scripts",
                "--no-audit",
                "--no-fund",
                "--package-lock=false",
                "--no-save",
                &package,
            ])
            .output()
            .context("start npm for managed ACP adapter")?;
        if !output.status.success() || !recipe_binary(root, recipe).is_file() {
            let detail = String::from_utf8_lossy(&output.stderr);
            bail!(
                "managed {} ACP adapter installation failed: {}",
                recipe.name,
                detail.trim()
            );
        }
    }

    // Runs are started by a service manager whose PATH may not contain the
    // Node used during setup. Pin the interpreter alongside the managed
    // adapter instead of relying on its `#!/usr/bin/env node` shebang.
    let node = command_path("node").context("Node.js is required to run managed ACP adapters")?;
    let node = node
        .canonicalize()
        .with_context(|| format!("resolve Node.js runtime {}", node.display()))?;
    fs::write(
        recipe_node_path(root, recipe),
        node.to_str()
            .context("Node.js runtime path is not UTF-8")?
            .as_bytes(),
    )?;
    Ok(())
}

pub(super) fn inherited_launch_environment() -> BTreeMap<std::ffi::OsString, std::ffi::OsString> {
    // Deliberately exclude provider configuration variables such as CODEX_HOME,
    // CLAUDE_CONFIG_DIR and XDG_CONFIG_HOME. They would defeat profile isolation.
    const NAMES: &[&str] = &[
        "HOME",
        "USER",
        "LOGNAME",
        "SHELL",
        "PATH",
        "TMPDIR",
        "TMP",
        "TEMP",
        "LANG",
        "LANGUAGE",
        "TERM",
        "COLORTERM",
        "NO_COLOR",
        "SSH_AUTH_SOCK",
        "SSH_AGENT_PID",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "REQUESTS_CA_BUNDLE",
        "CURL_CA_BUNDLE",
        "NODE_EXTRA_CA_CERTS",
        "GIT_SSH_COMMAND",
        "GIT_SSH_VARIANT",
        "CARGO_HOME",
        "RUSTUP_HOME",
        "GOPATH",
        "GOMODCACHE",
        "JAVA_HOME",
        "ANDROID_HOME",
        "ANDROID_SDK_ROOT",
        "__CF_USER_TEXT_ENCODING",
    ];
    NAMES
        .iter()
        .filter_map(|name| env::var_os(name).map(|value| ((*name).into(), value)))
        .chain(env::vars_os().filter(|(name, _)| name.to_string_lossy().starts_with("LC_")))
        .collect()
}

pub(super) fn authenticate_profile(name: &str, launch: &HarnessLaunch) -> Result<()> {
    let (program, arguments) = launch
        .command
        .split_first()
        .context("managed ACP adapter command is empty")?;
    let mut command = Command::new(program);
    command.args(arguments).envs(&launch.environment);
    let recipe = recipe(name).with_context(|| {
        format!(
            "Harness {name:?} requires authentication, but its trusted definition has no managed login recipe"
        )
    })?;
    command.args(recipe.auth_args);
    let status = command
        .status()
        .with_context(|| format!("start managed {name} authentication"))?;
    if !status.success() {
        bail!("managed {name} authentication did not complete successfully");
    }
    Ok(())
}

pub(super) fn command_on_path(command: &str) -> bool {
    command_path(command).is_some()
}

pub(super) fn command_path(command: &str) -> Option<PathBuf> {
    let candidate = PathBuf::from(command);
    if candidate.components().count() > 1 {
        return candidate.is_file().then_some(candidate);
    }
    env::var_os("PATH").and_then(|paths| {
        env::split_paths(&paths)
            .map(|directory| directory.join(command))
            .find(|candidate| candidate.is_file())
    })
}
