use std::{
    collections::BTreeMap,
    env,
    ffi::OsStr,
    fs,
    io::IsTerminal,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use mews_protocol::{
    HarnessAvailability, HarnessDescriptor, HarnessModelCapability, HarnessProtocol,
    HarnessReadiness,
};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{
    cache::{
        ProbeCache, cache_path, load_cached_descriptor, mark_profile_authenticated,
        persist_cached_descriptor, profile_is_authenticated, remove_profile_auth_marker,
    },
    recipes::{
        NAMES, apply_profile_environment, authenticate_profile, command_on_path,
        inherited_launch_environment, install_recipe, recipe, recipe_command,
    },
};

#[derive(Clone, Debug, Default)]
pub struct HarnessCatalog {
    descriptors: Vec<HarnessDescriptor>,
    commands: BTreeMap<String, Vec<std::ffi::OsString>>,
}

/// The result of the Host-local portion of Harness setup.
///
/// External authentication and ACP capability probing deliberately remain
/// pending: creating an isolated profile must not claim either has happened.
#[derive(Clone, Debug)]
pub struct HarnessSetup {
    pub descriptor: HarnessDescriptor,
    pub managed_profile: Option<PathBuf>,
    pub profile_created: bool,
}

/// Trusted launch information retained on one Host. It is intentionally
/// separate from the portable Agent and wire-visible descriptor.
#[derive(Clone, Debug)]
pub struct HarnessLaunch {
    pub command: Vec<std::ffi::OsString>,
    pub environment: BTreeMap<std::ffi::OsString, std::ffi::OsString>,
}

impl HarnessCatalog {
    pub fn discover(root: Option<&Path>) -> Result<Self> {
        let mut definitions = root.map(load_definitions).transpose()?.unwrap_or_default();
        let mut descriptors = vec![native_mews()];
        let mut commands = BTreeMap::new();

        for recipe in NAMES {
            if let Some(definition) = definitions.remove(recipe) {
                commands.insert(
                    definition.definition.name.clone(),
                    definition
                        .definition
                        .command
                        .iter()
                        .map(Into::into)
                        .collect(),
                );
                descriptors.push(definition_descriptor(root, definition));
            } else {
                let (descriptor, command) = detected_recipe(root, recipe);
                if let Some(command) = command {
                    commands.insert(recipe.into(), command);
                }
                descriptors.push(descriptor);
            }
        }
        for definition in definitions.into_values() {
            commands.insert(
                definition.definition.name.clone(),
                definition
                    .definition
                    .command
                    .iter()
                    .map(Into::into)
                    .collect(),
            );
            descriptors.push(definition_descriptor(root, definition));
        }
        descriptors.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(Self {
            descriptors,
            commands,
        })
    }

    pub fn descriptors(&self) -> Vec<HarnessDescriptor> {
        self.descriptors.clone()
    }

    /// Probes configured ACP Harnesses once and persists their public,
    /// bounded descriptors. Normal catalog reads use this cache and never
    /// start an adapter merely to render a UI.
    pub async fn refresh(root: &Path) -> Result<Self> {
        let mut catalog = Self::discover(Some(root))?;
        for index in 0..catalog.descriptors.len() {
            let descriptor = catalog.descriptors[index].clone();
            if descriptor.protocol != HarnessProtocol::Acp {
                continue;
            }
            let Ok(launch) = catalog.launch(root, &descriptor.name) else {
                continue;
            };
            let descriptor = probe_descriptor(root, descriptor, launch).await;
            persist_cached_descriptor(root, &descriptor)?;
            catalog.descriptors[index] = descriptor;
        }
        Ok(catalog)
    }

    /// A trusted Host-local ACP command for the named logical Harness. This
    /// value is intentionally never included in a `HarnessDescriptor`.
    pub fn command(&self, name: &str) -> Option<Vec<std::ffi::OsString>> {
        self.commands.get(name).cloned()
    }

    pub fn launch(&self, root: &Path, name: &str) -> Result<HarnessLaunch> {
        let command = self
            .command(name)
            .with_context(|| format!("Harness {name:?} has no trusted ACP definition"))?;
        let profile = root.join("harnesses").join(name).join("profile");
        if !profile.is_dir() {
            bail!(
                "Harness {name:?} has no managed profile; run `mews harnesses setup {name}` first"
            );
        }
        // ACP processes start from an empty environment so that provider-specific
        // configuration cannot leak in accidentally. Preserve the ordinary Host
        // execution context needed by native tools, then override only the
        // provider's profile location below.
        let mut environment = inherited_launch_environment();
        apply_profile_environment(name, profile, &mut environment);
        Ok(HarnessLaunch {
            command,
            environment,
        })
    }

    /// Invalidates the optimistic authentication marker after a provider reports
    /// expired or revoked credentials. The next catalog refresh will advertise
    /// authentication as required and setup can retry the login flow.
    pub fn invalidate_authentication(root: &Path, name: &str) -> Result<()> {
        remove_profile_auth_marker(root, name)?;
        let path = cache_path(root, name);
        let Ok(bytes) = fs::read(&path) else {
            return Ok(());
        };
        let Ok(mut cache) = serde_json::from_slice::<ProbeCache>(&bytes) else {
            return Ok(());
        };
        cache.descriptor.availability.authentication = HarnessReadiness::Required;
        cache.descriptor.availability.catalog = HarnessReadiness::Stale;
        cache.descriptor.availability.detail =
            Some("managed profile authentication must be renewed".into());
        persist_cached_descriptor(root, &cache.descriptor)
    }

    /// Prepares the MEWS-owned state needed by a configured Harness.
    ///
    /// This operation is idempotent. Built-in recipes install only into their
    /// versioned MEWS-owned runtime directory; trusted custom definitions keep
    /// their explicitly supplied executable and receive just a clean profile.
    pub async fn setup(root: &Path, name: &str) -> Result<HarnessSetup> {
        if name == "mews" {
            return Ok(HarnessSetup {
                descriptor: native_mews(),
                managed_profile: None,
                profile_created: false,
            });
        }
        if !valid_name(name) {
            bail!("invalid Harness name {name:?}");
        }

        if let Some(recipe) = recipe(name) {
            let profile = root.join("harnesses").join(name).join("profile");
            let profile_created = create_managed_profile(&profile)?;
            install_recipe(root, recipe)?;
            let descriptor = HarnessCatalog::discover(Some(root))?
                .descriptors()
                .into_iter()
                .find(|descriptor| descriptor.name == name)
                .expect("built-in recipe is always discovered");
            let descriptor = setup_probe(
                root,
                name,
                descriptor,
                HarnessCatalog::discover(Some(root))?.launch(root, name)?,
            )
            .await?;
            persist_cached_descriptor(root, &descriptor)?;
            return Ok(HarnessSetup {
                descriptor,
                managed_profile: Some(profile),
                profile_created,
            });
        }
        let definitions = load_definitions(root)?;
        let definition = definitions.get(name).with_context(|| {
            format!(
                "Harness {name:?} has no trusted ACP definition in {}",
                root.join("harnesses").display()
            )
        })?;
        let profile = root.join("harnesses").join(name).join("profile");
        let profile_created = create_managed_profile(&profile)?;

        let descriptor = setup_probe(
            root,
            name,
            definition_descriptor(Some(root), definition.clone()),
            HarnessCatalog::discover(Some(root))?.launch(root, name)?,
        )
        .await?;
        persist_cached_descriptor(root, &descriptor)?;
        Ok(HarnessSetup {
            descriptor,
            managed_profile: Some(profile),
            profile_created,
        })
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HarnessDefinition {
    name: String,
    protocol: HarnessProtocol,
    command: Vec<String>,
}

fn load_definitions(root: &Path) -> Result<BTreeMap<String, TrustedDefinition>> {
    let directory = root.join("harnesses");
    if !directory.exists() {
        return Ok(BTreeMap::new());
    }
    let mut definitions = BTreeMap::new();
    for entry in fs::read_dir(&directory)
        .with_context(|| format!("read Harness definitions in {}", directory.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension() != Some(OsStr::new("toml")) {
            continue;
        }
        let bytes = fs::read(&path)?;
        let definition: HarnessDefinition = toml::from_str(&String::from_utf8(bytes.clone())?)
            .with_context(|| format!("parse Harness definition {}", path.display()))?;
        validate_definition(&definition, &path)?;
        if definitions
            .insert(
                definition.name.clone(),
                TrustedDefinition {
                    definition,
                    hash: format!("{:x}", Sha256::digest(bytes)),
                },
            )
            .is_some()
        {
            bail!(
                "duplicate Harness definition name in {}",
                directory.display()
            );
        }
    }
    Ok(definitions)
}

fn validate_definition(definition: &HarnessDefinition, path: &Path) -> Result<()> {
    if !valid_name(&definition.name) {
        bail!(
            "invalid Harness name {:?} in {}",
            definition.name,
            path.display()
        );
    }
    if definition.protocol != HarnessProtocol::Acp {
        bail!(
            "trusted Harness definitions must use ACP in {}",
            path.display()
        );
    }
    if definition.command.is_empty() || definition.command[0].trim().is_empty() {
        bail!("Harness command must not be empty in {}", path.display());
    }
    Ok(())
}

fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
}

#[derive(Clone)]
struct TrustedDefinition {
    definition: HarnessDefinition,
    hash: String,
}

fn create_managed_profile(profile: &Path) -> Result<bool> {
    let harness_directory = profile
        .parent()
        .context("managed Harness profile must have a parent directory")?;
    let definitions_directory = harness_directory
        .parent()
        .context("managed Harness directory must have a parent directory")?;
    fs::create_dir_all(definitions_directory).with_context(|| {
        format!(
            "create MEWS Harness directory {}",
            definitions_directory.display()
        )
    })?;
    ensure_directory_is_not_a_symlink(definitions_directory)?;
    create_private_directory(harness_directory)?;
    create_private_directory(profile)
}

pub(super) fn create_private_directory(path: &Path) -> Result<bool> {
    let created = match fs::create_dir(path) {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if !directory_is_not_a_symlink(path)? {
                bail!("managed Harness path {} is not a directory", path.display());
            }
            false
        }
        Err(error) => return Err(error).with_context(|| format!("create {}", path.display())),
    };
    restrict_directory_permissions(path)?;
    Ok(created)
}

pub(super) fn ensure_directory_is_not_a_symlink(path: &Path) -> Result<()> {
    if !directory_is_not_a_symlink(path)? {
        bail!("managed Harness path {} is not a directory", path.display());
    }
    Ok(())
}

fn directory_is_not_a_symlink(path: &Path) -> Result<bool> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect managed Harness path {}", path.display()))?;
    Ok(metadata.is_dir() && !metadata.file_type().is_symlink())
}

pub(super) fn restrict_directory_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .with_context(|| format!("restrict permissions on {}", path.display()))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn native_mews() -> HarnessDescriptor {
    HarnessDescriptor {
        name: "mews".into(),
        protocol: HarnessProtocol::Mews,
        definition_hash: format!("builtin-mews-{}", env!("CARGO_PKG_VERSION")),
        availability: HarnessAvailability {
            runtime: HarnessReadiness::Ready,
            adapter: HarnessReadiness::NotApplicable,
            authentication: HarnessReadiness::NotApplicable,
            catalog: HarnessReadiness::Ready,
            detail: None,
        },
        executable_version: Some(env!("CARGO_PKG_VERSION").into()),
        native_tools: vec!["read".into(), "write".into(), "edit".into(), "bash".into()],
        modes: Vec::new(),
        supports_mcp: false,
        supports_continuation: false,
        models: Vec::new(),
        config_options: Vec::new(),
        probed_at: None,
    }
}

fn detected_recipe(
    root: Option<&Path>,
    name: &str,
) -> (HarnessDescriptor, Option<Vec<std::ffi::OsString>>) {
    let recipe = recipe(name).expect("only built-in recipes are detected");
    let command = root.and_then(|root| recipe_command(root, recipe));
    // Both managed ACP packages ship their compatible provider runtime. A
    // globally installed CLI is useful for discovery, but is not a dependency
    // once the managed adapter exists.
    let global_runtime = command_on_path(recipe.runtime);
    let runtime = if command.is_some() || global_runtime {
        HarnessReadiness::Ready
    } else {
        HarnessReadiness::Missing
    };
    let adapter = if command.is_some() {
        HarnessReadiness::Ready
    } else {
        HarnessReadiness::Missing
    };
    let descriptor = HarnessDescriptor {
        name: name.into(),
        protocol: HarnessProtocol::Acp,
        definition_hash: format!("builtin-{name}-{}", recipe.version),
        availability: HarnessAvailability {
            runtime,
            adapter,
            authentication: HarnessReadiness::Required,
            catalog: HarnessReadiness::NotApplicable,
            detail: Some(if global_runtime {
                "existing provider CLI detected; managed ACP adapter requires setup".into()
            } else {
                "managed ACP adapter is available to install during setup".into()
            }),
        },
        executable_version: command.as_ref().map(|_| recipe.version.into()),
        native_tools: Vec::new(),
        modes: Vec::new(),
        supports_mcp: false,
        supports_continuation: false,
        models: Vec::new(),
        config_options: Vec::new(),
        probed_at: None,
    };
    (
        root.and_then(|root| load_cached_descriptor(root, &descriptor))
            .unwrap_or(descriptor),
        command,
    )
}

fn definition_descriptor(root: Option<&Path>, definition: TrustedDefinition) -> HarnessDescriptor {
    let command = &definition.definition.command[0];
    let runtime = if command_on_path(command) {
        HarnessReadiness::Ready
    } else {
        HarnessReadiness::Missing
    };
    let configured = matches!(runtime, HarnessReadiness::Ready);
    let descriptor = HarnessDescriptor {
        name: definition.definition.name,
        protocol: definition.definition.protocol,
        definition_hash: definition.hash,
        availability: HarnessAvailability {
            runtime,
            adapter: if configured {
                HarnessReadiness::Ready
            } else {
                HarnessReadiness::Missing
            },
            // ACP probing, including managed-profile auth, is intentionally
            // separate from passive definition loading.
            authentication: HarnessReadiness::Stale,
            catalog: HarnessReadiness::Stale,
            detail: Some("ACP readiness has not been probed".into()),
        },
        executable_version: None,
        native_tools: Vec::new(),
        modes: Vec::new(),
        supports_mcp: false,
        supports_continuation: false,
        models: Vec::new(),
        config_options: Vec::new(),
        probed_at: None,
    };
    root.and_then(|root| load_cached_descriptor(root, &descriptor))
        .unwrap_or(descriptor)
}

async fn probe_descriptor(
    root: &Path,
    descriptor: HarnessDescriptor,
    launch: HarnessLaunch,
) -> HarnessDescriptor {
    let mut config = match mews_acp::AcpHarnessConfig::new(launch.command) {
        Ok(config) => config,
        Err(error) => return probe_failure(descriptor, error.to_string()),
    };
    config.environment = launch.environment;
    let built_in = recipe(&descriptor.name).is_some();
    let name = descriptor.name.clone();
    match mews_acp::probe_acp(config, root.to_path_buf()).await {
        Ok(probe) => {
            let mut descriptor = normalize_probe(descriptor, probe);
            if built_in && descriptor.availability.authentication == HarnessReadiness::Required {
                let _ = remove_profile_auth_marker(root, &name);
            }
            if built_in && !profile_is_authenticated(root, &name) {
                descriptor.availability.authentication = HarnessReadiness::Required;
                descriptor.availability.detail =
                    Some("managed profile authentication is required before Runs may start".into());
            }
            descriptor
        }
        Err(error) => probe_failure(descriptor, error.to_string()),
    }
}

async fn setup_probe(
    root: &Path,
    name: &str,
    descriptor: HarnessDescriptor,
    launch: HarnessLaunch,
) -> Result<HarnessDescriptor> {
    let descriptor = probe_descriptor(root, descriptor, launch.clone()).await;
    if descriptor.availability.authentication != HarnessReadiness::Required {
        return Ok(descriptor);
    }
    if !std::io::stdin().is_terminal() {
        return Ok(descriptor);
    }
    authenticate_profile(name, &launch)?;
    mark_profile_authenticated(root, name)?;
    Ok(probe_descriptor(root, descriptor, launch).await)
}

fn normalize_probe(
    mut descriptor: HarnessDescriptor,
    probe: mews_acp::AcpProbe,
) -> HarnessDescriptor {
    descriptor.availability.runtime = HarnessReadiness::Ready;
    descriptor.availability.adapter = HarnessReadiness::Ready;
    descriptor.supports_mcp = ["http", "sse", "acp"].into_iter().any(|transport| {
        probe
            .initialize
            .pointer(&format!("/agentCapabilities/mcpCapabilities/{transport}"))
            .and_then(Value::as_bool)
            == Some(true)
    });
    descriptor.supports_continuation = probe
        .initialize
        .pointer("/agentCapabilities/sessionCapabilities/resume")
        .is_some()
        || probe
            .initialize
            .pointer("/agentCapabilities/loadSession")
            .and_then(Value::as_bool)
            == Some(true);
    descriptor.probed_at = Some(now_unix());
    if let Some(session) = probe.session {
        descriptor.availability.authentication = HarnessReadiness::Ready;
        descriptor.availability.catalog = HarnessReadiness::Ready;
        descriptor.availability.detail = None;
        descriptor.config_options = bounded_config_options(&session);
        descriptor.models = models_from_options(&descriptor.config_options);
        descriptor.modes = bounded_strings(
            session
                .pointer("/modes/availableModes")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|mode| mode.get("id").and_then(Value::as_str)),
        );
    } else {
        descriptor.availability.authentication = if probe
            .session_error
            .as_deref()
            .is_some_and(authentication_error)
        {
            HarnessReadiness::Required
        } else {
            HarnessReadiness::Failed
        };
        descriptor.availability.catalog = HarnessReadiness::Stale;
        descriptor.availability.detail = probe.session_error.map(bounded_detail);
    }
    descriptor
}

fn probe_failure(mut descriptor: HarnessDescriptor, error: String) -> HarnessDescriptor {
    descriptor.availability.adapter = HarnessReadiness::Failed;
    descriptor.availability.catalog = HarnessReadiness::Stale;
    descriptor.availability.detail = Some(bounded_detail(error));
    descriptor.probed_at = Some(now_unix());
    descriptor
}

fn authentication_error(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("auth") || error.contains("login") || error.contains("credential")
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn bounded_detail(value: String) -> String {
    value.chars().take(512).collect()
}

fn bounded_config_options(session: &Value) -> Vec<Value> {
    session
        .get("configOptions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|option| option.to_string().len() <= 16 * 1024)
        .take(32)
        .cloned()
        .collect()
}

fn bounded_strings<'a>(values: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    values.into_iter().take(64).map(str::to_owned).collect()
}

fn option_values(option: &Value) -> Vec<(String, Option<String>)> {
    let Some(options) = option.get("options").and_then(Value::as_array) else {
        return Vec::new();
    };
    options
        .iter()
        .flat_map(|entry| {
            let entries = entry
                .get("options")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or_else(|| std::slice::from_ref(entry));
            entries.iter().filter_map(|entry| {
                Some((
                    entry.get("value")?.as_str()?.to_owned(),
                    entry.get("name").and_then(Value::as_str).map(str::to_owned),
                ))
            })
        })
        .take(64)
        .collect()
}

fn models_from_options(options: &[Value]) -> Vec<HarnessModelCapability> {
    let reasoning = options
        .iter()
        .filter(|option| {
            option
                .get("id")
                .and_then(Value::as_str)
                .is_some_and(|id| id.contains("reason") || id.contains("effort"))
        })
        .flat_map(option_values)
        .map(|(value, _)| value)
        .collect::<Vec<_>>();
    options
        .iter()
        .find(|option| option.get("id").and_then(Value::as_str) == Some("model"))
        .into_iter()
        .flat_map(option_values)
        .map(|(id, display_name)| HarnessModelCapability {
            id,
            display_name,
            reasoning: reasoning.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harnesses::recipes::{recipe_binary, recipe_node_path};

    #[test]
    fn discovery_always_publishes_the_native_harness_and_external_recipes() {
        let catalog = HarnessCatalog::discover(None).unwrap();
        let names: Vec<_> = catalog
            .descriptors()
            .into_iter()
            .map(|descriptor| descriptor.name)
            .collect();
        assert_eq!(names, vec!["claude", "codex", "mews"]);
    }

    #[test]
    fn trusted_definition_stays_host_local_and_requires_a_later_probe() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("harnesses");
        fs::create_dir(&directory).unwrap();
        fs::write(
            directory.join("fixture.toml"),
            "name = \"fixture\"\nprotocol = \"acp\"\ncommand = [\"definitely-missing\"]\n",
        )
        .unwrap();

        let descriptor = HarnessCatalog::discover(Some(root.path()))
            .unwrap()
            .descriptors()
            .into_iter()
            .find(|descriptor| descriptor.name == "fixture")
            .unwrap();
        assert_eq!(descriptor.protocol, HarnessProtocol::Acp);
        assert_eq!(descriptor.availability.runtime, HarnessReadiness::Missing);
        assert_eq!(descriptor.availability.catalog, HarnessReadiness::Stale);
        assert!(!descriptor.availability.ready());
    }

    #[test]
    fn malformed_trusted_definitions_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("harnesses");
        fs::create_dir(&directory).unwrap();
        fs::write(
            directory.join("unsafe.toml"),
            "name = \"Codex\"\nprotocol = \"acp\"\ncommand = [\"codex-acp\"]\n",
        )
        .unwrap();
        assert!(HarnessCatalog::discover(Some(root.path())).is_err());
    }

    #[test]
    fn managed_adapters_pin_node_and_keep_host_baseline() {
        let root = tempfile::tempdir().unwrap();
        let node = env::current_exe().unwrap();
        for name in ["codex", "claude"] {
            let recipe = recipe(name).unwrap();
            let binary = recipe_binary(root.path(), recipe);
            fs::create_dir_all(binary.parent().unwrap()).unwrap();
            fs::write(binary, b"").unwrap();
            fs::write(
                recipe_node_path(root.path(), recipe),
                node.as_os_str().as_encoded_bytes(),
            )
            .unwrap();
            fs::create_dir_all(root.path().join("harnesses").join(name).join("profile")).unwrap();
        }

        let catalog = HarnessCatalog::discover(Some(root.path())).unwrap();
        for (name, profile_variable) in [("codex", "CODEX_HOME"), ("claude", "CLAUDE_CONFIG_DIR")] {
            let recipe = recipe(name).unwrap();
            let descriptor = catalog
                .descriptors()
                .into_iter()
                .find(|descriptor| descriptor.name == name)
                .unwrap();
            assert_eq!(descriptor.availability.runtime, HarnessReadiness::Ready);
            assert_eq!(descriptor.availability.adapter, HarnessReadiness::Ready);

            let launch = catalog.launch(root.path(), name).unwrap();
            assert_eq!(
                launch.command,
                vec![
                    node.clone().into_os_string(),
                    recipe_binary(root.path(), recipe).into_os_string()
                ]
            );
            let profile = root.path().join("harnesses").join(name).join("profile");
            assert_eq!(
                launch.environment.get(OsStr::new(profile_variable)),
                Some(&profile.into_os_string())
            );
            for name in ["HOME", "USER", "SHELL", "TMPDIR", "SSH_AUTH_SOCK"] {
                if let Some(expected) = env::var_os(name) {
                    assert_eq!(launch.environment.get(OsStr::new(name)), Some(&expected));
                }
            }
            assert!(
                !launch
                    .environment
                    .contains_key(OsStr::new("XDG_CONFIG_HOME"))
            );
        }
    }

    #[test]
    fn invalidating_authentication_removes_marker_and_updates_cache() {
        let root = tempfile::tempdir().unwrap();
        let profile = root.path().join("harnesses/codex/profile");
        fs::create_dir_all(&profile).unwrap();
        mark_profile_authenticated(root.path(), "codex").unwrap();
        let mut descriptor = detected_recipe(Some(root.path()), "codex").0;
        descriptor.availability.authentication = HarnessReadiness::Ready;
        descriptor.availability.catalog = HarnessReadiness::Ready;
        persist_cached_descriptor(root.path(), &descriptor).unwrap();

        HarnessCatalog::invalidate_authentication(root.path(), "codex").unwrap();

        assert!(!profile_is_authenticated(root.path(), "codex"));
        let cache: ProbeCache =
            serde_json::from_slice(&fs::read(cache_path(root.path(), "codex")).unwrap()).unwrap();
        assert_eq!(
            cache.descriptor.availability.authentication,
            HarnessReadiness::Required
        );
        assert_eq!(
            cache.descriptor.availability.catalog,
            HarnessReadiness::Stale
        );
    }

    #[tokio::test]
    async fn setup_treats_native_mews_as_ready_without_creating_managed_state() {
        let root = tempfile::tempdir().unwrap();

        let setup = HarnessCatalog::setup(root.path(), "mews").await.unwrap();

        assert_eq!(setup.descriptor.name, "mews");
        assert_eq!(setup.managed_profile, None);
        assert!(!setup.profile_created);
        assert!(!root.path().join("harnesses/mews/profile").exists());
    }

    #[tokio::test]
    async fn setup_creates_one_private_profile_for_a_trusted_acp_definition() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("harnesses");
        fs::create_dir(&directory).unwrap();
        fs::write(
            directory.join("fixture.toml"),
            "name = \"fixture\"\nprotocol = \"acp\"\ncommand = [\"definitely-missing\"]\n",
        )
        .unwrap();

        let first = HarnessCatalog::setup(root.path(), "fixture").await.unwrap();
        let profile = first.managed_profile.clone().unwrap();
        assert!(first.profile_created);
        assert!(profile.is_dir());
        assert_eq!(
            first.descriptor.availability.authentication,
            HarnessReadiness::Stale
        );
        assert_eq!(
            first.descriptor.availability.catalog,
            HarnessReadiness::Stale
        );

        let second = HarnessCatalog::setup(root.path(), "fixture").await.unwrap();
        assert_eq!(second.managed_profile.as_deref(), Some(profile.as_path()));
        assert!(!second.profile_created);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            assert_eq!(
                fs::metadata(&profile).unwrap().permissions().mode() & 0o777,
                0o700
            );
            assert_eq!(
                fs::metadata(profile.parent().unwrap())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
    }

    #[tokio::test]
    async fn setup_requires_an_explicit_trusted_acp_definition() {
        let root = tempfile::tempdir().unwrap();

        let error = HarnessCatalog::setup(root.path(), "fixture")
            .await
            .unwrap_err();

        assert!(error.to_string().contains("no trusted ACP definition"));
        assert!(!root.path().join("harnesses/fixture/profile").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn setup_refuses_a_profile_directory_symlink() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let directory = root.path().join("harnesses");
        fs::create_dir(&directory).unwrap();
        fs::write(
            directory.join("fixture.toml"),
            "name = \"fixture\"\nprotocol = \"acp\"\ncommand = [\"fixture-acp\"]\n",
        )
        .unwrap();
        symlink(outside.path(), directory.join("fixture")).unwrap();

        assert!(HarnessCatalog::setup(root.path(), "fixture").await.is_err());
        assert!(!outside.path().join("profile").exists());
    }
}
