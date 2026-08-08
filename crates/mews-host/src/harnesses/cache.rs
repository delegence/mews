use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use mews_protocol::HarnessDescriptor;
use serde::{Deserialize, Serialize};

use super::catalog::create_private_directory;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProbeCache {
    pub(super) descriptor: HarnessDescriptor,
}

pub(super) fn cache_path(root: &Path, name: &str) -> PathBuf {
    root.join("harnesses").join(name).join("probe.json")
}

pub(super) fn load_cached_descriptor(
    root: &Path,
    expected: &HarnessDescriptor,
) -> Option<HarnessDescriptor> {
    let cache = fs::read(cache_path(root, &expected.name)).ok()?;
    let descriptor = serde_json::from_slice::<ProbeCache>(&cache)
        .ok()?
        .descriptor;
    (descriptor.name == expected.name
        && descriptor.definition_hash == expected.definition_hash
        && descriptor.availability.runtime == expected.availability.runtime
        && descriptor.availability.adapter == expected.availability.adapter)
        .then_some(descriptor)
}

pub(super) fn persist_cached_descriptor(root: &Path, descriptor: &HarnessDescriptor) -> Result<()> {
    let path = cache_path(root, &descriptor.name);
    let directory = path.parent().expect("probe cache has a parent");
    create_private_directory(directory)?;
    fs::write(
        &path,
        serde_json::to_vec(&ProbeCache {
            descriptor: descriptor.clone(),
        })?,
    )
    .with_context(|| format!("write Harness probe cache {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

pub(super) fn profile_auth_marker(root: &Path, name: &str) -> PathBuf {
    root.join("harnesses")
        .join(name)
        .join("profile")
        .join(".mews-authenticated")
}

pub(super) fn profile_is_authenticated(root: &Path, name: &str) -> bool {
    profile_auth_marker(root, name).is_file()
}

pub(super) fn mark_profile_authenticated(root: &Path, name: &str) -> Result<()> {
    let marker = profile_auth_marker(root, name);
    fs::write(&marker, b"authenticated\n")
        .with_context(|| format!("record managed {name} authentication"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(marker, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

pub(super) fn remove_profile_auth_marker(root: &Path, name: &str) -> Result<()> {
    let marker = profile_auth_marker(root, name);
    match fs::remove_file(&marker) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove {}", marker.display())),
    }
}
