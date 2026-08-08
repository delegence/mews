use std::{collections::BTreeMap, env, fs, path::Path};

use anyhow::{Context, Result, bail};
use mews_protocol::{AuthCredential, AuthStatus};
use serde::{Deserialize, Serialize};
use serde_json::Value;

static AUTH_WRITE: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AuthStore {
    // TODO: Support multiple named accounts per provider with explicit routing
    // and failover policy. For now, a new login replaces that provider's credential.
    entries: BTreeMap<String, Value>,
}

impl AuthStore {
    pub fn initialize(root: &Path) -> Result<()> {
        Self::initialize_with(root, |name| env::var(name).ok())
    }

    fn initialize_with(root: &Path, variable: impl Fn(&str) -> Option<String>) -> Result<()> {
        let path = root.join("auth.json");
        if path.exists() || fs::symlink_metadata(&path).is_ok() {
            Self::load(root)?;
            return Ok(());
        }
        let mut store = Self::default();
        for (provider, key_var, base_var) in [
            ("openai", "OPENAI_API_KEY", "OPENAI_BASE_URL"),
            ("anthropic", "ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL"),
            ("google", "GEMINI_API_KEY", "GEMINI_BASE_URL"),
        ] {
            if let Some(key) = variable(key_var) {
                store.set_api_key_value(provider, key, variable(base_var))?;
            }
        }
        store.save(root)
    }

    pub fn load(root: &Path) -> Result<Self> {
        let path = root.join("auth.json");
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(error) => return Err(error.into()),
        };
        if !metadata.file_type().is_file() {
            bail!("auth.json must be a regular file");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                bail!("auth.json must not be accessible by group or others");
            }
        }
        Ok(serde_json::from_slice(&fs::read(path)?)?)
    }

    pub fn statuses(&self) -> Vec<AuthStatus> {
        self.entries
            .iter()
            .map(|(provider, value)| AuthStatus {
                provider: provider.clone(),
                kind: value
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned(),
            })
            .collect()
    }

    pub fn set_api_key(root: &Path, provider: &str, key: String) -> Result<()> {
        let _guard = AUTH_WRITE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut store = Self::load(root)?;
        let base_url = store
            .entries
            .get(provider)
            .and_then(|entry| entry.get("base_url"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        store.set_api_key_value(provider, key, base_url)?;
        store.save(root)
    }

    fn set_api_key_value(
        &mut self,
        provider: &str,
        key: String,
        base_url: Option<String>,
    ) -> Result<()> {
        if provider.is_empty() || key.is_empty() {
            bail!("provider and API key must not be empty");
        }
        self.insert(provider, &AuthCredential::ApiKey { key, base_url })
    }

    pub fn set(root: &Path, provider: &str, credential: &AuthCredential) -> Result<()> {
        let _guard = AUTH_WRITE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut store = Self::load(root)?;
        store.insert(provider, credential)?;
        store.save(root)
    }

    fn insert(&mut self, provider: &str, credential: &AuthCredential) -> Result<()> {
        if provider.is_empty() {
            bail!("provider must not be empty");
        }
        self.entries
            .insert(provider.to_owned(), serde_json::to_value(credential)?);
        Ok(())
    }

    pub fn remove(root: &Path, provider: &str) -> Result<()> {
        let _guard = AUTH_WRITE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut store = Self::load(root)?;
        if store.entries.remove(provider).is_none() {
            bail!("provider {provider:?} is not authenticated");
        }
        store.save(root)
    }

    pub(crate) fn credential(&self, provider: &str) -> Result<AuthCredential> {
        let value = self.entries.get(provider).with_context(|| {
            format!(
                "no {provider} credential in Hub auth.json; run `mews providers login {provider}`"
            )
        })?;
        Ok(serde_json::from_value(value.clone())?)
    }

    fn save(&self, root: &Path) -> Result<()> {
        let path = root.join("auth.json");
        let temporary = root.join(format!(".auth-{}.tmp", uuid::Uuid::now_v7()));
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        use std::io::Write;
        let mut file = options.open(&temporary)?;
        file.write_all(&serde_json::to_vec_pretty(self)?)?;
        file.sync_all()?;
        fs::rename(temporary, path)?;
        fs::File::open(root)?.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initialization_only_imports_explicit_environment_credentials() {
        let root = tempfile::tempdir().unwrap();
        AuthStore::initialize_with(root.path(), |_| None).unwrap();
        assert!(AuthStore::load(root.path()).unwrap().statuses().is_empty());
    }

    #[test]
    fn initialization_imports_gemini_environment_credentials() {
        let root = tempfile::tempdir().unwrap();
        AuthStore::initialize_with(root.path(), |name| match name {
            "GEMINI_API_KEY" => Some("gemini-secret".into()),
            "GEMINI_BASE_URL" => Some("https://gemini.example".into()),
            _ => None,
        })
        .unwrap();
        assert!(matches!(
            AuthStore::load(root.path())
                .unwrap()
                .credential("google")
                .unwrap(),
            AuthCredential::ApiKey { key, base_url }
                if key == "gemini-secret" && base_url.as_deref() == Some("https://gemini.example")
        ));
    }

    #[test]
    fn round_trips_auth_store_and_rejects_symlinks() {
        let root = tempfile::tempdir().unwrap();
        AuthStore::set_api_key(root.path(), "anthropic", "secret".into()).unwrap();
        assert_eq!(
            AuthStore::load(root.path()).unwrap().statuses()[0].provider,
            "anthropic"
        );
        fs::remove_file(root.path().join("auth.json")).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(root.path().join("missing"), root.path().join("auth.json"))
            .unwrap();
        #[cfg(unix)]
        assert!(AuthStore::load(root.path()).is_err());
    }

    #[test]
    fn concurrent_updates_do_not_overwrite_each_other() {
        let root = tempfile::tempdir().unwrap();
        let first_root = root.path().to_owned();
        let second_root = first_root.clone();
        let first = std::thread::spawn(move || {
            AuthStore::set_api_key(&first_root, "openai", "one".into()).unwrap();
        });
        let second = std::thread::spawn(move || {
            AuthStore::set_api_key(&second_root, "anthropic", "two".into()).unwrap();
        });
        first.join().unwrap();
        second.join().unwrap();
        let statuses = AuthStore::load(root.path()).unwrap().statuses();
        assert_eq!(statuses.len(), 2);
    }
}
