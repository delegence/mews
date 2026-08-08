use std::{fs, path::Path};

use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand_core::OsRng;
use zeroize::Zeroizing;

const KEY_BYTES: usize = 32;

/// Persistent Host identity. Its secret never leaves the Host state directory.
pub struct HostIdentity(SigningKey);

/// Independent X25519 static identity used only by Noise handshakes.
pub struct NoiseIdentity {
    private: Zeroizing<Vec<u8>>,
    public: Vec<u8>,
}

impl NoiseIdentity {
    pub fn load_or_create(path: &Path) -> Result<Self> {
        if path.exists() {
            return Self::load(path);
        }
        let params = "Noise_XX_25519_ChaChaPoly_BLAKE2s".parse()?;
        let pair = snow::Builder::new(params).generate_keypair()?;
        let mut encoded = Zeroizing::new(pair.private);
        encoded.extend_from_slice(&pair.public);
        write_secret(path, &encoded)?;
        Ok(Self {
            private: Zeroizing::new(encoded[..32].to_vec()),
            public: encoded[32..].to_vec(),
        })
    }

    pub fn load(path: &Path) -> Result<Self> {
        let metadata =
            fs::symlink_metadata(path).with_context(|| format!("inspect {}", path.display()))?;
        if !metadata.file_type().is_file() {
            anyhow::bail!("Noise identity must be a regular file");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                anyhow::bail!("Noise identity permissions allow group or other access");
            }
        }
        let encoded = fs::read(path)?;
        if encoded.len() != 64 {
            anyhow::bail!("invalid Noise identity key length");
        }
        Ok(Self {
            private: Zeroizing::new(encoded[..32].to_vec()),
            public: encoded[32..].to_vec(),
        })
    }

    pub fn public_key(&self) -> String {
        URL_SAFE_NO_PAD.encode(&self.public)
    }

    pub(crate) fn private_key(&self) -> &[u8] {
        &self.private
    }

    pub fn public_bytes(&self) -> &[u8] {
        &self.public
    }

    pub fn validate_public_key(encoded: &str) -> Result<()> {
        let decoded = URL_SAFE_NO_PAD.decode(encoded)?;
        if decoded.len() != 32 {
            anyhow::bail!("invalid Noise public key length");
        }
        Ok(())
    }
}

impl HostIdentity {
    pub fn from_seed(seed: [u8; 32]) -> Self {
        Self(SigningKey::from_bytes(&seed))
    }
    pub fn load_or_create(path: &Path) -> Result<Self> {
        if path.exists() {
            return Self::load(path);
        }
        let identity = Self(SigningKey::generate(&mut OsRng));
        write_secret(path, identity.0.as_bytes())?;
        Ok(identity)
    }

    pub fn load(path: &Path) -> Result<Self> {
        let metadata =
            fs::symlink_metadata(path).with_context(|| format!("inspect {}", path.display()))?;
        if !metadata.file_type().is_file() {
            anyhow::bail!("Host identity must be a regular file");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                anyhow::bail!("Host identity permissions must not allow group or other access");
            }
        }
        let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
        let key: [u8; KEY_BYTES] = bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("invalid Host identity key length"))?;
        Ok(Self(SigningKey::from_bytes(&key)))
    }

    pub fn public_key(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.0.verifying_key().as_bytes())
    }

    pub fn sign(&self, message: &[u8]) -> String {
        URL_SAFE_NO_PAD.encode(self.0.sign(message).to_bytes())
    }

    pub fn verify(public_key: &str, message: &[u8], signature: &str) -> Result<()> {
        let key = decode_array(public_key, "public key")?;
        let signature = decode_array(signature, "signature")?;
        VerifyingKey::from_bytes(&key)?
            .verify(message, &Signature::from_bytes(&signature))
            .map_err(|_| anyhow::anyhow!("invalid Host identity signature"))
    }
}

impl mews_relay::RelaySigner for HostIdentity {
    fn public_key(&self) -> String {
        self.public_key()
    }

    fn sign(&self, message: &[u8]) -> String {
        self.sign(message)
    }
}

fn decode_array<const N: usize>(encoded: &str, label: &str) -> Result<[u8; N]> {
    let decoded = URL_SAFE_NO_PAD
        .decode(encoded)
        .with_context(|| format!("invalid {label} encoding"))?;
    decoded
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid {label} length"))
}

fn write_secret(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let temporary = path.with_extension(format!("tmp-{}", uuid::Uuid::now_v7()));
        let result = (|| {
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&temporary)
                .with_context(|| format!("create {}", temporary.display()))?;
            file.write_all(bytes)?;
            file.sync_all()?;
            fs::hard_link(&temporary, path)
                .with_context(|| format!("install {}", path.display()))?;
            if let Some(parent) = path.parent() {
                fs::File::open(parent)?.sync_all()?;
            }
            Ok(())
        })();
        let _ = fs::remove_file(temporary);
        result
    }
    #[cfg(not(unix))]
    {
        anyhow::bail!("secure Host identity storage is not implemented on this platform")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_round_trips_and_rejects_tampering() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("secrets/host.key");
        let first = HostIdentity::load_or_create(&path).unwrap();
        let signature = first.sign(b"enrollment challenge");
        HostIdentity::verify(&first.public_key(), b"enrollment challenge", &signature).unwrap();
        assert!(HostIdentity::verify(&first.public_key(), b"tampered", &signature).is_err());
        assert_eq!(
            first.public_key(),
            HostIdentity::load(&path).unwrap().public_key()
        );
    }
}
