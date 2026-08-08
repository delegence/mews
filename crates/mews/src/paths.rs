use std::{
    fs, io,
    path::{Path, PathBuf},
};

pub const SECRETS_DIR: &str = "secrets";
pub const LOGS_DIR: &str = "logs";

pub fn secret(root: &Path, name: &str) -> PathBuf {
    root.join(SECRETS_DIR).join(name)
}

pub fn log(root: &Path, name: &str) -> PathBuf {
    root.join(LOGS_DIR).join(name)
}

pub fn ensure_directories(root: &Path) -> io::Result<()> {
    for directory in [root.join(SECRETS_DIR), root.join(LOGS_DIR)] {
        fs::create_dir_all(&directory)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
        }
    }
    Ok(())
}
