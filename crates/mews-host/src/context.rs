use std::{fs, path::Path};

use anyhow::{Context, Result, bail};

const MAX_CONTEXT_BYTES: u64 = 192 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContextFile {
    pub path: std::path::PathBuf,
    pub content: String,
}

/// Loads project instructions from the filesystem root toward `cwd`, matching
/// Pi's broad-to-specific context ordering.
pub fn discover_agents_md(cwd: &Path) -> Result<Vec<ContextFile>> {
    let mut directories = cwd.ancestors().collect::<Vec<_>>();
    directories.reverse();
    let mut files = Vec::new();
    let mut total = 0;
    for directory in directories {
        let path = directory.join("AGENTS.md");
        let Ok(metadata) = fs::symlink_metadata(&path) else {
            continue;
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            bail!(
                "context file must be a regular non-symlink: {}",
                path.display()
            );
        }
        total += metadata.len();
        if total > MAX_CONTEXT_BYTES {
            bail!("combined AGENTS.md project context exceeds 192 KiB");
        }
        files.push(ContextFile {
            path: path.clone(),
            content: fs::read_to_string(&path)
                .with_context(|| format!("read context file {}", path.display()))?,
        });
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovers_agents_files_from_broadest_to_nearest() {
        let root = tempfile::tempdir().unwrap();
        let child = root.path().join("project/src");
        fs::create_dir_all(&child).unwrap();
        fs::write(root.path().join("AGENTS.md"), "broad").unwrap();
        fs::write(root.path().join("project/AGENTS.md"), "specific").unwrap();

        let files = discover_agents_md(&child).unwrap();
        let relevant = files
            .iter()
            .filter(|file| file.path.starts_with(root.path()))
            .map(|file| file.content.as_str())
            .collect::<Vec<_>>();
        assert_eq!(relevant, ["broad", "specific"]);
    }

    #[test]
    fn rejects_context_over_the_aggregate_limit() {
        let root = tempfile::tempdir().unwrap();
        let child = root.path().join("project");
        fs::create_dir(&child).unwrap();
        fs::write(root.path().join("AGENTS.md"), "a".repeat(100 * 1024)).unwrap();
        fs::write(child.join("AGENTS.md"), "b".repeat(100 * 1024)).unwrap();
        assert!(discover_agents_md(&child).is_err());
    }
}
