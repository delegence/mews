use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use mews_agent::CancellationToken;
use serde_json::{Value, json};
use tokio::{fs, io::AsyncReadExt, process::Command};

use super::{
    process::{self, MAX_OUTPUT, OutputLimit},
    registry::Tool,
};

pub(super) struct Read;
pub(super) struct Write;
pub(super) struct Edit;
pub(super) struct Bash;

fn string_argument<'a>(arguments: &'a Value, name: &str) -> Result<&'a str> {
    arguments
        .get(name)
        .and_then(Value::as_str)
        .with_context(|| format!("{name} must be a string"))
}

fn path(arguments: &Value, cwd: &Path) -> Result<PathBuf> {
    let value = Path::new(string_argument(arguments, "path")?);
    Ok(if value.is_absolute() {
        value.to_path_buf()
    } else {
        cwd.join(value)
    })
}

#[async_trait]
impl Tool for Read {
    fn name(&self) -> &str {
        "read"
    }
    fn description(&self) -> &str {
        "Read a UTF-8 text file"
    }
    fn schema(&self) -> Value {
        json!({"type":"object","properties":{"path":{"type":"string"}},"required":["path"],"additionalProperties":false})
    }
    async fn execute(
        &self,
        arguments: Value,
        cwd: &Path,
        _cancellation: &CancellationToken,
    ) -> Result<Value> {
        let path = path(&arguments, cwd)?;
        let mut bytes = Vec::with_capacity(MAX_OUTPUT + 1);
        fs::File::open(&path)
            .await
            .with_context(|| format!("read {}", path.display()))?
            .take((MAX_OUTPUT + 1) as u64)
            .read_to_end(&mut bytes)
            .await
            .with_context(|| format!("read {}", path.display()))?;
        if bytes.len() > MAX_OUTPUT {
            bail!("file exceeds 64 KiB read limit");
        }
        Ok(json!({"path":path,"content":String::from_utf8(bytes).context("file is not UTF-8")?}))
    }
}

#[async_trait]
impl Tool for Write {
    fn name(&self) -> &str {
        "write"
    }
    fn description(&self) -> &str {
        "Create or replace a UTF-8 text file"
    }
    fn schema(&self) -> Value {
        json!({"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"}},"required":["path","content"],"additionalProperties":false})
    }
    async fn execute(
        &self,
        arguments: Value,
        cwd: &Path,
        _cancellation: &CancellationToken,
    ) -> Result<Value> {
        let path = path(&arguments, cwd)?;
        let content = string_argument(&arguments, "content")?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(&path, content)
            .await
            .with_context(|| format!("write {}", path.display()))?;
        Ok(json!({"path":path,"bytes":content.len()}))
    }
}

#[async_trait]
impl Tool for Edit {
    fn name(&self) -> &str {
        "edit"
    }
    fn description(&self) -> &str {
        "Replace one exact, unique string in a UTF-8 file"
    }
    fn schema(&self) -> Value {
        json!({"type":"object","properties":{"path":{"type":"string"},"old_text":{"type":"string"},"new_text":{"type":"string"}},"required":["path","old_text","new_text"],"additionalProperties":false})
    }
    async fn execute(
        &self,
        arguments: Value,
        cwd: &Path,
        _cancellation: &CancellationToken,
    ) -> Result<Value> {
        let path = path(&arguments, cwd)?;
        let old = string_argument(&arguments, "old_text")?;
        let new = string_argument(&arguments, "new_text")?;
        if old.is_empty() {
            bail!("old_text must not be empty");
        }
        let content = fs::read_to_string(&path).await?;
        if content.matches(old).count() != 1 {
            bail!("old_text must match exactly once");
        }
        fs::write(&path, content.replacen(old, new, 1)).await?;
        Ok(json!({"path":path,"replacements":1}))
    }
}

#[async_trait]
impl Tool for Bash {
    fn name(&self) -> &str {
        "bash"
    }
    fn description(&self) -> &str {
        "Run a shell command with the Host user's authority"
    }
    fn schema(&self) -> Value {
        json!({"type":"object","properties":{"command":{"type":"string"},"timeout_seconds":{"type":["integer","null"],"minimum":1,"maximum":3600}},"required":["command","timeout_seconds"],"additionalProperties":false})
    }
    async fn execute(
        &self,
        arguments: Value,
        cwd: &Path,
        cancellation: &CancellationToken,
    ) -> Result<Value> {
        let command = string_argument(&arguments, "command")?;
        let seconds = match arguments.get("timeout_seconds") {
            None | Some(Value::Null) => 120,
            Some(value) => value
                .as_u64()
                .filter(|seconds| (1..=3600).contains(seconds))
                .context("timeout_seconds must be an integer from 1 through 3600")?,
        };
        let mut process = Command::new("sh");
        process
            .arg("-lc")
            .arg(command)
            .current_dir(cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let output = process::capture(
            process,
            None,
            Duration::from_secs(seconds),
            cancellation,
            OutputLimit::Truncate,
        )
        .await?;
        let decode = |bytes: Vec<u8>| String::from_utf8_lossy(&bytes).into_owned();
        Ok(
            json!({"status":output.status.code(),"success":output.status.success(),"stdout":decode(output.stdout),"stderr":decode(output.stderr)}),
        )
    }
}
