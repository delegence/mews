#[cfg(not(unix))]
compile_error!("the MEWS Hub transport requires Unix sockets");

mod dispatch;
mod handoff;
pub(crate) mod runs;

use std::{
    collections::HashMap,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicBool},
};

use anyhow::{Context, Result, bail};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::{Mutex, Semaphore, watch},
};

use crate::{
    host::{ConnectedHost, HostControl},
    service::Mews,
};
use mews_protocol::{
    Frame, HubRequest, HubResponse, PROTOCOL_VERSION, ProtocolError, decode_hub_envelope,
    encode_hub_frame,
};

pub(crate) type RemoteHosts = Arc<Mutex<HashMap<crate::HostId, Arc<ConnectedHost>>>>;

#[derive(Clone)]
pub(crate) struct HubControl {
    pub moving: Arc<AtomicBool>,
    pub handoff_gate: Arc<tokio::sync::RwLock<()>>,
    pub session_locks: Arc<Mutex<HashMap<crate::SessionId, Arc<Mutex<()>>>>>,
    pub run_tasks: Arc<Mutex<HashMap<crate::RunId, RunTask>>>,
    pub event_notify: Arc<tokio::sync::Notify>,
    pub permission_waiters: Arc<Mutex<HashMap<String, PermissionWaiter>>>,
}

pub(crate) struct RunTask {
    pub cancellation: mews_agent::CancellationToken,
    pub abort: tokio::task::AbortHandle,
    pub finished: Arc<tokio::sync::Notify>,
}

pub(crate) struct PermissionWaiter {
    pub request_id: String,
    pub session_id: crate::SessionId,
    pub run_id: crate::RunId,
    pub sender: tokio::sync::oneshot::Sender<Option<String>>,
}

pub(crate) struct HubRuntime {
    pub(crate) remote_hosts: RemoteHosts,
    pub(crate) local_host: Arc<ConnectedHost>,
    pub(crate) control: HubControl,
}

pub(crate) use dispatch::{RequestOrigin, dispatch};

#[derive(serde::Serialize, serde::Deserialize)]
struct HubMoveRecovery {
    target_host_id: crate::HostId,
    move_nonce: String,
}

pub fn socket_path(root: &Path) -> PathBuf {
    root.join("hub.sock")
}

pub async fn serve(root: PathBuf) -> Result<()> {
    if root.join("hub-move.phase").exists()
        && fs::read_to_string(root.join("hub-move.phase"))?.trim() == "activating"
    {
        let demoted = tokio::task::LocalSet::new()
            .run_until(serve_local(root.clone(), true))
            .await?;
        if demoted {
            demote_hub_files(&root)?;
            return crate::host::serve_joined_host(root).await;
        }
        return Ok(());
    }
    let demoted = tokio::task::LocalSet::new()
        .run_until(serve_local(root.clone(), false))
        .await?;
    if demoted {
        demote_hub_files(&root)?;
        crate::host::serve_joined_host(root).await
    } else {
        Ok(())
    }
}

fn demote_hub_files(root: &Path) -> Result<()> {
    for name in [
        "mews.db",
        "mews.db-wal",
        "mews.db-shm",
        "secrets/installation.key",
        "secrets/hub-noise.key",
        "auth.json",
        "hub-move.phase",
        "hub-move-recovery.json",
    ] {
        let path = root.join(name);
        if path.exists() {
            fs::remove_file(path)?;
        }
    }
    fs::File::open(root)?.sync_all()?;
    Ok(())
}

async fn serve_local(root: PathBuf, recovering_handoff: bool) -> Result<bool> {
    let path = socket_path(&root);
    if path.exists() {
        match UnixStream::connect(&path).await {
            Ok(_) => bail!("Hub is already running"),
            Err(_) => fs::remove_file(&path)
                .with_context(|| format!("remove stale {}", path.display()))?,
        }
    }
    let listener = UnixListener::bind(&path).with_context(|| format!("bind {}", path.display()))?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    let initial = if recovering_handoff {
        Mews::open_handoff(&root)?
    } else {
        Mews::open(&root)?
    };
    if root.join("hub-promote").exists() {
        let state = root.join("hub.json");
        if state.exists() {
            fs::rename(
                state,
                root.join(format!("host-state.previous-{}.json", uuid::Uuid::now_v7())),
            )?;
            fs::File::open(&root)?.sync_all()?;
            retain_previous_host_states(&root)?;
        }
        fs::remove_file(root.join("hub-promote"))?;
        fs::File::open(&root)?.sync_all()?;
    }
    let recovery = if recovering_handoff {
        Some(serde_json::from_slice::<HubMoveRecovery>(&fs::read(
            root.join("hub-move-recovery.json"),
        )?)?)
    } else {
        None
    };
    let reconnecting = if let Some(recovery) = &recovery {
        vec![initial.remote_host_acceptance(&recovery.target_host_id)?]
    } else {
        initial.remote_host_acceptances()?
    };
    let local_host = Arc::new(
        ConnectedHost::in_process(
            initial.installation()?.hub_host_id,
            mews_host::ToolRegistry::with_host_extensions(&root)?,
        )
        .await?,
    );
    // Keep the primary connection alive for the lifetime of the Hub lock. Each
    // request opens its own SQLite connection so no async operation holds a
    // coarse service mutex.
    let mews = initial;
    let remote_hosts: RemoteHosts = Arc::new(Mutex::new(HashMap::new()));
    let control = HubControl {
        moving: Arc::new(AtomicBool::new(recovering_handoff)),
        handoff_gate: Arc::new(tokio::sync::RwLock::new(())),
        session_locks: Arc::new(Mutex::new(HashMap::new())),
        run_tasks: Arc::new(Mutex::new(HashMap::new())),
        event_notify: Arc::new(tokio::sync::Notify::new()),
        permission_waiters: Arc::new(Mutex::new(HashMap::new())),
    };
    let runtime = Arc::new(HubRuntime {
        remote_hosts: Arc::clone(&remote_hosts),
        local_host,
        control: control.clone(),
    });
    let client_capacity = Arc::new(Semaphore::new(128));
    for (relay_urls, accepted) in reconnecting {
        let root = root.clone();
        let remote_hosts = Arc::clone(&remote_hosts);
        let control = control.clone();
        let local_host = Arc::clone(&runtime.local_host);
        tokio::task::spawn_local(async move {
            let _ = crate::host::serve_hub_host(
                root,
                relay_urls,
                accepted,
                remote_hosts,
                control,
                local_host,
            )
            .await;
        });
    }
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    if let Some(recovery) = recovery {
        let hosts = Arc::clone(&remote_hosts);
        let shutdown = shutdown_tx.clone();
        tokio::task::spawn_local(async move {
            loop {
                if let Some(host) = hosts.lock().await.get(&recovery.target_host_id).cloned()
                    && host.arm_hub_transfer(&recovery.move_nonce).await.is_ok()
                    && host.activate_hub_transfer().await.is_ok()
                {
                    let _ = shutdown.send(true);
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
    }
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let Ok(permit) = Arc::clone(&client_capacity).try_acquire_owned() else {
                    // Refuse excess clients without blocking Hub shutdown or handoff.
                    drop(stream);
                    continue;
                };
                let root = root.clone();
                let shutdown = shutdown_tx.clone();
                let runtime = Arc::clone(&runtime);
                tokio::task::spawn_local(async move {
                    let _permit = permit;
                    if let Err(error) = connection(stream, runtime, root, shutdown).await {
                        eprintln!("local client connection failed: {error:#}");
                    }
                });
            }
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && *shutdown_rx.borrow() { break; }
            }
        }
    }
    let _ = fs::remove_file(path);
    drop(mews);
    Ok(root.join("hub-move.phase").exists()
        && fs::read_to_string(root.join("hub-move.phase"))?.trim() == "activating")
}

fn retain_previous_host_states(root: &Path) -> Result<()> {
    let prefix = "host-state.previous-";
    let mut previous = fs::read_dir(root)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with(prefix))
        .collect::<Vec<_>>();
    previous.sort_by_key(|entry| entry.file_name());
    for stale in previous.into_iter().rev().skip(1) {
        fs::remove_file(stale.path())?;
    }
    Ok(())
}

async fn connection(
    stream: UnixStream,
    runtime: Arc<HubRuntime>,
    root: PathBuf,
    shutdown: watch::Sender<bool>,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    loop {
        let mut encoded = Vec::new();
        let read = (&mut reader)
            .take(1024 * 1024 + 1)
            .read_until(b'\n', &mut encoded)
            .await?;
        if read == 0 {
            break;
        }
        if encoded.len() > 1024 * 1024 || !encoded.ends_with(b"\n") {
            bail!("Hub frame exceeds 1 MiB")
        }
        let frame = decode_hub_envelope(&encoded)?;
        let request_id = frame.request_id;
        let response = if frame.protocol != PROTOCOL_VERSION {
            HubResponse::Error(ProtocolError::unsupported_version(frame.protocol))
        } else {
            let request: HubRequest = serde_json::from_value(frame.body)?;
            let result = match resolve_request_location(RequestOrigin::Local, request) {
                Ok(request) => dispatch(&runtime, &root, RequestOrigin::Local, request).await,
                Err(error) => Err(error),
            };
            match result {
                Ok((response, should_shutdown)) => {
                    if should_shutdown {
                        let _ = shutdown.send(true);
                    }
                    response
                }
                Err(error) => HubResponse::Error(protocol_error(&error)),
            }
        };
        let response_frame = Frame::with_request_id(response, request_id);
        let encoded = encode_hub_frame(&response_frame)?;
        writer.write_all(&encoded).await?;
        writer.write_all(b"\n").await?;
    }
    Ok(())
}

pub(crate) fn protocol_error(error: &anyhow::Error) -> ProtocolError {
    use mews_protocol::ProtocolErrorCode;
    use mews_store::StoreError;

    let code = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<StoreError>())
        .map(|error| match error {
            StoreError::NotFound { .. } => ProtocolErrorCode::NotFound,
            StoreError::DuplicateAgent(_)
            | StoreError::RevisionConflict { .. }
            | StoreError::LeafConflict { .. } => ProtocolErrorCode::Conflict,
            StoreError::InvalidAgent(_) | StoreError::InvalidData(_) => {
                ProtocolErrorCode::InvalidRequest
            }
            StoreError::Database(_) => ProtocolErrorCode::Internal,
        })
        .unwrap_or_else(|| {
            let message = error.to_string().to_ascii_lowercase();
            if message.contains("offline")
                || message.contains("not connected")
                || message.contains("moving")
            {
                ProtocolErrorCode::Unavailable
            } else if message.contains("not found") {
                ProtocolErrorCode::NotFound
            } else {
                ProtocolErrorCode::Internal
            }
        });
    ProtocolError {
        retryable: code == ProtocolErrorCode::Unavailable,
        code,
        message: format!("{error:#}"),
    }
}

fn hub_home() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is unavailable for a locationless client")?
        .canonicalize()
        .context("resolve Hub user's home directory")
}

pub(crate) fn resolve_request_location(
    origin: RequestOrigin<'_>,
    request: HubRequest,
) -> Result<HubRequest> {
    Ok(match request {
        HubRequest::StartSession {
            slug,
            working_directory: Some(working_directory),
        } => match origin {
            RequestOrigin::Host(host) => HubRequest::StartSessionOn {
                slug,
                host_id: host.host_id().clone(),
                working_directory,
            },
            RequestOrigin::Local => HubRequest::StartSession {
                slug,
                working_directory: Some(working_directory),
            },
        },
        HubRequest::StartSession {
            slug,
            working_directory: None,
        } => HubRequest::StartSession {
            slug,
            working_directory: Some(hub_home()?),
        },
        request => request,
    })
}

#[cfg(test)]
mod protocol_error_tests {
    use super::*;
    use mews_protocol::ProtocolErrorCode;
    use mews_store::StoreError;

    #[test]
    fn preserves_actionable_error_categories() {
        let missing = anyhow::Error::new(StoreError::NotFound {
            kind: "Session",
            id: "missing".into(),
        });
        assert_eq!(protocol_error(&missing).code, ProtocolErrorCode::NotFound);

        let conflict = anyhow::Error::new(StoreError::DuplicateAgent("coder".into()));
        assert_eq!(protocol_error(&conflict).code, ProtocolErrorCode::Conflict);

        let unavailable = anyhow::anyhow!("target Host is offline");
        let mapped = protocol_error(&unavailable);
        assert_eq!(mapped.code, ProtocolErrorCode::Unavailable);
        assert!(mapped.retryable);
    }
}
