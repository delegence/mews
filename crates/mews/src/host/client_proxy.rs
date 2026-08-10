#[cfg(not(unix))]
compile_error!("the MEWS Host daemon requires Unix sockets");

use std::{
    collections::HashMap,
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, bail};
use mews_protocol::{
    Frame, HubRequest, HubResponse, PROTOCOL_VERSION, PeerEnvelope, ProtocolError, RequestId,
    decode_hub_envelope, encode_hub_frame,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::{Semaphore, mpsc, oneshot},
};

pub(super) async fn serve_local_clients(
    root: &Path,
    peer: mpsc::Sender<PeerEnvelope>,
    pending: Arc<Mutex<HashMap<RequestId, oneshot::Sender<HubResponse>>>>,
) -> Result<()> {
    let path = crate::hub::socket_path(root);
    if path.exists() {
        fs::remove_file(&path)?;
    }
    let listener = UnixListener::bind(&path)?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let client_capacity = Arc::new(Semaphore::new(128));
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let Ok(permit) = Arc::clone(&client_capacity).try_acquire_owned() else {
                    // Keep the proxy responsive to shutdown while at capacity.
                    drop(stream);
                    continue;
                };
                let peer = peer.clone();
                let pending = Arc::clone(&pending);
                let shutdown = shutdown_tx.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(error) = local_client(stream, peer, pending, shutdown).await {
                        eprintln!("local client connection failed: {error:#}");
                    }
                });
            }
            changed = shutdown_rx.changed() => {
                if changed.is_ok() && *shutdown_rx.borrow() {
                    let _ = fs::remove_file(&path);
                    return Ok(());
                }
            }
        }
    }
}

async fn local_client(
    stream: UnixStream,
    peer: mpsc::Sender<PeerEnvelope>,
    pending: Arc<Mutex<HashMap<RequestId, oneshot::Sender<HubResponse>>>>,
    shutdown: tokio::sync::watch::Sender<bool>,
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
            return Ok(());
        }
        if encoded.len() > 1024 * 1024 || !encoded.ends_with(b"\n") {
            bail!("client frame exceeds 1 MiB");
        }
        let frame = decode_hub_envelope(&encoded)?;
        let correlation_id = frame.request_id.clone();
        let response = if frame.protocol != PROTOCOL_VERSION {
            HubResponse::Error(ProtocolError::unsupported_version(frame.protocol))
        } else {
            let request: HubRequest = serde_json::from_value(frame.body)?;
            if matches!(request, HubRequest::Shutdown) {
                let _ = shutdown.send(true);
                let response_frame = Frame::with_request_id(HubResponse::Ack, correlation_id);
                let encoded = encode_hub_frame(&response_frame)?;
                writer.write_all(&encoded).await?;
                writer.write_all(b"\n").await?;
                continue;
            }
            let request_id = RequestId::new();
            let timeout_seconds = client_request_timeout(&request);
            let (reply, receive) = oneshot::channel();
            pending
                .lock()
                .expect("pending clients poisoned")
                .insert(request_id.clone(), reply);
            if let Err(error) = peer
                .send(PeerEnvelope::ClientRequest {
                    request_id: request_id.clone(),
                    body: request,
                })
                .await
            {
                pending
                    .lock()
                    .expect("pending clients poisoned")
                    .remove(&request_id);
                return Err(error).context("Host connection closed");
            }
            match tokio::time::timeout(std::time::Duration::from_secs(timeout_seconds), receive)
                .await
            {
                Ok(Ok(response)) => response,
                Ok(Err(_)) => {
                    HubResponse::Error(ProtocolError::unavailable("Hub connection closed"))
                }
                Err(_) => {
                    pending
                        .lock()
                        .expect("pending clients poisoned")
                        .remove(&request_id);
                    HubResponse::Error(ProtocolError::unavailable("Hub request timed out"))
                }
            }
        };
        let response = Frame::with_request_id(response, correlation_id);
        writer.write_all(&encode_hub_frame(&response)?).await?;
        writer.write_all(b"\n").await?;
    }
}

fn client_request_timeout(request: &HubRequest) -> u64 {
    match request {
        HubRequest::PollEvents { wait_ms, .. } => u64::from(*wait_ms / 1000) + 5,
        HubRequest::MoveHub { .. } => 300,
        _ => 30,
    }
}

#[cfg(test)]
mod timeout_tests {
    use super::*;

    #[test]
    fn remote_mutations_have_enough_time_to_finish() {
        assert_eq!(client_request_timeout(&HubRequest::Status), 30);
        assert_eq!(
            client_request_timeout(&HubRequest::MoveHub {
                host: "mini-pc".into()
            }),
            300
        );
        assert_eq!(
            client_request_timeout(&HubRequest::PollEvents {
                consumer_id: crate::ConsumerId::new(),
                limit: 10,
                wait_ms: 25_000,
            }),
            30
        );
    }
}
