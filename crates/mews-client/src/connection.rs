use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use mews_protocol::{
    Frame, HubRequest, HubResponse, RequestId, decode_hub_body, decode_hub_envelope,
    encode_hub_frame, validate_hub_version,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
};

struct Transport {
    reader: tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
    writer: tokio::net::unix::OwnedWriteHalf,
}

pub(crate) struct LocalConnection {
    root: PathBuf,
    transport: Transport,
}

impl LocalConnection {
    pub(crate) async fn connect(root: &Path) -> Result<Self> {
        Ok(Self {
            root: root.to_path_buf(),
            transport: Self::connect_transport(root).await?,
        })
    }

    async fn connect_transport(root: &Path) -> Result<Transport> {
        let stream = UnixStream::connect(root.join("hub.sock"))
            .await
            .context("connect to local MEWS daemon; run mews setup")?;
        let (reader, writer) = stream.into_split();
        Ok(Transport {
            reader: BufReader::new(reader).lines(),
            writer,
        })
    }

    pub(crate) async fn request(&mut self, request: HubRequest) -> Result<HubResponse> {
        let safe = retry_safe(&request);
        match self.request_once(&request).await {
            Ok(response) => Ok(response),
            Err(first) if safe => {
                let mut delay = 25;
                for attempt in 1..=5 {
                    match Self::connect_transport(&self.root).await {
                        Ok(transport) => {
                            self.transport = transport;
                            match self.request_once(&request).await {
                                Ok(response) => return Ok(response),
                                Err(error) if attempt == 5 => {
                                    return Err(error).with_context(|| {
                                        format!(
                                            "request failed after reconnect; first error: {first:#}"
                                        )
                                    });
                                }
                                Err(_) => {}
                            }
                        }
                        Err(error) if attempt == 5 => {
                            return Err(error).with_context(|| {
                                format!("could not reconnect; first request error: {first:#}")
                            });
                        }
                        Err(_) => {}
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                    delay *= 2;
                }
                unreachable!()
            }
            Err(error) => Err(error
                .context("request outcome may be unknown; only idempotent requests are retried")),
        }
    }

    async fn request_once(&mut self, request: &HubRequest) -> Result<HubResponse> {
        let request_id = RequestId::new();
        let encoded = encode_hub_frame(&Frame::with_request_id(request, request_id.clone()))?;
        self.transport.writer.write_all(&encoded).await?;
        self.transport.writer.write_all(b"\n").await?;
        let line = self
            .transport
            .reader
            .next_line()
            .await?
            .context(
                "MEWS daemon closed the connection; it may be running an older protocol version, run `mews restart`",
            )?;
        let frame = decode_hub_envelope(line.as_bytes())?;
        validate_hub_version(&frame)?;
        let frame: Frame<HubResponse> = decode_hub_body(frame)?;
        if frame.request_id != request_id {
            bail!("daemon response request ID does not match request");
        }
        match frame.body {
            HubResponse::Error(error) => bail!(error),
            response => Ok(response),
        }
    }
}

fn retry_safe(request: &HubRequest) -> bool {
    matches!(
        request,
        HubRequest::Status
            | HubRequest::ListAgents
            | HubRequest::ListSessions
            | HubRequest::GetSession { .. }
            | HubRequest::GetSessionModelConfig { .. }
            | HubRequest::ListHosts
            | HubRequest::ListAuth
            | HubRequest::ListModels
            | HubRequest::GetProviderDefaults
            | HubRequest::GetRun { .. }
            | HubRequest::PollEvents { .. }
            | HubRequest::SubscribeSession { .. }
            | HubRequest::UnsubscribeSession { .. }
            | HubRequest::AcknowledgeEvents { .. }
            | HubRequest::StartTurn { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

    #[test]
    fn only_idempotent_requests_are_automatically_retried() {
        assert!(retry_safe(&HubRequest::Status));
        assert!(retry_safe(&HubRequest::StartTurn {
            idempotency_key: "turn-1".into(),
            session_id: mews_protocol::SessionId::new(),
            prompt: "hello".into(),
            metadata: serde_json::Value::Null,
            source: None,
        }));
        assert!(!retry_safe(&HubRequest::CreateAgent {
            slug: "coder".into(),
            harness: None,
            harness_options: Default::default(),
        }));
    }

    #[tokio::test]
    async fn rejects_a_response_for_another_request() {
        let root = tempfile::tempdir().unwrap();
        let listener = UnixListener::bind(root.path().join("hub.sock")).unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (reader, mut writer) = stream.into_split();
            let mut lines = BufReader::new(reader).lines();
            lines.next_line().await.unwrap().unwrap();
            let response = Frame::with_request_id(HubResponse::Ack, RequestId::new());
            writer
                .write_all(&encode_hub_frame(&response).unwrap())
                .await
                .unwrap();
            writer.write_all(b"\n").await.unwrap();
        });

        let mut connection = LocalConnection::connect(root.path()).await.unwrap();
        let error = connection
            .request(HubRequest::ArchiveAgent {
                slug: "coder".into(),
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("outcome may be unknown"));
        assert!(format!("{error:#}").contains("request ID does not match"));
    }

    #[tokio::test]
    async fn reports_an_incompatible_response_before_decoding_its_body() {
        let root = tempfile::tempdir().unwrap();
        let listener = UnixListener::bind(root.path().join("hub.sock")).unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (reader, mut writer) = stream.into_split();
            let mut lines = BufReader::new(reader).lines();
            let request = lines.next_line().await.unwrap().unwrap();
            let request: Frame<serde_json::Value> =
                decode_hub_envelope(request.as_bytes()).unwrap();
            let response = serde_json::json!({
                "protocol": 999,
                "request_id": request.request_id,
                "body": { "type": "future_response" }
            });
            writer
                .write_all(serde_json::to_string(&response).unwrap().as_bytes())
                .await
                .unwrap();
            writer.write_all(b"\n").await.unwrap();
        });

        let mut connection = LocalConnection::connect(root.path()).await.unwrap();
        let error = connection
            .request(HubRequest::ArchiveAgent {
                slug: "coder".into(),
            })
            .await
            .unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("protocol version 999 is incompatible"));
        assert!(message.contains("restart the MEWS daemon"));
    }
}
