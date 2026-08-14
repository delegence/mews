use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::{fmt, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    time,
};

use crate::{
    mcp::{TurnMcpBridge, TurnMcpHttp},
    permissions::{AcpPermissionDecision, AcpPermissionHandler, AcpPermissionRequest},
};

pub(crate) struct RpcClient<'a, W> {
    writer: &'a mut W,
    reader: &'a mut BufReader<tokio::process::ChildStdout>,
    timeout: Duration,
    permission_handler: &'a dyn AcpPermissionHandler,
    next_id: Arc<AtomicU64>,
}

const MAX_ACP_LINE_BYTES: usize = 1024 * 1024;
const CANCELLATION_GRACE: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcpErrorKind {
    AuthenticationRequired,
    ResourceNotFound,
    Other,
}

/// Cancellation is a control-flow outcome, not an adapter failure.  Keep it
/// typed while crossing the otherwise anyhow-based ACP boundary.
#[derive(Debug)]
pub struct AcpCancelled;

impl fmt::Display for AcpCancelled {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ACP harness Turn cancelled")
    }
}

impl std::error::Error for AcpCancelled {}

pub fn is_cancelled(error: &anyhow::Error) -> bool {
    error.downcast_ref::<AcpCancelled>().is_some()
}

#[derive(Debug)]
struct AcpRpcError {
    method: String,
    code: i64,
    message: String,
    data: Option<Value>,
}

impl fmt::Display for AcpRpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "ACP {} failed ({}): {}",
            self.method, self.code, self.message
        )?;
        if let Some(data) = &self.data {
            write!(formatter, " ({data})")?;
        }
        Ok(())
    }
}

impl std::error::Error for AcpRpcError {}

pub(crate) fn is_resource_not_found(error: &anyhow::Error) -> bool {
    classify_error(error) == Some(AcpErrorKind::ResourceNotFound)
}

pub fn classify_error(error: &anyhow::Error) -> Option<AcpErrorKind> {
    error
        .downcast_ref::<AcpRpcError>()
        .map(|error| match error.code {
            -32000 => AcpErrorKind::AuthenticationRequired,
            -32002 => AcpErrorKind::ResourceNotFound,
            _ => AcpErrorKind::Other,
        })
}

impl<'a, W: AsyncWriteExt + Unpin> RpcClient<'a, W> {
    pub(crate) fn new(
        writer: &'a mut W,
        reader: &'a mut BufReader<tokio::process::ChildStdout>,
        timeout: Duration,
        permission_handler: &'a dyn AcpPermissionHandler,
    ) -> Self {
        Self {
            writer,
            reader,
            timeout,
            permission_handler,
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub(crate) fn new_with_next_id(
        writer: &'a mut W,
        reader: &'a mut BufReader<tokio::process::ChildStdout>,
        timeout: Duration,
        permission_handler: &'a dyn AcpPermissionHandler,
        next_id: Arc<AtomicU64>,
    ) -> Self {
        Self {
            writer,
            reader,
            timeout,
            permission_handler,
            next_id,
        }
    }

    pub(crate) async fn request<F>(
        &mut self,
        method: &str,
        params: Value,
        cancellation: &mews_agent::CancellationToken,
        mcp: Option<&TurnMcpBridge<'_>>,
        mcp_http: Option<&TurnMcpHttp>,
        on_update: F,
    ) -> Result<Value>
    where
        F: FnMut(&Value) -> Result<()>,
    {
        self.request_with_dispatch(
            method,
            params,
            cancellation,
            mcp,
            mcp_http,
            || Ok(()),
            on_update,
        )
        .await
    }

    /// Sends a request and reports the precise external-effect boundary after
    /// its bytes have been flushed to the Harness stdin.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn request_with_dispatch<F, D>(
        &mut self,
        method: &str,
        params: Value,
        cancellation: &mews_agent::CancellationToken,
        mcp: Option<&TurnMcpBridge<'_>>,
        mcp_http: Option<&TurnMcpHttp>,
        mut on_dispatched: D,
        mut on_update: F,
    ) -> Result<Value>
    where
        F: FnMut(&Value) -> Result<()>,
        D: FnMut() -> Result<()>,
    {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let session_id = params
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let request = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
        self.writer
            .write_all(serde_json::to_string(&request)?.as_bytes())
            .await?;
        self.writer.write_all(b"\n").await?;
        self.writer.flush().await?;
        on_dispatched()?;

        let deadline = time::Instant::now() + self.timeout;

        loop {
            let line = if let Some(http) = mcp_http {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        self.cancel_with_grace(session_id.as_deref()).await;
                        return Err(anyhow!(AcpCancelled));
                    }
                    result = http.accept_and_handle(mcp.context("MCP endpoint requires a Turn bridge")?, deadline) => {
                        result?;
                        continue;
                    }
                    _ = time::sleep_until(deadline) => bail!("ACP request timed out"),
                    line = read_acp_line(self.reader, deadline) => line?,
                }
            } else {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        self.cancel_with_grace(session_id.as_deref()).await;
                        return Err(anyhow!(AcpCancelled));
                    }
                    _ = time::sleep_until(deadline) => bail!("ACP request timed out"),
                    line = read_acp_line(self.reader, deadline) => line?,
                }
            };
            let line = line.context("ACP Harness closed stdout before replying")?;
            let message: Value = serde_json::from_str(&line)
                .with_context(|| format!("invalid ACP JSON-RPC message: {line}"))?;
            if message.get("method").and_then(Value::as_str) == Some("session/update") {
                validate_inbound_session(&message, session_id.as_deref())?;
                if let Some(update) = message
                    .get("params")
                    .and_then(|params| params.get("update"))
                {
                    on_update(update)?;
                }
                continue;
            }
            if message.get("method").is_some() && message.get("id").is_some() {
                self.handle_client_request(
                    &message,
                    session_id.as_deref(),
                    cancellation,
                    deadline,
                    &mut on_update,
                )
                .await?;
                continue;
            }
            if message.get("id") != Some(&json!(id)) {
                continue;
            }
            if let Some(error) = message.get("error") {
                return Err(acp_rpc_error(method, error));
            }
            return message
                .get("result")
                .cloned()
                .context("ACP response did not include result");
        }
    }

    /// ACP agents can issue JSON-RPC requests back to their client while an
    /// outer request is pending. MEWS does not yet have an interactive
    /// approval channel, so permission requests are answered conservatively:
    /// choose an adapter-provided reject option, or report cancellation when
    /// no reject option exists. This is deliberately based on ACP option kind
    /// rather than provider-specific option IDs.
    async fn handle_client_request<F>(
        &mut self,
        message: &Value,
        expected_session_id: Option<&str>,
        cancellation: &mews_agent::CancellationToken,
        deadline: time::Instant,
        on_update: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&Value) -> Result<()>,
    {
        let id = message
            .get("id")
            .cloned()
            .context("ACP client request did not include an id")?;
        let method = message
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let response = if method == "session/request_permission" {
            validate_inbound_session(message, expected_session_id)?;
            let params = message.get("params").cloned().unwrap_or(Value::Null);
            on_update(&json!({
                "sessionUpdate": "permission_request",
                "request": params,
            }))?;
            let request: AcpPermissionRequest = serde_json::from_value(params)
                .context("invalid ACP session/request_permission parameters")?;
            let decision =
                request_permission(self.permission_handler, &request, cancellation, deadline)
                    .await?;
            let outcome = match decision {
                AcpPermissionDecision::Selected(option_id) => {
                    if !request
                        .options
                        .iter()
                        .any(|option| option.option_id == option_id)
                    {
                        bail!(
                            "ACP permission handler selected an option that was not offered: {option_id:?}"
                        );
                    }
                    json!({ "outcome": "selected", "optionId": option_id })
                }
                AcpPermissionDecision::Cancelled => json!({ "outcome": "cancelled" }),
            };
            json!({ "jsonrpc": "2.0", "id": id, "result": { "outcome": outcome } })
        } else {
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": -32601, "message": format!("Unsupported ACP client method: {method}") }
            })
        };
        self.write_message(&response).await
    }

    async fn cancel_session(&mut self, session_id: Option<&str>) -> Result<()> {
        if let Some(session_id) = session_id {
            self.write_message(&json!({
                "jsonrpc": "2.0",
                "method": "session/cancel",
                "params": { "sessionId": session_id },
            }))
            .await?;
        }
        Ok(())
    }

    async fn cancel_with_grace(&mut self, session_id: Option<&str>) {
        let deadline = time::Instant::now() + CANCELLATION_GRACE;
        let _ = time::timeout_at(deadline, self.cancel_session(session_id)).await;
        time::sleep_until(deadline).await;
    }

    async fn write_message(&mut self, message: &Value) -> Result<()> {
        self.writer
            .write_all(serde_json::to_string(message)?.as_bytes())
            .await?;
        self.writer.write_all(b"\n").await?;
        self.writer.flush().await?;
        Ok(())
    }
}

fn validate_inbound_session(message: &Value, expected_session_id: Option<&str>) -> Result<()> {
    let actual_session_id = message.pointer("/params/sessionId").and_then(Value::as_str);
    if actual_session_id != expected_session_id || expected_session_id.is_none() {
        bail!(
            "ACP message belongs to unexpected Session {:?}; expected {:?}",
            actual_session_id,
            expected_session_id
        );
    }
    Ok(())
}

async fn request_permission(
    handler: &dyn AcpPermissionHandler,
    request: &AcpPermissionRequest,
    cancellation: &mews_agent::CancellationToken,
    deadline: time::Instant,
) -> Result<AcpPermissionDecision> {
    tokio::select! {
        _ = cancellation.cancelled() => Ok(AcpPermissionDecision::Cancelled),
        _ = time::sleep_until(deadline) => bail!("ACP request timed out"),
        decision = handler.request_permission(request, cancellation) => decision,
    }
}

async fn read_acp_line<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    deadline: time::Instant,
) -> Result<Option<String>> {
    let mut encoded = Vec::new();
    let read = time::timeout_at(
        deadline,
        (&mut *reader)
            .take((MAX_ACP_LINE_BYTES + 1) as u64)
            .read_until(b'\n', &mut encoded),
    )
    .await
    .context("ACP request timed out")??;
    if read == 0 {
        return Ok(None);
    }
    if encoded.len() > MAX_ACP_LINE_BYTES || !encoded.ends_with(b"\n") {
        bail!("ACP JSON-RPC line exceeds 1 MiB");
    }
    encoded.pop();
    if encoded.last() == Some(&b'\r') {
        encoded.pop();
    }
    String::from_utf8(encoded)
        .map(Some)
        .context("ACP JSON-RPC line is not UTF-8")
}

pub(crate) fn acp_rpc_error(method: &str, error: &Value) -> anyhow::Error {
    anyhow!(AcpRpcError {
        method: method.to_owned(),
        code: error
            .get("code")
            .and_then(Value::as_i64)
            .unwrap_or_default(),
        message: error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("unknown ACP error")
            .to_owned(),
        data: error.get("data").cloned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    struct StalledPermissionHandler;

    #[async_trait]
    impl AcpPermissionHandler for StalledPermissionHandler {
        async fn request_permission(
            &self,
            _: &AcpPermissionRequest,
            _: &mews_agent::CancellationToken,
        ) -> Result<AcpPermissionDecision> {
            std::future::pending().await
        }
    }

    struct AllowPermissionHandler;

    #[async_trait]
    impl AcpPermissionHandler for AllowPermissionHandler {
        async fn request_permission(
            &self,
            _: &AcpPermissionRequest,
            _: &mews_agent::CancellationToken,
        ) -> Result<AcpPermissionDecision> {
            Ok(AcpPermissionDecision::Selected("allow".into()))
        }
    }

    fn permission_request() -> AcpPermissionRequest {
        serde_json::from_value(json!({
            "sessionId": "fixture",
            "toolCall": {},
            "options": [{
                "optionId": "allow",
                "name": "Allow once",
                "kind": "allow_once"
            }]
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn rejects_oversized_no_newline_output_at_the_ingress_bound() {
        let bytes = vec![b'x'; MAX_ACP_LINE_BYTES + 1];
        let mut reader = BufReader::new(bytes.as_slice());
        let error = read_acp_line(&mut reader, time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("exceeds 1 MiB"));
    }

    #[tokio::test]
    async fn stalled_permission_handler_respects_the_request_deadline() {
        let cancellation = mews_agent::CancellationToken::new();
        let error = time::timeout(
            Duration::from_secs(1),
            request_permission(
                &StalledPermissionHandler,
                &permission_request(),
                &cancellation,
                time::Instant::now() + Duration::from_millis(10),
            ),
        )
        .await
        .expect("permission wait must finish at the request deadline")
        .unwrap_err();

        assert!(error.to_string().contains("ACP request timed out"));
    }

    #[tokio::test]
    async fn prompt_permission_handler_completes_before_the_request_deadline() {
        let cancellation = mews_agent::CancellationToken::new();
        let decision = request_permission(
            &AllowPermissionHandler,
            &permission_request(),
            &cancellation,
            time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();

        assert_eq!(decision, AcpPermissionDecision::Selected("allow".into()));
    }
}
