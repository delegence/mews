use std::{fmt, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    time,
};

use crate::{
    mcp::{RunMcpBridge, RunMcpHttp},
    permissions::{AcpPermissionDecision, AcpPermissionHandler, AcpPermissionRequest},
};

pub(crate) struct RpcClient<'a, W> {
    writer: &'a mut W,
    reader: &'a mut tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    timeout: Duration,
    permission_handler: &'a dyn AcpPermissionHandler,
    next_id: u64,
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
    error
        .downcast_ref::<AcpRpcError>()
        .is_some_and(|error| error.code == -32002)
}

impl<'a, W: AsyncWriteExt + Unpin> RpcClient<'a, W> {
    pub(crate) fn new(
        writer: &'a mut W,
        reader: &'a mut tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
        timeout: Duration,
        permission_handler: &'a dyn AcpPermissionHandler,
    ) -> Self {
        Self {
            writer,
            reader,
            timeout,
            permission_handler,
            next_id: 1,
        }
    }

    pub(crate) async fn request<F>(
        &mut self,
        method: &str,
        params: Value,
        cancellation: &mews_agent::CancellationToken,
        mcp: Option<&RunMcpBridge<'_>>,
        mcp_http: Option<&RunMcpHttp>,
        mut on_update: F,
    ) -> Result<Value>
    where
        F: FnMut(&Value) -> Result<()>,
    {
        let id = self.next_id;
        self.next_id += 1;
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

        loop {
            let line = if let Some(http) = mcp_http {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        self.cancel_session(session_id.as_deref()).await?;
                        bail!("ACP Harness run cancelled");
                    }
                    result = http.accept_and_handle(mcp.context("MCP endpoint requires a Run bridge")?) => {
                        result?;
                        continue;
                    }
                    line = time::timeout(self.timeout, self.reader.next_line()) => line.context("ACP request timed out")??,
                }
            } else {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        self.cancel_session(session_id.as_deref()).await?;
                        bail!("ACP Harness run cancelled");
                    }
                    line = time::timeout(self.timeout, self.reader.next_line()) => line.context("ACP request timed out")??,
                }
            };
            let line = line.context("ACP Harness closed stdout before replying")?;
            let message: Value = serde_json::from_str(&line)
                .with_context(|| format!("invalid ACP JSON-RPC message: {line}"))?;
            if message.get("method").and_then(Value::as_str) == Some("session/update") {
                if let Some(update) = message
                    .get("params")
                    .and_then(|params| params.get("update"))
                {
                    on_update(update)?;
                }
                continue;
            }
            if message.get("method").is_some() && message.get("id").is_some() {
                self.handle_client_request(&message, cancellation, &mut on_update)
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

    pub(crate) async fn request_plain<F>(
        &mut self,
        method: &str,
        params: Value,
        cancellation: &mews_agent::CancellationToken,
        mut on_update: F,
    ) -> Result<Value>
    where
        F: FnMut(&Value) -> Result<()>,
    {
        let id = self.next_id;
        self.next_id += 1;
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
        loop {
            let line = tokio::select! {
                _ = cancellation.cancelled() => {
                    self.cancel_session(session_id.as_deref()).await?;
                    bail!("ACP Harness run cancelled");
                }
                line = time::timeout(self.timeout, self.reader.next_line()) => line.context("ACP request timed out")??,
            };
            let line = line.context("ACP Harness closed stdout before replying")?;
            let message: Value = serde_json::from_str(&line)
                .with_context(|| format!("invalid ACP JSON-RPC message: {line}"))?;
            if message.get("method").and_then(Value::as_str) == Some("session/update") {
                if let Some(update) = message
                    .get("params")
                    .and_then(|params| params.get("update"))
                {
                    on_update(update)?;
                }
                continue;
            }
            if message.get("method").is_some() && message.get("id").is_some() {
                self.handle_client_request(&message, cancellation, &mut on_update)
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
        cancellation: &mews_agent::CancellationToken,
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
            let params = message.get("params").cloned().unwrap_or(Value::Null);
            on_update(&json!({
                "sessionUpdate": "permission_request",
                "request": params,
            }))?;
            let request: AcpPermissionRequest = serde_json::from_value(params)
                .context("invalid ACP session/request_permission parameters")?;
            let decision = tokio::select! {
                _ = cancellation.cancelled() => AcpPermissionDecision::Cancelled,
                decision = self.permission_handler.request_permission(&request, cancellation) => decision?,
            };
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

    async fn write_message(&mut self, message: &Value) -> Result<()> {
        self.writer
            .write_all(serde_json::to_string(message)?.as_bytes())
            .await?;
        self.writer.write_all(b"\n").await?;
        self.writer.flush().await?;
        Ok(())
    }
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
