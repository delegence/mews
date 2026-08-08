//! A deliberately small MCP server for one external Harness Run.
//!
//! The bridge snapshots only Host extension definitions at Run start. It never
//! manufactures filesystem or shell tools, and it delegates actual execution
//! to the Host capability boundary.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use mews_agent::{
    AgentCapabilities, CancellationToken, ProgressReporter, ToolCall, ToolDefinition, ToolResult,
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use uuid::Uuid;

const MCP_PROTOCOL_VERSION: &str = "2024-11-05";

/// A least-authority MCP capability valid only while its owning Run keeps it
/// alive. Catalog changes cannot add authority to an existing Run.
pub struct RunMcpBridge<'a> {
    environment: &'a dyn AgentCapabilities,
    cwd: PathBuf,
    cancellation: CancellationToken,
    tools: BTreeMap<String, ToolDefinition>,
    active: AtomicBool,
    next_call_id: AtomicU64,
}

impl<'a> RunMcpBridge<'a> {
    pub fn for_extensions(
        environment: &'a dyn AgentCapabilities,
        cwd: PathBuf,
        cancellation: CancellationToken,
        allowed_tools: &[String],
    ) -> Self {
        let tools = environment
            .extension_tools()
            .into_iter()
            // Defense in depth: native MEWS tools cannot be exposed over MCP
            // even if a Host implementation incorrectly includes one here.
            .filter(|tool| !is_native_mews_tool(&tool.name))
            .filter(|tool| {
                allowed_tools
                    .iter()
                    .any(|pattern| tool_allowed(pattern, &tool.name))
            })
            .map(|tool| (tool.name.clone(), tool))
            .collect();
        Self {
            environment,
            cwd,
            cancellation,
            tools,
            active: AtomicBool::new(true),
            next_call_id: AtomicU64::new(1),
        }
    }

    /// Invalidates the capability before the owning Run is dropped.
    pub fn revoke(&self) {
        self.active.store(false, Ordering::SeqCst);
    }

    pub fn tool_definitions(&self) -> Vec<ToolDefinition> {
        self.tools.values().cloned().collect()
    }

    /// Creates a local, unguessable HTTP endpoint for this Run. Both managed
    /// ACP recipes support this MCP transport; the endpoint is never written
    /// to a profile or reused by another Run.
    pub async fn bind_http(&self) -> Result<RunMcpHttp> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .context("bind MEWS run-scoped MCP endpoint")?;
        Ok(RunMcpHttp {
            listener,
            path: format!("/mcp/{}", Uuid::now_v7()),
        })
    }

    /// Handles one MCP JSON-RPC request. Notifications have no response.
    pub async fn handle(&self, request: Value) -> Option<Value> {
        let id = request.get("id").cloned();
        match self.handle_inner(&request).await {
            Ok(result) => id.map(|id| json!({"jsonrpc":"2.0", "id": id, "result": result})),
            Err(error) => id.map(|id| {
                json!({
                    "jsonrpc": "2.0", "id": id,
                    "error": {"code": error.code, "message": error.message},
                })
            }),
        }
    }

    async fn handle_inner(&self, request: &Value) -> std::result::Result<Value, McpError> {
        if !self.is_available() {
            return Err(McpError::unavailable());
        }
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .ok_or_else(|| McpError::invalid("MCP request method is required"))?;
        match method {
            "initialize" => Ok(json!({
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {"tools": {"listChanged": false}},
                "serverInfo": {"name": "mews-run-extensions", "version": env!("CARGO_PKG_VERSION")},
            })),
            "notifications/initialized" => Ok(Value::Null),
            "tools/list" => Ok(json!({
                "tools": self.tools.values().map(|tool| json!({
                    "name": tool.name,
                    "description": tool.description,
                    "inputSchema": tool.schema,
                })).collect::<Vec<_>>(),
            })),
            "tools/call" => {
                self.call(request.get("params").unwrap_or(&Value::Null))
                    .await
            }
            _ => Err(McpError {
                code: -32601,
                message: format!("MCP method {method:?} is not supported"),
            }),
        }
    }

    async fn call(&self, params: &Value) -> std::result::Result<Value, McpError> {
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| McpError::invalid("tools/call requires a tool name"))?;
        if !self.tools.contains_key(name) {
            return Err(McpError::invalid(format!(
                "MCP tool {name:?} is unavailable or not allowed for this Run"
            )));
        }
        let call = ToolCall {
            id: format!("mcp-{}", self.next_call_id.fetch_add(1, Ordering::Relaxed)),
            name: name.to_owned(),
            arguments: params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| Value::Object(Default::default())),
            thought_signature: None,
        };
        let progress = NoProgress;
        let result = self
            .environment
            .execute(&call, &self.cwd, &self.cancellation, &progress)
            .await
            .map_err(|error| McpError {
                code: -32000,
                message: error.to_string(),
            })?;
        Ok(tool_result(result))
    }

    fn is_available(&self) -> bool {
        self.active.load(Ordering::SeqCst) && !self.cancellation.is_cancelled()
    }
}

fn tool_allowed(pattern: &str, name: &str) -> bool {
    pattern == "*"
        || pattern == name
        || pattern
            .strip_suffix('*')
            .is_some_and(|prefix| name.starts_with(prefix))
}

/// A minimal Streamable HTTP MCP transport. It deliberately supports only
/// request/response JSON-RPC because MEWS extension calls do not need server
/// initiated streaming. The listener and its capability path die with a Run.
pub struct RunMcpHttp {
    listener: TcpListener,
    path: String,
}

impl RunMcpHttp {
    pub fn url(&self) -> String {
        format!(
            "http://{}{}",
            self.listener.local_addr().expect("bound listener"),
            self.path
        )
    }

    pub async fn accept_and_handle(&self, bridge: &RunMcpBridge<'_>) -> Result<()> {
        let (stream, _) = self
            .listener
            .accept()
            .await
            .context("accept MCP connection")?;
        handle_http_connection(stream, &self.path, bridge).await
    }
}

async fn handle_http_connection(
    stream: TcpStream,
    expected_path: &str,
    bridge: &RunMcpBridge<'_>,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut request_line = String::new();
    reader
        .read_line(&mut request_line)
        .await
        .context("read MCP HTTP request line")?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();
    let mut content_length = 0usize;
    loop {
        let mut header = String::new();
        reader.read_line(&mut header).await?;
        if header == "\r\n" || header.is_empty() {
            break;
        }
        if let Some(value) = header
            .strip_prefix("Content-Length:")
            .or_else(|| header.strip_prefix("content-length:"))
        {
            content_length = value.trim().parse().context("parse MCP content length")?;
        }
    }
    if method != "POST" || path != expected_path {
        return write_http_response(&mut writer, 404, Value::Null).await;
    }
    if content_length > 1024 * 1024 {
        return write_http_response(&mut writer, 413, Value::Null).await;
    }
    let mut body = vec![0; content_length];
    reader
        .read_exact(&mut body)
        .await
        .context("read MCP HTTP body")?;
    let request = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return write_http_response(&mut writer, 400, Value::Null).await,
    };
    let response = bridge.handle(request).await.unwrap_or(Value::Null);
    write_http_response(&mut writer, 200, response).await
}

async fn write_http_response<W: AsyncWrite + Unpin>(
    writer: &mut W,
    status: u16,
    response: Value,
) -> Result<()> {
    let body = serde_json::to_vec(&response)?;
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        413 => "Payload Too Large",
        _ => "Error",
    };
    writer
        .write_all(
            format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nMcp-Session-Id: mews-run\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .await?;
    writer.write_all(&body).await?;
    writer.flush().await?;
    Ok(())
}

fn is_native_mews_tool(name: &str) -> bool {
    matches!(name, "read" | "write" | "edit" | "bash")
}

fn tool_result(result: ToolResult) -> Value {
    let text = match &result.value {
        Value::String(value) => value.clone(),
        value => value.to_string(),
    };
    json!({"content": [{"type": "text", "text": text}], "isError": result.is_error})
}

struct NoProgress;

#[async_trait(?Send)]
impl ProgressReporter for NoProgress {
    async fn report(&self, _: Value) -> Result<()> {
        Ok(())
    }
}

struct McpError {
    code: i64,
    message: String,
}

impl McpError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
        }
    }

    fn unavailable() -> Self {
        Self {
            code: -32000,
            message: "MCP capability is no longer available for this Run".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Mutex};

    use anyhow::{Result, bail};
    use async_trait::async_trait;
    use mews_agent::{ContextSnapshot, LifecycleHook};
    use serde_json::json;

    use super::*;

    struct Capabilities {
        calls: Mutex<Vec<ToolCall>>,
    }

    #[async_trait(?Send)]
    impl AgentCapabilities for Capabilities {
        async fn context(&self, _: &Path) -> Result<ContextSnapshot> {
            Ok(ContextSnapshot::default())
        }
        fn tools(&self) -> Vec<ToolDefinition> {
            Vec::new()
        }
        fn extension_tools(&self) -> Vec<ToolDefinition> {
            ["issue_lookup", "deploy_preview", "read"]
                .into_iter()
                .map(|name| ToolDefinition {
                    name: name.into(),
                    description: format!("{name} description"),
                    schema: json!({"type":"object"}),
                })
                .collect()
        }
        async fn execute(
            &self,
            call: &ToolCall,
            _: &Path,
            _: &CancellationToken,
            _: &dyn ProgressReporter,
        ) -> Result<ToolResult> {
            if call.name == "read" {
                bail!("native tool must not be called")
            }
            self.calls.lock().unwrap().push(call.clone());
            Ok(ToolResult::success(json!({"called": call.name})))
        }
        async fn hook(&self, _: LifecycleHook, _: Value, _: &Path) -> Result<Value> {
            Ok(Value::Null)
        }
    }

    fn bridge(capabilities: &Capabilities) -> RunMcpBridge<'_> {
        RunMcpBridge::for_extensions(
            capabilities,
            Path::new("/tmp").to_owned(),
            CancellationToken::new(),
            &["issue_*".into()],
        )
    }

    #[tokio::test]
    async fn exposes_only_allowed_extensions_and_routes_calls_through_capabilities() {
        let capabilities = Capabilities {
            calls: Mutex::new(Vec::new()),
        };
        let bridge = bridge(&capabilities);
        let listed = bridge
            .handle(json!({"jsonrpc":"2.0", "id":1, "method":"tools/list"}))
            .await
            .unwrap();
        assert_eq!(listed["result"]["tools"][0]["name"], "issue_lookup");
        let called = bridge.handle(json!({"jsonrpc":"2.0", "id":2, "method":"tools/call", "params":{"name":"issue_lookup", "arguments":{"id":"MEWS-1"}}})).await.unwrap();
        assert_eq!(called["result"]["isError"], false);
        assert_eq!(capabilities.calls.lock().unwrap()[0].name, "issue_lookup");
    }

    #[tokio::test]
    async fn http_transport_keeps_the_run_scoped_extension_boundary() {
        let capabilities = Capabilities {
            calls: Mutex::new(Vec::new()),
        };
        let bridge = bridge(&capabilities);
        let endpoint = bridge.bind_http().await.unwrap();
        let address = endpoint.listener.local_addr().unwrap();
        let body = serde_json::to_vec(&json!({
            "jsonrpc": "2.0", "id": 7, "method": "tools/call",
            "params": {"name": "issue_lookup", "arguments": {"id": "MEWS-7"}},
        }))
        .unwrap();
        let client = async {
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream
                .write_all(
                    format!(
                        "POST {} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
                        endpoint.path,
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            stream.write_all(&body).await.unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            response
        };
        let (served, response) = tokio::join!(endpoint.accept_and_handle(&bridge), client);
        served.unwrap();
        let response = String::from_utf8(response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("issue_lookup"));
        assert_eq!(capabilities.calls.lock().unwrap()[0].name, "issue_lookup");
    }

    #[tokio::test]
    async fn rejects_disallowed_unavailable_and_native_tools() {
        let capabilities = Capabilities {
            calls: Mutex::new(Vec::new()),
        };
        let bridge = bridge(&capabilities);
        for name in ["deploy_preview", "missing", "read"] {
            let response = bridge
                .handle(
                    json!({"jsonrpc":"2.0", "id":1, "method":"tools/call", "params":{"name":name}}),
                )
                .await
                .unwrap();
            assert_eq!(response["error"]["code"], -32602, "{name}");
        }
        assert!(capabilities.calls.lock().unwrap().is_empty());
        bridge.revoke();
        let response = bridge
            .handle(json!({"jsonrpc":"2.0", "id":1, "method":"tools/list"}))
            .await
            .unwrap();
        assert_eq!(response["error"]["code"], -32000);
    }
}
