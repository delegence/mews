//! A deliberately small MCP server for one external Harness Run.
//!
//! The bridge snapshots only Host extension definitions at Run start. It never
//! manufactures filesystem or shell tools, and it delegates actual execution
//! to the Host capability boundary.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use mews_agent::{
    AgentCapabilities, CancellationToken, LifecycleHook, ProgressReporter, ToolCall, ToolCatalog,
    ToolDefinition, ToolResult,
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    time::{Instant, timeout_at},
};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcpSkill {
    pub name: String,
    pub description: String,
    pub hash: String,
    pub content: String,
}

/// Stable metadata included in every extension hook. The provider session id
/// becomes available only after session/new or session/resume succeeds.
#[derive(Clone, Debug)]
pub struct McpCorrelation {
    pub mews_session_id: String,
    pub run_id: String,
    pub harness: String,
    pub acp_session_id: std::sync::Arc<Mutex<Option<String>>>,
}

const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
const MAX_REQUEST_LINE_BYTES: usize = 8 * 1024;
const MAX_HEADER_BYTES: usize = 32 * 1024;
const MAX_BODY_BYTES: usize = 1024 * 1024;

/// A least-authority MCP capability valid only while its owning Run keeps it
/// alive. Catalog changes cannot add authority to an existing Run.
pub struct RunMcpBridge<'a> {
    environment: &'a dyn AgentCapabilities,
    cwd: PathBuf,
    cancellation: CancellationToken,
    tools: BTreeMap<String, ToolDefinition>,
    catalog: ToolCatalog,
    skills: BTreeMap<String, AcpSkill>,
    active: AtomicBool,
    next_call_id: AtomicU64,
    hook_outcomes: Mutex<Vec<McpHookOutcome>>,
    correlation: Option<McpCorrelation>,
}

#[derive(Clone, Debug)]
pub struct McpHookOutcome {
    pub hook: String,
    pub ok: bool,
    pub detail: Option<String>,
    pub tool: Option<String>,
    pub call_id: Option<String>,
}

impl<'a> RunMcpBridge<'a> {
    #[cfg(test)]
    pub fn for_extensions(
        environment: &'a dyn AgentCapabilities,
        cwd: PathBuf,
        cancellation: CancellationToken,
        allowed_tools: &[String],
    ) -> Result<Self> {
        Self::for_extensions_and_skills(environment, cwd, cancellation, allowed_tools, Vec::new())
    }

    pub fn for_extensions_and_skills(
        environment: &'a dyn AgentCapabilities,
        cwd: PathBuf,
        cancellation: CancellationToken,
        allowed_tools: &[String],
        skills: Vec<AcpSkill>,
    ) -> Result<Self> {
        let mut definitions = environment
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
            .collect::<Vec<_>>();
        let skills = skills
            .into_iter()
            .map(|skill| (skill.name.clone(), skill))
            .collect::<BTreeMap<_, _>>();
        for (name, description, schema) in [
            (
                "mews_list_skills",
                "List selected-agent skill metadata.",
                json!({"type":"object","additionalProperties":false}),
            ),
            (
                "mews_read_skill",
                "Read one selected-agent SKILL.md snapshot.",
                json!({"type":"object","properties":{"name":{"type":"string"}},"required":["name"],"additionalProperties":false}),
            ),
        ] {
            if definitions.iter().any(|tool| tool.name == name) {
                anyhow::bail!("Host extension conflicts with reserved MCP tool {name:?}");
            }
            definitions.push(ToolDefinition {
                name: name.into(),
                description: description.into(),
                schema,
            });
        }
        let catalog = ToolCatalog::compile(definitions.clone())?;
        let tools = definitions
            .into_iter()
            .map(|tool| (tool.name.clone(), tool))
            .collect();
        Ok(Self {
            environment,
            cwd,
            cancellation,
            tools,
            catalog,
            skills,
            active: AtomicBool::new(true),
            next_call_id: AtomicU64::new(1),
            hook_outcomes: Mutex::new(Vec::new()),
            correlation: None,
        })
    }

    pub fn set_correlation(&mut self, correlation: McpCorrelation) {
        self.correlation = Some(correlation);
    }

    pub fn set_acp_session_id(&self, session_id: String) {
        if let Some(correlation) = &self.correlation {
            *correlation
                .acp_session_id
                .lock()
                .expect("MCP correlation poisoned") = Some(session_id);
        }
    }

    /// Invalidates the capability before the owning Run is dropped.
    pub fn revoke(&self) {
        self.active.store(false, Ordering::SeqCst);
    }

    pub fn needs_transport(&self) -> bool {
        !self.skills.is_empty()
            || self
                .tools
                .keys()
                .any(|name| name != "mews_list_skills" && name != "mews_read_skill")
    }

    pub fn drain_hook_outcomes(&self) -> Vec<McpHookOutcome> {
        std::mem::take(
            &mut *self
                .hook_outcomes
                .lock()
                .expect("MCP hook outcomes poisoned"),
        )
    }

    fn record_hook(&self, hook: &str, ok: bool, detail: Option<String>, call: Option<&ToolCall>) {
        self.hook_outcomes
            .lock()
            .expect("MCP hook outcomes poisoned")
            .push(McpHookOutcome {
                hook: hook.into(),
                ok,
                detail: detail.map(|value| value.chars().take(1024).collect()),
                tool: call.map(|call| call.name.clone()),
                call_id: call.map(|call| call.id.clone()),
            });
    }

    fn hook_payload(&self, call: &ToolCall, extra: Value) -> Value {
        let mut payload = serde_json::Map::from_iter([
            ("tool".into(), Value::String(call.name.clone())),
            ("arguments".into(), call.arguments.clone()),
            ("call_id".into(), Value::String(call.id.clone())),
        ]);
        if let Some(correlation) = &self.correlation {
            payload.insert(
                "session_id".into(),
                Value::String(correlation.mews_session_id.clone()),
            );
            payload.insert("run_id".into(), Value::String(correlation.run_id.clone()));
            payload.insert("harness".into(), Value::String(correlation.harness.clone()));
            if let Some(session_id) = correlation
                .acp_session_id
                .lock()
                .expect("MCP correlation poisoned")
                .clone()
            {
                payload.insert("acp_session_id".into(), Value::String(session_id));
            }
        }
        if let Value::Object(extra) = extra {
            payload.extend(extra);
        }
        Value::Object(payload)
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
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| Value::Object(Default::default()));
        let mut call = ToolCall {
            id: format!("mcp-{}", self.next_call_id.fetch_add(1, Ordering::Relaxed)),
            name: name.to_owned(),
            arguments,
            thought_signature: None,
        };
        let before = self
            .environment
            .hook(
                LifecycleHook::BeforeTool,
                self.hook_payload(&call, Value::Null),
                &self.cwd,
                &self.cancellation,
            )
            .await;
        let before = match before {
            Ok(value) => value,
            Err(error) => {
                self.record_hook("before_tool", false, Some(error.to_string()), Some(&call));
                return Err(McpError::invalid(format!(
                    "before_tool hook failed: {error:#}"
                )));
            }
        };
        let before = match parse_before_tool(before) {
            Ok(before) => before,
            Err(detail) => {
                self.record_hook("before_tool", false, Some(detail.clone()), Some(&call));
                return Err(McpError::invalid(format!(
                    "invalid before_tool hook response: {detail}"
                )));
            }
        };
        if let Some(reason) = before.block {
            self.record_hook("before_tool", false, Some(reason.to_owned()), Some(&call));
            return Err(McpError::invalid(format!(
                "before_tool hook blocked MCP call: {reason}"
            )));
        }
        if let Some(tool) = before.name {
            call.name = tool;
        }
        if let Some(arguments) = before.arguments {
            call.arguments = arguments;
        }
        self.catalog.validate(&call).map_err(|error| {
            self.record_hook("before_tool", false, Some(error.to_string()), Some(&call));
            McpError::invalid(error.to_string())
        })?;
        self.record_hook("before_tool", true, None, Some(&call));
        let mut result = if call.name == "mews_list_skills" {
            let inventory = self.skills.values().map(|skill| json!({"name":skill.name,"description":skill.description,"hash":skill.hash})).collect::<Vec<_>>();
            mews_agent::ToolResult::success(Value::String(
                serde_json::to_string(&inventory).expect("skill inventory serializes"),
            ))
        } else if call.name == "mews_read_skill" {
            match call
                .arguments
                .get("name")
                .and_then(Value::as_str)
                .and_then(|name| self.skills.get(name))
            {
                Some(skill) => {
                    mews_agent::ToolResult::success(Value::String(skill.content.clone()))
                }
                None => mews_agent::ToolResult::error("selected Agent skill is unavailable"),
            }
        } else {
            let progress = NoProgress;
            match self
                .environment
                .execute(&call, &self.cwd, &self.cancellation, &progress)
                .await
            {
                Ok(result) => result,
                Err(error) => mews_agent::ToolResult::error(error),
            }
        };
        // A failed after hook cannot undo a provider-visible extension action.
        match self
            .environment
            .hook(
                LifecycleHook::AfterTool,
                self.hook_payload(
                    &call,
                    json!({"result": result.value, "is_error": result.is_error}),
                ),
                &self.cwd,
                &self.cancellation,
            )
            .await
        {
            Ok(after) => match parse_after_tool(after) {
                Ok(after) => {
                    self.record_hook("after_tool", true, None, Some(&call));
                    if let Some(value) = after.result {
                        result.value = value.clone();
                    }
                    if let Some(is_error) = after.is_error {
                        result.is_error = is_error;
                    }
                }
                Err(detail) => self.record_hook("after_tool", false, Some(detail), Some(&call)),
            },
            Err(error) => {
                self.record_hook("after_tool", false, Some(error.to_string()), Some(&call))
            }
        }
        Ok(tool_result(result))
    }

    fn is_available(&self) -> bool {
        self.active.load(Ordering::SeqCst) && !self.cancellation.is_cancelled()
    }
}

struct BeforeTool {
    block: Option<String>,
    name: Option<String>,
    arguments: Option<Value>,
}

fn hook_object(
    value: Value,
) -> std::result::Result<Option<serde_json::Map<String, Value>>, String> {
    match value {
        Value::Null => Ok(None),
        Value::Object(object) => Ok(Some(object)),
        _ => Err("hook response must be an object or null".into()),
    }
}

fn optional_string(
    object: &serde_json::Map<String, Value>,
    key: &str,
) -> std::result::Result<Option<String>, String> {
    object.get(key).map_or(Ok(None), |value| {
        value
            .as_str()
            .map(str::to_owned)
            .map(Some)
            .ok_or_else(|| format!("{key} must be a string"))
    })
}

fn parse_before_tool(value: Value) -> std::result::Result<BeforeTool, String> {
    let Some(object) = hook_object(value)? else {
        return Ok(BeforeTool {
            block: None,
            name: None,
            arguments: None,
        });
    };
    let name = optional_string(&object, "name")?.or(optional_string(&object, "tool")?);
    Ok(BeforeTool {
        block: optional_string(&object, "block")?,
        name,
        arguments: object.get("arguments").cloned(),
    })
}

struct AfterTool {
    result: Option<Value>,
    is_error: Option<bool>,
}

fn parse_after_tool(value: Value) -> std::result::Result<AfterTool, String> {
    let Some(object) = hook_object(value)? else {
        return Ok(AfterTool {
            result: None,
            is_error: None,
        });
    };
    let is_error = match object.get("is_error") {
        Some(value) => Some(
            value
                .as_bool()
                .ok_or_else(|| "is_error must be a boolean".to_owned())?,
        ),
        None => None,
    };
    Ok(AfterTool {
        result: object.get("result").cloned(),
        is_error,
    })
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

    pub async fn accept_and_handle(
        &self,
        bridge: &RunMcpBridge<'_>,
        deadline: Instant,
    ) -> Result<()> {
        let (stream, _) = timeout_at(deadline, self.listener.accept())
            .await
            .context("MCP request timed out")?
            .context("accept MCP connection")?;
        timeout_at(deadline, handle_http_connection(stream, &self.path, bridge))
            .await
            .context("MCP request timed out")?
    }
}

async fn read_bounded_line<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    limit: usize,
) -> Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    let read = (&mut *reader)
        .take((limit + 1) as u64)
        .read_until(b'\n', &mut line)
        .await?;
    if read == 0 {
        return Ok(None);
    }
    if line.len() > limit || !line.ends_with(b"\n") {
        anyhow::bail!("MCP HTTP line exceeds its byte limit");
    }
    Ok(Some(line))
}

async fn handle_http_connection(
    stream: TcpStream,
    expected_path: &str,
    bridge: &RunMcpBridge<'_>,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let request_line = read_bounded_line(&mut reader, MAX_REQUEST_LINE_BYTES)
        .await
        .context("read MCP HTTP request line")?
        .context("MCP HTTP request is empty")?;
    let request_line =
        std::str::from_utf8(&request_line).context("MCP request line is not UTF-8")?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();
    let mut content_length = 0usize;
    let mut header_bytes = 0usize;
    let mut saw_content_length = false;
    loop {
        let remaining = MAX_HEADER_BYTES.saturating_sub(header_bytes);
        let header = read_bounded_line(&mut reader, remaining)
            .await?
            .context("MCP HTTP headers ended before the blank line")?;
        header_bytes += header.len();
        if header == b"\r\n" || header == b"\n" {
            break;
        }
        let header = std::str::from_utf8(&header).context("MCP header is not UTF-8")?;
        if let Some(value) = header
            .strip_prefix("Content-Length:")
            .or_else(|| header.strip_prefix("content-length:"))
        {
            if saw_content_length {
                anyhow::bail!("duplicate MCP Content-Length header");
            }
            saw_content_length = true;
            content_length = value.trim().parse().context("parse MCP content length")?;
        }
    }
    if method != "POST" || path != expected_path {
        return write_http_response(&mut writer, 404, Value::Null).await;
    }
    if content_length > MAX_BODY_BYTES {
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
        delay: std::time::Duration,
    }

    #[async_trait]
    impl AgentCapabilities for Capabilities {
        async fn context(&self, _: &str, _: &Path) -> Result<ContextSnapshot> {
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
            tokio::time::sleep(self.delay).await;
            self.calls.lock().unwrap().push(call.clone());
            Ok(ToolResult::success(json!({"called": call.name})))
        }
        async fn hook(
            &self,
            _: LifecycleHook,
            _: Value,
            _: &Path,
            _: &CancellationToken,
        ) -> Result<Value> {
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
        .unwrap()
    }

    #[tokio::test]
    async fn exposes_only_allowed_extensions_and_routes_calls_through_capabilities() {
        let capabilities = Capabilities {
            calls: Mutex::new(Vec::new()),
            delay: std::time::Duration::ZERO,
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
            delay: std::time::Duration::ZERO,
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
        let (served, response) = tokio::join!(
            endpoint
                .accept_and_handle(&bridge, Instant::now() + std::time::Duration::from_secs(2),),
            client
        );
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
            delay: std::time::Duration::ZERO,
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

    #[tokio::test]
    async fn validates_extension_arguments_against_the_advertised_schema() {
        struct StrictCapabilities;
        #[async_trait]
        impl AgentCapabilities for StrictCapabilities {
            async fn context(&self, _: &str, _: &Path) -> Result<ContextSnapshot> {
                Ok(ContextSnapshot::default())
            }
            fn tools(&self) -> Vec<ToolDefinition> {
                Vec::new()
            }
            fn extension_tools(&self) -> Vec<ToolDefinition> {
                vec![ToolDefinition {
                    name: "lookup".into(),
                    description: "Lookup".into(),
                    schema: json!({
                        "type":"object",
                        "properties":{"id":{"type":"string"}},
                        "required":["id"],
                        "additionalProperties":false
                    }),
                }]
            }
            async fn execute(
                &self,
                _: &ToolCall,
                _: &Path,
                _: &CancellationToken,
                _: &dyn ProgressReporter,
            ) -> Result<ToolResult> {
                panic!("invalid arguments must not cross the capability boundary")
            }
            async fn hook(
                &self,
                _: LifecycleHook,
                _: Value,
                _: &Path,
                _: &CancellationToken,
            ) -> Result<Value> {
                Ok(Value::Null)
            }
        }
        let bridge = RunMcpBridge::for_extensions(
            &StrictCapabilities,
            PathBuf::from("/tmp"),
            CancellationToken::new(),
            &["lookup".into()],
        )
        .unwrap();

        let response = bridge
            .handle(json!({
                "jsonrpc":"2.0", "id":1, "method":"tools/call",
                "params":{"name":"lookup", "arguments":{"id":7}}
            }))
            .await
            .unwrap();
        assert_eq!(response["error"]["code"], -32602);
    }

    #[tokio::test]
    async fn skill_tools_return_one_direct_mcp_content_envelope() {
        let capabilities = Capabilities {
            calls: Mutex::new(Vec::new()),
            delay: std::time::Duration::ZERO,
        };
        let bridge = RunMcpBridge::for_extensions_and_skills(
            &capabilities,
            PathBuf::from("/tmp"),
            CancellationToken::new(),
            &[],
            vec![AcpSkill {
                name: "review".into(),
                description: "Review code".into(),
                hash: "a".repeat(64),
                content: "---\nname: review\n---\nbody".into(),
            }],
        )
        .unwrap();
        let listed = bridge.handle(json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"mews_list_skills","arguments":{}}})).await.unwrap();
        let inventory: Value =
            serde_json::from_str(listed["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(inventory[0]["name"], "review");
        let read = bridge.handle(json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"mews_read_skill","arguments":{"name":"review"}}})).await.unwrap();
        assert_eq!(
            read["result"]["content"][0]["text"],
            "---\nname: review\n---\nbody"
        );
        let invalid = bridge.handle(json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"mews_read_skill","arguments":{}}})).await.unwrap();
        assert_eq!(invalid["error"]["code"], -32602);
    }

    #[tokio::test]
    async fn empty_skills_need_no_transport_but_remain_listable_with_extensions() {
        let capabilities = Capabilities {
            calls: Mutex::new(Vec::new()),
            delay: std::time::Duration::ZERO,
        };
        let empty = RunMcpBridge::for_extensions_and_skills(
            &capabilities,
            PathBuf::from("/tmp"),
            CancellationToken::new(),
            &[],
            Vec::new(),
        )
        .unwrap();
        assert!(!empty.needs_transport());

        let with_extension = RunMcpBridge::for_extensions_and_skills(
            &capabilities,
            PathBuf::from("/tmp"),
            CancellationToken::new(),
            &["issue_lookup".into()],
            Vec::new(),
        )
        .unwrap();
        assert!(with_extension.needs_transport());
        let listed = with_extension
            .handle(json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"mews_list_skills","arguments":{}}}))
            .await
            .unwrap();
        assert_eq!(listed["result"]["content"][0]["text"], "[]");
    }

    #[tokio::test]
    async fn rejects_oversized_http_lines_before_unbounded_allocation() {
        for limit in [MAX_REQUEST_LINE_BYTES, MAX_HEADER_BYTES] {
            let mut line = vec![b'x'; limit + 1];
            line.push(b'\n');
            let mut reader = BufReader::new(line.as_slice());
            assert!(
                read_bounded_line(&mut reader, limit)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("byte limit")
            );
        }
    }

    #[tokio::test]
    async fn mcp_deadline_covers_tool_execution() {
        let capabilities = Capabilities {
            calls: Mutex::new(Vec::new()),
            delay: std::time::Duration::from_secs(30),
        };
        let bridge = bridge(&capabilities);
        let endpoint = bridge.bind_http().await.unwrap();
        let address = endpoint.listener.local_addr().unwrap();
        let body = serde_json::to_vec(&json!({
            "jsonrpc":"2.0", "id":1, "method":"tools/call",
            "params":{"name":"issue_lookup", "arguments":{}}
        }))
        .unwrap();
        let client = async {
            let mut stream = TcpStream::connect(address).await.unwrap();
            let request = format!(
                "POST {} HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
                endpoint.path,
                body.len()
            );
            stream.write_all(request.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
        };
        let (served, ()) = tokio::join!(
            endpoint.accept_and_handle(
                &bridge,
                Instant::now() + std::time::Duration::from_millis(50),
            ),
            client
        );
        assert!(served.unwrap_err().to_string().contains("timed out"));
        assert!(capabilities.calls.lock().unwrap().is_empty());
    }
}
