//! A deliberately small MCP server for one external Harness Turn.
//!
//! The bridge snapshots only Host extension definitions at Turn start. It never
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
use base64::Engine;
use mews_agent::{
    AgentCapabilities, CancellationToken, LifecycleHook, ProgressReporter, ToolCall, ToolCatalog,
    ToolDefinition, ToolResult, UNCERTAIN_EFFECT_INSTRUCTION, effect_uncertainty, tool_allowed,
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    time::{Instant, timeout_at},
};
use uuid::Uuid;

const STATELESS_MCP_VERSION: &str = "2026-07-28";
const MAX_REQUEST_LINE_BYTES: usize = 8 * 1024;
const MAX_HEADER_BYTES: usize = 32 * 1024;
const MAX_BODY_BYTES: usize = 1024 * 1024;
const TOOL_LIST_TTL_MS: u64 = 60_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum McpProtocol {
    Legacy(LegacyMcpVersion),
    Stateless,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LegacyMcpVersion {
    V2024_11_05,
    V2025_03_26,
    V2025_06_18,
    V2025_11_25,
}

impl LegacyMcpVersion {
    const LATEST: Self = Self::V2025_11_25;
    const ALL: [Self; 4] = [
        Self::V2024_11_05,
        Self::V2025_03_26,
        Self::V2025_06_18,
        Self::V2025_11_25,
    ];

    fn parse(value: &str) -> Option<Self> {
        match value {
            "2024-11-05" => Some(Self::V2024_11_05),
            "2025-03-26" => Some(Self::V2025_03_26),
            "2025-06-18" => Some(Self::V2025_06_18),
            "2025-11-25" => Some(Self::V2025_11_25),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::V2024_11_05 => "2024-11-05",
            Self::V2025_03_26 => "2025-03-26",
            Self::V2025_06_18 => "2025-06-18",
            Self::V2025_11_25 => "2025-11-25",
        }
    }
}

fn supported_versions() -> Vec<&'static str> {
    std::iter::once(STATELESS_MCP_VERSION)
        .chain(LegacyMcpVersion::ALL.into_iter().rev().map(|v| v.as_str()))
        .collect()
}

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
    pub turn_id: String,
    pub harness: String,
    pub acp_session_id: std::sync::Arc<Mutex<Option<String>>>,
}

/// A least-authority MCP capability valid only while its owning Turn keeps it
/// alive. Catalog changes cannot add authority to an existing Turn.
pub struct TurnMcpBridge<'a> {
    environment: &'a dyn AgentCapabilities,
    agent_id: mews_protocol::AgentId,
    cwd: PathBuf,
    cancellation: CancellationToken,
    tools: BTreeMap<String, ToolDefinition>,
    catalog: ToolCatalog,
    skills: BTreeMap<String, AcpSkill>,
    active: AtomicBool,
    next_call_id: AtomicU64,
    hook_outcomes: Mutex<Vec<McpHookOutcome>>,
    uncertain_effect: Mutex<Option<String>>,
    correlation: Option<McpCorrelation>,
    legacy_version: Mutex<Option<LegacyMcpVersion>>,
}

#[derive(Clone, Debug)]
pub struct McpHookOutcome {
    pub hook: String,
    pub ok: bool,
    pub detail: Option<String>,
    pub tool: Option<String>,
    pub call_id: Option<String>,
}

impl<'a> TurnMcpBridge<'a> {
    #[cfg(test)]
    pub fn for_extensions(
        environment: &'a dyn AgentCapabilities,
        cwd: PathBuf,
        cancellation: CancellationToken,
        allowed_tools: &[String],
    ) -> Result<Self> {
        Self::for_extensions_and_skills(
            environment,
            &mews_protocol::AgentId::new(),
            cwd,
            cancellation,
            allowed_tools,
            Vec::new(),
        )
    }

    pub fn for_extensions_and_skills(
        environment: &'a dyn AgentCapabilities,
        agent_id: &mews_protocol::AgentId,
        cwd: PathBuf,
        cancellation: CancellationToken,
        allowed_tools: &[String],
        skills: Vec<AcpSkill>,
    ) -> Result<Self> {
        let snapshot = environment.extension_tools(agent_id);
        let generation = snapshot.generation;
        let mut definitions = snapshot
            .tools
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
        if definitions
            .iter()
            .any(|tool| mews_protocol::is_reserved_acp_skill_tool(&tool.name))
        {
            anyhow::bail!("Host extension conflicts with a reserved MCP skill tool");
        }
        if !skills.is_empty() {
            for ((name, description), schema) in mews_protocol::ACP_SKILL_TOOL_NAMES
                .into_iter()
                .zip([
                    "List selected-agent skill metadata.",
                    "Read one selected-agent SKILL.md snapshot.",
                ])
                .zip([
                    json!({"type":"object","additionalProperties":false}),
                    json!({"type":"object","properties":{"name":{"type":"string"}},"required":["name"],"additionalProperties":false}),
                ])
            {
                definitions.push(ToolDefinition {
                    name: name.into(),
                    description: description.into(),
                    schema,
                    agent_id: None,
                });
            }
        }
        let catalog = ToolCatalog::compile(mews_protocol::ToolCatalogSnapshot {
            generation,
            tools: definitions.clone(),
        })?;
        let tools = definitions
            .into_iter()
            .map(|tool| (tool.name.clone(), tool))
            .collect();
        Ok(Self {
            environment,
            agent_id: agent_id.clone(),
            cwd,
            cancellation,
            tools,
            catalog,
            skills,
            active: AtomicBool::new(true),
            next_call_id: AtomicU64::new(1),
            hook_outcomes: Mutex::new(Vec::new()),
            uncertain_effect: Mutex::new(None),
            correlation: None,
            legacy_version: Mutex::new(None),
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

    /// Invalidates the capability before the owning Turn is dropped.
    pub fn revoke(&self) {
        self.active.store(false, Ordering::SeqCst);
    }

    pub fn needs_transport(&self) -> bool {
        !self.skills.is_empty()
            || self
                .tools
                .keys()
                .any(|name| !mews_protocol::is_reserved_acp_skill_tool(name))
    }

    pub fn drain_hook_outcomes(&self) -> Vec<McpHookOutcome> {
        std::mem::take(
            &mut *self
                .hook_outcomes
                .lock()
                .expect("MCP hook outcomes poisoned"),
        )
    }

    pub fn take_effect_uncertainty(&self) -> Option<String> {
        self.uncertain_effect
            .lock()
            .expect("MCP effect uncertainty poisoned")
            .take()
    }

    fn preserve_effect_uncertainty(&self, error: &anyhow::Error) {
        if let Some(uncertain) = effect_uncertainty(error) {
            *self
                .uncertain_effect
                .lock()
                .expect("MCP effect uncertainty poisoned") = Some(uncertain.reason().to_owned());
        }
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
            payload.insert("turn_id".into(), Value::String(correlation.turn_id.clone()));
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

    /// Creates a local, unguessable HTTP endpoint for this Turn. Both managed
    /// ACP recipes support this MCP transport; the endpoint is never written
    /// to a profile or reused by another Turn.
    pub async fn bind_http(&self) -> Result<TurnMcpHttp> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .context("bind MEWS Turn-scoped MCP endpoint")?;
        Ok(TurnMcpHttp {
            listener,
            path: format!("/mcp/{}", Uuid::now_v7()),
        })
    }

    /// Handles one MCP JSON-RPC request. Notifications have no response.
    #[cfg(test)]
    pub async fn handle(&self, request: Value) -> Option<Value> {
        let protocol = self.protocol_for_request(&request);
        self.handle_for(protocol, request).await
    }

    async fn handle_for(&self, protocol: McpProtocol, request: Value) -> Option<Value> {
        let id = request.get("id").cloned();
        match self.handle_inner(protocol, &request).await {
            Ok(result) => id.map(|id| json!({"jsonrpc":"2.0", "id": id, "result": result})),
            Err(error) => id.map(|id| {
                let mut response = json!({
                    "jsonrpc": "2.0", "id": id,
                    "error": {"code": error.code, "message": error.message},
                });
                if let Some(data) = error.data {
                    response["error"]["data"] = data;
                }
                response
            }),
        }
    }

    fn protocol_for_request(&self, request: &Value) -> McpProtocol {
        if request
            .pointer("/params/_meta/io.modelcontextprotocol~1protocolVersion")
            .and_then(Value::as_str)
            == Some(STATELESS_MCP_VERSION)
        {
            return McpProtocol::Stateless;
        }
        let negotiated = *self
            .legacy_version
            .lock()
            .expect("MCP legacy version poisoned");
        McpProtocol::Legacy(negotiated.unwrap_or(LegacyMcpVersion::LATEST))
    }

    async fn handle_inner(
        &self,
        protocol: McpProtocol,
        request: &Value,
    ) -> std::result::Result<Value, McpError> {
        if !self.is_available() {
            return Err(McpError::unavailable());
        }
        let method = request
            .get("method")
            .and_then(Value::as_str)
            .ok_or_else(|| McpError::invalid("MCP request method is required"))?;
        match method {
            "initialize" => self.initialize(request),
            "notifications/initialized" => Ok(Value::Null),
            "server/discover" if protocol == McpProtocol::Stateless => Ok(self.complete_result(
                protocol,
                json!({
                    "supportedVersions": supported_versions(),
                    "capabilities": {"tools": {"listChanged": false}},
                    "ttlMs": TOOL_LIST_TTL_MS,
                    "cacheScope": "private",
                }),
            )),
            "tools/list" => Ok(self.complete_result(
                protocol,
                json!({
                    "tools": self.tools.values().map(|tool| json!({
                        "name": tool.name,
                        "description": tool.description,
                        "inputSchema": tool.schema,
                    })).collect::<Vec<_>>(),
                    "ttlMs": TOOL_LIST_TTL_MS,
                    "cacheScope": "private",
                }),
            )),
            "tools/call" => {
                let result = self
                    .call(request.get("params").unwrap_or(&Value::Null))
                    .await?;
                Ok(self.complete_result(protocol, result))
            }
            _ => Err(McpError {
                code: -32601,
                message: format!("MCP method {method:?} is not supported"),
                data: None,
            }),
        }
    }

    fn initialize(&self, request: &Value) -> std::result::Result<Value, McpError> {
        let requested = request
            .pointer("/params/protocolVersion")
            .and_then(Value::as_str)
            .ok_or_else(|| McpError::invalid("initialize requires a protocol version"))?;
        let version = LegacyMcpVersion::parse(requested).unwrap_or(LegacyMcpVersion::LATEST);
        *self
            .legacy_version
            .lock()
            .expect("MCP legacy version poisoned") = Some(version);
        Ok(json!({
            "protocolVersion": version.as_str(),
            "capabilities": {"tools": {"listChanged": false}},
            "serverInfo": server_info(),
        }))
    }

    fn complete_result(&self, protocol: McpProtocol, mut result: Value) -> Value {
        if protocol == McpProtocol::Stateless {
            let object = result
                .as_object_mut()
                .expect("MCP operation results are objects");
            object.insert("resultType".into(), Value::String("complete".into()));
            object.insert(
                "_meta".into(),
                json!({"io.modelcontextprotocol/serverInfo": server_info()}),
            );
        } else if let Some(object) = result.as_object_mut() {
            object.remove("ttlMs");
            object.remove("cacheScope");
        }
        result
    }

    async fn call(&self, params: &Value) -> std::result::Result<Value, McpError> {
        let name = params
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| McpError::invalid("tools/call requires a tool name"))?;
        if !self.tools.contains_key(name) {
            return Err(McpError::invalid(format!(
                "MCP tool {name:?} is unavailable or not allowed for this Turn"
            )));
        }
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| Value::Object(Default::default()));
        let call = ToolCall {
            id: format!("mcp-{}", self.next_call_id.fetch_add(1, Ordering::Relaxed)),
            name: name.to_owned(),
            arguments,
            thought_signature: None,
            catalog_generation: self.catalog.generation(),
        };
        if let Err(error) = self.catalog.validate(&call) {
            self.record_hook("before_tool", false, Some(error.to_string()), Some(&call));
            return Ok(tool_result(ToolResult::error(error)));
        }
        let before = self
            .environment
            .hook(
                &self.agent_id,
                LifecycleHook::BeforeTool,
                self.hook_payload(&call, Value::Null),
                &self.cwd,
                &self.cancellation,
                Some(call.catalog_generation),
            )
            .await;
        let before = match before {
            Ok(value) => value,
            Err(error) => {
                self.preserve_effect_uncertainty(&error);
                self.record_hook("before_tool", false, Some(error.to_string()), Some(&call));
                return Ok(tool_result(ToolResult::error(format!(
                    "before_tool hook failed: {error:#}"
                ))));
            }
        };
        let before = match mews_agent::before_tool_decision(&call, before) {
            Ok(before) => before,
            Err(detail) => {
                self.record_hook("before_tool", false, Some(detail.to_string()), Some(&call));
                return Ok(tool_result(ToolResult::error(format!(
                    "invalid before_tool hook response: {detail}"
                ))));
            }
        };
        if let mews_agent::ToolDecision::Block(reason) = before {
            self.record_hook("before_tool", false, Some(reason.clone()), Some(&call));
            return Ok(tool_result(ToolResult::error(reason)));
        }
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
                .execute(
                    &self.agent_id,
                    &call,
                    &self.cwd,
                    &self.cancellation,
                    &progress,
                )
                .await
            {
                Ok(result) => result,
                Err(error) => match effect_uncertainty(&error) {
                    Some(uncertain) => {
                        let reason = uncertain.reason().to_owned();
                        *self
                            .uncertain_effect
                            .lock()
                            .expect("MCP effect uncertainty poisoned") = Some(reason.clone());
                        mews_agent::ToolResult::uncertain(reason)
                    }
                    None => mews_agent::ToolResult::error(error),
                },
            }
        };
        // A failed after hook cannot undo a provider-visible extension action.
        match self
            .environment
            .hook(
                &self.agent_id,
                LifecycleHook::AfterTool,
                self.hook_payload(
                    &call,
                    json!({"result": result.value, "is_error": result.is_error}),
                ),
                &self.cwd,
                &self.cancellation,
                Some(call.catalog_generation),
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
                self.preserve_effect_uncertainty(&error);
                self.record_hook("after_tool", false, Some(error.to_string()), Some(&call))
            }
        }
        Ok(tool_result(result))
    }

    fn is_available(&self) -> bool {
        self.active.load(Ordering::SeqCst) && !self.cancellation.is_cancelled()
    }
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

/// A minimal Streamable HTTP MCP transport. It deliberately supports only
/// request/response JSON-RPC because MEWS extension calls do not need server
/// initiated streaming. The listener and its capability path die with a Turn.
pub struct TurnMcpHttp {
    listener: TcpListener,
    path: String,
}

impl TurnMcpHttp {
    pub fn url(&self) -> String {
        format!(
            "http://{}{}",
            self.listener.local_addr().expect("bound listener"),
            self.path
        )
    }

    pub async fn accept_and_handle(
        &self,
        bridge: &TurnMcpBridge<'_>,
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
    bridge: &TurnMcpBridge<'_>,
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
    let mut headers = BTreeMap::new();
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
        let Some((name, value)) = header.split_once(':') else {
            return write_http_response(&mut writer, 400, None).await;
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim().to_owned();
        if name == "content-length" {
            if saw_content_length {
                anyhow::bail!("duplicate MCP Content-Length header");
            }
            saw_content_length = true;
            content_length = value.parse().context("parse MCP content length")?;
        }
        if headers.insert(name, value).is_some() {
            return write_http_response(&mut writer, 400, None).await;
        }
    }
    if path != expected_path {
        return write_http_response(&mut writer, 404, None).await;
    }
    if method != "POST" {
        return write_http_response(&mut writer, 405, None).await;
    }
    if headers
        .get("origin")
        .is_some_and(|origin| !allowed_origin(origin))
    {
        return write_http_response(&mut writer, 403, None).await;
    }
    if content_length > MAX_BODY_BYTES {
        return write_http_response(&mut writer, 413, None).await;
    }
    let mut body = vec![0; content_length];
    reader
        .read_exact(&mut body)
        .await
        .context("read MCP HTTP body")?;
    let request = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => return write_http_response(&mut writer, 400, None).await,
    };
    let protocol = match protocol_for_http_request(bridge, &request, &headers) {
        Ok(protocol) => protocol,
        Err(error) => {
            let id = request.get("id").cloned().unwrap_or(Value::Null);
            return write_http_response(&mut writer, 400, Some(error_response(id, error))).await;
        }
    };
    let response = bridge.handle_for(protocol, request).await;
    let status = match response
        .as_ref()
        .and_then(|value| value.pointer("/error/code"))
    {
        Some(Value::Number(code))
            if protocol == McpProtocol::Stateless && code.as_i64() == Some(-32601) =>
        {
            404
        }
        _ if response.is_none() => 202,
        _ => 200,
    };
    write_http_response(&mut writer, status, response).await
}

async fn write_http_response<W: AsyncWrite + Unpin>(
    writer: &mut W,
    status: u16,
    response: Option<Value>,
) -> Result<()> {
    let body = response.map_or_else(Vec::new, |response| {
        serde_json::to_vec(&response).expect("MCP response serializes")
    });
    let reason = match status {
        200 => "OK",
        202 => "Accepted",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        413 => "Payload Too Large",
        _ => "Error",
    };
    let content_type = if body.is_empty() {
        String::new()
    } else {
        "Content-Type: application/json\r\n".into()
    };
    writer
        .write_all(
            format!(
                "HTTP/1.1 {status} {reason}\r\n{content_type}Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .await?;
    writer.write_all(&body).await?;
    writer.flush().await?;
    Ok(())
}

fn protocol_for_http_request(
    bridge: &TurnMcpBridge<'_>,
    request: &Value,
    headers: &BTreeMap<String, String>,
) -> std::result::Result<McpProtocol, McpError> {
    if request.get("method").and_then(Value::as_str) == Some("initialize") {
        let requested = request
            .pointer("/params/protocolVersion")
            .and_then(Value::as_str)
            .and_then(LegacyMcpVersion::parse)
            .unwrap_or(LegacyMcpVersion::LATEST);
        return Ok(McpProtocol::Legacy(requested));
    }

    let requested = request
        .pointer("/params/_meta/io.modelcontextprotocol~1protocolVersion")
        .and_then(Value::as_str);
    let Some(requested) = requested else {
        if headers.contains_key("mcp-protocol-version") {
            return Err(McpError::header_mismatch(
                "missing MCP protocol version metadata",
            ));
        }
        return Ok(bridge.protocol_for_request(request));
    };
    if requested != STATELESS_MCP_VERSION {
        return Err(McpError::unsupported_version(requested));
    }
    validate_stateless_headers(request, headers)?;
    Ok(McpProtocol::Stateless)
}

fn validate_stateless_headers(
    request: &Value,
    headers: &BTreeMap<String, String>,
) -> std::result::Result<(), McpError> {
    if headers
        .get("content-type")
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        != Some("application/json")
    {
        return Err(McpError::header_mismatch(
            "content-type must be application/json",
        ));
    }
    let accept = headers
        .get("accept")
        .map(String::as_str)
        .unwrap_or_default();
    if !accept
        .split(',')
        .any(|value| value.trim() == "application/json")
        || !accept
            .split(',')
            .any(|value| value.trim() == "text/event-stream")
    {
        return Err(McpError::header_mismatch(
            "accept must include application/json and text/event-stream",
        ));
    }
    if request
        .pointer("/params/_meta/io.modelcontextprotocol~1clientCapabilities")
        .and_then(Value::as_object)
        .is_none()
    {
        return Err(McpError::invalid("missing MCP client capabilities"));
    }
    for (header, body) in [
        (
            "mcp-protocol-version",
            request.pointer("/params/_meta/io.modelcontextprotocol~1protocolVersion"),
        ),
        ("mcp-method", request.get("method")),
    ] {
        let body = body.and_then(Value::as_str).unwrap_or_default();
        if headers.get(header).map(String::as_str) != Some(body) {
            return Err(McpError::header_mismatch(format!(
                "{header} header does not match the request body"
            )));
        }
    }
    if request.get("method").and_then(Value::as_str) == Some("tools/call") {
        let body = request
            .pointer("/params/name")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let header = headers
            .get("mcp-name")
            .ok_or_else(|| McpError::header_mismatch("missing mcp-name header"))?;
        if decode_header_value(header).as_deref() != Some(body) {
            return Err(McpError::header_mismatch(
                "mcp-name header does not match the request body",
            ));
        }
    }
    Ok(())
}

fn decode_header_value(value: &str) -> Option<String> {
    let encoded = value
        .strip_prefix("=?base64?")
        .and_then(|value| value.strip_suffix("?="));
    match encoded {
        Some(encoded) => base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .ok()
            .and_then(|bytes| String::from_utf8(bytes).ok()),
        None => Some(value.to_owned()),
    }
}

fn allowed_origin(origin: &str) -> bool {
    [
        "http://localhost",
        "https://localhost",
        "http://127.0.0.1",
        "https://127.0.0.1",
        "http://[::1]",
        "https://[::1]",
    ]
    .into_iter()
    .any(|allowed| {
        origin == allowed
            || origin
                .strip_prefix(allowed)
                .and_then(|suffix| suffix.strip_prefix(':'))
                .is_some_and(|port| {
                    !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit())
                })
    })
}

fn error_response(id: Value, error: McpError) -> Value {
    let mut response = json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": error.code, "message": error.message},
    });
    if let Some(data) = error.data {
        response["error"]["data"] = data;
    }
    response
}

fn is_native_mews_tool(name: &str) -> bool {
    matches!(name, "read" | "write" | "edit" | "bash")
}

fn tool_result(result: ToolResult) -> Value {
    let mut text = match &result.value {
        Value::String(value) => value.clone(),
        value => value.to_string(),
    };
    if result.uncertain {
        text = format!("Effect outcome is uncertain: {text}. {UNCERTAIN_EFFECT_INSTRUCTION}");
    }
    json!({"content": [{"type": "text", "text": text}], "isError": result.is_error})
}

fn server_info() -> Value {
    json!({"name": "mews-turn-extensions", "version": env!("CARGO_PKG_VERSION")})
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
    data: Option<Value>,
}

impl McpError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
            data: None,
        }
    }

    fn unavailable() -> Self {
        Self {
            code: -32000,
            message: "MCP capability is no longer available for this Turn".into(),
            data: None,
        }
    }

    fn header_mismatch(message: impl Into<String>) -> Self {
        Self {
            code: -32020,
            message: message.into(),
            data: None,
        }
    }

    fn unsupported_version(requested: &str) -> Self {
        Self {
            code: -32022,
            message: "Unsupported protocol version".into(),
            data: Some(json!({
                "supported": supported_versions(),
                "requested": requested,
            })),
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
        fn tools(&self) -> mews_protocol::ToolCatalogSnapshot {
            mews_protocol::ToolCatalogSnapshot::default()
        }
        fn extension_tools(
            &self,
            _: &mews_protocol::AgentId,
        ) -> mews_protocol::ToolCatalogSnapshot {
            mews_protocol::ToolCatalogSnapshot {
                generation: 1,
                tools: ["issue_lookup", "deploy_preview", "read"]
                    .into_iter()
                    .map(|name| ToolDefinition {
                        name: name.into(),
                        description: format!("{name} description"),
                        schema: json!({"type":"object"}),
                        agent_id: None,
                    })
                    .collect(),
            }
        }
        async fn execute(
            &self,
            _: &mews_protocol::AgentId,
            call: &ToolCall,
            _: &Path,
            _: &CancellationToken,
            _: &dyn ProgressReporter,
        ) -> Result<ToolResult> {
            if call.name == "read" {
                bail!("native tool must not be called")
            }
            if call.name == "deploy_preview" {
                return Err(mews_agent::EffectUncertain::new("deployment reply was lost").into());
            }
            tokio::time::sleep(self.delay).await;
            self.calls.lock().unwrap().push(call.clone());
            Ok(ToolResult::success(json!({"called": call.name})))
        }
        async fn hook(
            &self,
            _: &mews_protocol::AgentId,
            _: LifecycleHook,
            _: Value,
            _: &Path,
            _: &CancellationToken,
            _: Option<u64>,
        ) -> Result<Value> {
            Ok(Value::Null)
        }
    }

    fn bridge(capabilities: &Capabilities) -> TurnMcpBridge<'_> {
        TurnMcpBridge::for_extensions(
            capabilities,
            Path::new("/tmp").to_owned(),
            CancellationToken::new(),
            &["issue_*".into()],
        )
        .unwrap()
    }

    fn stateless_request(id: u64, method: &str, mut params: Value) -> Value {
        params["_meta"] = json!({
            "io.modelcontextprotocol/protocolVersion": STATELESS_MCP_VERSION,
            "io.modelcontextprotocol/clientInfo": {"name": "test", "version": "1"},
            "io.modelcontextprotocol/clientCapabilities": {},
        });
        json!({"jsonrpc":"2.0", "id":id, "method":method, "params":params})
    }

    #[tokio::test]
    async fn negotiates_each_supported_legacy_version_and_prefers_the_latest_fallback() {
        let capabilities = Capabilities {
            calls: Mutex::new(Vec::new()),
            delay: std::time::Duration::ZERO,
        };
        let bridge = bridge(&capabilities);
        for version in LegacyMcpVersion::ALL {
            let response = bridge
                .handle(json!({
                    "jsonrpc":"2.0", "id":1, "method":"initialize",
                    "params":{"protocolVersion":version.as_str(), "capabilities":{}, "clientInfo":{"name":"test","version":"1"}}
                }))
                .await
                .unwrap();
            assert_eq!(response["result"]["protocolVersion"], version.as_str());
        }
        let response = bridge
            .handle(json!({
                "jsonrpc":"2.0", "id":2, "method":"initialize",
                "params":{"protocolVersion":"2099-01-01", "capabilities":{}, "clientInfo":{"name":"test","version":"1"}}
            }))
            .await
            .unwrap();
        assert_eq!(
            response["result"]["protocolVersion"],
            LegacyMcpVersion::LATEST.as_str()
        );
    }

    #[tokio::test]
    async fn stateless_discovery_and_tools_include_modern_envelopes() {
        let capabilities = Capabilities {
            calls: Mutex::new(Vec::new()),
            delay: std::time::Duration::ZERO,
        };
        let bridge = bridge(&capabilities);
        let discovered = bridge
            .handle_for(
                McpProtocol::Stateless,
                stateless_request(1, "server/discover", json!({})),
            )
            .await
            .unwrap();
        assert_eq!(discovered["result"]["resultType"], "complete");
        assert_eq!(
            discovered["result"]["supportedVersions"][0],
            STATELESS_MCP_VERSION
        );
        assert_eq!(
            discovered["result"]["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
            "mews-turn-extensions"
        );

        let listed = bridge
            .handle_for(
                McpProtocol::Stateless,
                stateless_request(2, "tools/list", json!({})),
            )
            .await
            .unwrap();
        assert_eq!(listed["result"]["resultType"], "complete");
        assert_eq!(listed["result"]["cacheScope"], "private");
        assert_eq!(listed["result"]["ttlMs"], TOOL_LIST_TTL_MS);
    }

    #[test]
    fn stateless_requests_reject_mismatched_headers_and_unsupported_versions() {
        let capabilities = Capabilities {
            calls: Mutex::new(Vec::new()),
            delay: std::time::Duration::ZERO,
        };
        let bridge = bridge(&capabilities);
        let request = stateless_request(1, "tools/call", json!({"name":"issue_lookup"}));
        let headers = BTreeMap::from([
            ("content-type".into(), "application/json".into()),
            (
                "accept".into(),
                "application/json, text/event-stream".into(),
            ),
            ("mcp-protocol-version".into(), STATELESS_MCP_VERSION.into()),
            ("mcp-method".into(), "tools/call".into()),
            ("mcp-name".into(), "different_tool".into()),
        ]);
        let error = protocol_for_http_request(&bridge, &request, &headers).unwrap_err();
        assert_eq!(error.code, -32020);

        let mut unsupported = request;
        unsupported["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"] =
            Value::String("2099-01-01".into());
        let error = protocol_for_http_request(&bridge, &unsupported, &headers).unwrap_err();
        assert_eq!(error.code, -32022);
        assert_eq!(error.data.unwrap()["supported"][0], STATELESS_MCP_VERSION);
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
    async fn http_transport_keeps_the_turn_scoped_extension_boundary() {
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
        assert!(!response.to_ascii_lowercase().contains("mcp-session-id"));
        assert_eq!(capabilities.calls.lock().unwrap()[0].name, "issue_lookup");
    }

    #[tokio::test]
    async fn stateless_http_validates_routing_headers() {
        let capabilities = Capabilities {
            calls: Mutex::new(Vec::new()),
            delay: std::time::Duration::ZERO,
        };
        let bridge = bridge(&capabilities);
        let endpoint = bridge.bind_http().await.unwrap();
        let address = endpoint.listener.local_addr().unwrap();
        let body = serde_json::to_vec(&stateless_request(7, "tools/list", json!({}))).unwrap();
        let client = async {
            let mut stream = TcpStream::connect(address).await.unwrap();
            stream
                .write_all(
                    format!(
                        "POST {} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nMCP-Protocol-Version: {}\r\nMcp-Method: tools/list\r\nContent-Length: {}\r\n\r\n",
                        endpoint.path,
                        STATELESS_MCP_VERSION,
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            stream.write_all(&body).await.unwrap();
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await.unwrap();
            String::from_utf8(response).unwrap()
        };
        let (served, response) = tokio::join!(
            endpoint
                .accept_and_handle(&bridge, Instant::now() + std::time::Duration::from_secs(2),),
            client
        );
        served.unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("\"resultType\":\"complete\""));
        assert!(!response.to_ascii_lowercase().contains("mcp-session-id"));
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
            fn tools(&self) -> mews_protocol::ToolCatalogSnapshot {
                mews_protocol::ToolCatalogSnapshot::default()
            }
            fn extension_tools(
                &self,
                _: &mews_protocol::AgentId,
            ) -> mews_protocol::ToolCatalogSnapshot {
                mews_protocol::ToolCatalogSnapshot {
                    generation: 1,
                    tools: vec![ToolDefinition {
                        name: "lookup".into(),
                        description: "Lookup".into(),
                        agent_id: None,
                        schema: json!({
                            "type":"object",
                            "properties":{"id":{"type":"string"}},
                            "required":["id"],
                            "additionalProperties":false
                        }),
                    }],
                }
            }
            async fn execute(
                &self,
                _: &mews_protocol::AgentId,
                _: &ToolCall,
                _: &Path,
                _: &CancellationToken,
                _: &dyn ProgressReporter,
            ) -> Result<ToolResult> {
                panic!("invalid arguments must not cross the capability boundary")
            }
            async fn hook(
                &self,
                _: &mews_protocol::AgentId,
                _: LifecycleHook,
                _: Value,
                _: &Path,
                _: &CancellationToken,
                _: Option<u64>,
            ) -> Result<Value> {
                panic!("invalid arguments must not launch a hook")
            }
        }
        let bridge = TurnMcpBridge::for_extensions(
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
        assert_eq!(response["result"]["isError"], true);
    }

    #[tokio::test]
    async fn before_tool_identity_rewrite_is_a_call_error() {
        struct RewriteCapabilities;
        #[async_trait]
        impl AgentCapabilities for RewriteCapabilities {
            async fn context(&self, _: &str, _: &Path) -> Result<ContextSnapshot> {
                Ok(ContextSnapshot::default())
            }
            fn tools(&self) -> mews_protocol::ToolCatalogSnapshot {
                mews_protocol::ToolCatalogSnapshot::default()
            }
            fn extension_tools(
                &self,
                _: &mews_protocol::AgentId,
            ) -> mews_protocol::ToolCatalogSnapshot {
                mews_protocol::ToolCatalogSnapshot {
                    generation: 1,
                    tools: vec![ToolDefinition {
                        name: "lookup".into(),
                        description: "Lookup".into(),
                        agent_id: None,
                        schema: json!({"type":"object"}),
                    }],
                }
            }
            async fn execute(
                &self,
                _: &mews_protocol::AgentId,
                _: &ToolCall,
                _: &Path,
                _: &CancellationToken,
                _: &dyn ProgressReporter,
            ) -> Result<ToolResult> {
                panic!("rewritten call must not execute")
            }
            async fn hook(
                &self,
                _: &mews_protocol::AgentId,
                hook: LifecycleHook,
                _: Value,
                _: &Path,
                _: &CancellationToken,
                _: Option<u64>,
            ) -> Result<Value> {
                assert_eq!(hook, LifecycleHook::BeforeTool);
                Ok(json!({"name":"lookup","tool":"other","arguments":{}}))
            }
        }
        let bridge = TurnMcpBridge::for_extensions(
            &RewriteCapabilities,
            PathBuf::from("/tmp"),
            CancellationToken::new(),
            &["lookup".into()],
        )
        .unwrap();

        let response = bridge
            .handle(json!({
                "jsonrpc":"2.0", "id":1, "method":"tools/call",
                "params":{"name":"lookup", "arguments":{}}
            }))
            .await
            .unwrap();

        assert_eq!(response["result"]["isError"], true);
        assert!(
            response["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("may not change its tool name")
        );
    }

    #[tokio::test]
    async fn uncertain_extension_effect_is_visible_and_propagated_to_the_turn() {
        let capabilities = Capabilities {
            calls: Mutex::new(Vec::new()),
            delay: std::time::Duration::ZERO,
        };
        let bridge = TurnMcpBridge::for_extensions(
            &capabilities,
            PathBuf::from("/tmp"),
            CancellationToken::new(),
            &["deploy_preview".into()],
        )
        .unwrap();

        let response = bridge
            .handle(json!({
                "jsonrpc":"2.0", "id":1, "method":"tools/call",
                "params":{"name":"deploy_preview", "arguments":{}}
            }))
            .await
            .unwrap();

        assert_eq!(response["result"]["isError"], true);
        assert!(
            response["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("Do not retry automatically")
        );
        assert_eq!(
            bridge.take_effect_uncertainty().as_deref(),
            Some("deployment reply was lost")
        );
    }

    #[tokio::test]
    async fn skill_tools_return_one_direct_mcp_content_envelope() {
        let capabilities = Capabilities {
            calls: Mutex::new(Vec::new()),
            delay: std::time::Duration::ZERO,
        };
        let bridge = TurnMcpBridge::for_extensions_and_skills(
            &capabilities,
            &mews_protocol::AgentId::new(),
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
        assert_eq!(invalid["result"]["isError"], true);
    }

    #[tokio::test]
    async fn skill_tools_exist_only_for_a_nonempty_skill_snapshot() {
        let capabilities = Capabilities {
            calls: Mutex::new(Vec::new()),
            delay: std::time::Duration::ZERO,
        };
        let empty = TurnMcpBridge::for_extensions_and_skills(
            &capabilities,
            &mews_protocol::AgentId::new(),
            PathBuf::from("/tmp"),
            CancellationToken::new(),
            &[],
            Vec::new(),
        )
        .unwrap();
        assert!(!empty.needs_transport());
        let listed = empty
            .handle(json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
            .await
            .unwrap();
        assert_eq!(listed["result"]["tools"], json!([]));

        let with_extension = TurnMcpBridge::for_extensions_and_skills(
            &capabilities,
            &mews_protocol::AgentId::new(),
            PathBuf::from("/tmp"),
            CancellationToken::new(),
            &["issue_lookup".into()],
            Vec::new(),
        )
        .unwrap();
        assert!(with_extension.needs_transport());
        let listed = with_extension
            .handle(json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
            .await
            .unwrap();
        assert_eq!(listed["result"]["tools"].as_array().unwrap().len(), 1);
        assert_eq!(listed["result"]["tools"][0]["name"], "issue_lookup");
    }

    #[test]
    fn reserved_skill_tool_collisions_are_rejected_for_all_skill_snapshots() {
        struct ReservedCapabilities(&'static str);

        #[async_trait]
        impl AgentCapabilities for ReservedCapabilities {
            async fn context(&self, _: &str, _: &Path) -> Result<ContextSnapshot> {
                unreachable!()
            }

            fn tools(&self) -> mews_protocol::ToolCatalogSnapshot {
                mews_protocol::ToolCatalogSnapshot::default()
            }

            fn extension_tools(
                &self,
                _: &mews_protocol::AgentId,
            ) -> mews_protocol::ToolCatalogSnapshot {
                mews_protocol::ToolCatalogSnapshot {
                    generation: 1,
                    tools: vec![ToolDefinition {
                        name: self.0.into(),
                        description: "Reserved collision".into(),
                        schema: json!({"type":"object"}),
                        agent_id: None,
                    }],
                }
            }

            async fn execute(
                &self,
                _: &mews_protocol::AgentId,
                _: &ToolCall,
                _: &Path,
                _: &CancellationToken,
                _: &dyn ProgressReporter,
            ) -> Result<ToolResult> {
                unreachable!()
            }

            async fn hook(
                &self,
                _: &mews_protocol::AgentId,
                _: LifecycleHook,
                _: Value,
                _: &Path,
                _: &CancellationToken,
                _: Option<u64>,
            ) -> Result<Value> {
                unreachable!()
            }
        }

        for name in mews_protocol::ACP_SKILL_TOOL_NAMES {
            for skills in [
                Vec::new(),
                vec![AcpSkill {
                    name: "review".into(),
                    description: "Review code".into(),
                    hash: "a".repeat(64),
                    content: "body".into(),
                }],
            ] {
                let error = TurnMcpBridge::for_extensions_and_skills(
                    &ReservedCapabilities(name),
                    &mews_protocol::AgentId::new(),
                    PathBuf::from("/tmp"),
                    CancellationToken::new(),
                    &["*".into()],
                    skills,
                )
                .err()
                .expect("reserved collision must fail");
                assert!(error.to_string().contains("reserved MCP skill tool"));
            }
        }
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
