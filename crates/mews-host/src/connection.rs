use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, RwLock},
};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::{Semaphore, mpsc, oneshot};

use crate::ToolRegistry;
use mews_agent::{
    AgentCapabilities, CancellationToken, ContextDocument, ContextSnapshot, LifecycleHook,
    ProgressReporter, ToolCall, ToolResult,
};
use mews_protocol::{
    AcpEvent, Agent, AgentReplica, AgentRevision, HarnessDescriptor, HostId, HostToHub, HubToHost,
    HubTransferStart, RequestId, ToolDefinition,
};

use crate::lifecycle::handle_host_request_streaming;

enum HostReply {
    Tool(Value),
    Directory(std::path::PathBuf),
    AgentSynchronized,
    AgentReplica(Option<AgentReplica>),
    ProjectContext(String),
    Hook(Value),
    Prompt(Option<String>),
    HubTransfer(Option<u64>),
    Configured,
    Harnesses(Vec<HarnessDescriptor>),
    Acp(mews_acp::AcpSessionOutcome),
}
struct PendingRequest {
    reply: oneshot::Sender<Result<HostReply, String>>,
    acp_events: Option<mpsc::Sender<AcpEvent>>,
}
type PendingRequests = Arc<std::sync::Mutex<HashMap<RequestId, PendingRequest>>>;

pub struct RemoteAcpRun {
    pub harness: String,
    pub harness_options: std::collections::BTreeMap<String, String>,
    pub tools: Vec<String>,
    pub cwd: std::path::PathBuf,
    pub prompt: String,
    pub recovery_prompt: String,
    pub agent_slug: String,
    pub soul: String,
    pub mews_session_id: String,
    pub run_id: String,
    pub transition: mews_protocol::AcpBindingTransition,
    pub context: Option<mews_protocol::AcpBindingContext>,
}

#[async_trait]
pub trait HostControl: Send + Sync {
    fn host_id(&self) -> &HostId;
    fn harness_descriptor(&self, name: &str) -> Option<HarnessDescriptor>;
    async fn attest_directory(&self, path: &Path) -> Result<std::path::PathBuf>;
    async fn synchronize_agent(
        &self,
        agent: &Agent,
        revision: &AgentRevision,
        expected_replica: Option<&AgentReplica>,
        previous_slug: Option<&str>,
    ) -> Result<()>;
    async fn agent_replica(&self, slug: &str) -> Result<Option<AgentReplica>>;
    async fn begin_hub_transfer(&self, transfer: HubTransferStart) -> Result<()>;
    async fn write_hub_transfer(&self, offset: u64, data: Vec<u8>) -> Result<u64>;
    async fn commit_hub_transfer(&self) -> Result<()>;
    async fn arm_hub_transfer(&self, move_nonce: &str) -> Result<()>;
    async fn activate_hub_transfer(&self) -> Result<()>;
    async fn configure_relay(
        &self,
        active: bool,
        stop_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<()>;
    async fn update_relay_candidates(&self, relay_urls: Vec<String>) -> Result<()>;
    async fn refresh_harness_catalog(&self) -> Result<Vec<HarnessDescriptor>>;
    async fn run_acp(
        &self,
        run: RemoteAcpRun,
        events: mpsc::Sender<AcpEvent>,
        cancellation: &CancellationToken,
    ) -> Result<mews_acp::AcpSessionOutcome>;
    async fn acknowledge_acp_session_binding(&self, acknowledgement_id: String) -> Result<()>;
}

/// Composition boundary for a connected Host that provides both control-plane
/// operations and the neutral execution environment used by agent runs.
pub trait HostExecutor: HostControl + AgentCapabilities {
    fn agent_capabilities(&self) -> &dyn AgentCapabilities;
}

impl<T: HostControl + AgentCapabilities> HostExecutor for T {
    fn agent_capabilities(&self) -> &dyn AgentCapabilities {
        self
    }
}

/// Hub-side handle for any Host transport. Only serialized protocol frames
/// cross this boundary; tools remain owned by the Host.
pub struct ConnectedHost {
    id: HostId,
    tools: Arc<RwLock<Vec<ToolDefinition>>>,
    harnesses: Arc<RwLock<Vec<HarnessDescriptor>>>,
    sender: mpsc::Sender<HubToHost>,
    pending: PendingRequests,
    capacity: Arc<Semaphore>,
}

impl ConnectedHost {
    pub(crate) fn tool_catalog(&self) -> Vec<ToolDefinition> {
        self.tools
            .read()
            .expect("Host tool catalog poisoned")
            .clone()
    }

    /// The most recently published Host-local Harness catalog. This is live
    /// connection state, not durable Hub configuration.
    pub fn harness_catalog(&self) -> Vec<HarnessDescriptor> {
        self.harnesses
            .read()
            .expect("Host Harness catalog poisoned")
            .clone()
    }

    /// Refreshing the Hub's own Host must update the same live catalog used
    /// for dispatch, not only the catalog returned to a client.
    pub fn replace_harness_catalog(&self, harnesses: Vec<HarnessDescriptor>) {
        *self
            .harnesses
            .write()
            .expect("Host Harness catalog poisoned") = harnesses;
    }

    pub(crate) async fn execute_tool(
        &self,
        tool: &str,
        arguments: Value,
        cwd: &Path,
    ) -> Result<Value> {
        let request_id = RequestId::new();
        match self
            .request_inner(
                HubToHost::ExecuteTool {
                    request_id: request_id.clone(),
                    tool: tool.to_owned(),
                    arguments,
                    canonical_cwd: cwd.to_path_buf(),
                },
                None,
                Some(request_id),
            )
            .await?
        {
            HostReply::Tool(value) => Ok(value),
            _ => bail!("Host returned the wrong response type"),
        }
    }

    async fn execute_hook(
        &self,
        hook: &str,
        payload: Value,
        cwd: &Path,
        cancellation: &CancellationToken,
    ) -> Result<Value> {
        cancellation.check()?;
        let request_id = RequestId::new();
        let request = self.request_inner(
            HubToHost::ExecuteHook {
                request_id: request_id.clone(),
                hook: hook.to_owned(),
                payload,
                canonical_cwd: cwd.to_path_buf(),
            },
            None,
            Some(request_id),
        );
        tokio::pin!(request);
        let reply = tokio::select! {
            result = &mut request => result?,
            _ = cancellation.cancelled() => {
                cancellation.check()?;
                unreachable!()
            }
        };
        match reply {
            HostReply::Hook(payload) => Ok(payload),
            _ => bail!("Host returned the wrong response type"),
        }
    }

    async fn fetch_project_context(&self, agent_slug: &str, cwd: &Path) -> Result<String> {
        match self
            .request(HubToHost::ReadProjectContext {
                request_id: RequestId::new(),
                agent_slug: agent_slug.to_owned(),
                canonical_cwd: cwd.to_path_buf(),
            })
            .await?
        {
            HostReply::ProjectContext(context) => Ok(context),
            _ => bail!("Host returned the wrong response type"),
        }
    }

    async fn fetch_prompt(&self, cwd: &Path, name: &str) -> Result<Option<String>> {
        match self
            .request(HubToHost::ReadPrompt {
                request_id: RequestId::new(),
                name: name.to_owned(),
                canonical_cwd: cwd.to_path_buf(),
            })
            .await?
        {
            HostReply::Prompt(content) => Ok(content),
            _ => bail!("Host returned the wrong response type"),
        }
    }

    pub async fn in_process(id: HostId, registry: ToolRegistry) -> Result<Self> {
        let tools = registry.definitions();
        let harnesses = crate::HarnessCatalog::discover(registry.root())?.descriptors();
        if let Some(root) = registry.root().map(Path::to_path_buf) {
            tokio::spawn({
                let registry = registry.clone();
                async move { registry.watch_host_extensions(root).await }
            });
        }
        let (hub_sender, host_receiver) = mpsc::channel(32);
        let (host_sender, hub_receiver) = mpsc::channel(32);
        tokio::spawn(serve_host(
            registry,
            harnesses.clone(),
            host_receiver,
            host_sender,
        ));
        Self::from_channels_with_catalog(id, tools, harnesses, hub_sender, hub_receiver).await
    }

    pub async fn from_channels(
        id: HostId,
        initial_tools: Vec<ToolDefinition>,
        sender: mpsc::Sender<HubToHost>,
        receiver: mpsc::Receiver<HostToHub>,
    ) -> Result<Self> {
        Self::from_channels_with_catalog(id, initial_tools, Vec::new(), sender, receiver).await
    }

    pub async fn from_channels_with_catalog(
        id: HostId,
        initial_tools: Vec<ToolDefinition>,
        initial_harnesses: Vec<HarnessDescriptor>,
        sender: mpsc::Sender<HubToHost>,
        mut receiver: mpsc::Receiver<HostToHub>,
    ) -> Result<Self> {
        let tools = Arc::new(RwLock::new(initial_tools));
        let harnesses = Arc::new(RwLock::new(initial_harnesses));
        let pending: PendingRequests = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let response_tools = Arc::clone(&tools);
        let response_harnesses = Arc::clone(&harnesses);
        let response_pending = Arc::clone(&pending);
        tokio::spawn(async move {
            while let Some(message) = receiver.recv().await {
                // Enforce the same versioned and bounded boundary used by a
                // network Host even when both ends share this process.
                let message = match mews_protocol::encode(message)
                    .and_then(|bytes| mews_protocol::decode(&bytes))
                {
                    Ok(message) => message,
                    Err(_) => break,
                };
                match message {
                    HostToHub::ConfigurationResult { request_id, error } => {
                        if let Some(reply) = response_pending
                            .lock()
                            .expect("Host pending requests poisoned")
                            .remove(&request_id)
                        {
                            let _ = reply
                                .reply
                                .send(error.map_or(Ok(HostReply::Configured), Err));
                        }
                    }
                    HostToHub::Ready { tools, harnesses } => {
                        *response_tools.write().expect("Host tool catalog poisoned") = tools;
                        *response_harnesses
                            .write()
                            .expect("Host Harness catalog poisoned") = harnesses;
                    }
                    HostToHub::ToolCatalogChanged { tools } => {
                        *response_tools.write().expect("Host tool catalog poisoned") = tools;
                    }
                    HostToHub::HarnessCatalog {
                        request_id,
                        harnesses,
                        error,
                    } => {
                        if error.is_none() {
                            *response_harnesses
                                .write()
                                .expect("Host Harness catalog poisoned") = harnesses.clone();
                        }
                        if let Some(reply) = response_pending
                            .lock()
                            .expect("Host pending requests poisoned")
                            .remove(&request_id)
                        {
                            let _ = reply
                                .reply
                                .send(error.map_or(Ok(HostReply::Harnesses(harnesses)), Err));
                        }
                    }
                    HostToHub::ToolResult {
                        request_id,
                        result,
                        error,
                    } => {
                        if let Some(reply) = response_pending
                            .lock()
                            .expect("Host pending requests poisoned")
                            .remove(&request_id)
                        {
                            let _ = reply
                                .reply
                                .send(error.map_or(Ok(HostReply::Tool(result)), Err));
                        }
                    }
                    HostToHub::HookResult {
                        request_id,
                        payload,
                        error,
                    } => {
                        if let Some(reply) = response_pending
                            .lock()
                            .expect("Host pending requests poisoned")
                            .remove(&request_id)
                        {
                            let result = match (payload, error) {
                                (Some(payload), None) => Ok(HostReply::Hook(payload)),
                                (_, Some(error)) => Err(error),
                                _ => Err("Host returned an empty hook result".into()),
                            };
                            let _ = reply.reply.send(result);
                        }
                    }
                    HostToHub::AcpResult {
                        request_id,
                        answer,
                        acp_session_id,
                        session_replaced,
                        stop_reason,
                        timings,
                        error,
                    } => {
                        if let Some(pending) = response_pending
                            .lock()
                            .expect("Host pending requests poisoned")
                            .remove(&request_id)
                        {
                            let result = match (answer, acp_session_id, stop_reason, timings, error)
                            {
                                (
                                    Some(answer),
                                    Some(session_id),
                                    Some(stop_reason),
                                    Some(timings),
                                    None,
                                ) => Ok(HostReply::Acp(mews_acp::AcpSessionOutcome {
                                    answer,
                                    session_id,
                                    session_replaced,
                                    timings,
                                    stop_reason,
                                })),
                                (_, _, _, _, Some(error)) => Err(error),
                                _ => Err("Host returned an empty ACP result".into()),
                            };
                            let _ = pending.reply.send(result);
                        }
                    }
                    HostToHub::AcpEvent { request_id, event } => {
                        let sender = response_pending
                            .lock()
                            .expect("Host pending requests poisoned")
                            .get(&request_id)
                            .and_then(|pending| pending.acp_events.as_ref())
                            .cloned();
                        if let Some(sender) = sender {
                            let _ = sender.send(event).await;
                        }
                    }
                    HostToHub::DirectoryAttested {
                        request_id,
                        canonical_path,
                        error,
                    } => {
                        if let Some(reply) = response_pending
                            .lock()
                            .expect("Host pending requests poisoned")
                            .remove(&request_id)
                        {
                            let response = match (canonical_path, error) {
                                (Some(path), None) => Ok(HostReply::Directory(path)),
                                (_, Some(error)) => Err(error),
                                _ => Err("Host returned an invalid directory attestation".into()),
                            };
                            let _ = reply.reply.send(response);
                        }
                    }
                    HostToHub::AgentSynchronized { request_id, error } => {
                        if let Some(reply) = response_pending
                            .lock()
                            .expect("Host pending requests poisoned")
                            .remove(&request_id)
                        {
                            let _ = reply
                                .reply
                                .send(error.map_or(Ok(HostReply::AgentSynchronized), Err));
                        }
                    }
                    HostToHub::AgentReplica {
                        request_id,
                        replica,
                        error,
                    } => {
                        if let Some(reply) = response_pending
                            .lock()
                            .expect("Host pending requests poisoned")
                            .remove(&request_id)
                        {
                            let _ = reply
                                .reply
                                .send(error.map_or(Ok(HostReply::AgentReplica(replica)), Err));
                        }
                    }
                    HostToHub::ProjectContext {
                        request_id,
                        context,
                        error,
                    } => {
                        if let Some(reply) = response_pending
                            .lock()
                            .expect("Host pending requests poisoned")
                            .remove(&request_id)
                        {
                            let response = match (context, error) {
                                (Some(context), None) => Ok(HostReply::ProjectContext(context)),
                                (_, Some(error)) => Err(error),
                                _ => Err("Host returned invalid project context".into()),
                            };
                            let _ = reply.reply.send(response);
                        }
                    }
                    HostToHub::Prompt {
                        request_id,
                        content,
                        error,
                    } => {
                        if let Some(reply) = response_pending
                            .lock()
                            .expect("Host pending requests poisoned")
                            .remove(&request_id)
                        {
                            let _ = reply
                                .reply
                                .send(error.map_or(Ok(HostReply::Prompt(content)), Err));
                        }
                    }
                    HostToHub::HubTransferResult {
                        request_id,
                        next_offset,
                        error,
                    } => {
                        if let Some(reply) = response_pending
                            .lock()
                            .expect("Host pending requests poisoned")
                            .remove(&request_id)
                        {
                            let _ = reply
                                .reply
                                .send(error.map_or(Ok(HostReply::HubTransfer(next_offset)), Err));
                        }
                    }
                    HostToHub::Pong { .. } => {}
                }
            }
            for (_, reply) in response_pending
                .lock()
                .expect("Host pending requests poisoned")
                .drain()
            {
                let _ = reply.reply.send(Err("Host disconnected".into()));
            }
        });
        Ok(Self {
            id,
            tools,
            harnesses,
            sender,
            pending,
            capacity: Arc::new(Semaphore::new(32)),
        })
    }
}

#[async_trait]
impl HostControl for ConnectedHost {
    fn host_id(&self) -> &HostId {
        &self.id
    }

    fn harness_descriptor(&self, name: &str) -> Option<HarnessDescriptor> {
        self.harness_catalog()
            .into_iter()
            .find(|descriptor| descriptor.name == name)
    }

    async fn attest_directory(&self, path: &Path) -> Result<std::path::PathBuf> {
        match self
            .request(HubToHost::AttestDirectory {
                request_id: RequestId::new(),
                path: path.to_path_buf(),
            })
            .await?
        {
            HostReply::Directory(path) => Ok(path),
            HostReply::Tool(_) => bail!("Host returned the wrong response type"),
            HostReply::AgentSynchronized => bail!("Host returned the wrong response type"),
            HostReply::AgentReplica(_) => bail!("Host returned the wrong response type"),
            HostReply::ProjectContext(_) => bail!("Host returned the wrong response type"),
            HostReply::Hook(_) => bail!("Host returned the wrong response type"),
            HostReply::HubTransfer(_) => bail!("Host returned the wrong response type"),
            HostReply::Configured => bail!("Host returned the wrong response type"),
            HostReply::Prompt(_) => bail!("Host returned the wrong response type"),
            HostReply::Harnesses(_) => bail!("Host returned the wrong response type"),
            HostReply::Acp(_) => bail!("Host returned the wrong response type"),
        }
    }

    async fn synchronize_agent(
        &self,
        agent: &Agent,
        revision: &AgentRevision,
        expected_replica: Option<&AgentReplica>,
        previous_slug: Option<&str>,
    ) -> Result<()> {
        match self
            .request(HubToHost::SynchronizeAgent {
                request_id: RequestId::new(),
                agent: agent.clone(),
                revision: revision.clone(),
                expected_replica: expected_replica.cloned(),
                previous_slug: previous_slug.map(str::to_owned),
            })
            .await?
        {
            HostReply::AgentSynchronized => Ok(()),
            _ => bail!("Host returned the wrong response type"),
        }
    }

    async fn agent_replica(&self, slug: &str) -> Result<Option<AgentReplica>> {
        match self
            .request(HubToHost::ReadAgentReplica {
                request_id: RequestId::new(),
                slug: slug.to_owned(),
            })
            .await?
        {
            HostReply::AgentReplica(replica) => Ok(replica),
            _ => bail!("Host returned the wrong response type"),
        }
    }

    async fn begin_hub_transfer(&self, transfer: HubTransferStart) -> Result<()> {
        match self
            .request(HubToHost::BeginHubTransfer {
                request_id: RequestId::new(),
                transfer,
            })
            .await?
        {
            HostReply::HubTransfer(None) => Ok(()),
            _ => bail!("Host returned the wrong response type"),
        }
    }

    async fn write_hub_transfer(&self, offset: u64, data: Vec<u8>) -> Result<u64> {
        match self
            .request(HubToHost::WriteHubTransfer {
                request_id: RequestId::new(),
                offset,
                data,
            })
            .await?
        {
            HostReply::HubTransfer(Some(offset)) => Ok(offset),
            _ => bail!("Host returned the wrong response type"),
        }
    }

    async fn commit_hub_transfer(&self) -> Result<()> {
        match self
            .request(HubToHost::CommitHubTransfer {
                request_id: RequestId::new(),
            })
            .await?
        {
            HostReply::HubTransfer(None) => Ok(()),
            _ => bail!("Host returned the wrong response type"),
        }
    }

    async fn arm_hub_transfer(&self, move_nonce: &str) -> Result<()> {
        match self
            .request(HubToHost::ArmHubTransfer {
                request_id: RequestId::new(),
                move_nonce: move_nonce.to_owned(),
            })
            .await?
        {
            HostReply::HubTransfer(None) => Ok(()),
            _ => bail!("Host returned the wrong response type"),
        }
    }

    async fn activate_hub_transfer(&self) -> Result<()> {
        match self
            .request(HubToHost::ActivateHubTransfer {
                request_id: RequestId::new(),
            })
            .await?
        {
            HostReply::HubTransfer(None) => Ok(()),
            _ => bail!("Host returned the wrong response type"),
        }
    }

    async fn configure_relay(
        &self,
        active: bool,
        stop_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<()> {
        match self
            .request(HubToHost::ConfigureRelay {
                request_id: RequestId::new(),
                active,
                stop_at,
            })
            .await?
        {
            HostReply::Configured => Ok(()),
            _ => bail!("Host returned the wrong response type"),
        }
    }

    async fn update_relay_candidates(&self, relay_urls: Vec<String>) -> Result<()> {
        match self
            .request(HubToHost::UpdateRelayCandidates {
                request_id: RequestId::new(),
                relay_urls,
            })
            .await?
        {
            HostReply::Configured => Ok(()),
            _ => bail!("Host returned the wrong response type"),
        }
    }

    async fn refresh_harness_catalog(&self) -> Result<Vec<HarnessDescriptor>> {
        match self
            .request(HubToHost::RefreshHarnessCatalog {
                request_id: RequestId::new(),
            })
            .await?
        {
            HostReply::Harnesses(harnesses) => Ok(harnesses),
            _ => bail!("Host returned the wrong response type"),
        }
    }

    async fn run_acp(
        &self,
        run: RemoteAcpRun,
        events: mpsc::Sender<AcpEvent>,
        cancellation: &CancellationToken,
    ) -> Result<mews_acp::AcpSessionOutcome> {
        let request_id = RequestId::new();
        let reply = self.request_with_events(
            HubToHost::RunAcp {
                request_id: request_id.clone(),
                harness: run.harness,
                harness_options: run.harness_options,
                tools: run.tools,
                canonical_cwd: run.cwd,
                prompt: run.prompt,
                recovery_prompt: run.recovery_prompt,
                agent_slug: run.agent_slug,
                soul: run.soul,
                mews_session_id: run.mews_session_id,
                run_id: run.run_id,
                transition: run.transition,
                context: run.context,
            },
            events,
        );
        tokio::pin!(reply);
        let reply = tokio::select! {
            reply = &mut reply => reply?,
            _ = cancellation.cancelled() => {
                self.sender.send(HubToHost::CancelAcp { request_id }).await
                    .context("Host disconnected while cancelling ACP Run")?;
                tokio::time::timeout(std::time::Duration::from_secs(5), reply)
                    .await
                    .context("Host did not stop the cancelled ACP Run")??
            }
        };
        match reply {
            HostReply::Acp(answer) => Ok(answer),
            _ => bail!("Host returned the wrong response type"),
        }
    }

    async fn acknowledge_acp_session_binding(&self, acknowledgement_id: String) -> Result<()> {
        self.sender
            .send(HubToHost::AcknowledgeAcpSessionBinding { acknowledgement_id })
            .await
            .context("Host disconnected")
    }
}

#[async_trait]
impl AgentCapabilities for ConnectedHost {
    async fn context(&self, agent_slug: &str, cwd: &Path) -> Result<ContextSnapshot> {
        let content = self.fetch_project_context(agent_slug, cwd).await?;
        Ok(ContextSnapshot {
            documents: vec![ContextDocument {
                path: cwd.join("<host-context>"),
                content,
            }],
            ..ContextSnapshot::default()
        })
    }

    async fn read_prompt(&self, cwd: &Path, name: &str) -> Result<Option<String>> {
        self.fetch_prompt(cwd, name).await
    }

    fn tools(&self) -> Vec<mews_agent::ToolDefinition> {
        self.tool_catalog()
    }

    fn extension_tools(&self) -> Vec<mews_agent::ToolDefinition> {
        // The Host protocol currently publishes one catalog. Its four native
        // MEWS names are fixed and extensions may not shadow them, so this is
        // a safe projection until the protocol carries a separate catalog.
        self.tool_catalog()
            .into_iter()
            .filter(|tool| !matches!(tool.name.as_str(), "read" | "write" | "edit" | "bash"))
            .collect()
    }

    async fn execute(
        &self,
        call: &ToolCall,
        cwd: &Path,
        cancellation: &CancellationToken,
        _progress: &dyn ProgressReporter,
    ) -> Result<ToolResult> {
        cancellation.check()?;
        let execution = self.execute_tool(&call.name, call.arguments.clone(), cwd);
        tokio::pin!(execution);
        let result = tokio::select! {
            result = &mut execution => result?,
            _ = cancellation.cancelled() => {
                cancellation.check()?;
                unreachable!()
            }
        };
        Ok(ToolResult::success(result))
    }

    async fn hook(
        &self,
        hook: LifecycleHook,
        payload: Value,
        cwd: &Path,
        cancellation: &CancellationToken,
    ) -> Result<Value> {
        let name = match hook {
            LifecycleHook::RunStart => "run_start",
            LifecycleHook::BeforeModel => "before_model",
            LifecycleHook::BeforeTool => "before_tool",
            LifecycleHook::AfterTool => "after_tool",
            LifecycleHook::AfterTurn => "after_turn",
            LifecycleHook::RunEnd => "run_end",
        };
        self.execute_hook(name, payload, cwd, cancellation).await
    }
}

impl ConnectedHost {
    async fn request(&self, request: HubToHost) -> Result<HostReply> {
        self.request_inner(request, None, None).await
    }

    async fn request_with_events(
        &self,
        request: HubToHost,
        events: mpsc::Sender<AcpEvent>,
    ) -> Result<HostReply> {
        self.request_inner(request, Some(events), None).await
    }

    async fn request_inner(
        &self,
        request: HubToHost,
        acp_events: Option<mpsc::Sender<AcpEvent>>,
        cancel_tool: Option<RequestId>,
    ) -> Result<HostReply> {
        let request = mews_protocol::decode(&mews_protocol::encode(request)?)?;
        let request_id = match &request {
            HubToHost::ExecuteTool { request_id, .. }
            | HubToHost::ExecuteHook { request_id, .. }
            | HubToHost::AttestDirectory { request_id, .. } => request_id.clone(),
            HubToHost::SynchronizeAgent { request_id, .. } => request_id.clone(),
            HubToHost::ReadAgentReplica { request_id, .. } => request_id.clone(),
            HubToHost::ReadProjectContext { request_id, .. } => request_id.clone(),
            HubToHost::ReadPrompt { request_id, .. } => request_id.clone(),
            HubToHost::RefreshHarnessCatalog { request_id } => request_id.clone(),
            HubToHost::RunAcp { request_id, .. } => request_id.clone(),
            HubToHost::CancelAcp { .. } => {
                bail!("ACP cancellation is not a correlated Host request")
            }
            HubToHost::CancelTool { .. } => {
                bail!("tool cancellation is not a correlated Host request")
            }
            HubToHost::BeginHubTransfer { request_id, .. }
            | HubToHost::WriteHubTransfer { request_id, .. }
            | HubToHost::CommitHubTransfer { request_id }
            | HubToHost::ArmHubTransfer { request_id, .. }
            | HubToHost::ActivateHubTransfer { request_id } => request_id.clone(),
            HubToHost::ConfigureRelay { request_id, .. }
            | HubToHost::UpdateRelayCandidates { request_id, .. } => request_id.clone(),
            HubToHost::Ping { .. } => bail!("Ping is not a correlated Host request"),
            HubToHost::AcknowledgeAcpSessionBinding { .. } => {
                bail!("ACP Session acknowledgements are not correlated Host requests")
            }
        };
        let _permit = self.capacity.acquire().await.context("Host link closed")?;
        let (sender, receiver) = oneshot::channel();
        self.pending
            .lock()
            .expect("Host pending requests poisoned")
            .insert(
                request_id.clone(),
                PendingRequest {
                    reply: sender,
                    acp_events,
                },
            );
        let _completion = PendingCompletion {
            request_id,
            pending: Arc::clone(&self.pending),
        };
        self.sender
            .send(request)
            .await
            .context("Host disconnected")?;
        let mut cancellation = cancel_tool.map(|request_id| ToolCancellationGuard {
            sender: self.sender.clone(),
            request_id,
            armed: true,
        });
        let result = tokio::time::timeout(std::time::Duration::from_secs(3605), receiver)
            .await
            .context("Host request timed out")?
            .context("Host disconnected before replying")?
            .map_err(anyhow::Error::msg);
        if let Some(cancellation) = &mut cancellation {
            cancellation.armed = false;
        }
        result
    }
}

struct ToolCancellationGuard {
    sender: mpsc::Sender<HubToHost>,
    request_id: RequestId,
    armed: bool,
}

pub struct CancellationRegistryOwner {
    cancellations: Arc<std::sync::Mutex<HashMap<RequestId, CancellationToken>>>,
}

impl CancellationRegistryOwner {
    pub fn new(
        cancellations: Arc<std::sync::Mutex<HashMap<RequestId, CancellationToken>>>,
    ) -> Self {
        Self { cancellations }
    }
}

impl Drop for CancellationRegistryOwner {
    fn drop(&mut self) {
        for (_, cancellation) in self
            .cancellations
            .lock()
            .expect("Host cancellations poisoned")
            .drain()
        {
            cancellation.cancel();
        }
    }
}

impl Drop for ToolCancellationGuard {
    fn drop(&mut self) {
        if self.armed {
            let sender = self.sender.clone();
            let request_id = self.request_id.clone();
            tokio::spawn(async move {
                let _ = sender.send(HubToHost::CancelTool { request_id }).await;
            });
        }
    }
}

struct PendingCompletion {
    request_id: RequestId,
    pending: PendingRequests,
}

impl Drop for PendingCompletion {
    fn drop(&mut self) {
        self.pending
            .lock()
            .expect("Host pending requests poisoned")
            .remove(&self.request_id);
    }
}

async fn serve_host(
    registry: ToolRegistry,
    harnesses: Vec<HarnessDescriptor>,
    mut receiver: mpsc::Receiver<HubToHost>,
    sender: mpsc::Sender<HostToHub>,
) {
    if sender
        .send(HostToHub::Ready {
            tools: registry.definitions(),
            harnesses,
        })
        .await
        .is_err()
    {
        return;
    }
    let mut catalog = registry.subscribe();
    let binding_waiters: crate::AcpBindingWaiters = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let acp_cancellations = Arc::new(std::sync::Mutex::new(
        HashMap::<RequestId, CancellationToken>::new(),
    ));
    let tool_cancellations = Arc::new(std::sync::Mutex::new(
        HashMap::<RequestId, CancellationToken>::new(),
    ));
    let _acp_cancellation_owner = CancellationRegistryOwner::new(Arc::clone(&acp_cancellations));
    let _tool_cancellation_owner = CancellationRegistryOwner::new(Arc::clone(&tool_cancellations));
    loop {
        tokio::select! {
            message = receiver.recv() => {
                let Some(message) = message else { return; };
                if let HubToHost::AcknowledgeAcpSessionBinding { acknowledgement_id } = message {
                    if let Some(waiter) = binding_waiters.lock().expect("ACP binding waiters poisoned").remove(&acknowledgement_id) {
                        let _ = waiter.send(());
                    }
                    continue;
                }
                if let HubToHost::CancelAcp { request_id } = message {
                    if let Some(cancellation) = acp_cancellations
                        .lock()
                        .expect("ACP cancellations poisoned")
                        .remove(&request_id)
                    {
                        cancellation.cancel();
                    }
                    continue;
                }
                if let HubToHost::CancelTool { request_id } = message {
                    if let Some(cancellation) = tool_cancellations
                        .lock()
                        .expect("tool cancellations poisoned")
                        .remove(&request_id)
                    {
                        cancellation.cancel();
                    }
                    continue;
                }
                if matches!(message, HubToHost::ExecuteTool { .. } | HubToHost::ExecuteHook { .. }) {
                    let request_id = match &message {
                        HubToHost::ExecuteTool { request_id, .. }
                        | HubToHost::ExecuteHook { request_id, .. } => request_id.clone(),
                        _ => unreachable!(),
                    };
                    let cancellation = CancellationToken::new();
                    tool_cancellations.lock().expect("tool cancellations poisoned")
                        .insert(request_id.clone(), cancellation.clone());
                    let registry = registry.clone();
                    let sender = sender.clone();
                    let cancellations = Arc::clone(&tool_cancellations);
                    tokio::spawn(async move {
                        let response = handle_host_request_streaming(
                            &registry, registry.root(), message, None, None,
                            Some(cancellation),
                        ).await;
                        cancellations.lock().expect("tool cancellations poisoned")
                            .remove(&request_id);
                        let _ = sender.send(response).await;
                    });
                    continue;
                }
                if matches!(message, HubToHost::RunAcp { .. }) {
                    let request_id = match &message {
                        HubToHost::RunAcp { request_id, .. } => request_id.clone(),
                        _ => unreachable!(),
                    };
                    let cancellation = CancellationToken::new();
                    acp_cancellations
                        .lock()
                        .expect("ACP cancellations poisoned")
                        .insert(request_id.clone(), cancellation.clone());
                    let registry = registry.clone();
                    let sender = sender.clone();
                    let binding_waiters = Arc::clone(&binding_waiters);
                    let cancellations = Arc::clone(&acp_cancellations);
                    tokio::spawn(async move {
                        let (event_sender, mut event_receiver) =
                            mpsc::channel(super::ACP_EVENT_CHANNEL_CAPACITY);
                        let response = handle_host_request_streaming(
                            &registry,
                            registry.root(),
                            message,
                            Some(event_sender),
                            Some(binding_waiters),
                            Some(cancellation),
                        );
                        tokio::pin!(response);
                        let response = loop {
                            tokio::select! {
                                response = &mut response => break response,
                                event = event_receiver.recv() => {
                                    if let Some(event) = event
                                        && sender.send(event).await.is_err()
                                    { return; }
                                }
                            }
                        };
                        while let Ok(event) = event_receiver.try_recv() {
                            if sender.send(event).await.is_err() { return; }
                        }
                        let _ = sender.send(response).await;
                        cancellations
                            .lock()
                            .expect("ACP cancellations poisoned")
                            .remove(&request_id);
                    });
                    continue;
                }
                let (event_sender, mut event_receiver) =
                    mpsc::channel(super::ACP_EVENT_CHANNEL_CAPACITY);
                let response = handle_host_request_streaming(&registry, registry.root(), message, Some(event_sender), Some(Arc::clone(&binding_waiters)), None);
                tokio::pin!(response);
                let response = loop {
                    tokio::select! {
                        response = &mut response => break response,
                        event = event_receiver.recv() => {
                            if let Some(event) = event
                                && sender.send(event).await.is_err()
                            { return; }
                        }
                    }
                };
                while let Ok(event) = event_receiver.try_recv() {
                    if sender.send(event).await.is_err() { return; }
                }
                if sender.send(response).await.is_err() { return; }
            }
            changed = catalog.changed() => {
                if changed.is_err() { return; }
                let tools = catalog.borrow().clone();
                if sender.send(HostToHub::ToolCatalogChanged {
                    tools,
                }).await.is_err() { return; }
            }
        }
    }
}

/// Runs the Host half of the authenticated serialized Host protocol.
pub async fn run_host_rpc(
    mut peer: mews_transport::EncryptedRelayPeer,
    registry: ToolRegistry,
) -> Result<()> {
    let harnesses = crate::HarnessCatalog::discover(registry.root())?.descriptors();
    let (requests, receiver) = mpsc::channel(32);
    let (sender, mut responses) = mpsc::channel(32);
    tokio::spawn(serve_host(registry, harnesses, receiver, sender));
    loop {
        tokio::select! {
            request = peer.receive_bytes() => {
                let request = mews_protocol::decode(&request?)?;
                requests.send(request).await.context("Host request loop closed")?;
            }
            response = responses.recv() => {
                let Some(response) = response else { return Ok(()); };
                peer.send_bytes(&mews_protocol::encode(response)?).await?;
            }
        }
    }
}
