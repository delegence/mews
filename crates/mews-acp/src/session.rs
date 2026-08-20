//! Persistent ACP session execution and recovery.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    ffi::OsString,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak, mpsc as std_mpsc},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    process::Child,
};

use mews_protocol::{
    AcpBindingTransition, AcpInstructionChannel, AcpReplacementReason, AcpStopReason, AcpTimings,
};

use crate::{
    mcp::{TurnMcpBridge, TurnMcpHttp},
    process::{AcpHarnessConfig, AcpProcess, ProcessTreeGuard, terminate_process_tree},
    rpc::{AcpCancelled, AcpErrorKind, RpcClient, classify_error, is_resource_not_found},
    updates::UpdateState,
};

// ACP v1 uses a negotiated integer protocol version. The date-shaped value
// belongs to MCP, not ACP.
const ACP_PROTOCOL_VERSION: u16 = 1;
const CODEX_RESTART_FOR_RECOVERY: &str = "mews: restart Codex adapter for ACP recovery";

/// Bounded ACP discovery output. Hosts normalize this into their public
/// catalog and persist it, so clients never start an adapter just to redraw a
/// selector.
#[derive(Clone, Debug)]
pub struct AcpProbe {
    pub initialize: Value,
    pub session: Option<Value>,
    pub session_error: Option<String>,
    pub session_error_kind: Option<AcpErrorKind>,
    pub timings: AcpProbeTimings,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AcpProbeTimings {
    pub spawn: Duration,
    pub initialize: Duration,
    pub session: Duration,
}

pub enum AcpStreamEvent {
    /// The `session/prompt` request has been flushed to the Harness stdin.
    PromptDispatched {
        event_key: mews_protocol::AcpEventKey,
        session_id: String,
    },
    AssistantDelta {
        event_key: mews_protocol::AcpEventKey,
        delta: String,
        message_id: Option<String>,
        raw: Value,
    },
    ReasoningDelta {
        event_key: mews_protocol::AcpEventKey,
        delta: String,
        message_id: Option<String>,
        raw: Value,
    },
    ToolActivity {
        event_key: mews_protocol::AcpEventKey,
        call_id: String,
        title: String,
        kind: Option<String>,
        status: Option<String>,
        input: Value,
    },
    ProviderState {
        event_key: mews_protocol::AcpEventKey,
        data: Value,
    },
    SessionBound {
        event_key: mews_protocol::AcpEventKey,
        session_id: String,
        transition: AcpBindingTransition,
    },
    /// The initialization channel has crossed the provider boundary.  This
    /// is deliberately separate from SessionBound for FirstPrompt adapters.
    ContextDispatched {
        event_key: mews_protocol::AcpEventKey,
        session_id: String,
    },
    HookOutcome {
        event_key: mews_protocol::AcpEventKey,
        hook: String,
        ok: bool,
        detail: Option<String>,
        tool: Option<String>,
        call_id: Option<String>,
    },
}

fn accepted_event_key() -> mews_protocol::AcpEventKey {
    // This process is the ACP ingress boundary. A new accepted notification
    // gets one identity here; forwarding and durable replay preserve it.
    uuid::Uuid::now_v7().to_string()
}

/// Stable MEWS identifiers and the initialization boundary supplied by the
/// caller.  ACP owns the exact prompt boundary; callers must not pre-run it.
#[derive(Clone, Debug)]
pub struct AcpHookMetadata {
    pub mews_session_id: String,
    pub turn_id: String,
    pub harness: String,
    pub context_hash: String,
    pub context_channel: AcpInstructionChannel,
    pub invoke_turn_start: bool,
}

#[derive(Clone, Debug)]
pub struct AcpSessionRequest {
    pub agent_id: mews_protocol::AgentId,
    pub agent_slug: String,
    pub transition: AcpBindingTransition,
    pub prompt: String,
    pub recovery_prompt: String,
    pub context_text: String,
    pub instruction_channel: AcpInstructionChannel,
    pub skills: Vec<crate::mcp::AcpSkill>,
    pub hook_metadata: Option<AcpHookMetadata>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcpSessionOutcome {
    pub answer: String,
    pub session_id: String,
    pub session_replaced: bool,
    pub stop_reason: AcpStopReason,
    pub timings: AcpTimings,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InitializeResult {
    protocol_version: u16,
    #[serde(default)]
    agent_capabilities: Value,
}

/// One initialized ACP transport. Sessions are resumed on every Turn so their
/// Turn-scoped MCP server can be refreshed, but the expensive agent process is
/// kept alive between Turns.
struct LiveAcpProcess {
    process: AcpProcess,
    cwd: PathBuf,
    child: Child,
    writer: tokio::process::ChildStdin,
    reader: BufReader<tokio::process::ChildStdout>,
    initialize: InitializeResult,
    next_request_id: Arc<std::sync::atomic::AtomicU64>,
    session_ids: BTreeSet<String>,
}

impl LiveAcpProcess {
    async fn start(
        config: AcpHarnessConfig,
        cwd: &Path,
        cancellation: &mews_agent::CancellationToken,
    ) -> Result<(Self, AcpTimings)> {
        let process = AcpProcess::new(config);
        let spawn_started = tokio::time::Instant::now();
        let mut child = process.spawn(&cwd.to_path_buf())?;
        let spawn_ms = spawn_started.elapsed().as_millis() as u64;
        let writer = child
            .stdin
            .take()
            .context("ACP Harness did not open stdin")?;
        let reader = BufReader::new(
            child
                .stdout
                .take()
                .context("ACP Harness did not open stdout")?,
        );
        let mut live = Self {
            process,
            cwd: cwd.to_owned(),
            child,
            writer,
            reader,
            initialize: InitializeResult {
                protocol_version: 0,
                agent_capabilities: Value::Null,
            },
            next_request_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            session_ids: BTreeSet::new(),
        };
        let initialize_started = tokio::time::Instant::now();
        let mut rpc = live.rpc();
        let initialize = rpc
            .request(
                "initialize",
                json!({
                    "protocolVersion": ACP_PROTOCOL_VERSION,
                    "clientInfo": { "name": "mews", "version": env!("CARGO_PKG_VERSION") },
                    "clientCapabilities": { "auth": { "terminal": true } },
                }),
                cancellation,
                None,
                None,
                |_| Ok(()),
            )
            .await?;
        drop(rpc);
        let initialize_ms = initialize_started.elapsed().as_millis() as u64;
        live.initialize =
            serde_json::from_value(initialize).context("invalid ACP initialize result")?;
        if live.initialize.protocol_version != ACP_PROTOCOL_VERSION {
            bail!(
                "ACP Harness negotiated unsupported protocol version {}",
                live.initialize.protocol_version
            );
        }
        Ok((
            live,
            AcpTimings {
                queue_ms: 0,
                spawn_ms,
                initialize_ms,
                continuation_ms: 0,
                prompt_to_first_update_ms: None,
                prompt_to_first_token_ms: None,
                prompt_ms: 0,
                total_ms: 0,
            },
        ))
    }

    fn rpc(&mut self) -> RpcClient<'_, tokio::process::ChildStdin> {
        RpcClient::new_with_next_id(
            &mut self.writer,
            &mut self.reader,
            self.process.config.request_timeout,
            self.process.config.permission_handler.as_ref(),
            self.next_request_id.clone(),
        )
    }

    fn is_compatible_with(&self, config: &AcpHarnessConfig, cwd: &Path) -> bool {
        self.cwd == cwd
            && self.process.config.command == config.command
            && self.process.config.environment == config.environment
            && self.process.config.request_timeout == config.request_timeout
    }

    async fn shutdown(mut self) {
        if self
            .initialize
            .agent_capabilities
            .pointer("/sessionCapabilities/close")
            .is_some_and(Value::is_object)
            && !self.session_ids.is_empty()
        {
            let cancellation = mews_agent::CancellationToken::new();
            let session_ids = std::mem::take(&mut self.session_ids);
            let _ = tokio::time::timeout(Duration::from_secs(2), async {
                let mut rpc = self.rpc();
                for session_id in session_ids {
                    let _ = rpc
                        .request(
                            "session/close",
                            json!({ "sessionId": session_id }),
                            &cancellation,
                            None,
                            None,
                            |_| Ok(()),
                        )
                        .await;
                }
            })
            .await;
        }
        let _ = tokio::io::AsyncWriteExt::shutdown(&mut self.writer).await;
        if tokio::time::timeout(Duration::from_secs(2), self.child.wait())
            .await
            .is_err()
        {
            terminate_process_tree(&mut self.child).await;
        }
    }

    async fn terminate(mut self) {
        terminate_process_tree(&mut self.child).await;
    }
}

impl Drop for LiveAcpProcess {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(process_id) = self.child.id() {
            // SAFETY: ACP children are spawned in their own process group.
            unsafe { libc::kill(-(process_id as i32), libc::SIGKILL) };
        }
        let _ = self.child.start_kill();
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PromptResult {
    stop_reason: AcpStopReason,
}

#[derive(Clone, Copy, Debug)]
enum ContinuationMethod {
    Resume,
    Load,
}

pub trait AcpEventSink {
    fn emit(&mut self, event: AcpStreamEvent) -> Result<()>;
}

impl<F> AcpEventSink for F
where
    F: FnMut(AcpStreamEvent) -> Result<()>,
{
    fn emit(&mut self, event: AcpStreamEvent) -> Result<()> {
        self(event)
    }
}

pub struct AcpTurnRequest<'a> {
    pub config: AcpHarnessConfig,
    pub cwd: PathBuf,
    pub harness_options: BTreeMap<String, String>,
    pub session: AcpSessionRequest,
    pub environment: &'a dyn mews_agent::AgentCapabilities,
    pub allowed_tools: &'a [String],
    pub cancellation: mews_agent::CancellationToken,
    pub events: &'a mut dyn AcpEventSink,
}

/// A Turn executed through a warm process shared by compatible ACP Sessions.
/// The callback remains on the caller's thread so binding acknowledgements
/// keep their existing ordering and durability guarantees.
pub struct PersistentAcpTurnRequest<'a> {
    /// Stable logical Session identity used for sticky pool routing.
    pub session_key: String,
    pub config: AcpHarnessConfig,
    pub cwd: PathBuf,
    pub harness_options: BTreeMap<String, String>,
    pub session: AcpSessionRequest,
    pub environment: Arc<dyn mews_agent::AgentCapabilities>,
    pub allowed_tools: Vec<String>,
    pub cancellation: mews_agent::CancellationToken,
    pub events: &'a mut dyn AcpEventSink,
}

/// Immutable process-start inputs. Only Sessions with the same fingerprint
/// may share a warm ACP process.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct AcpLaunchFingerprint {
    command: Vec<OsString>,
    environment: Vec<(OsString, OsString)>,
    cwd: PathBuf,
    request_timeout: Duration,
}

impl AcpLaunchFingerprint {
    fn new(config: &AcpHarnessConfig, cwd: &Path, session: &AcpSessionRequest) -> Self {
        // Fingerprint the effective launch configuration, not the recipe
        // input. Preparation errors are handled inside the Turn lifecycle.
        let mut config = config.clone();
        let _ = prepare_instruction_channel(&mut config, session);
        Self {
            command: config.command,
            environment: config.environment.into_iter().collect(),
            cwd: cwd.to_owned(),
            request_timeout: config.request_timeout,
        }
    }
}

struct PersistentEvent {
    event: AcpStreamEvent,
    acknowledged: std_mpsc::Sender<std::result::Result<(), String>>,
}

struct PersistentJob {
    state: Arc<std::sync::atomic::AtomicU8>,
    enqueued_at: std::time::Instant,
    config: AcpHarnessConfig,
    cwd: PathBuf,
    harness_options: BTreeMap<String, String>,
    session: AcpSessionRequest,
    environment: Arc<dyn mews_agent::AgentCapabilities>,
    allowed_tools: Vec<String>,
    cancellation: mews_agent::CancellationToken,
    events: tokio::sync::mpsc::UnboundedSender<PersistentEvent>,
    result: tokio::sync::oneshot::Sender<Result<AcpSessionOutcome>>,
}

const JOB_QUEUED: u8 = 0;
const JOB_RUNNING: u8 = 1;
const JOB_CANCELLED: u8 = 2;

enum PersistentCommand {
    Execute(Box<PersistentJob>),
    Shutdown,
}

/// Cancels worker-owned execution if its caller stops awaiting the Turn.
struct CancelOnDrop {
    cancellation: mews_agent::CancellationToken,
    job_state: Arc<std::sync::atomic::AtomicU8>,
    armed: bool,
}

impl CancelOnDrop {
    fn new(
        cancellation: mews_agent::CancellationToken,
        job_state: Arc<std::sync::atomic::AtomicU8>,
    ) -> Self {
        Self {
            cancellation,
            job_state,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.job_state.compare_exchange(
                JOB_QUEUED,
                JOB_CANCELLED,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            );
            self.cancellation.cancel();
        }
    }
}

#[derive(Clone)]
struct PersistentWorker {
    generation: u64,
    sender: std_mpsc::Sender<PersistentCommand>,
    accepting: Arc<Mutex<bool>>,
    pending: Arc<std::sync::atomic::AtomicUsize>,
}

impl PersistentWorker {
    fn send(&self, job: Box<PersistentJob>) -> std::result::Result<(), Box<PersistentJob>> {
        let accepting = self.accepting.lock().expect("ACP worker state poisoned");
        if !*accepting {
            return Err(job);
        }
        self.pending
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.sender
            .send(PersistentCommand::Execute(job))
            .map_err(|error| {
                self.pending
                    .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                match error.0 {
                    PersistentCommand::Execute(job) => job,
                    PersistentCommand::Shutdown => unreachable!("sent an execute command"),
                }
            })
    }

    fn stop_accepting(&self) {
        let mut accepting = self.accepting.lock().expect("ACP worker state poisoned");
        if std::mem::replace(&mut *accepting, false) {
            let _ = self.sender.send(PersistentCommand::Shutdown);
        }
    }

    fn is_accepting(&self) -> bool {
        *self.accepting.lock().expect("ACP worker state poisoned")
    }

    fn pending(&self) -> usize {
        self.pending.load(std::sync::atomic::Ordering::Relaxed)
    }
}

const ACP_PROCESS_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10 * 60);
const ACP_MAX_PROCESSES_PER_FINGERPRINT: usize = 4;
const ACP_MAX_TOTAL_PROCESSES: usize = 8;

#[derive(Clone)]
pub struct AcpRuntimePool {
    inner: Arc<AcpRuntimePoolInner>,
}

struct AcpRuntimePoolInner {
    groups: Mutex<HashMap<AcpLaunchFingerprint, ProcessGroup>>,
    next_generation: std::sync::atomic::AtomicU64,
    idle_timeout: Duration,
    max_processes_per_fingerprint: usize,
    max_total_processes: usize,
    capacity_changed: tokio::sync::Notify,
}

#[derive(Default)]
struct ProcessGroup {
    workers: Vec<PersistentWorker>,
    session_bindings: HashMap<String, u64>,
}

impl Default for AcpRuntimePool {
    fn default() -> Self {
        Self::new(ACP_PROCESS_IDLE_TIMEOUT)
    }
}

impl AcpRuntimePool {
    pub fn new(idle_timeout: Duration) -> Self {
        Self::with_limits(
            idle_timeout,
            ACP_MAX_PROCESSES_PER_FINGERPRINT,
            ACP_MAX_TOTAL_PROCESSES,
        )
    }

    pub fn with_max_processes(idle_timeout: Duration, max_processes: usize) -> Self {
        Self::with_limits(idle_timeout, max_processes, max_processes)
    }

    fn with_limits(
        idle_timeout: Duration,
        max_processes_per_fingerprint: usize,
        max_total_processes: usize,
    ) -> Self {
        Self {
            inner: Arc::new(AcpRuntimePoolInner {
                groups: Mutex::new(HashMap::new()),
                next_generation: std::sync::atomic::AtomicU64::new(1),
                idle_timeout,
                max_processes_per_fingerprint: max_processes_per_fingerprint.max(1),
                max_total_processes: max_total_processes.max(1),
                capacity_changed: tokio::sync::Notify::new(),
            }),
        }
    }

    async fn worker(
        &self,
        fingerprint: &AcpLaunchFingerprint,
        session_key: &str,
    ) -> Result<PersistentWorker> {
        loop {
            // Register before checking capacity so a completion between the
            // check and await cannot strand this request.
            let notified = self.inner.capacity_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let selection = self.try_worker(fingerprint, session_key)?;
            if let Some(worker) = selection {
                return Ok(worker);
            }
            notified.await;
        }
    }

    fn try_worker(
        &self,
        fingerprint: &AcpLaunchFingerprint,
        session_key: &str,
    ) -> Result<Option<PersistentWorker>> {
        let mut groups = self.inner.groups.lock().expect("ACP pool poisoned");
        for group in groups.values_mut() {
            group.workers.retain(PersistentWorker::is_accepting);
            let live_generations = group
                .workers
                .iter()
                .map(|worker| worker.generation)
                .collect::<Vec<_>>();
            group
                .session_bindings
                .retain(|_, generation| live_generations.contains(generation));
        }
        groups.retain(|_, group| !group.workers.is_empty());

        if let Some(group) = groups.get_mut(fingerprint) {
            if let Some(generation) = group.session_bindings.get(session_key)
                && let Some(worker) = group
                    .workers
                    .iter()
                    .find(|worker| worker.generation == *generation)
            {
                return Ok(Some(worker.clone()));
            }
            group.session_bindings.remove(session_key);
            let least_loaded = group
                .workers
                .iter()
                .min_by_key(|worker| worker.pending())
                .cloned();
            if let Some(worker) = &least_loaded
                && (worker.pending() == 0
                    || group.workers.len() >= self.inner.max_processes_per_fingerprint)
            {
                group
                    .session_bindings
                    .insert(session_key.to_owned(), worker.generation);
                return Ok(Some(worker.clone()));
            }
        }

        let process_count = groups
            .values()
            .map(|group| group.workers.len())
            .sum::<usize>();
        if process_count >= self.inner.max_total_processes {
            let idle = groups.iter().find_map(|(key, group)| {
                group
                    .workers
                    .iter()
                    .find(|worker| worker.pending() == 0)
                    .map(|worker| (key.clone(), worker.clone()))
            });
            let Some((idle_fingerprint, idle_worker)) = idle else {
                // A compatible busy worker can queue this Turn without
                // increasing the resident process count. A new fingerprint
                // waits until an existing worker becomes idle.
                if let Some(worker) = groups.get_mut(fingerprint).and_then(|group| {
                    group
                        .workers
                        .iter()
                        .min_by_key(|worker| worker.pending())
                        .cloned()
                }) {
                    groups
                        .get_mut(fingerprint)
                        .expect("compatible group exists")
                        .session_bindings
                        .insert(session_key.to_owned(), worker.generation);
                    return Ok(Some(worker));
                }
                return Ok(None);
            };
            idle_worker.stop_accepting();
            if let Some(group) = groups.get_mut(&idle_fingerprint) {
                group
                    .workers
                    .retain(|worker| worker.generation != idle_worker.generation);
                group
                    .session_bindings
                    .retain(|_, generation| *generation != idle_worker.generation);
            }
            groups.retain(|_, group| !group.workers.is_empty());
            return Ok(None);
        }

        let worker = self.spawn_worker(fingerprint)?;
        let group = groups.entry(fingerprint.clone()).or_default();
        group.workers.push(worker.clone());
        group
            .session_bindings
            .insert(session_key.to_owned(), worker.generation);
        Ok(Some(worker))
    }

    fn spawn_worker(&self, fingerprint: &AcpLaunchFingerprint) -> Result<PersistentWorker> {
        let generation = self
            .inner
            .next_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        spawn_persistent_worker(
            fingerprint.clone(),
            generation,
            self.inner.idle_timeout,
            Arc::downgrade(&self.inner),
        )
    }

    pub async fn execute_turn(
        &self,
        request: PersistentAcpTurnRequest<'_>,
    ) -> Result<AcpSessionOutcome> {
        execute_persistent_acp_turn(self, request).await
    }

    #[cfg(test)]
    fn process_count(&self) -> usize {
        self.inner
            .groups
            .lock()
            .expect("ACP pool poisoned")
            .values()
            .map(|group| group.workers.len())
            .sum()
    }
}

fn spawn_persistent_worker(
    fingerprint: AcpLaunchFingerprint,
    generation: u64,
    idle_timeout: Duration,
    pool: Weak<AcpRuntimePoolInner>,
) -> Result<PersistentWorker> {
    let (sender, receiver) = std_mpsc::channel::<PersistentCommand>();
    let (startup_sender, startup_receiver) = std_mpsc::sync_channel(1);
    let accepting = Arc::new(Mutex::new(true));
    let worker_accepting = accepting.clone();
    let pending = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let worker_pending = pending.clone();
    std::thread::Builder::new()
        .name(format!("mews-acp-{generation}"))
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = startup_sender.send(Err(error.to_string()));
                    return;
                }
            };
            if startup_sender.send(Ok(())).is_err() {
                return;
            }
            let mut live: Option<LiveAcpProcess> = None;
            loop {
                let command = match receiver.recv_timeout(idle_timeout) {
                    Ok(command) => command,
                    Err(std_mpsc::RecvTimeoutError::Timeout) => {
                        // Serialize the idle transition with enqueueing. A job
                        // accepted just as the timer fires must be observed;
                        // later callers are redirected to a new generation.
                        let mut accepting =
                            worker_accepting.lock().expect("ACP worker state poisoned");
                        match receiver.try_recv() {
                            Ok(command) => {
                                drop(accepting);
                                command
                            }
                            Err(std_mpsc::TryRecvError::Empty) => {
                                *accepting = false;
                                break;
                            }
                            Err(std_mpsc::TryRecvError::Disconnected) => break,
                        }
                    }
                    Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
                };
                let job = match command {
                    PersistentCommand::Execute(job) => job,
                    PersistentCommand::Shutdown => break,
                };
                let PersistentJob {
                    state,
                    enqueued_at,
                    config,
                    cwd,
                    harness_options,
                    session,
                    environment,
                    allowed_tools,
                    cancellation,
                    events,
                    result,
                } = *job;
                if state
                    .compare_exchange(
                        JOB_QUEUED,
                        JOB_RUNNING,
                        std::sync::atomic::Ordering::AcqRel,
                        std::sync::atomic::Ordering::Acquire,
                    )
                    .is_err()
                {
                    worker_pending.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    pool.upgrade()
                        .iter()
                        .for_each(|pool| pool.capacity_changed.notify_waiters());
                    let _ = result.send(Err(anyhow::anyhow!(AcpCancelled)));
                    continue;
                }
                let cancelled_before_start = cancellation.is_cancelled();
                let queue_ms = enqueued_at.elapsed().as_millis() as u64;
                let mut outcome = runtime.block_on(execute_acp_turn_inner_cached(
                    config,
                    cwd,
                    harness_options,
                    session,
                    environment.as_ref(),
                    &allowed_tools,
                    cancellation,
                    &mut |event| {
                        let (acknowledge, acknowledged) = std_mpsc::channel();
                        let sent = events.send(PersistentEvent {
                            event,
                            acknowledged: acknowledge,
                        });
                        if sent.is_err() && cancelled_before_start {
                            return Ok(());
                        }
                        sent.map_err(|_| anyhow::anyhow!("ACP event receiver unavailable"))?;
                        acknowledged
                            .recv()
                            .map_err(|_| anyhow::anyhow!("ACP event acknowledgement closed"))?
                            .map_err(anyhow::Error::msg)
                    },
                    Some(&mut live),
                ));
                if let Ok(outcome) = &mut outcome {
                    outcome.timings.queue_ms = queue_ms;
                } else if !cancelled_before_start
                    || !outcome.as_ref().is_err_and(crate::rpc::is_cancelled)
                {
                    live.take();
                }
                worker_pending.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                pool.upgrade()
                    .iter()
                    .for_each(|pool| pool.capacity_changed.notify_waiters());
                let _ = result.send(outcome);
            }
            *worker_accepting.lock().expect("ACP worker state poisoned") = false;
            let pool = pool.upgrade();
            if let Some(pool) = &pool {
                let mut groups = pool.groups.lock().expect("ACP pool poisoned");
                let remove_group = if let Some(group) = groups.get_mut(&fingerprint) {
                    group
                        .workers
                        .retain(|worker| worker.generation != generation);
                    group
                        .session_bindings
                        .retain(|_, bound| *bound != generation);
                    group.workers.is_empty()
                } else {
                    false
                };
                if remove_group {
                    groups.remove(&fingerprint);
                }
            }
            if let Some(live) = live.take() {
                runtime.block_on(live.shutdown());
            }
            if let Some(pool) = pool {
                pool.capacity_changed.notify_waiters();
            }
        })
        .context("start persistent ACP worker")?;
    // Do not publish a sender until the worker has a live runtime and receiver.
    startup_receiver
        .recv()
        .context("persistent ACP worker stopped during startup")?
        .map_err(anyhow::Error::msg)?;
    Ok(PersistentWorker {
        generation,
        sender,
        accepting,
        pending,
    })
}

async fn execute_persistent_acp_turn(
    pool: &AcpRuntimePool,
    request: PersistentAcpTurnRequest<'_>,
) -> Result<AcpSessionOutcome> {
    let PersistentAcpTurnRequest {
        session_key,
        config,
        cwd,
        harness_options,
        session,
        environment,
        allowed_tools,
        cancellation,
        events,
    } = request;
    if session_key.is_empty() {
        bail!("ACP pool Session key must not be empty");
    }
    let job_state = Arc::new(std::sync::atomic::AtomicU8::new(JOB_QUEUED));
    let mut cancel_on_drop = CancelOnDrop::new(cancellation.clone(), job_state.clone());
    let wait_cancellation = cancellation.clone();
    let cancellation_environment = environment.clone();
    let cancellation_agent_id = session.agent_id.clone();
    let cancellation_metadata = session.hook_metadata.clone();
    let cancellation_cwd = cwd.clone();
    let fingerprint = AcpLaunchFingerprint::new(&config, &cwd, &session);
    let (event_sender, mut event_receiver) = tokio::sync::mpsc::unbounded_channel();
    let (result_sender, mut result_receiver) = tokio::sync::oneshot::channel();
    let job = Box::new(PersistentJob {
        state: job_state.clone(),
        enqueued_at: std::time::Instant::now(),
        config,
        cwd,
        harness_options,
        session,
        environment,
        allowed_tools,
        cancellation: cancellation.clone(),
        events: event_sender,
        result: result_sender,
    });
    let mut job = job;
    loop {
        let worker = tokio::select! {
            worker = pool.worker(&fingerprint, &session_key) => worker?,
            _ = wait_cancellation.cancelled() => {
                cancel_on_drop.disarm();
                finalize_cancelled_before_start(
                    cancellation_environment.as_ref(),
                    &cancellation_agent_id,
                    cancellation_metadata.as_ref(),
                    &cancellation_cwd,
                    &wait_cancellation,
                    events,
                ).await?;
                return Err(anyhow::anyhow!(AcpCancelled));
            },
        };
        match worker.send(job) {
            Ok(()) => break,
            Err(returned) => job = returned,
        }
    }
    let mut events_open = true;
    let mut observe_cancellation = true;
    loop {
        tokio::select! {
            result = &mut result_receiver => {
                cancel_on_drop.disarm();
                return result.context("persistent ACP worker stopped")?;
            }
            _ = wait_cancellation.cancelled(), if observe_cancellation => {
                if job_state.compare_exchange(
                    JOB_QUEUED,
                    JOB_CANCELLED,
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                ).is_ok() {
                    cancel_on_drop.disarm();
                    finalize_cancelled_before_start(
                        cancellation_environment.as_ref(),
                        &cancellation_agent_id,
                        cancellation_metadata.as_ref(),
                        &cancellation_cwd,
                        &wait_cancellation,
                        events,
                    ).await?;
                    return Err(anyhow::anyhow!(AcpCancelled));
                }
                // Once running, the worker owns cleanup. Keep the event sink
                // alive until it has emitted the terminal hook observations.
                observe_cancellation = false;
            }
            event = event_receiver.recv(), if events_open => {
                if let Some(event) = event {
                    let outcome = events.emit(event.event).map_err(|error| error.to_string());
                    let _ = event.acknowledged.send(outcome);
                } else {
                    events_open = false;
                }
            }
        }
    }
}

async fn finalize_cancelled_before_start(
    environment: &dyn mews_agent::AgentCapabilities,
    agent_id: &mews_protocol::AgentId,
    metadata: Option<&AcpHookMetadata>,
    cwd: &Path,
    cancellation: &mews_agent::CancellationToken,
    events: &mut dyn AcpEventSink,
) -> Result<()> {
    let result = Err(anyhow::anyhow!(AcpCancelled));
    finalize_turn_hooks(
        environment,
        agent_id,
        metadata,
        &result,
        Vec::new(),
        cwd,
        cancellation,
        &mut |event| events.emit(event),
    )
    .await
}

pub async fn execute_acp_turn(request: AcpTurnRequest<'_>) -> Result<AcpSessionOutcome> {
    let AcpTurnRequest {
        config,
        cwd,
        harness_options,
        session,
        environment,
        allowed_tools,
        cancellation,
        events,
    } = request;
    let mut emit = |event| events.emit(event);
    execute_acp_turn_inner(
        config,
        cwd,
        harness_options,
        session,
        environment,
        allowed_tools,
        cancellation,
        &mut emit,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn execute_acp_turn_inner(
    config: AcpHarnessConfig,
    cwd: PathBuf,
    harness_options: BTreeMap<String, String>,
    session: AcpSessionRequest,
    environment: &dyn mews_agent::AgentCapabilities,
    allowed_tools: &[String],
    cancellation: mews_agent::CancellationToken,
    events: &mut dyn FnMut(AcpStreamEvent) -> Result<()>,
) -> Result<AcpSessionOutcome> {
    execute_acp_turn_inner_cached(
        config,
        cwd,
        harness_options,
        session,
        environment,
        allowed_tools,
        cancellation,
        events,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn execute_acp_turn_inner_cached(
    mut config: AcpHarnessConfig,
    cwd: PathBuf,
    harness_options: BTreeMap<String, String>,
    session: AcpSessionRequest,
    environment: &dyn mews_agent::AgentCapabilities,
    allowed_tools: &[String],
    cancellation: mews_agent::CancellationToken,
    events: &mut dyn FnMut(AcpStreamEvent) -> Result<()>,
    mut live_cache: Option<&mut Option<LiveAcpProcess>>,
) -> Result<AcpSessionOutcome> {
    let total_started = tokio::time::Instant::now();
    let replacement_config = config.clone();
    let replacement_session = session.clone();
    let agent_id = session.agent_id.clone();
    let hook_metadata = session.hook_metadata.clone();
    let mut observed_activities = Vec::new();
    let result = if cancellation.is_cancelled() {
        Err(anyhow::anyhow!(AcpCancelled))
    } else if let Err(error) = discard_exited_cached_process(live_cache.as_deref_mut()) {
        Err(error)
    } else {
        match invoke_turn_start(environment, &session, &cwd, &cancellation, events).await {
            Ok(()) => {
                execute_acp_attempt(
                    &mut config,
                    &cwd,
                    &harness_options,
                    session,
                    environment,
                    allowed_tools,
                    &cancellation,
                    events,
                    live_cache.as_deref_mut(),
                    &mut observed_activities,
                )
                .await
            }
            Err(error) => Err(error),
        }
    };
    if result
        .as_ref()
        .err()
        .is_some_and(|error| error.to_string().contains(CODEX_RESTART_FOR_RECOVERY))
    {
        if let Some(cache) = live_cache.as_deref_mut() {
            cache.take();
        }
        let mut replacement = replacement_session;
        replacement.transition = AcpBindingTransition::Replace {
            reason: AcpReplacementReason::ResourceNotFound,
        };
        if let Some(metadata) = &mut replacement.hook_metadata {
            metadata.invoke_turn_start = false;
        }
        let recovery = Box::pin(execute_acp_turn_inner_cached(
            replacement_config,
            cwd,
            harness_options,
            replacement,
            environment,
            allowed_tools,
            cancellation,
            events,
            live_cache,
        ))
        .await;
        return recovery.map(|mut outcome| {
            outcome.timings.total_ms = total_started.elapsed().as_millis() as u64;
            outcome
        });
    }
    finalize_turn_hooks(
        environment,
        &agent_id,
        hook_metadata.as_ref(),
        &result,
        observed_activities,
        &cwd,
        &cancellation,
        events,
    )
    .await?;
    if result.is_err()
        && let Some(cache) = live_cache
    {
        cache.take();
    }
    result
        .map(|mut outcome| {
            outcome.timings.total_ms = total_started.elapsed().as_millis() as u64;
            outcome
        })
        .context("ACP Harness failed")
}

fn discard_exited_cached_process(cache: Option<&mut Option<LiveAcpProcess>>) -> Result<()> {
    let Some(cache) = cache else {
        return Ok(());
    };
    let exited = cache
        .as_mut()
        .map(|live| live.child.try_wait())
        .transpose()
        .context("inspect cached ACP Harness")?
        .flatten()
        .is_some();
    if exited {
        cache.take();
    }
    Ok(())
}

async fn invoke_turn_start(
    environment: &dyn mews_agent::AgentCapabilities,
    session: &AcpSessionRequest,
    cwd: &Path,
    cancellation: &mews_agent::CancellationToken,
    events: &mut dyn FnMut(AcpStreamEvent) -> Result<()>,
) -> Result<()> {
    let Some(metadata) = session
        .hook_metadata
        .as_ref()
        .filter(|metadata| metadata.invoke_turn_start)
    else {
        return Ok(());
    };
    let payload = json!({
        "session_id": metadata.mews_session_id, "turn_id": metadata.turn_id,
        "harness": metadata.harness, "cwd": cwd, "binding": session.transition,
        "context_hash": metadata.context_hash, "context_channel": metadata.context_channel,
    });
    match environment
        .hook(
            &session.agent_id,
            mews_agent::LifecycleHook::TurnStart,
            payload,
            cwd,
            cancellation,
            None,
        )
        .await
    {
        Ok(_) => events(AcpStreamEvent::HookOutcome {
            event_key: accepted_event_key(),
            hook: "turn_start".into(),
            ok: true,
            detail: None,
            tool: None,
            call_id: None,
        }),
        Err(error) => {
            events(AcpStreamEvent::HookOutcome {
                event_key: accepted_event_key(),
                hook: "turn_start".into(),
                ok: false,
                detail: Some(bounded_detail(&error.to_string())),
                tool: None,
                call_id: None,
            })?;
            Err(error).context("ACP turn_start hook failed")
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_acp_attempt(
    config: &mut AcpHarnessConfig,
    cwd: &Path,
    harness_options: &BTreeMap<String, String>,
    session: AcpSessionRequest,
    environment: &dyn mews_agent::AgentCapabilities,
    allowed_tools: &[String],
    cancellation: &mews_agent::CancellationToken,
    events: &mut dyn FnMut(AcpStreamEvent) -> Result<()>,
    mut live_cache: Option<&mut Option<LiveAcpProcess>>,
    observed_activities: &mut Vec<Value>,
) -> Result<AcpSessionOutcome> {
    prepare_instruction_channel(config, &session)?;
    let incompatible = live_cache
        .as_deref()
        .and_then(|cache| cache.as_ref())
        .is_some_and(|live| !live.is_compatible_with(config, cwd));
    if incompatible && let Some(live) = live_cache.as_deref_mut().and_then(Option::take) {
        live.shutdown().await;
    }

    let owns_process = live_cache.is_none();
    let mut owned_live = None;
    let mut timings = AcpTimings {
        queue_ms: 0,
        spawn_ms: 0,
        initialize_ms: 0,
        continuation_ms: 0,
        prompt_to_first_update_ms: None,
        prompt_to_first_token_ms: None,
        prompt_ms: 0,
        total_ms: 0,
    };
    if let Some(cache) = live_cache.as_deref_mut()
        && cache.is_none()
    {
        let (live, started) = LiveAcpProcess::start(config.clone(), cwd, cancellation).await?;
        *cache = Some(live);
        timings = started;
    } else if live_cache.is_none() {
        let (live, started) = LiveAcpProcess::start(config.clone(), cwd, cancellation).await?;
        owned_live = Some(live);
        timings = started;
    }
    let live = match live_cache {
        Some(cache) => cache.as_mut().expect("live ACP process initialized"),
        None => owned_live.as_mut().expect("turn owns its ACP process"),
    };
    let result = AcpProcess::run_session_with_extensions(
        live,
        timings,
        cwd.to_path_buf(),
        harness_options.clone(),
        session,
        environment,
        allowed_tools,
        cancellation.clone(),
        &mut |event| {
            if observed_activities.len() < 64 {
                match &event {
                    AcpStreamEvent::ProviderState { data, .. } => observed_activities.push(json!({"type":"provider_state","data": crate::updates::bounded_json(data)})),
                    AcpStreamEvent::ToolActivity { call_id, title, status, .. } => observed_activities.push(json!({"type":"tool_activity","call_id":call_id,"title":title,"status":status})),
                    _ => {}
                }
            }
            events(event)
        },
    )
    .await;
    if owns_process && let Some(live) = owned_live.take() {
        if result.is_ok() {
            live.shutdown().await;
        } else {
            // A failed Harness can be stuck and unable to complete the graceful
            // shutdown protocol. Do not add the shutdown grace period to the
            // request deadline observed by the caller.
            live.terminate().await;
        }
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn finalize_turn_hooks(
    environment: &dyn mews_agent::AgentCapabilities,
    agent_id: &mews_protocol::AgentId,
    metadata: Option<&AcpHookMetadata>,
    result: &Result<AcpSessionOutcome>,
    observed_activities: Vec<Value>,
    cwd: &Path,
    cancellation: &mews_agent::CancellationToken,
    events: &mut dyn FnMut(AcpStreamEvent) -> Result<()>,
) -> Result<()> {
    let Some(metadata) = metadata else {
        return Ok(());
    };
    let after_step = if let Ok(outcome) = result {
        record_telemetry_hook(
            environment,
            agent_id,
            mews_agent::LifecycleHook::AfterStep,
            json!({"session_id": metadata.mews_session_id, "turn_id": metadata.turn_id,
                "acp_session_id": outcome.session_id, "answer": bounded_detail(&outcome.answer),
                "stop_reason": outcome.stop_reason, "activities": observed_activities}),
            cwd,
            cancellation,
            events,
        )
        .await
    } else {
        Ok(())
    };
    let (status, detail) = match result {
        Ok(outcome) => ("succeeded", Some(bounded_detail(&outcome.answer))),
        Err(error) if crate::rpc::is_cancelled(error) => ("cancelled", None),
        Err(error) => ("failed", Some(bounded_detail(&error.to_string()))),
    };
    let turn_end = record_telemetry_hook(
        environment,
        agent_id,
        mews_agent::LifecycleHook::TurnEnd,
        json!({"session_id": metadata.mews_session_id, "turn_id": metadata.turn_id,
                "status": status, "outcome": detail}),
        cwd,
        cancellation,
        events,
    )
    .await;
    match (after_step, turn_end) {
        (Err(after_step), Err(turn_end)) => {
            Err(after_step).context(format!("turn_end telemetry also failed: {turn_end:#}"))
        }
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn record_telemetry_hook(
    environment: &dyn mews_agent::AgentCapabilities,
    agent_id: &mews_protocol::AgentId,
    lifecycle: mews_agent::LifecycleHook,
    payload: Value,
    cwd: &std::path::Path,
    cancellation: &mews_agent::CancellationToken,
    events: &mut dyn FnMut(AcpStreamEvent) -> Result<()>,
) -> Result<()> {
    let hook = match lifecycle {
        mews_agent::LifecycleHook::TurnStart => "turn_start",
        mews_agent::LifecycleHook::BeforeModel => "before_model",
        mews_agent::LifecycleHook::BeforeTool => "before_tool",
        mews_agent::LifecycleHook::AfterTool => "after_tool",
        mews_agent::LifecycleHook::AfterStep => "after_step",
        mews_agent::LifecycleHook::TurnEnd => "turn_end",
    };
    // A Turn-end hook is cleanup: it must remain runnable after the Turn's
    // cooperative cancellation token has fired.
    let cleanup_cancellation = mews_agent::CancellationToken::new();
    let cancellation = if lifecycle == mews_agent::LifecycleHook::TurnEnd {
        &cleanup_cancellation
    } else {
        cancellation
    };
    match environment
        .hook(agent_id, lifecycle, payload, cwd, cancellation, None)
        .await
    {
        Ok(_) => events(AcpStreamEvent::HookOutcome {
            event_key: accepted_event_key(),
            hook: hook.into(),
            ok: true,
            detail: None,
            tool: None,
            call_id: None,
        }),
        Err(error) => events(AcpStreamEvent::HookOutcome {
            event_key: accepted_event_key(),
            hook: hook.into(),
            ok: false,
            detail: Some(bounded_detail(&error.to_string())),
            tool: None,
            call_id: None,
        }),
    }
}

pub async fn probe_acp(config: AcpHarnessConfig, cwd: PathBuf) -> Result<AcpProbe> {
    let process = AcpProcess::new(config);
    let spawn_started = tokio::time::Instant::now();
    let mut child = process.spawn(&cwd)?;
    let mut process_guard = ProcessTreeGuard::new(&child);
    let spawn = spawn_started.elapsed();
    let result = async {
        let stdin = child
            .stdin
            .take()
            .context("ACP Harness did not open stdin")?;
        let stdout = child
            .stdout
            .take()
            .context("ACP Harness did not open stdout")?;
        let mut writer = stdin;
        let mut reader = BufReader::new(stdout);
        let cancellation = mews_agent::CancellationToken::new();
        let mut rpc = RpcClient::new(
            &mut writer,
            &mut reader,
            process.config.request_timeout,
            process.config.permission_handler.as_ref(),
        );
        let initialize_started = tokio::time::Instant::now();
        let initialize = rpc
            .request(
                "initialize",
                json!({
                    "protocolVersion": ACP_PROTOCOL_VERSION,
                    "clientInfo": { "name": "mews", "version": env!("CARGO_PKG_VERSION") },
                    "clientCapabilities": { "auth": { "terminal": true } },
                }),
                &cancellation,
                None,
                None,
                |_| Ok(()),
            )
            .await?;
        let initialize_elapsed = initialize_started.elapsed();
        let session_started = tokio::time::Instant::now();
        let session = rpc
            .request(
                "session/new",
                json!({ "cwd": cwd, "mcpServers": [] }),
                &cancellation,
                None,
                None,
                |_| Ok(()),
            )
            .await;
        let session_elapsed = session_started.elapsed();
        let session_error_kind = session.as_ref().err().and_then(classify_error);
        Ok(AcpProbe {
            initialize,
            session: session.as_ref().ok().cloned(),
            session_error: session.err().map(|error| error.to_string()),
            session_error_kind,
            timings: AcpProbeTimings {
                spawn,
                initialize: initialize_elapsed,
                session: session_elapsed,
            },
        })
    }
    .await;
    terminate_process_tree(&mut child).await;
    process_guard.disarm();
    result
}

impl AcpProcess {
    #[allow(clippy::too_many_arguments)]
    async fn run_session_with_extensions(
        live: &mut LiveAcpProcess,
        mut timings: AcpTimings,
        cwd: PathBuf,
        harness_options: BTreeMap<String, String>,
        session: AcpSessionRequest,
        environment: &dyn mews_agent::AgentCapabilities,
        allowed_tools: &[String],
        cancellation: mews_agent::CancellationToken,
        events: &mut dyn FnMut(AcpStreamEvent) -> Result<()>,
    ) -> Result<AcpSessionOutcome> {
        let initialize = live.initialize.clone();
        let mut rpc = live.rpc();
        let mut mcp = TurnMcpBridge::for_extensions_and_skills(
            environment,
            &session.agent_id,
            cwd.clone(),
            cancellation.clone(),
            allowed_tools,
            session.skills.clone(),
        )?;
        if let Some(metadata) = &session.hook_metadata {
            mcp.set_correlation(crate::mcp::McpCorrelation {
                mews_session_id: metadata.mews_session_id.clone(),
                turn_id: metadata.turn_id.clone(),
                harness: metadata.harness.clone(),
                acp_session_id: std::sync::Arc::new(std::sync::Mutex::new(None)),
            });
        }
        let mut active_session_id = None;
        let lifecycle = async {
        let mcp_http = if !mcp.needs_transport() {
            None
        } else if initialize
            .agent_capabilities
            .pointer("/mcpCapabilities/http")
            .and_then(Value::as_bool)
            == Some(true)
        {
            Some(mcp.bind_http().await?)
        } else {
            bail!("ACP Harness does not support HTTP MCP required for MEWS extensions")
        };
        let mcp_servers = mcp_http.as_ref().map_or_else(Vec::new, |http| {
            vec![json!({ "type": "http", "name": "mews_extensions", "url": http.url(), "headers": [] })]
        });
        let continuation_started = tokio::time::Instant::now();
        let (session_id, session_replaced, prompt, binding_transition) =
            if let AcpBindingTransition::Resume {
                acp_session_id: session_id,
            } = &session.transition
            {
                let method = if initialize
                    .agent_capabilities
                    .pointer("/sessionCapabilities/resume")
                    .is_some_and(|capability| capability.is_object())
                {
                    ContinuationMethod::Resume
                } else if initialize
                    .agent_capabilities
                    .pointer("/loadSession")
                    .and_then(Value::as_bool)
                    == Some(true)
                {
                    ContinuationMethod::Load
                } else {
                    bail!("ACP Harness cannot resume persistent Sessions")
                };
                let resumed = rpc
                .request(
                    match method {
                        ContinuationMethod::Resume => "session/resume",
                        ContinuationMethod::Load => "session/load",
                    },
                    json!({ "sessionId": session_id, "cwd": cwd.clone(), "mcpServers": mcp_servers.clone() }),
                    &cancellation,
                    Some(&mcp),
                    mcp_http.as_ref(),
                    |_| Ok(()),
                )
                .await;
                match resumed {
                    Ok(_) => (session_id.clone(), false, session.prompt.clone(), None),
                    Err(error) if is_resource_not_found(&error) => {
                        if session.instruction_channel == AcpInstructionChannel::CodexDeveloper {
                            bail!(CODEX_RESTART_FOR_RECOVERY);
                        }
                        let created = rpc
                            .request(
                                "session/new",
                                session_new_params(&cwd, mcp_servers, &session),
                                &cancellation,
                                Some(&mcp),
                                mcp_http.as_ref(),
                                |_| Ok(()),
                            )
                            .await?;
                        (
                            acp_session_id(&created)?,
                            true,
                            outbound_prompt(&session, &session.recovery_prompt),
                            Some(AcpBindingTransition::Replace {
                                reason: AcpReplacementReason::ResourceNotFound,
                            }),
                        )
                    }
                    Err(error) => return Err(error),
                }
            } else {
                let created = rpc
                    .request(
                        "session/new",
                        session_new_params(&cwd, mcp_servers, &session),
                        &cancellation,
                        Some(&mcp),
                        mcp_http.as_ref(),
                        |_| Ok(()),
                    )
                    .await?;
                let transition = session.transition.clone();
                let replacement = matches!(transition, AcpBindingTransition::Replace { .. });
                (
                    acp_session_id(&created)?,
                    replacement,
                    outbound_prompt(
                        &session,
                        if replacement {
                            &session.recovery_prompt
                        } else {
                            &session.prompt
                        },
                    ),
                    Some(transition),
                )
            };
        let continuation_elapsed = continuation_started.elapsed();
        active_session_id = Some(session_id.clone());
        if let Some(transition) = &binding_transition {
            events(AcpStreamEvent::SessionBound {
                event_key: format!("binding:{session_id}"),
                session_id: session_id.clone(),
                transition: transition.clone(),
            })?;
        }
        mcp.set_acp_session_id(session_id.clone());
        apply_harness_options(
            &mut rpc,
            &session_id,
            &harness_options,
            &cancellation,
            Some(&mcp),
            mcp_http.as_ref(),
        )
        .await?;
        let mut updates = UpdateState::for_turn(
            session
                .hook_metadata
                .as_ref()
                .map_or("local", |metadata| metadata.turn_id.as_str()),
        );
        let prompt = before_model(
            environment,
            &session,
            &session_id,
            binding_transition.as_ref().unwrap_or(&session.transition),
            prompt,
            &cwd,
            &cancellation,
            events,
        )
        .await;
        let prompt = match prompt {
            Ok(prompt) => prompt,
            Err(error) => {
                return Err(error);
            }
        };
        if binding_transition.is_some()
            && session.instruction_channel == AcpInstructionChannel::FirstPrompt
        {
            events(AcpStreamEvent::ContextDispatched {
                event_key: format!("context_dispatched:{session_id}"),
                session_id: session_id.clone(),
            })?;
        }
        let event_sink = std::cell::RefCell::new(&mut *events);
        let prompt_started = tokio::time::Instant::now();
        let mut first_update_ms = None;
        let mut first_token_ms = None;
        let prompt_result = rpc
            .request_with_dispatch(
                "session/prompt",
                json!({ "sessionId": session_id, "prompt": [{ "type": "text", "text": prompt }] }),
                &cancellation,
                Some(&mcp),
                mcp_http.as_ref(),
                || {
                    (event_sink.borrow_mut())(AcpStreamEvent::PromptDispatched {
                        event_key: format!("prompt_dispatched:{session_id}"),
                        session_id: session_id.clone(),
                    })
                },
                |update| {
                    let elapsed = prompt_started.elapsed().as_millis() as u64;
                    first_update_ms.get_or_insert(elapsed);
                    if update.get("sessionUpdate").and_then(Value::as_str)
                        == Some("agent_message_chunk")
                    {
                        first_token_ms.get_or_insert(elapsed);
                    }
                    updates.apply(update, &mut **event_sink.borrow_mut())
                },
            )
            .await;
        let prompt_ms = prompt_started.elapsed().as_millis() as u64;
        drop(rpc);
        let prompt_result = match prompt_result {
            Ok(result) => result,
            Err(error) if crate::rpc::is_cancelled(&error) || classify_error(&error).is_some() => {
                return Err(error);
            }
            Err(error) => {
                return Err(mews_agent::EffectUncertain::new(format!(
                    "ACP prompt was dispatched but no definitive outcome was observed: {error:#}"
                ))
                .into());
            }
        };
        let prompt_result: PromptResult = serde_json::from_value(prompt_result).map_err(|error| {
            mews_agent::EffectUncertain::new(format!(
                "ACP prompt returned an invalid terminal result: {error}"
            ))
        })?;
        if prompt_result.stop_reason == AcpStopReason::Cancelled {
            return Err(anyhow::anyhow!(AcpCancelled));
        }
        Ok(AcpSessionOutcome {
            answer: updates.answer(),
            session_id,
            session_replaced,
            stop_reason: prompt_result.stop_reason,
            timings: {
                timings.continuation_ms = continuation_elapsed.as_millis() as u64;
                timings.prompt_to_first_update_ms = first_update_ms;
                timings.prompt_to_first_token_ms = first_token_ms;
                timings.prompt_ms = prompt_ms;
                timings
            },
        })
        }.await;
        if let Some(session_id) = active_session_id {
            live.session_ids.insert(session_id);
        }
        let uncertain_effect = mcp.take_effect_uncertainty();
        let audit = drain_mcp_hook_outcomes(&mcp, events);
        mcp.revoke();
        if let Some(reason) = uncertain_effect {
            let error = anyhow::Error::from(mews_agent::EffectUncertain::new(reason));
            return match audit {
                Ok(()) => Err(error),
                Err(audit) => Err(error.context(format!(
                    "failed to persist MCP hook audit after uncertain effect: {audit:#}"
                ))),
            };
        }
        match (lifecycle, audit) {
            (Ok(outcome), Ok(())) => Ok(outcome),
            (Err(primary), Ok(())) => Err(primary),
            (Ok(_), Err(audit)) => Err(audit.context("failed to persist MCP hook audit")),
            (Err(primary), Err(audit)) => Err(primary.context(format!(
                "ACP lifecycle failed; additionally failed to persist MCP hook audit: {audit:#}"
            ))),
        }
    }
}

fn drain_mcp_hook_outcomes(
    mcp: &TurnMcpBridge<'_>,
    events: &mut dyn FnMut(AcpStreamEvent) -> Result<()>,
) -> Result<()> {
    for outcome in mcp.drain_hook_outcomes() {
        events(AcpStreamEvent::HookOutcome {
            event_key: accepted_event_key(),
            hook: outcome.hook,
            ok: outcome.ok,
            detail: outcome.detail,
            tool: outcome.tool,
            call_id: outcome.call_id,
        })?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // Turn-scoped hook inputs are assembled at the ACP boundary.
async fn before_model(
    environment: &dyn mews_agent::AgentCapabilities,
    session: &AcpSessionRequest,
    acp_session_id: &str,
    transition: &AcpBindingTransition,
    prompt: String,
    cwd: &std::path::Path,
    cancellation: &mews_agent::CancellationToken,
    events: &mut dyn FnMut(AcpStreamEvent) -> Result<()>,
) -> Result<String> {
    let Some(metadata) = &session.hook_metadata else {
        return Ok(prompt);
    };
    let payload = json!({
        "session_id": metadata.mews_session_id, "turn_id": metadata.turn_id,
        "acp_session_id": acp_session_id, "harness": metadata.harness,
        "mode": transition, "prompt": prompt,
    });
    let response = match environment
        .hook(
            &session.agent_id,
            mews_agent::LifecycleHook::BeforeModel,
            payload,
            cwd,
            cancellation,
            None,
        )
        .await
    {
        Ok(response) => response,
        Err(error) => {
            let detail = bounded_detail(&error.to_string());
            events(AcpStreamEvent::HookOutcome {
                event_key: accepted_event_key(),
                hook: "before_model".into(),
                ok: false,
                detail: Some(detail.clone()),
                tool: None,
                call_id: None,
            })?;
            return Err(error).context("ACP before_model hook failed");
        }
    };
    let response = match parse_before_model(response) {
        Ok(response) => response,
        Err(detail) => {
            events(AcpStreamEvent::HookOutcome {
                event_key: accepted_event_key(),
                hook: "before_model".into(),
                ok: false,
                detail: Some(detail.clone()),
                tool: None,
                call_id: None,
            })?;
            bail!("invalid ACP before_model hook response: {detail}");
        }
    };
    if let Some(reason) = response.block {
        let detail = bounded_detail(&reason);
        events(AcpStreamEvent::HookOutcome {
            event_key: accepted_event_key(),
            hook: "before_model".into(),
            ok: false,
            detail: Some(detail.clone()),
            tool: None,
            call_id: None,
        })?;
        bail!("ACP before_model hook blocked prompt: {detail}");
    }
    let prompt = response.prompt.unwrap_or(prompt);
    events(AcpStreamEvent::HookOutcome {
        event_key: accepted_event_key(),
        hook: "before_model".into(),
        ok: true,
        detail: None,
        tool: None,
        call_id: None,
    })?;
    Ok(prompt)
}

struct BeforeModel {
    block: Option<String>,
    prompt: Option<String>,
}

fn parse_before_model(value: Value) -> std::result::Result<BeforeModel, String> {
    let object = match value {
        Value::Null => {
            return Ok(BeforeModel {
                block: None,
                prompt: None,
            });
        }
        Value::Object(object) => object,
        _ => return Err("hook response must be an object or null".into()),
    };
    let string = |key: &str| {
        object.get(key).map_or(Ok(None), |value| {
            value
                .as_str()
                .map(str::to_owned)
                .map(Some)
                .ok_or_else(|| format!("{key} must be a string"))
        })
    };
    Ok(BeforeModel {
        block: string("block")?,
        prompt: string("prompt")?,
    })
}

fn bounded_detail(value: &str) -> String {
    value.chars().take(1024).collect()
}

fn acp_session_id(response: &Value) -> Result<String> {
    let session_id = response
        .get("sessionId")
        .and_then(Value::as_str)
        .filter(|session_id| !session_id.is_empty())
        .filter(|session_id| session_id.len() <= mews_protocol::MAX_ACP_SESSION_ID_BYTES)
        .map(str::to_owned)
        .context("ACP session/new response did not include a valid sessionId")?;
    Ok(session_id)
}

fn outbound_prompt(session: &AcpSessionRequest, text: &str) -> String {
    match session.instruction_channel {
        AcpInstructionChannel::FirstPrompt => format!("{}\n\n{}", session.context_text, text),
        AcpInstructionChannel::CodexDeveloper | AcpInstructionChannel::ClaudeSystemAppend => {
            text.to_owned()
        }
    }
}

fn session_new_params(
    cwd: &PathBuf,
    mcp_servers: Vec<Value>,
    session: &AcpSessionRequest,
) -> Value {
    let mut params = json!({ "cwd": cwd, "mcpServers": mcp_servers });
    if session.instruction_channel == AcpInstructionChannel::ClaudeSystemAppend {
        params["_meta"] = json!({ "systemPrompt": { "append": session.context_text } });
    }
    params
}

/// Codex ACP reads CODEX_CONFIG at process start. Merge only the developer
/// channel; base instructions and unrelated trusted recipe configuration win.
fn prepare_instruction_channel(
    config: &mut AcpHarnessConfig,
    session: &AcpSessionRequest,
) -> Result<()> {
    // Codex ACP reads this process-scoped configuration at startup and applies
    // it when starting or resuming the provider Session. It must therefore be
    // present on every cold process start, including Resume after idle eviction.
    if session.instruction_channel != AcpInstructionChannel::CodexDeveloper {
        return Ok(());
    }
    let key = std::ffi::OsString::from("CODEX_CONFIG");
    let mut value = match config.environment.get(&key) {
        Some(value) => serde_json::from_str::<Value>(&value.to_string_lossy())
            .context("CODEX_CONFIG must be a JSON object")?,
        None => json!({}),
    };
    let object = value
        .as_object_mut()
        .context("CODEX_CONFIG must be a JSON object")?;
    object.insert(
        "developer_instructions".into(),
        Value::String(session.context_text.clone()),
    );
    config
        .environment
        .insert(key, serde_json::to_string(&value)?.into());
    Ok(())
}

async fn apply_harness_options<W>(
    rpc: &mut RpcClient<'_, W>,
    session_id: &str,
    options: &BTreeMap<String, String>,
    cancellation: &mews_agent::CancellationToken,
    mcp: Option<&TurnMcpBridge<'_>>,
    mcp_http: Option<&TurnMcpHttp>,
) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    for (config_id, value) in options {
        rpc.request(
            "session/set_config_option",
            json!({ "sessionId": session_id, "configId": config_id, "value": value }),
            cancellation,
            mcp,
            mcp_http,
            |_| Ok(()),
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        AcpEventSink, AcpHarnessConfig, AcpHookMetadata, AcpRuntimePool, AcpSessionRequest,
        AcpStopReason, AcpStreamEvent, PersistentAcpTurnRequest, execute_acp_turn_inner,
        prepare_instruction_channel, probe_acp, session_new_params,
    };
    use crate::rpc::{AcpErrorKind, acp_rpc_error, classify_error, is_resource_not_found};
    use crate::updates::update_text;
    use anyhow::{Result, bail};
    use async_trait::async_trait;
    use mews_agent::{
        AgentCapabilities, CancellationToken, ContextSnapshot, LifecycleHook, ProgressReporter,
        ToolCall, ToolResult,
    };
    use mews_protocol::{AcpBindingTransition, AcpInstructionChannel, AcpReplacementReason};
    use serde_json::{Value, json};
    use std::{collections::BTreeMap, path::Path, time::Duration};

    #[test]
    fn only_agent_messages_are_answer_text() {
        let message = json!({
            "sessionUpdate": "agent_message_chunk",
            "content": { "type": "text", "text": "answer" }
        });
        let thought = json!({
            "sessionUpdate": "agent_thought_chunk",
            "content": { "type": "text", "text": "internal reasoning" }
        });

        assert_eq!(update_text(&message), Some("answer"));
        assert_eq!(update_text(&thought), None);
    }

    #[test]
    fn only_typed_resource_not_found_is_reconstructable() {
        assert!(is_resource_not_found(&acp_rpc_error(
            "session/resume",
            &json!({"code": -32002, "message": "Resource not found"}),
        )));
        assert!(!is_resource_not_found(&acp_rpc_error(
            "session/resume",
            &json!({"code": -32603, "message": "Session not found"}),
        )));
        assert!(!is_resource_not_found(&anyhow::anyhow!(
            "ACP request timed out"
        )));
        let authentication = acp_rpc_error(
            "session/new",
            &json!({"code":-32000,"message":"any diagnostic prose"}),
        )
        .context("wrapped Turn failure");
        assert_eq!(
            classify_error(&authentication),
            Some(AcpErrorKind::AuthenticationRequired)
        );
    }

    #[test]
    fn managed_instruction_channels_prepare_every_cold_process_start() {
        let mut config = AcpHarnessConfig::new(["fixture"]).unwrap();
        let codex_config = std::ffi::OsString::from("CODEX_CONFIG");
        config.environment.insert(
            codex_config.clone(),
            r#"{"approval_policy":"never"}"#.into(),
        );
        let request = AcpSessionRequest {
            agent_id: mews_protocol::AgentId::new(),
            agent_slug: "coder".into(),
            transition: AcpBindingTransition::New,
            prompt: "user".into(),
            recovery_prompt: String::new(),
            context_text: "MEWS context".into(),
            instruction_channel: AcpInstructionChannel::CodexDeveloper,
            skills: Vec::new(),
            hook_metadata: None,
        };
        prepare_instruction_channel(&mut config, &request).unwrap();
        let merged: Value =
            serde_json::from_str(&config.environment[&codex_config].to_string_lossy()).unwrap();
        assert_eq!(merged["approval_policy"], "never");
        assert_eq!(merged["developer_instructions"], "MEWS context");
        let claude = AcpSessionRequest {
            instruction_channel: AcpInstructionChannel::ClaudeSystemAppend,
            ..request.clone()
        };
        assert_eq!(
            session_new_params(&Path::new("/tmp").to_path_buf(), Vec::new(), &claude)["_meta"]["systemPrompt"]
                ["append"],
            "MEWS context"
        );
        let resume = AcpSessionRequest {
            transition: AcpBindingTransition::Resume {
                acp_session_id: "saved".into(),
            },
            ..request
        };
        let mut resumed_config = AcpHarnessConfig::new(["fixture"]).unwrap();
        prepare_instruction_channel(&mut resumed_config, &resume).unwrap();
        let resumed: Value =
            serde_json::from_str(&resumed_config.environment[&codex_config].to_string_lossy())
                .unwrap();
        assert_eq!(resumed["developer_instructions"], "MEWS context");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn probe_classifies_authentication_by_acp_error_code_and_reports_timings() {
        use std::{fs, os::unix::fs::PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("auth-acp");
        fs::write(
            &fixture,
            r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"id":1'*) sleep 0.02; printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{}}}' ;;
    *'"id":2'*) sleep 0.02; printf '%s\n' '{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"Provider sign-in required"}}'; sleep 30 ;;
  esac
done
"#,
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

        let probe = probe_acp(
            AcpHarnessConfig::new([fixture.into_os_string()]).unwrap(),
            directory.path().to_owned(),
        )
        .await
        .unwrap();
        assert_eq!(
            probe.session_error_kind,
            Some(AcpErrorKind::AuthenticationRequired)
        );
        assert!(probe.timings.initialize >= Duration::from_millis(10));
        assert!(probe.timings.session >= Duration::from_millis(10));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_an_unsupported_negotiated_protocol_version() {
        use std::{fs, os::unix::fs::PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("wrong-version-acp");
        fs::write(
            &fixture,
            r#"#!/bin/sh
IFS= read -r line
printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":2,"agentCapabilities":{}}}'
sleep 30
"#,
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

        let error = execute_acp_turn_inner(
            AcpHarnessConfig::new([fixture.into_os_string()]).unwrap(),
            directory.path().to_owned(),
            BTreeMap::new(),
            AcpSessionRequest {
                agent_id: mews_protocol::AgentId::new(),
                agent_slug: "coder".into(),
                transition: AcpBindingTransition::New,
                prompt: "hello".into(),
                recovery_prompt: "hello".into(),
                context_text: String::new(),
                instruction_channel: AcpInstructionChannel::FirstPrompt,
                skills: Vec::new(),
                hook_metadata: None,
            },
            &NoCapabilities,
            &[],
            CancellationToken::new(),
            &mut |_| Ok(()),
        )
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains("unsupported protocol version 2"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn continuous_updates_do_not_extend_the_absolute_request_deadline() {
        use std::{fs, os::unix::fs::PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("updates-forever-acp");
        fs::write(
            &fixture,
            r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{}}}' ;;
    *'"id":2'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"fixture"}}' ;;
    *'"id":3'*) while true; do printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"agent_thought_chunk","text":"busy"}}}'; sleep 0.01; done ;;
  esac
done
"#,
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();
        let mut config = AcpHarnessConfig::new([fixture.into_os_string()]).unwrap();
        config.request_timeout = Duration::from_millis(100);

        let error = tokio::time::timeout(
            Duration::from_secs(2),
            execute_acp_turn_inner(
                config,
                directory.path().to_owned(),
                BTreeMap::new(),
                AcpSessionRequest {
                    agent_id: mews_protocol::AgentId::new(),
                    agent_slug: "coder".into(),
                    transition: AcpBindingTransition::New,
                    prompt: "never finishes".into(),
                    recovery_prompt: "never finishes".into(),
                    context_text: String::new(),
                    instruction_channel: AcpInstructionChannel::FirstPrompt,
                    skills: Vec::new(),
                    hook_metadata: None,
                },
                &NoCapabilities,
                &[],
                CancellationToken::new(),
                &mut |_| Ok(()),
            ),
        )
        .await
        .expect("absolute deadline must not be reset by updates")
        .unwrap_err();
        assert!(format!("{error:#}").contains("timed out"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn first_prompt_dispatch_is_reported_before_an_ambiguous_disconnect() {
        use std::{fs, os::unix::fs::PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("disconnect-after-prompt-acp");
        fs::write(
            &fixture,
            r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{}}}' ;;
    *'"id":2'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"fixture"}}' ;;
    *'"id":3'*) exit 0 ;;
  esac
done
"#,
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();
        let mut events = Vec::new();

        execute_acp_turn_inner(
            AcpHarnessConfig::new([fixture.into_os_string()]).unwrap(),
            directory.path().to_owned(),
            BTreeMap::new(),
            AcpSessionRequest {
                agent_id: mews_protocol::AgentId::new(),
                agent_slug: "coder".into(),
                transition: AcpBindingTransition::New,
                prompt: "may execute".into(),
                recovery_prompt: "may execute".into(),
                context_text: "context".into(),
                instruction_channel: AcpInstructionChannel::FirstPrompt,
                skills: Vec::new(),
                hook_metadata: None,
            },
            &NoCapabilities,
            &[],
            CancellationToken::new(),
            &mut |event| {
                events.push(event);
                Ok(())
            },
        )
        .await
        .unwrap_err();
        assert!(events.iter().any(|event| matches!(
            event,
            AcpStreamEvent::ContextDispatched { session_id, .. } if session_id == "fixture"
        )));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_terminates_adapter_descendants() {
        use std::{fs, os::unix::fs::PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("cancel-acp");
        let descendant_path = directory.path().join("descendant.pid");
        fs::write(
            &fixture,
            format!(
                r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"id":1'*) printf '%s\n' '{{"jsonrpc":"2.0","id":1,"result":{{"protocolVersion":1,"agentCapabilities":{{}}}}}}' ;;
    *'"id":2'*) printf '%s\n' '{{"jsonrpc":"2.0","id":2,"result":{{"sessionId":"fixture"}}}}' ;;
    *'"id":3'*) sleep 30 & child=$!; printf %s "$child" > {}; wait ;;
  esac
done
"#,
                descendant_path.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();
        let cancellation = CancellationToken::new();
        let mut events = |_| Ok(());
        let execution = execute_acp_turn_inner(
            AcpHarnessConfig::new([fixture.into_os_string()]).unwrap(),
            directory.path().to_owned(),
            BTreeMap::new(),
            AcpSessionRequest {
                agent_id: mews_protocol::AgentId::new(),
                agent_slug: "coder".into(),
                transition: AcpBindingTransition::New,
                prompt: "start child".into(),
                recovery_prompt: "start child".into(),
                context_text: String::new(),
                instruction_channel: AcpInstructionChannel::FirstPrompt,
                skills: Vec::new(),
                hook_metadata: None,
            },
            &NoCapabilities,
            &[],
            cancellation.clone(),
            &mut events,
        );
        tokio::pin!(execution);
        let descendant = loop {
            tokio::select! {
                result = &mut execution => panic!("adapter exited before cancellation: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(10)) => {
                    if let Ok(pid) = fs::read_to_string(&descendant_path)
                        && let Ok(pid) = pid.trim().parse::<i32>()
                    {
                        break pid;
                    }
                }
            }
        };
        cancellation.cancel();
        let error = execution.await.unwrap_err();
        assert!(format!("{error:#}").contains("cancelled"));
        for _ in 0..100 {
            // SAFETY: signal 0 only checks whether this test descendant exists.
            if unsafe { libc::kill(descendant, 0) } == -1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("cancelled ACP descendant {descendant} is still running");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dropping_persistent_turn_terminates_the_active_process_tree() {
        use std::{fs, os::unix::fs::PermissionsExt, sync::Arc};

        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("drop-persistent-acp");
        let descendant_path = directory.path().join("persistent-descendant.pid");
        fs::write(
            &fixture,
            format!(
                r#"#!/bin/sh
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) printf '{{"jsonrpc":"2.0","id":%s,"result":{{"protocolVersion":1,"agentCapabilities":{{}}}}}}\n' "$id" ;;
    *'"method":"session/new"'*) printf '{{"jsonrpc":"2.0","id":%s,"result":{{"sessionId":"fixture"}}}}\n' "$id" ;;
    *'"method":"session/prompt"'*) sleep 30 & child=$!; printf %s "$child" > {}; wait ;;
  esac
done
"#,
                descendant_path.display()
            ),
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

        let pool = AcpRuntimePool::new(Duration::from_secs(30));
        let mut events = |_| Ok(());
        let mut execution = Box::pin(pool.execute_turn(persistent_fixture_request(
            &fixture,
            directory.path(),
            Arc::new(NoCapabilities),
            AcpBindingTransition::New,
            "stable context",
            &mut events,
        )));
        let descendant = loop {
            tokio::select! {
                result = &mut execution => {
                    panic!("persistent ACP Turn exited before its caller was dropped: {result:?}");
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
            if let Ok(pid) = fs::read_to_string(&descendant_path)
                && let Ok(pid) = pid.trim().parse::<i32>()
            {
                break pid;
            }
        };
        drop(execution);

        for _ in 0..100 {
            // SAFETY: signal 0 only checks whether this test descendant exists.
            if unsafe { libc::kill(descendant, 0) } == -1 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("dropped persistent ACP descendant {descendant} is still running");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn resumes_existing_session_without_replaying_recovery_context() {
        use std::{fs, os::unix::fs::PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("resume-acp");
        fs::write(
            &fixture,
            r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"id":1'*) sleep 0.02; printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"resume":{}}}}}' ;;
    *'"id":2'*'session/resume'*'native-1'*) sleep 0.02; printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{}}' ;;
    *'"id":3'*'second turn'*) printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"native-1","update":{"sessionUpdate":"agent_message_chunk","content":{"text":"resumed"}}}}'; printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn"}}'; exit 0 ;;
    *) exit 9 ;;
  esac
done
"#,
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();
        let outcome = execute_acp_turn_inner(
            AcpHarnessConfig::new(vec![fixture.to_string_lossy().into_owned()]).unwrap(),
            directory.path().to_path_buf(),
            BTreeMap::new(),
            AcpSessionRequest {
                agent_id: mews_protocol::AgentId::new(),
                agent_slug: "coder".into(),
                transition: AcpBindingTransition::Resume {
                    acp_session_id: "native-1".into(),
                },
                prompt: "second turn".into(),
                recovery_prompt: "MUST NOT BE SENT".into(),
                context_text: String::new(),
                instruction_channel: AcpInstructionChannel::FirstPrompt,
                skills: Vec::new(),
                hook_metadata: None,
            },
            &NoCapabilities,
            &[],
            CancellationToken::new(),
            &mut |_| Ok(()),
        )
        .await
        .unwrap();
        assert_eq!(outcome.answer, "resumed");
        assert_eq!(outcome.session_id, "native-1");
        assert!(!outcome.session_replaced);
        assert!(outcome.timings.initialize_ms >= 10);
        assert!(outcome.timings.continuation_ms >= 10);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn resume_null_uses_load_session_when_advertised() {
        use std::{fs, os::unix::fs::PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("load-acp");
        fs::write(&fixture, r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"resume":null},"loadSession":true}}}' ;;
    *'"id":2'*'session/load'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{}}' ;;
    *'"id":3'*) printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn"}}'; exit 0 ;;
    *) exit 9 ;;
  esac
done
"#).unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();
        execute_acp_turn_inner(
            AcpHarnessConfig::new(vec![fixture.to_string_lossy().into_owned()]).unwrap(),
            directory.path().to_path_buf(),
            BTreeMap::new(),
            AcpSessionRequest {
                agent_id: mews_protocol::AgentId::new(),
                agent_slug: "coder".into(),
                transition: AcpBindingTransition::Resume {
                    acp_session_id: "saved".into(),
                },
                prompt: "next".into(),
                recovery_prompt: String::new(),
                context_text: String::new(),
                instruction_channel: AcpInstructionChannel::FirstPrompt,
                skills: Vec::new(),
                hook_metadata: None,
            },
            &NoCapabilities,
            &[],
            CancellationToken::new(),
            &mut |_| Ok(()),
        )
        .await
        .unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reconstructs_only_after_resource_not_found() {
        use std::{fs, os::unix::fs::PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("recover-acp");
        fs::write(
            &fixture,
            r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"resume":{}}}}}' ;;
    *'"id":2'*'session/resume'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"error":{"code":-32002,"message":"Resource not found"}}' ;;
    *'"id":3'*'session/new'*) printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"sessionId":"native-2"}}' ;;
    *'"id":4'*'recovery history'*) printf '%s\n' '{"jsonrpc":"2.0","id":4,"result":{"stopReason":"end_turn"}}'; exit 0 ;;
    *) exit 9 ;;
  esac
done
"#,
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();
        let mut bound = Vec::new();
        let outcome = execute_acp_turn_inner(
            AcpHarnessConfig::new(vec![fixture.to_string_lossy().into_owned()]).unwrap(),
            directory.path().to_path_buf(),
            BTreeMap::new(),
            AcpSessionRequest {
                agent_id: mews_protocol::AgentId::new(),
                agent_slug: "coder".into(),
                transition: AcpBindingTransition::Resume {
                    acp_session_id: "native-1".into(),
                },
                prompt: "second turn".into(),
                recovery_prompt: "recovery history".into(),
                context_text: String::new(),
                instruction_channel: AcpInstructionChannel::FirstPrompt,
                skills: Vec::new(),
                hook_metadata: None,
            },
            &NoCapabilities,
            &[],
            CancellationToken::new(),
            &mut |event| {
                if let super::AcpStreamEvent::SessionBound {
                    session_id,
                    transition,
                    ..
                } = event
                {
                    bound.push((
                        session_id,
                        matches!(transition, AcpBindingTransition::Replace { .. }),
                    ));
                }
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(outcome.session_id, "native-2");
        assert!(outcome.session_replaced);
        assert_eq!(bound, vec![("native-2".into(), true)]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn recovery_timings_include_the_failed_resume_without_inflating_queue_time() {
        use std::{fs, os::unix::fs::PermissionsExt, sync::Arc};

        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("timed-recovery-acp");
        let starts = directory.path().join("starts");
        let script = r#"#!/bin/sh
printf 'start\n' >> '__STARTS__'
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"resume":{}}}}}\n' "$id" ;;
    *'"method":"session/resume"'*) sleep 0.15; printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32002,"message":"Resource not found"}}\n' "$id" ;;
    *'"method":"session/new"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"recovered"}}\n' "$id" ;;
    *'"method":"session/prompt"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"end_turn"}}\n' "$id" ;;
  esac
done
"#
        .replace("__STARTS__", &starts.to_string_lossy());
        fs::write(&fixture, script).unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

        let pool = AcpRuntimePool::new(Duration::from_secs(30));
        let mut events = |_| Ok(());
        let outcome = pool
            .execute_turn(persistent_fixture_request(
                &fixture,
                directory.path(),
                Arc::new(NoCapabilities),
                AcpBindingTransition::Resume {
                    acp_session_id: "missing".into(),
                },
                "stable context",
                &mut events,
            ))
            .await
            .unwrap();

        assert_eq!(fs::read_to_string(starts).unwrap(), "start\nstart\n");
        assert_eq!(outcome.session_id, "recovered");
        assert!(outcome.timings.total_ms >= 100);
        assert!(outcome.timings.queue_ms < 100);
    }

    struct NoCapabilities;
    #[async_trait]
    impl AgentCapabilities for NoCapabilities {
        async fn context(&self, _: &str, _: &Path) -> Result<ContextSnapshot> {
            Ok(ContextSnapshot::default())
        }
        fn tools(&self) -> mews_protocol::ToolCatalogSnapshot {
            mews_protocol::ToolCatalogSnapshot::default()
        }
        async fn execute(
            &self,
            _: &mews_protocol::AgentId,
            _: &ToolCall,
            _: &Path,
            _: &CancellationToken,
            _: &dyn ProgressReporter,
        ) -> Result<ToolResult> {
            bail!("ACP fixture must not call MEWS extension tools")
        }
        async fn hook(
            &self,
            _: &mews_protocol::AgentId,
            _: LifecycleHook,
            _: serde_json::Value,
            _: &Path,
            _: &CancellationToken,
            _: Option<u64>,
        ) -> Result<serde_json::Value> {
            Ok(serde_json::Value::Null)
        }
    }

    fn persistent_fixture_request<'a>(
        fixture: &Path,
        cwd: &Path,
        environment: std::sync::Arc<dyn AgentCapabilities>,
        transition: AcpBindingTransition,
        context: &str,
        events: &'a mut dyn AcpEventSink,
    ) -> PersistentAcpTurnRequest<'a> {
        let agent_id = mews_protocol::AgentId::new();
        PersistentAcpTurnRequest {
            session_key: agent_id.to_string(),
            config: AcpHarnessConfig::new([fixture.as_os_str().to_owned()]).unwrap(),
            cwd: cwd.to_owned(),
            harness_options: BTreeMap::new(),
            session: AcpSessionRequest {
                agent_id,
                agent_slug: "coder".into(),
                transition,
                prompt: "hello".into(),
                recovery_prompt: "hello".into(),
                context_text: context.into(),
                instruction_channel: AcpInstructionChannel::CodexDeveloper,
                skills: Vec::new(),
                hook_metadata: None,
            },
            environment,
            allowed_tools: Vec::new(),
            cancellation: CancellationToken::new(),
            events,
        }
    }

    #[derive(Default)]
    struct RecordingCapabilities {
        hooks: std::sync::Mutex<Vec<(LifecycleHook, Value)>>,
    }

    #[async_trait]
    impl AgentCapabilities for RecordingCapabilities {
        async fn context(&self, _: &str, _: &Path) -> Result<ContextSnapshot> {
            Ok(ContextSnapshot::default())
        }

        fn tools(&self) -> mews_protocol::ToolCatalogSnapshot {
            mews_protocol::ToolCatalogSnapshot::default()
        }

        async fn execute(
            &self,
            _: &mews_protocol::AgentId,
            _: &ToolCall,
            _: &Path,
            _: &CancellationToken,
            _: &dyn ProgressReporter,
        ) -> Result<ToolResult> {
            bail!("ACP fixture must not call MEWS extension tools")
        }

        async fn hook(
            &self,
            _: &mews_protocol::AgentId,
            hook: LifecycleHook,
            payload: Value,
            _: &Path,
            _: &CancellationToken,
            _: Option<u64>,
        ) -> Result<Value> {
            self.hooks.lock().unwrap().push((hook, payload));
            Ok(Value::Null)
        }
    }

    #[tokio::test]
    async fn turn_end_runs_when_the_acp_process_cannot_start() {
        let directory = tempfile::tempdir().unwrap();
        let environment = RecordingCapabilities::default();

        execute_acp_turn_inner(
            AcpHarnessConfig::new([directory.path().join("missing-acp").into_os_string()]).unwrap(),
            directory.path().to_owned(),
            BTreeMap::new(),
            AcpSessionRequest {
                agent_id: mews_protocol::AgentId::new(),
                agent_slug: "coder".into(),
                transition: AcpBindingTransition::New,
                prompt: "hello".into(),
                recovery_prompt: "hello".into(),
                context_text: String::new(),
                instruction_channel: AcpInstructionChannel::FirstPrompt,
                skills: Vec::new(),
                hook_metadata: Some(AcpHookMetadata {
                    mews_session_id: "session".into(),
                    turn_id: "turn".into(),
                    harness: "fixture".into(),
                    context_hash: String::new(),
                    context_channel: AcpInstructionChannel::FirstPrompt,
                    invoke_turn_start: true,
                }),
            },
            &environment,
            &[],
            CancellationToken::new(),
            &mut |_| Ok(()),
        )
        .await
        .unwrap_err();

        let hooks = environment.hooks.lock().unwrap();
        assert_eq!(
            hooks.iter().map(|(hook, _)| *hook).collect::<Vec<_>>(),
            vec![LifecycleHook::TurnStart, LifecycleHook::TurnEnd]
        );
        assert_eq!(hooks[1].1["status"], "failed");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stdio_new_rejects_an_oversized_session_id_before_emitting_a_binding() {
        use std::{fs, os::unix::fs::PermissionsExt};

        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("oversized-session-id-acp");
        let session_id = "x".repeat(mews_protocol::MAX_ACP_SESSION_ID_BYTES + 1);
        let script = r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{}}}' ;;
    *'"id":2'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"__SESSION_ID__"}}'; exit 0 ;;
  esac
done
"#
        .replace("__SESSION_ID__", &session_id);
        fs::write(
            &fixture,
            /* format!(
                "#!/bin/sh\nwhile IFS= read -r line; do\n  case \\\"$line\\\" in\n    *'\\\"id\\\":1'*) printf '%s\\n' '{{\\\"jsonrpc\\\":\\\"2.0\\\",\\\"id\\\":1,\\\"result\\\":{{\\\"protocolVersion\\\":1,\\\"agentCapabilities\\\":{{}}}}}}' ;;\n    *'\\\"id\\\":2'*) printf '%s\\n' '{{\\\"jsonrpc\\\":\\\"2.0\\\",\\\"id\\\":2,\\\"result\\\":{{\\\"sessionId\\\":\\\"{}\\\"}}}}'; exit 0 ;;\n  esac\ndone\n",
                session_id
            ), */
            script,
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

        let mut events = Vec::new();
        let error = execute_acp_turn_inner(
            AcpHarnessConfig::new([fixture.into_os_string()]).unwrap(),
            directory.path().to_owned(),
            BTreeMap::new(),
            AcpSessionRequest {
                agent_id: mews_protocol::AgentId::new(),
                agent_slug: "coder".into(),
                transition: AcpBindingTransition::New,
                prompt: "hello".into(),
                recovery_prompt: "hello".into(),
                context_text: String::new(),
                instruction_channel: AcpInstructionChannel::FirstPrompt,
                skills: Vec::new(),
                hook_metadata: None,
            },
            &NoCapabilities,
            &[],
            CancellationToken::new(),
            &mut |event| {
                events.push(event);
                Ok(())
            },
        )
        .await
        .unwrap_err();
        assert!(format!("{error:#}").contains("valid sessionId"));
        assert!(events.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn codex_developer_new_injects_config_and_keeps_the_prompt_user_only() {
        use std::{fs, os::unix::fs::PermissionsExt};

        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("capture-codex-acp");
        let environment_path = directory.path().join("environment.json");
        let transcript_path = directory.path().join("transcript.jsonl");
        let script = r#"#!/bin/sh
printf '%s' "$CODEX_CONFIG" > "__ENVIRONMENT__"
while IFS= read -r line; do
  printf '%s\n' "$line" >> "__TRANSCRIPT__"
  case "$line" in
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{}}}' ;;
    *'"id":2'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"fixture"}}' ;;
    *'"id":3'*) printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn"}}'; exit 0 ;;
  esac
done
"#
        .replace("__ENVIRONMENT__", &environment_path.to_string_lossy())
        .replace("__TRANSCRIPT__", &transcript_path.to_string_lossy());
        fs::write(&fixture, script).unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

        let mut config = AcpHarnessConfig::new([fixture.into_os_string()]).unwrap();
        config.environment.insert(
            "CODEX_CONFIG".into(),
            r#"{"approval_policy":"never","unrelated":true}"#.into(),
        );
        execute_acp_turn_inner(
            config,
            directory.path().to_owned(),
            BTreeMap::new(),
            AcpSessionRequest {
                agent_id: mews_protocol::AgentId::new(),
                agent_slug: "coder".into(),
                transition: AcpBindingTransition::New,
                prompt: "user text".into(),
                recovery_prompt: "recovery text".into(),
                context_text: "EXACT MEWS CONTEXT".into(),
                instruction_channel: AcpInstructionChannel::CodexDeveloper,
                skills: Vec::new(),
                hook_metadata: None,
            },
            &NoCapabilities,
            &[],
            CancellationToken::new(),
            &mut |_| Ok(()),
        )
        .await
        .unwrap();

        let environment: Value =
            serde_json::from_str(&fs::read_to_string(environment_path).unwrap()).unwrap();
        assert_eq!(environment["approval_policy"], "never");
        assert_eq!(environment["unrelated"], true);
        assert_eq!(environment["developer_instructions"], "EXACT MEWS CONTEXT");
        let requests = fs::read_to_string(transcript_path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(requests[1]["method"], "session/new");
        assert_eq!(requests[2]["method"], "session/prompt");
        assert_eq!(requests[2]["params"]["prompt"][0]["text"], "user text");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn claude_system_append_uses_raw_session_new_metadata_for_new_and_replace() {
        use std::{fs, os::unix::fs::PermissionsExt};

        for (transition, expected_prompt) in [
            (AcpBindingTransition::New, "user text"),
            (
                AcpBindingTransition::Replace {
                    reason: AcpReplacementReason::ContextNotDispatched,
                },
                "recovery text",
            ),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let fixture = directory.path().join("capture-claude-acp");
            let transcript_path = directory.path().join("transcript.jsonl");
            let script = r#"#!/bin/sh
while IFS= read -r line; do
  printf '%s\n' "$line" >> "__TRANSCRIPT__"
  case "$line" in
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{}}}' ;;
    *'"id":2'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"fixture"}}' ;;
    *'"id":3'*) printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn"}}'; exit 0 ;;
  esac
done
"#
            .replace("__TRANSCRIPT__", &transcript_path.to_string_lossy());
            fs::write(&fixture, script).unwrap();
            fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

            execute_acp_turn_inner(
                AcpHarnessConfig::new([fixture.into_os_string()]).unwrap(),
                directory.path().to_owned(),
                BTreeMap::new(),
                AcpSessionRequest {
                    agent_id: mews_protocol::AgentId::new(),
                    agent_slug: "coder".into(),
                    transition,
                    prompt: "user text".into(),
                    recovery_prompt: "recovery text".into(),
                    context_text: "EXACT MEWS CONTEXT".into(),
                    instruction_channel: AcpInstructionChannel::ClaudeSystemAppend,
                    skills: Vec::new(),
                    hook_metadata: None,
                },
                &NoCapabilities,
                &[],
                CancellationToken::new(),
                &mut |_| Ok(()),
            )
            .await
            .unwrap();

            let requests = fs::read_to_string(transcript_path)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                requests[1]["params"]["_meta"]["systemPrompt"],
                json!({"append": "EXACT MEWS CONTEXT"})
            );
            assert_eq!(requests[2]["params"]["prompt"][0]["text"], expected_prompt);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stdio_fixture_initializes_creates_a_session_and_streams_a_reply() {
        use std::{fs, os::unix::fs::PermissionsExt};

        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("fixture-acp");
        fs::write(
            &fixture,
            r#"#!/bin/sh
while IFS= read -r line; do
  case "$line" in
    *'"id":1'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":1,"agentCapabilities":{}}}' ;;
    *'"id":2'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"sessionId":"fixture"}}' ;;
    *'"id":3'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":90,"method":"session/request_permission","params":{"sessionId":"fixture","toolCall":{"sessionUpdate":"tool_call","toolCallId":"native-1","title":"Run command"},"options":[{"optionId":"allow","name":"Allow Once","kind":"allow_once"},{"optionId":"decline","name":"Decline","kind":"reject_once"}],"_meta":{"provider":"fixture"}}}'
      ;;
    *'"id":90'*'"result"'*'"optionId":"allow"'*)
      printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"agent_message_chunk","messageId":"message-1","content":{"type":"text","text":"intro"}}}}'
      printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"agent_thought_chunk","messageId":"thought-1","content":{"type":"text","text":"checking source"}}}}'
      printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"tool_call","toolCallId":"web-1","title":"Web search","kind":"search","status":"in_progress","rawInput":{"query":""}}}}'
      printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"tool_call_update","toolCallId":"web-1","status":"completed","rawInput":{"query":"weather in Tashkent"}}}}'
      printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"fixture","update":{"sessionUpdate":"agent_message_chunk","messageId":"message-2","content":{"type":"text","text":"fixture reply"}}}}'
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"stopReason":"end_turn"}}'
      exit 0
      ;;
  esac
done
"#,
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

        let mut events = Vec::new();
        let outcome = execute_acp_turn_inner(
            AcpHarnessConfig::new([fixture.into_os_string()]).unwrap(),
            directory.path().to_owned(),
            BTreeMap::new(),
            AcpSessionRequest {
                agent_id: mews_protocol::AgentId::new(),
                agent_slug: "coder".into(),
                transition: AcpBindingTransition::New,
                prompt: "hello".into(),
                recovery_prompt: "hello".into(),
                context_text: String::new(),
                instruction_channel: AcpInstructionChannel::FirstPrompt,
                skills: Vec::new(),
                hook_metadata: None,
            },
            &NoCapabilities,
            &[],
            CancellationToken::new(),
            &mut |event| {
                events.push(event);
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(outcome.answer, "intro\n\nfixture reply");
        assert_eq!(outcome.stop_reason, AcpStopReason::EndTurn);
        assert!(outcome.timings.prompt_to_first_update_ms.is_some());
        assert!(outcome.timings.prompt_to_first_token_ms.is_some());
        let dispatched = events
            .iter()
            .position(|event| matches!(event, AcpStreamEvent::PromptDispatched { session_id, .. } if session_id == "fixture"))
            .expect("prompt dispatch event");
        let first_reply = events
            .iter()
            .position(|event| matches!(event, AcpStreamEvent::AssistantDelta { .. }))
            .expect("assistant reply event");
        assert!(dispatched < first_reply);
        assert!(events.iter().any(|event| {
            matches!(
                event,
                AcpStreamEvent::ProviderState { data, .. }
                    if data["sessionUpdate"] == "permission_request"
                        && data["request"]["_meta"]["provider"] == "fixture"
            )
        }));
        assert!(events.iter().any(
            |event| matches!(event, AcpStreamEvent::AssistantDelta { delta, message_id, .. } if delta == "fixture reply" && message_id.as_deref() == Some("message-2"))
        ));
        assert!(events.iter().any(
            |event| matches!(event, AcpStreamEvent::ReasoningDelta { delta, message_id, .. } if delta == "checking source" && message_id.as_deref() == Some("thought-1"))
        ));
        assert!(events.iter().any(|event| matches!(
            event,
            AcpStreamEvent::ToolActivity { call_id, title, kind, status, input, .. }
                if call_id == "web-1"
                    && title == "Web search"
                    && kind.as_deref() == Some("search")
                    && status.as_deref() == Some("completed")
                    && input["query"] == "weather in Tashkent"
        )));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_updates_for_a_different_session() {
        use std::{fs, os::unix::fs::PermissionsExt};

        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("misrouted-acp");
        fs::write(
            &fixture,
            r#"#!/bin/sh
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"agentCapabilities":{}}}\n' "$id" ;;
    *'"method":"session/new"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"session-a"}}\n' "$id" ;;
    *'"method":"session/prompt"'*)
      printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"session-b","update":{"sessionUpdate":"agent_message_chunk","content":{"text":"wrong answer"}}}}'
      printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"end_turn"}}\n' "$id"
      ;;
  esac
done
"#,
        )
        .unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();
        let mut events = Vec::new();

        let error = execute_acp_turn_inner(
            AcpHarnessConfig::new([fixture.into_os_string()]).unwrap(),
            directory.path().to_owned(),
            BTreeMap::new(),
            AcpSessionRequest {
                agent_id: mews_protocol::AgentId::new(),
                agent_slug: "coder".into(),
                transition: AcpBindingTransition::New,
                prompt: "hello".into(),
                recovery_prompt: "hello".into(),
                context_text: String::new(),
                instruction_channel: AcpInstructionChannel::FirstPrompt,
                skills: Vec::new(),
                hook_metadata: None,
            },
            &NoCapabilities,
            &[],
            CancellationToken::new(),
            &mut |event| {
                events.push(event);
                Ok(())
            },
        )
        .await
        .unwrap_err();

        assert!(format!("{error:#}").contains("unexpected Session"));
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, AcpStreamEvent::AssistantDelta { .. }))
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dead_cached_process_is_restarted_before_resume() {
        use std::{fs, os::unix::fs::PermissionsExt, sync::Arc};

        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("restart-acp");
        let starts = directory.path().join("starts");
        let script = r#"#!/bin/sh
printf 'start\n' >> '__STARTS__'
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"resume":{}}}}}\n' "$id" ;;
    *'"method":"session/new"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"fixture"}}\n' "$id" ;;
    *'"method":"session/resume"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id" ;;
    *'"method":"session/prompt"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"end_turn"}}\n' "$id"; exit 0 ;;
  esac
done
"#
        .replace("__STARTS__", &starts.to_string_lossy());
        fs::write(&fixture, script).unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

        let pool = AcpRuntimePool::new(Duration::from_secs(30));
        let environment: Arc<dyn AgentCapabilities> = Arc::new(NoCapabilities);
        let mut first_events = |_| Ok(());
        let mut first = persistent_fixture_request(
            &fixture,
            directory.path(),
            environment.clone(),
            AcpBindingTransition::New,
            "stable context",
            &mut first_events,
        );
        first.session_key = "session".into();
        pool.execute_turn(first).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut second_events = |_| Ok(());
        let mut second = persistent_fixture_request(
            &fixture,
            directory.path(),
            environment,
            AcpBindingTransition::Resume {
                acp_session_id: "fixture".into(),
            },
            "stable context",
            &mut second_events,
        );
        second.session_key = "session".into();
        pool.execute_turn(second).await.unwrap();

        assert_eq!(fs::read_to_string(starts).unwrap(), "start\nstart\n");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn persistent_pool_shares_only_compatible_sessions() {
        use std::{fs, os::unix::fs::PermissionsExt, sync::Arc};

        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("persistent-acp");
        let starts = directory.path().join("starts");
        let closes = directory.path().join("closes");
        let script = r#"#!/bin/sh
printf '%s\n' "$CODEX_CONFIG" >> '__STARTS__'
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"resume":{},"close":{}}}}}\n' "$id" ;;
    *'"method":"session/new"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"fixture-%s"}}\n' "$id" "$id" ;;
    *'"method":"session/resume"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id" ;;
    *'"method":"session/prompt"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"end_turn"}}\n' "$id" ;;
    *'"method":"session/close"'*) printf 'close\n' >> '__CLOSES__'; printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id" ;;
  esac
done
"#
        .replace("__STARTS__", &starts.to_string_lossy())
        .replace("__CLOSES__", &closes.to_string_lossy());
        fs::write(&fixture, script).unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

        let pool = AcpRuntimePool::new(Duration::from_millis(40));
        let agent_id = mews_protocol::AgentId::new();
        let environment: Arc<dyn mews_agent::AgentCapabilities> = Arc::new(NoCapabilities);
        for (runtime_id, transition, context) in [
            ("session-a", AcpBindingTransition::New, "shared context"),
            ("session-b", AcpBindingTransition::New, "shared context"),
            (
                "session-a",
                AcpBindingTransition::Resume {
                    acp_session_id: "fixture-2".into(),
                },
                "shared context",
            ),
            ("session-c", AcpBindingTransition::New, "isolated context"),
        ] {
            pool.execute_turn(PersistentAcpTurnRequest {
                session_key: runtime_id.into(),
                config: AcpHarnessConfig::new([fixture.clone().into_os_string()]).unwrap(),
                cwd: directory.path().to_owned(),
                harness_options: BTreeMap::new(),
                session: AcpSessionRequest {
                    agent_id: agent_id.clone(),
                    agent_slug: "coder".into(),
                    transition,
                    prompt: "hello".into(),
                    recovery_prompt: "hello".into(),
                    context_text: context.into(),
                    instruction_channel: AcpInstructionChannel::CodexDeveloper,
                    skills: Vec::new(),
                    hook_metadata: Some(AcpHookMetadata {
                        mews_session_id: runtime_id.into(),
                        turn_id: uuid::Uuid::now_v7().to_string(),
                        harness: "fixture".into(),
                        context_hash: String::new(),
                        context_channel: AcpInstructionChannel::CodexDeveloper,
                        invoke_turn_start: false,
                    }),
                },
                environment: environment.clone(),
                allowed_tools: Vec::new(),
                cancellation: CancellationToken::new(),
                events: &mut |_| Ok(()),
            })
            .await
            .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(120)).await;

        let starts = fs::read_to_string(starts)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(starts.len(), 2);
        assert_eq!(starts[0]["developer_instructions"], "shared context");
        assert_eq!(starts[1]["developer_instructions"], "isolated context");
        assert_eq!(fs::read_to_string(closes).unwrap().lines().count(), 3);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn persistent_pool_scales_concurrency_without_exceeding_its_limit() {
        use std::{fs, os::unix::fs::PermissionsExt, sync::Arc};

        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("concurrent-acp");
        let starts = directory.path().join("starts");
        let script = r#"#!/bin/sh
printf 'start\n' >> '__STARTS__'
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"agentCapabilities":{}}}\n' "$id" ;;
    *'"method":"session/new"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"fixture"}}\n' "$id" ;;
    *'"method":"session/prompt"'*) sleep 0.1; printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"end_turn"}}\n' "$id" ;;
  esac
done
"#
        .replace("__STARTS__", &starts.to_string_lossy());
        fs::write(&fixture, script).unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

        let pool = AcpRuntimePool::with_max_processes(Duration::from_millis(40), 2);
        let environment: Arc<dyn AgentCapabilities> = Arc::new(NoCapabilities);
        let mut events_a = |_| Ok(());
        let mut events_b = |_| Ok(());
        let mut events_c = |_| Ok(());
        let a = pool.execute_turn(persistent_fixture_request(
            &fixture,
            directory.path(),
            environment.clone(),
            AcpBindingTransition::New,
            "shared context",
            &mut events_a,
        ));
        let b = pool.execute_turn(persistent_fixture_request(
            &fixture,
            directory.path(),
            environment.clone(),
            AcpBindingTransition::New,
            "shared context",
            &mut events_b,
        ));
        let c = pool.execute_turn(persistent_fixture_request(
            &fixture,
            directory.path(),
            environment,
            AcpBindingTransition::New,
            "shared context",
            &mut events_c,
        ));
        let (a, b, c) = tokio::join!(a, b, c);
        a.unwrap();
        b.unwrap();
        c.unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;

        assert_eq!(fs::read_to_string(starts).unwrap(), "start\nstart\n");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn persistent_pool_evicts_idle_workers_at_the_global_limit() {
        use std::{fs, os::unix::fs::PermissionsExt, sync::Arc};

        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("globally-bounded-acp");
        let starts = directory.path().join("starts");
        let script = r#"#!/bin/sh
printf 'start\n' >> '__STARTS__'
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"agentCapabilities":{}}}\n' "$id" ;;
    *'"method":"session/new"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"fixture"}}\n' "$id" ;;
    *'"method":"session/prompt"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"end_turn"}}\n' "$id" ;;
  esac
done
"#
        .replace("__STARTS__", &starts.to_string_lossy());
        fs::write(&fixture, script).unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

        let pool = AcpRuntimePool::with_max_processes(Duration::from_secs(30), 2);
        let environment: Arc<dyn AgentCapabilities> = Arc::new(NoCapabilities);
        for context in ["context-a", "context-b", "context-c"] {
            let mut events = |_| Ok(());
            pool.execute_turn(persistent_fixture_request(
                &fixture,
                directory.path(),
                environment.clone(),
                AcpBindingTransition::New,
                context,
                &mut events,
            ))
            .await
            .unwrap();
            assert!(pool.process_count() <= 2);
        }

        assert_eq!(fs::read_to_string(starts).unwrap().lines().count(), 3);
        assert_eq!(pool.process_count(), 2);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn queued_persistent_turn_cancels_without_reaching_the_adapter() {
        use std::{fs, os::unix::fs::PermissionsExt, sync::Arc};

        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("queued-cancel-acp");
        let prompts = directory.path().join("prompts");
        let script = r#"#!/bin/sh
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"agentCapabilities":{}}}\n' "$id" ;;
    *'"method":"session/new"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"fixture"}}\n' "$id" ;;
    *'"method":"session/prompt"'*) printf 'prompt\n' >> '__PROMPTS__'; sleep 30 ;;
  esac
done
"#
        .replace("__PROMPTS__", &prompts.to_string_lossy());
        fs::write(&fixture, script).unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

        let pool = AcpRuntimePool::with_max_processes(Duration::from_secs(30), 1);
        let environment: Arc<dyn AgentCapabilities> = Arc::new(NoCapabilities);
        let first_cancellation = CancellationToken::new();
        let mut first_events = |_| Ok(());
        let mut first_request = persistent_fixture_request(
            &fixture,
            directory.path(),
            environment.clone(),
            AcpBindingTransition::New,
            "stable context",
            &mut first_events,
        );
        first_request.cancellation = first_cancellation.clone();
        let first = pool.execute_turn(first_request);
        tokio::pin!(first);
        loop {
            tokio::select! {
                result = &mut first => panic!("first Turn stopped before cancellation: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
            if prompts.is_file() {
                break;
            }
        }

        let second_cancellation = CancellationToken::new();
        let second_environment = Arc::new(RecordingCapabilities::default());
        let mut turn_end_observed = false;
        let mut second_events = |event| {
            if matches!(
                event,
                AcpStreamEvent::HookOutcome { ref hook, .. } if hook == "turn_end"
            ) {
                turn_end_observed = true;
            }
            Ok(())
        };
        let mut second_request = persistent_fixture_request(
            &fixture,
            directory.path(),
            second_environment.clone(),
            AcpBindingTransition::New,
            "stable context",
            &mut second_events,
        );
        second_request.session.hook_metadata = Some(AcpHookMetadata {
            mews_session_id: "queued-session".into(),
            turn_id: "queued-turn".into(),
            harness: "fixture".into(),
            context_hash: String::new(),
            context_channel: AcpInstructionChannel::CodexDeveloper,
            invoke_turn_start: true,
        });
        second_request.cancellation = second_cancellation.clone();
        let error = {
            let second = pool.execute_turn(second_request);
            tokio::pin!(second);
            tokio::select! {
                result = &mut second => panic!("queued Turn stopped before cancellation: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
            second_cancellation.cancel();
            tokio::time::timeout(Duration::from_millis(100), &mut second)
                .await
                .expect("queued cancellation should return promptly")
                .unwrap_err()
        };
        assert!(crate::rpc::is_cancelled(&error));
        assert!(turn_end_observed);
        {
            let hooks = second_environment.hooks.lock().unwrap();
            assert_eq!(hooks.len(), 1);
            assert_eq!(hooks[0].0, LifecycleHook::TurnEnd);
            assert_eq!(hooks[0].1["status"], "cancelled");
        }

        first_cancellation.cancel();
        first.await.unwrap_err();
        assert_eq!(fs::read_to_string(prompts).unwrap(), "prompt\n");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn active_persistent_cancellation_persists_turn_end_before_returning() {
        use std::{fs, os::unix::fs::PermissionsExt, sync::Arc};

        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("active-cancel-acp");
        let prompts = directory.path().join("prompts");
        let script = r#"#!/bin/sh
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"agentCapabilities":{}}}\n' "$id" ;;
    *'"method":"session/new"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"fixture"}}\n' "$id" ;;
    *'"method":"session/prompt"'*) printf 'prompt\n' >> '__PROMPTS__'; sleep 30 ;;
  esac
done
"#
        .replace("__PROMPTS__", &prompts.to_string_lossy());
        fs::write(&fixture, script).unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

        let pool = AcpRuntimePool::new(Duration::from_secs(30));
        let environment = Arc::new(RecordingCapabilities::default());
        let cancellation = CancellationToken::new();
        let mut turn_end_observed = false;
        let mut events = |event| {
            if matches!(
                event,
                AcpStreamEvent::HookOutcome { ref hook, .. } if hook == "turn_end"
            ) {
                turn_end_observed = true;
            }
            Ok(())
        };
        let mut request = persistent_fixture_request(
            &fixture,
            directory.path(),
            environment.clone(),
            AcpBindingTransition::New,
            "stable context",
            &mut events,
        );
        request.session.hook_metadata = Some(AcpHookMetadata {
            mews_session_id: "active-session".into(),
            turn_id: "active-turn".into(),
            harness: "fixture".into(),
            context_hash: String::new(),
            context_channel: AcpInstructionChannel::CodexDeveloper,
            invoke_turn_start: true,
        });
        request.cancellation = cancellation.clone();

        let error = {
            let execution = pool.execute_turn(request);
            tokio::pin!(execution);
            loop {
                tokio::select! {
                    result = &mut execution => panic!("active Turn stopped before cancellation: {result:?}"),
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                if prompts.is_file() {
                    break;
                }
            }
            cancellation.cancel();
            execution.await.unwrap_err()
        };

        assert!(crate::rpc::is_cancelled(&error));
        assert!(turn_end_observed);
        let hooks = environment.hooks.lock().unwrap();
        assert_eq!(
            hooks.iter().map(|(hook, _)| *hook).collect::<Vec<_>>(),
            vec![
                LifecycleHook::TurnStart,
                LifecycleHook::BeforeModel,
                LifecycleHook::TurnEnd,
            ]
        );
        assert_eq!(hooks.last().unwrap().1["status"], "cancelled");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn idle_runtime_is_evicted_and_resumed_with_its_context() {
        use std::{fs, os::unix::fs::PermissionsExt, sync::Arc};

        let directory = tempfile::tempdir().unwrap();
        let fixture = directory.path().join("evictable-acp");
        let starts = directory.path().join("starts");
        let closes = directory.path().join("closes");
        let script = r#"#!/bin/sh
printf '%s\n' "$CODEX_CONFIG" >> '__STARTS__'
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"resume":{},"close":{}}}}}\n' "$id" ;;
    *'"method":"session/new"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"fixture"}}\n' "$id" ;;
    *'"method":"session/resume"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id" ;;
    *'"method":"session/prompt"'*) printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"end_turn"}}\n' "$id" ;;
    *'"method":"session/close"'*) printf 'close\n' >> '__CLOSES__'; printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id" ;;
  esac
done
"#
        .replace("__STARTS__", &starts.to_string_lossy())
        .replace("__CLOSES__", &closes.to_string_lossy());
        fs::write(&fixture, script).unwrap();
        fs::set_permissions(&fixture, fs::Permissions::from_mode(0o700)).unwrap();

        let pool = AcpRuntimePool::new(Duration::from_millis(40));
        let environment: Arc<dyn mews_agent::AgentCapabilities> = Arc::new(NoCapabilities);
        let mut first_events = |_| Ok(());
        pool.execute_turn(persistent_fixture_request(
            &fixture,
            directory.path(),
            environment.clone(),
            AcpBindingTransition::New,
            "stable context",
            &mut first_events,
        ))
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
        let mut second_events = |_| Ok(());
        pool.execute_turn(persistent_fixture_request(
            &fixture,
            directory.path(),
            environment,
            AcpBindingTransition::Resume {
                acp_session_id: "fixture".into(),
            },
            "stable context",
            &mut second_events,
        ))
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;

        let starts = fs::read_to_string(starts).unwrap();
        assert_eq!(starts.lines().count(), 2);
        assert!(starts.lines().all(|line| {
            serde_json::from_str::<Value>(line).unwrap()["developer_instructions"]
                == "stable context"
        }));
        assert_eq!(fs::read_to_string(closes).unwrap(), "close\nclose\n");
    }
}
