# MEWS architecture and protocols

MEWS is a home for durable AI agents. Hub supplies durable coordination; Hosts supply execution; clients and channel adapters translate user interactions into a small client protocol. Concrete Slack, Telegram, web, or TUI behavior does not belong in Hub.

## Ownership

Exactly one Hub generation owns the canonical SQLite database, installation signing authority, shared Hub Noise identity, Agent revisions, Sessions, Messages, and Runs. Every physical machine is a Host, including the Hub machine. A Host owns its OS authority, current working directories, Agent replicas, and live tool implementations.

An Agent is an immutable ID with a mutable slug and immutable revisions. Each revision contains `SOUL.md` and `agent.toml`. A Session permanently captures Agent ID/revision, Host ID, and the Host-canonicalized working directory. Project files never synchronize. The Host reads applicable project `AGENTS.md` files and returns bounded context; Hub never interprets a remote path as a local path.

SQLite is the canonical store. It uses one clean schema, WAL, foreign keys, bounded metadata, one active Run per Session, and recovery records for interrupted Runs/tool calls. Agent folders are editable replicas. Synchronization compares their observed base revision, pushes valid edits through optimistic revision creation, and replaces pulled replicas by an atomic directory swap while retaining the previous directory for recovery.

Successful file and replica replacement retains at most one MEWS-generated predecessor beside the active object. Database and credential backups use `<name>.previous-<uuid>`; Agent replicas use `.<slug>.previous-<uuid>`. Cleanup only matches those generated names.

## Extension boundaries

The public Rust interfaces are deliberately small:

- `mews-protocol` owns stable IDs and data-transfer objects plus the versioned Hub/client and Hub/Host serialized contracts. It contains no storage, networking, or cryptographic key operations.
- `mews-relay` owns relay admission verification, registration, bounded stateless routing, and its WebSocket client and server. It sees only opaque application ciphertext; MEWS retains relay lifecycle policy, persistence, and supervision.
- `mews-client` connects only to the local MEWS daemon and exposes typed Hub operations. Its Channel runtime persists conversation-to-Session mappings, queues inbound messages per Session, and acknowledges durable outbound events after platform delivery.
- `mews-router` is a separately running local model gateway. It implements the `mews-agent` provider contract, deterministic `provider/model` dispatch, provider adapters, auth storage, OAuth refresh, and retries. Hub reaches it through an owner-only Unix socket and does not call provider adapters in-process.
- `mews-agent` owns the complete provider-agnostic harness API: the streamed model/tool loop, schema validation, scheduling, steering/follow-ups, context transforms, cancellation, progress, lifecycle events, and the `AgentCapabilities` boundary for context, prompts, tools, and hooks. Its public `MessageQueue` supports reusable runtimes that implement steering/follow-ups; the MEWS composition root does not currently use that helper. It has no knowledge of Hub, concrete Hosts, SQLite, Sessions, or resource discovery.
- `mews-runtime` composes Agent soul and environment context, adapts durable conversation storage, and connects `mews-agent` to a model provider and execution environment. It has no SQLite or concrete Host dependency.
- `mews-acp` owns the generic external-Harness boundary: isolated process launch, ACP JSON-RPC, persistent session resume/recovery, permissions, and the Run-scoped MCP extension bridge. It has no provider-specific installation or profile policy.
- `mews-store` owns the concrete SQLite schema, queries, and transactional persistence invariants. It has no knowledge of Hub process lifecycle, MEWS home-directory layout, networking, or runtime execution.
- `mews-host` owns concrete full-authority hands: project context and resource discovery, built-in and executable tools, extensions, hooks, process limits, and catalog hot reload.
- `mews::runtime_store` implements `ConversationStore` for the current SQLite Store and binds Session identity. `mews-runtime` drives Run begin/completion/failure through that contract.
- `Tool` is a named schema plus an asynchronous Host-side execution function.
- `ToolRegistry` supports live registration/replacement/removal and publishes catalog changes to connected Hub handles.
- `HostControl` owns remote Host coordination: attestation, Agent synchronization, relay configuration, Hub transfer, Harness catalog inspection, and ACP execution/session coordination. `AgentCapabilities` independently contains native Harness context, prompts, tools, and hooks. `HostExecutor` is only the composition marker for a connected endpoint implementing both interfaces.
- `HubRequest`/`HubResponse` is the client/adapter protocol. Adapters may attach up to 64 KiB of arbitrary JSON metadata to a user message. Metadata and source attribution are preserved as untrusted context; adapters—not Agents—choose identities, threads, or channel routing.

The standalone binary exposes the client protocol over an owner-only Unix socket. An external adapter can speak the versioned JSON-line frames without being compiled into Hub. It can keep channel-specific state, map a Telegram/Slack user or thread to a Session, start a fresh Session, or explicitly resume one. Hub remains unaware of those products.

The runtime depends only on `AgentCapabilities`. Local and remote Hosts implement the same interface; transport, process execution, and filesystem location are invisible to the agent brain. The `mews` binary is the composition root that binds a durable Session to capabilities and a model provider.

Channels are client implementations, not Hub plugins. A concrete channel implements the small `Channel` receive/send interface and delegates Session mapping, Run submission, replay, and delivery bookkeeping to `mews-client`. Channel credentials and platform state stay on the machine running that client.

Hub stores a checkpointed client event journal. Consumers subscribe to Sessions and long-poll through their local daemon using a stable `ConsumerId`. Assistant messages and terminal Run events are committed with Hub state; durable consumers acknowledge monotonically increasing checkpoints only after delivery, and rows are pruned after every relevant durable consumer passes them. Streaming deltas are transient and never retained beyond the relevant consumer floor. Unacknowledged durable events replay after reconnect. Delivery is at least once because no external messaging API and Hub transaction can commit atomically; local delivery receipts reduce duplicates across ordinary restarts.

`StartTurn` is idempotent, creates a durable Run, and returns immediately. Runs can be fetched, awaited, or cancelled. The typed client's synchronous `send_message` convenience is implemented by starting a Run and consuming its durable events. Run start, tool start/completion, assistant message, and terminal events share the journal. Channel clients can continue receiving platform traffic while a Run executes; their local durable queue serializes additional messages for the same Session.

Each external MEWS Session owns one durable ACP Session binding on its selected
Host. New bindings are acknowledged by the Hub before the first prompt executes;
later Runs resume that native Session and submit the current user turn in the
standard MEWS identity wrapper. Only the
typed ACP `resource_not_found` error permits silent reconstruction from canonical
MEWS history. Timeouts, disconnects, crashes, and other ambiguous failures never
retry the prompt or replace the binding.

The client reconnects automatically only for reads and explicitly idempotent operations; it reports an unknown outcome instead of blindly replaying unsafe mutations. A Channel state file is protected by an exclusive process lock, preventing two instances from consuming the same identity. On restart the runtime reconciles recorded active Runs with Hub. Outbound failures use bounded exponential retry and are moved to the local dead-letter table after exhaustion so later Hub events are not permanently blocked.

A location-aware client supplies its canonicalizable working directory and therefore runs on the Host carrying that client connection. A locationless adapter omits the directory; Hub then selects the Hub Host and that OS user's home directory. Subsequent turns follow the Session's permanent Host binding.

`agent.toml` selects an explicit logical Harness and its opaque Harness-owned options, a tool allowlist, and `tool_execution = "parallel" | "sequential"` (parallel by default). The allowlist applies to every tool exposed to the Harness, including the native `read`, `write`, `edit`, and `bash` tools. The native `mews` Harness owns its `model` and `reasoning` options; external Harnesses own their own selectors. A Session never silently changes Host and never falls back when its Host is offline.

Each provider adapter owns authenticated model discovery and filtering. OpenAI Codex and Anthropic support OAuth login; Anthropic uses authorization-code PKCE with a localhost callback, and both adapters refresh expiring tokens inside the router. Successful login or API-key updates attempt to refresh that provider in the owner-only `models.json` cache; operators can force a full refresh with `mews providers models update`. Installation default model and reasoning selections live in Hub SQLite and are copied when a native `mews` Agent is created. Native Agents that omit either option inherit the current installation default at run time.

Provider IDs follow Pi's model namespace: `openai` for OpenAI API keys, `openai-codex` for ChatGPT/Codex OAuth, `anthropic` for Anthropic credentials, and `google` for Gemini Developer API keys.

For the native `mews` Harness, `/model <provider/model>` installs a durable Session-level model override; `/model default` clears it. The Harness reads the override at the beginning of each Run, so an in-progress Run remains internally consistent.

Host executable extensions live in `<MEWS_HOME>/tools/*.toml`:

```toml
name = "issue_lookup"
description = "Look up an issue by ID"
command = ["/opt/mews-tools/issue-lookup"]
schema = { type = "object", properties = { id = { type = "string" } }, required = ["id"] }
```

The executable starts in the Session cwd, reads one JSON argument value from stdin, and must write one JSON result value to stdout. Exit failure, invalid JSON, timeout, and output limits become tool errors. The Host daemon rescans manifests and publishes catalog changes without a restart. Extensions run with the Host user's full authority and are therefore trusted Host configuration.

For the native `mews` Harness, Skills use the Agent Skills layout and are
discovered from `<MEWS_HOME>/skills/` plus `.agents/skills/` in the Session
working directory and its ancestors. Only name, description, and `SKILL.md`
path enter the system prompt; the Agent reads the full skill on demand. Prompt
templates are discovered from `<MEWS_HOME>/prompts/` and ancestor
`.agents/prompts/`. Sending `/name args` expands the matching Markdown template
and replaces `$ARGUMENTS` with `args` before any Harness runs.

Full-authority runtime extensions live in `<MEWS_HOME>/extensions/*.toml`:

```toml
name = "policy"
command = ["/opt/mews-extensions/policy"]
hooks = ["run_start", "before_model", "before_tool", "after_tool", "after_turn", "run_end"]

[[tools]]
name = "lookup"
description = "Look up a record"
schema = { type = "object", properties = { id = { type = "string" } }, required = ["id"] }
```

The executable receives one JSON envelope on stdin and writes one JSON value to
stdout. Tool envelopes have `{type, name, arguments}`. Hook envelopes have
`{type, extension, hook, payload}`; returning `null` preserves the payload and
any other value replaces it for the next handler. `before_tool` may return
`{ "block": "reason" }` or replace `name`/`arguments`; `after_tool` may
replace `result`/`is_error`; `before_model` returns the model request. Extension
files and registered tools hot-reload with the Host catalog. Native `mews` runs
invoke these lifecycle hooks. ACP Harnesses currently receive extension tools
through the run-scoped MCP bridge, but do not invoke lifecycle hooks or receive
the Skill inventory. TODO: explore passing Skills and lifecycle hooks to ACP
Harnesses. Like Pi extensions, they are unsandboxed and may modify MEWS itself
with the Host user's authority.

## Remote protocol

Enrollment offers are signed, expire after 15 minutes, and contain a single-use bearer secret. Joining proves possession of independent Ed25519 signing and X25519 Noise identities. The installation authority issues a signed relay admission valid for 100 years to the enrolled Host. This lets the relay remain stateless: it verifies the ticket and proof of possession without consulting Hub storage. Removing a Host prevents Hub reconnection, but does not revoke a ticket already held by that Host at the relay.

The relay is stateless. It authenticates authority-scoped peer admission and proof of possession, enforces connection/frame/queue bounds, and routes opaque frames. Hub and Host then perform Noise XX, bind protocol version, roles, installation, peer IDs, stream ID, static Noise keys, and Ed25519 identities into the authenticated transcript, and encrypt all application data end to end. Encrypted messages are ordered and fragmented into bounded records. Thirty-second encrypted heartbeats preserve idle links; both sides reconnect, and Hub reconstructs per-Host listeners from SQLite after restart.

Host frames are versioned and capped at 256 KiB. Client frames are capped at 1 MiB. Project context is capped at 192 KiB in aggregate, individual context/tool outputs are bounded, relay registration has a deadline, and relay connections/queues are bounded.

Hub setup supervises a bundled relay on `0.0.0.0:8787` by default and advertises a `.local` address derived from the configured Host name. LAN and private-overlay deployments may use `ws://` because Hub/Host application frames remain authenticated and end-to-end encrypted. Public relay deployment should advertise `wss://` and supply `--relay-listen`; the bundled relay serves plain WebSocket and expects TLS termination by a reverse proxy.

## Hub movement

`mews hub move <host>` acquires a fair installation-wide write barrier shared by local requests, remote client requests, and delayed enrollment acceptance. A move is rejected while any Run is active; later mutations wait and are rejected once handoff begins.

Hub advances the generation with a compare-and-set, creates a consistent SQLite backup, and sends bounded chunks plus credentials over the authenticated Host stream. The target checks total length/hash, installation ID, generation, target Host ID, authority key, and its physical Host identity. Preparation cannot promote by itself. After the source durably records its decision to demote, it sends a matching one-time activation token. Target activation is idempotent and crash-resumable. Only an armed target can run `mews hub recover`.

Failures before activation roll the source forward to a newer generation. During activation, a durable source recovery coordinator restarts fenced, reconnects only the target, and retries arm/activate before demoting. Once activation is armed, the source favors single-writer safety over availability if the final acknowledgment is lost. Successful demotion removes the old database, installation authority, provider credentials, and Hub Noise secret, then reconnects the same physical identity as a normal Host. Its relay retires after a migration grace period: 10 minutes when every enrolled remote Host acknowledged the replacement candidates, or 10 days otherwise. Moving back is the same protocol and is covered by the public end-to-end test.

## Operations and security boundary

State directories are mode `0700`; identities, provider credentials, sockets, state files, and databases are owner-only. When the router first creates `auth.json`, it imports supported provider variables visible in the router process's environment; an installed launchd or systemd service does not inherit the shell that invoked setup, so interactive provider commands are the reliable configuration path. The resulting file participates in Hub movement. Identity loading rejects symlinks and permissive modes. Tools deliberately execute with the Host OS user's authority—MEWS does not claim to be a sandbox. A channel adapter must treat user IDs and metadata as untrusted input, and a public relay should be rate-limited and TLS-terminated at the edge.

Useful commands:

```text
mews                         Show help
mews setup                   Create Hub and its first Host
mews setup --join <offer>    Enroll this machine as a Host
mews agents list             List Agents
mews agents new <slug> [--harness <name>] [--option <key=value>]...  Create an Agent
mews agents rename <old> <new> Rename an Agent
mews agents delete <slug>    Archive an Agent
mews agents <slug>           Start a fresh interactive Session
mews sessions list           List Sessions
mews sessions <id>           Explicitly resume a Session
mews sessions <id> ask …     Resume from any machine on the bound Host
mews hosts list              List enrolled Hosts and connection status
mews hosts invite --relay …  Create one enrollment offer
mews hosts remove <host>     Revoke and remove a Host
mews harnesses list          Inspect live Host Harness catalogs
mews harnesses refresh       Refresh Host-local Harness discovery
mews harnesses setup <name>  Install, authenticate, and probe one local managed Harness
mews providers status             List configured provider credentials
mews providers login              Select a provider and authenticate
mews providers set-key [provider] Add or rotate an API key
mews providers logout <provider>  Remove a provider credential
mews hub move <host>         Move Hub to a connected Host
mews hub recover             Resume an armed target after uncertain activation
mews relay serve             Run the stateless relay
```

Setup installs one role-detecting per-user daemon through launchd on macOS or systemd on Linux. After Hub movement the same process becomes a Host, while the target Host process becomes Hub; restarts infer the current role from local state.
