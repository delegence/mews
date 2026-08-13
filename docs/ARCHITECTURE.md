# MEWS architecture and protocols

MEWS is a home for durable AI agents. Hub supplies durable coordination; Hosts supply execution; clients and channel adapters translate user interactions into a small client protocol. Concrete Slack, Telegram, web, or TUI behavior does not belong in Hub.

## Ownership

Exactly one Hub generation owns the canonical SQLite database, installation signing authority, shared Hub Noise identity, Agent revisions, Sessions, Messages, and Turns. Every physical machine is a Host, including the Hub machine. A Host owns its OS authority, current working directories, Agent replicas, and live tool implementations.

An Agent is an immutable ID with a mutable slug and immutable revisions. Each revision contains `SOUL.md` and `agent.toml`. A Session permanently captures the Agent ID, Host ID, and Host-canonicalized working directory. Each Turn atomically snapshots the Agent's current revision when accepted, preserving exact provenance while allowing long-lived Sessions to use later improvements. Project files never synchronize. The Host reads applicable project `AGENTS.md` files and returns bounded context; Hub never interprets a remote path as a local path.

SQLite is the canonical store. Ordinary relational tables are the authoritative current state: installation, Hosts, Agents and revisions, Sessions and transcript entries, Turns, effects, ACP bindings, delivery queues, and command receipts. SQLite runs with WAL, foreign keys, `synchronous=FULL`, bounded metadata, one active Turn per Session, and recovery records for interrupted Turns and tool calls. Agent folders are editable replicas. Synchronization compares their observed base revision, pushes valid edits through optimistic revision creation, and replaces pulled replicas by an atomic directory swap while retaining the previous directory for recovery.

### Transactional state and journal

A command asks Hub to change state. A journal entry describes a semantic change committed by Hub. A runtime signal reports live progress that may be lost. These are separate types and APIs.

Each journal entry has an ID, a global position, a typed subject (`Installation`, `Host`, `Agent`, or `Session`) and subject ID, a stable event type, timestamp, actor, optional command and correlation IDs, and a typed payload. Turns and effects use their Session as the journal subject. The global position supports ordered inspection and resumable journal queries.

For an idempotent command, a receipt stores the hash of the complete canonical request and its stable result. Retrying the same command ID and request returns that result without rerunning the mutation; reusing the ID for different input is a conflict. State changes, journal entries, durable delivery rows, and the receipt commit in one SQLite transaction or all roll back. Uniqueness constraints, compare-and-set updates, and `BEGIN IMMEDIATE` transactions protect state concurrency directly.

The journal is an audit and subscription record, not a state reconstruction mechanism. Hub never rebuilds tables from it, keeps projector checkpoints, or uses per-subject versions. Reads and command decisions use the authoritative tables. The journal covers semantic installation, Host, Agent, Session, Turn, ACP, and effect changes; credentials, sockets, transfer chunks, discovered catalogs, streaming deltas, process handles, and consumer cursors stay outside it.

External work crosses an explicit effect boundary. The authoritative effect row records `scheduled`, `started`, `succeeded`, `failed`, or `uncertain`, and the journal records the corresponding semantic changes. Recovery never guesses that an ambiguous started effect is safe to retry. For tools, the effect row stores the immutable raw external result before any hook runs; `ToolExecutionCompleted` records that result in the journal, while the transcript and `ToolResultRecorded` store the possibly transformed result presented to the model. Rich completed assistant responses and tool outcomes are durable; text/reasoning deltas and tool progress are transient signals.

### Conversation authority and replay

The Session transcript is the durable semantic center of MEWS. Every Harness writes the same typed entry vocabulary: user messages, Turn lifecycle, rich assistant responses, tool lifecycle and results, visible reasoning, permissions, compaction, and opaque Harness observations. Contextual entries advance the Session leaf and participate in model replay; observational entries anchor to that leaf without entering model context. Streaming text/reasoning deltas remain transient delivery events and never become canonical transcript history.

`sessions`, `session_entries`, `turns`, effects, ACP bindings, and client delivery rows are authoritative state. Their indexes and constraints enforce one-active-Turn behavior, cancellation, crash recovery, idempotency, external Session resume, and bounded polling. A durable delivery row references the journal entry committed with it; transient delivery rows carry only live progress.

The native Harness maps provider and tool-loop events directly into typed transcript entries. ACP adapters normalize lossless common facts into the same entries and retain protocol-specific or incomplete facts as `HarnessObservation`; an ACP status-only tool completion, for example, is not fabricated into a semantic `ToolResult`. The ACP Session binding remains the external continuation authority, while the normalized transcript supplies portable user-visible history.

The native `mews` Harness persists each provider invocation as one ordered rich assistant response. A response records provider/model/API provenance, an optional provider response ID, usage and stop reason, plus ordered visible-reasoning, text, tool-call, and opaque-state blocks. Signatures and encrypted or redacted provider state stay on their exact blocks. Visible reasoning is a separate typed block; opaque state is never presented as reasoning.

The rich transcript is native continuation authority. Full canonical replay always works. OpenAI Responses may additionally use a response ID as an optimization cursor: MEWS selects the newest active-branch response with the same provider, model, and API, sends only the suffix after that anchor, and disables the cursor after branch movement or context compaction. A typed missing, expired, or incompatible cursor rejection permits exactly one retry with full replay. Timeout, disconnect, ambiguous transport failure, and any result that may have executed are never retried. Provider response IDs are attached to their producing response, never to a Session.

Cross-provider or cross-model replay uses one portable projection: user text, final assistant text, tool calls without provider signatures, and necessary semantic tool outcomes. It strips response IDs, opaque/encrypted/redacted reasoning, signatures, raw provider state, and private metadata. ACP reconstruction and canonical ACP prompts use the same projection.

`ContextCompaction` records a summary, the first retained entry ID, and pre-compaction token count. History is not deleted; the latest applicable compaction on the active branch deterministically supplies the summary plus its retained suffix and invalidates older response cursors.

Successful file and replica replacement retains at most one MEWS-generated predecessor beside the active object. Database and credential backups use `<name>.previous-<uuid>`; Agent replicas use `.<slug>.previous-<uuid>`. Cleanup only matches those generated names.

## Extension boundaries

The public Rust interfaces are deliberately small:

- `mews-protocol` owns stable IDs and data-transfer objects plus the versioned Hub/client and Hub/Host serialized contracts. It contains no storage, networking, or cryptographic key operations.
- `mews-relay` owns relay admission verification, registration, bounded stateless routing, and its WebSocket client and server. It sees only opaque application ciphertext; MEWS retains relay lifecycle policy, persistence, and supervision.
- `mews-client` connects only to the local MEWS daemon and exposes typed Hub operations. `mews-channel` is an optional standalone adapter runtime that persists conversation-to-Session mappings, queues inbound messages per Session, and routes subscribed Hub events to bounded per-conversation delivery lanes.
- `mews-router` is a separately running local model gateway. It implements the `mews-agent` provider contract, deterministic `provider/model` dispatch, provider adapters, auth storage, OAuth refresh, and retries. Hub reaches it through an owner-only Unix socket and does not call provider adapters in-process.
- `mews-agent` owns the complete provider-agnostic harness API: the streamed model/tool loop, schema validation, scheduling, steering/follow-ups, context transforms, cancellation, progress, lifecycle events, and the `AgentCapabilities` boundary for context, prompts, tools, and hooks. Its public `MessageQueue` supports reusable runtimes that implement steering/follow-ups; the MEWS composition root does not currently use that helper. It has no knowledge of Hub, concrete Hosts, SQLite, Sessions, or resource discovery.
- `mews-runtime` composes Agent soul and environment context, adapts durable conversation storage, and connects `mews-agent` to a model provider and execution environment. It has no SQLite or concrete Host dependency.
- `mews-acp` owns the generic external-Harness boundary: isolated process launch, ACP JSON-RPC, persistent session resume/recovery, permissions, and the Turn-scoped MCP extension bridge. It has no provider-specific installation or profile policy.
- `mews-store` owns the concrete SQLite schema, queries, and transactional persistence invariants. It has no knowledge of Hub process lifecycle, MEWS home-directory layout, networking, or runtime execution.
- `mews-host` owns concrete full-authority hands: project context and resource discovery, built-in and executable tools, extensions, hooks, process limits, and catalog hot reload.
- `mews` is the application and Hub composition root. Its native execution adapter implements `ConversationStore` for the SQLite Store and binds Session identity; `mews-runtime` drives Turn begin/completion/failure through that contract.
- `Tool` is a named schema plus an asynchronous Host-side execution function.
- `ToolRegistry` supports live registration/replacement/removal and publishes catalog changes to connected Hub handles.
- `HostControl` owns remote Host coordination: attestation, Agent synchronization, relay configuration, Hub transfer, Harness catalog inspection, and ACP execution/session coordination. `AgentCapabilities` independently contains native Harness context, prompts, tools, and hooks. `HostExecutor` is only the composition marker for a connected endpoint implementing both interfaces.
- `HubRequest`/`HubResponse` is the client/adapter protocol. Adapters may attach up to 64 KiB of arbitrary JSON metadata to a user message. Metadata and source attribution are preserved as untrusted context; adapters—not Agents—choose identities, threads, or channel routing.

The standalone binary exposes the client protocol over an owner-only Unix socket. An external adapter can speak the versioned JSON-line frames without being compiled into Hub. It can keep channel-specific state, map a Telegram/Slack user or thread to a Session, start a fresh Session, or explicitly resume one. Hub remains unaware of those products.

The runtime depends only on `AgentCapabilities`. Local and remote Hosts implement the same interface; transport, process execution, and filesystem location are invisible to the agent brain. The `mews` binary is the composition root that binds a durable Session to capabilities and a model provider.

Channels are client implementations, not Hub plugins. A concrete channel implements the small `mews-channel` inbound/outbound interface and delegates Session mapping, Turn submission, event routing, FIFO lanes, and bounded admission to that optional crate. Channel credentials and platform state stay on the machine running that client. Each standalone process configures its own worker and pending-delivery bounds; those defaults are deployment choices, not Hub protocol constants. A bundled process is only a deployment convenience and has the same boundary.

Hub exposes a checkpointed client-delivery queue. Consumers subscribe to Sessions and long-poll through their local daemon using a stable `ConsumerId`. Assistant responses, transcript state, journal entries, and durable delivery rows commit atomically. Channel consumers acknowledge a polled batch before admitting external delivery. A process crash after that acknowledgment may therefore lose the external copy intentionally; the canonical response remains in MEWS history. This best-effort ordering favors avoiding duplicate platform messages over preventing rare loss. Event polling is bounded by encoded bytes as well as count, and insertion rejects any single event that could not fit in a Hub frame. Streaming deltas are transient signals and never enter the journal.

Each channel-originated Turn durably records the standalone consumer identity and destination conversation that initiated it. Completed, streaming, and lifecycle delivery events carry that typed origin, so normal responses route only through the matching channel identity even when several channel processes subscribe to the same Session. Explicit broadcasts use a separate channel handle and enter each destination's ordinary FIFO lane. A destination key is the adapter-defined account/chat/thread conversation string; all messages and attachment batches for that key are serialized, while different keys may use the configurable worker pool concurrently. The pending-work bound includes queued, running, and delayed work. Empty lanes are removed.

Adapters advertise only the capabilities they implement (currently streaming, edits, attachments, and typing) and subscribe only to the event families they consume (completed messages, streaming updates, and lifecycle events). Streaming subscriptions require streaming capability. The adapter owns platform rate limits, send timeouts, transient/permanent classification, retry counts, `Retry-After`, formatting, attachments, edits, and typing behavior. Its typed delivery outcome is delivered, retry after a chosen delay, or dropped. A delayed retry keeps its conversation lane blocked for FIFO but releases the generic worker slot. Terminal delivered receipts and dropped reasons are published through the channel handle's bounded in-process diagnostic stream; core neither persists them, invents retry policy, disables a channel, nor notifies an owner. Shutdown immediately cancels running sends and drops queued or delayed best-effort work; it does not promise draining.

`StartTurn` is idempotent, creates a durable Turn, and returns immediately. Channel turn keys are deterministically scoped by the stable channel consumer identity, destination conversation, and platform-local external message ID, so same-named adapters and shared Sessions cannot collapse distinct inputs. Turns can be fetched, awaited, or cancelled. The typed client's synchronous `send_message` convenience is implemented by starting a Turn and consuming its durable delivery events. Turn start, tool start/completion, assistant message, and terminal changes are recorded in the journal. Channel clients can continue receiving platform traffic while a Turn executes; their local durable queue serializes additional messages for the same Session.

Each external MEWS Session owns one durable ACP Session binding on its selected
Host. New bindings persist a versioned, hash-addressed MEWS context snapshot and
are acknowledged by the Hub before the first prompt executes; later compatible
Turns resume that native Session and submit only the current user turn. Managed
Codex and Claude recipes deliver the initial context through their private
instruction channels; unknown adapters receive a one-time first-prompt envelope.
For those adapters, the Hub durably records and acknowledges context dispatch
immediately before the Host sends the first prompt. Once that irreversible boundary
is crossed, the binding is resumable even when the prompt outcome is ambiguous.
Only the
typed ACP `resource_not_found` error permits silent reconstruction from canonical
MEWS history. Timeouts, disconnects, crashes, and other ambiguous failures never
retry the prompt or replace the binding.

An ACP binding is compatible only while its Host, Harness definition, rendered
instruction context, and conversation continuation remain compatible. Agent soul
or skill changes replace the binding using the rendered context hash. A Turn run
through another Harness also replaces the old binding before ACP resumes, because
the external Session cannot contain that intervening canonical history.

ACP and native continuation remain deliberately distinct. An ACP Session ID is required continuation authority and stays in `acp_session_bindings`; provider response IDs hidden behind an ACP adapter remain invisible. Live ACP reasoning deltas are transient client events. MEWS durably stores at most one bounded completed semantic reasoning observation per provider item/message when visible, with explicit visible/redacted/omitted semantics, rather than retaining an unbounded delta log.

The client reconnects automatically only for reads and explicitly idempotent operations; it reports an unknown outcome instead of blindly replaying unsafe mutations. A Channel state file is protected by an exclusive process lock, preventing two instances from consuming the same identity. On restart the runtime reconciles recorded active Turns with Hub. Outbound delivery receipts, universal retries, and dead letters are deliberately not persisted; an adapter that returns a delayed retry owns whether another attempt should occur.

A location-aware client supplies its canonicalizable working directory and therefore runs on the Host carrying that client connection. A locationless adapter omits the directory; Hub then selects the Hub Host and that OS user's home directory. Subsequent turns follow the Session's permanent Host binding. Remote tool calls are request-correlated; dropping or cancelling an in-flight request sends a Host cancellation that terminates the registered execution token and, for shell tools, its isolated process group.

`agent.toml` selects an explicit logical Harness and its opaque Harness-owned options, a tool allowlist, and `tool_execution = "parallel" | "sequential"` (parallel by default). The allowlist applies to every tool exposed to the Harness, including the native `read`, `write`, `edit`, and `bash` tools. The native `mews` Harness owns its `model` and `reasoning` options; external Harnesses own their own selectors. A Session never silently changes Host and never falls back when its Host is offline.

Each provider adapter owns authenticated model discovery and filtering. OpenAI Codex and Anthropic support OAuth login; Anthropic uses authorization-code PKCE with a localhost callback, and both adapters refresh expiring tokens inside the router. Successful login or API-key updates attempt to refresh that provider in the owner-only `models.json` cache; operators can force a full refresh with `mews providers models update`. Installation default model and reasoning selections live in Hub SQLite and are copied when a native `mews` Agent is created. Native Agents that omit either option inherit the current installation default at run time.

Provider IDs follow Pi's model namespace: `openai` for OpenAI API keys, `openai-codex` for ChatGPT/Codex OAuth, `anthropic` for Anthropic credentials, and `google` for Gemini Developer API keys.

For the native `mews` Harness, `/model <provider/model>` installs a durable Session-level model override; `/model default` clears it. The Harness reads the override at the beginning of each Turn, so an in-progress Turn remains internally consistent.

For the native `mews` Harness, Skills use the Agent Skills layout and are
discovered from `<MEWS_HOME>/agents/<agent-slug>/skills/` plus `.agents/skills/`
in the Session working directory and its ancestors. Project Skills override
same-named Agent Skills. Skills are Host-local mutable resources outside Agent
revisions. Only name, description, and `SKILL.md` path enter the system prompt;
the Agent reads the full skill on demand. Prompt
templates are discovered from `<MEWS_HOME>/prompts/` and ancestor
`.agents/prompts/`. Sending `/name args` expands the matching Markdown template
and replaces `$ARGUMENTS` with `args` before any Harness runs.

Full-authority Agent extensions live in
`<MEWS_HOME>/agents/<agent-slug>/extensions/*.toml`:

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
files and registered tools hot-reload with the Host catalog. Only the owning
Agent sees their tools or invokes their lifecycle hooks. Native `mews` turns
invoke these lifecycle hooks. ACP Harnesses receive selected-Agent skill
snapshots only through `mews_list_skills` and `mews_read_skill` on their
turn-scoped MCP bridge; project/global skills and filesystem paths are never
exposed. The same private endpoint negotiates initialization-based MCP through
`2025-11-25` or serves stateless MCP `2026-07-28`; it does not create a durable
MCP session. ACP turns invoke run/model/turn lifecycle boundaries, and extension MCP
calls are subject to `before_tool`/`after_tool` validation; provider-owned tools
remain only observable ACP activity. Like Pi extensions, they are unsandboxed and may modify MEWS itself
with the Host user's authority.

## Remote protocol

Enrollment offers are signed, expire after 15 minutes, and contain a single-use bearer secret. Joining proves possession of independent Ed25519 signing and X25519 Noise identities. The installation authority issues a signed relay admission valid for 100 years to the enrolled Host. This lets the relay remain stateless: it verifies the ticket and proof of possession without consulting Hub storage. Removing a Host prevents Hub reconnection, but does not revoke a ticket already held by that Host at the relay.

The relay is stateless. It authenticates authority-scoped peer admission and proof of possession, enforces connection/frame/queue bounds, and routes opaque frames. Hub and Host then perform Noise XX, bind protocol version, roles, installation, peer IDs, stream ID, static Noise keys, and Ed25519 identities into the authenticated transcript, and encrypt all application data end to end. Encrypted messages are ordered and fragmented into bounded records. Thirty-second encrypted heartbeats preserve idle links; both sides reconnect, and Hub reconstructs per-Host listeners from SQLite after restart.

Host frames are versioned and capped at 256 KiB. Project context uses up to 192 KiB, reserving 64 KiB for its request envelope, Agent configuration, tools, and metadata. Client frames are capped at 1 MiB; paginated transcript and event bodies reserve 256 KiB for their serialized response envelope. Individual context/tool outputs are bounded, relay registration has a deadline, and relay connections/queues are bounded.

Hub setup supervises a bundled relay on `0.0.0.0:8787` by default and advertises a `.local` address derived from the configured Host name. LAN and private-overlay deployments may use `ws://` because Hub/Host application frames remain authenticated and end-to-end encrypted. Public relay deployment should advertise `wss://` and supply `--relay-listen`; the bundled relay serves plain WebSocket and expects TLS termination by a reverse proxy.

## Hub movement

`mews hub move <host>` acquires a fair installation-wide write barrier shared by local requests, remote client requests, and delayed enrollment acceptance. A move is rejected while any Turn is active; later mutations wait and are rejected once handoff begins.

Hub advances the generation with a compare-and-set, creates a consistent SQLite backup, and sends bounded chunks plus credentials over the authenticated Host stream. The target checks total length/hash, installation ID, generation, target Host ID, authority key, and its physical Host identity. Preparation cannot promote by itself. After the source durably records its decision to demote, it sends a matching one-time activation token. Target activation is idempotent and crash-resumable. Only an armed target can run `mews hub recover`.

Failures before activation roll the source forward to a newer generation. During activation, a durable source recovery coordinator restarts fenced, reconnects only the target, and retries arm/activate before demoting. Once activation is armed, the source favors single-writer safety over availability if the final acknowledgment is lost. Successful demotion removes the old database, installation authority, provider credentials, and Hub Noise secret, then reconnects the same physical identity as a normal Host. Its relay retires after a migration grace period: 10 minutes when every enrolled remote Host acknowledged the replacement candidates, or 10 days otherwise. Moving back is the same protocol and is covered by the public end-to-end test.

## Operations and security boundary

The development persistence schema has no migrations or legacy decoder. A schema-version mismatch is rejected at startup; stop MEWS and clear `~/.mews/` before real local testing with a different schema version.

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
mews agents <slug> -p … [--detach]  Start a fresh Session with an initial prompt
mews sessions list           List Sessions
mews sessions <id>           Explicitly resume a Session
mews sessions <id> -p … [--detach]  Resume from any machine on the bound Host
mews hosts list              List enrolled Hosts and connection status
mews hosts invite --relay …  Create one enrollment offer
mews hosts remove <host>     Revoke and remove a Host
mews harnesses list          Inspect live Host Harness catalogs
mews harnesses refresh       Refresh Host-local Harness discovery
mews harnesses setup [name]  Choose Harnesses interactively, or set up one by name
mews providers status             List configured provider credentials
mews providers login              Select a provider and authenticate
mews providers set-key [provider] Add or rotate an API key
mews providers logout <provider>  Remove a provider credential
mews journal list [filters]   Query audit journal entries
mews journal watch [filters]  Follow audit journal entries
mews hub move <host>         Move Hub to a connected Host
mews hub recover             Resume an armed target after uncertain activation
mews relay serve             Run the stateless relay
```

Setup installs one role-detecting per-user daemon through launchd on macOS or systemd on Linux. After Hub movement the same process becomes a Host, while the target Host process becomes Hub; restarts infer the current role from local state.
