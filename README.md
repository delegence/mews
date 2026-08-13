# MEWS

MEWS is a platform for creating durable AI agents.

One **Hub** owns canonical Agent definitions, model credentials, sessions, and history.
A **Host** is any connected machine (including the machine running Hub), that provides harnesses, tools and a filesystem.
The built-in `mews` harness is deliberately minimal: a model loop with `read`, `write`, `edit`, and `bash`.

## Quick start

Install with stable Rust 1.88 or newer:

```sh
cargo install --path crates/mews
```

For development, [Mise](https://mise.jdx.dev/) keeps all MEWS state in the
repository's ignored `.mews/` directory and runs the workspace binary:

```sh
mise run mews -- setup
mise run mews -- providers login
mise run mews -- providers models
mise run mews -- agents new coder
```

Every command run through this task uses `MEWS_HOME=$PWD/.mews`. Inspect that
directory directly, or delete all local development state with
`mise run mews:reset`. An installed `mews` binary continues to use its normal
home unless `MEWS_HOME` is set separately.

Running `mews setup` in a terminal opens an inline wizard. Use the arrow keys and Enter to create a new MEWS or join an existing one, and Escape to cancel. The suggested Host name comes from the current machine and the suggested relay address is shown before it is used. Existing flags remain available for scripts and unattended setup.

An Agent is authored as two portable files:

```text
$MEWS_HOME/agents/coder/
├── SOUL.md
├── agent.toml
└── .revision            # MEWS-managed replica metadata
```

`SOUL.md` is the Agent's identity. `agent.toml` selects an explicit Harness and its Harness-owned options, plus the tool allowlist. These two files are portable; `.revision` is Host-local synchronization metadata and should not be edited. Hub stores immutable canonical revisions; Host copies are editable replicas.

Start work from any directory:

```sh
cd /path/to/project
mews agents coder -p "Summarize README.md"
```

The Session captures the invoking Host and the canonical current working directory. Tools start there with the Host OS user's authority. There is no managed or synchronized Agent workspace.

Calling `mews` without arguments shows help. Available foundational commands:

```text
mews setup
mews agents list
mews agents new <slug> [--harness <name>] [--option <key=value>]...
mews agents rename <slug> <new-slug>
mews agents delete <slug>
mews agents <slug> -p <message> [--detach]
mews sessions list
mews sessions <id> -p <message> [--detach]
mews hosts list
mews hosts invite
mews hosts remove <host>
mews harnesses list
mews harnesses refresh
mews harnesses setup [name]
mews providers
mews providers status
mews providers login [provider]
mews providers set-key [provider]
mews providers logout <provider>
mews providers models
mews providers models update
mews providers reasoning
mews journal list [--after <position>] [--limit <count>] [filters...]
mews journal watch [--after <position>] [--limit <count>] [filters...]
mews status
```

`mews journal list` reads one JSON page from the audit journal. `mews journal
watch` follows it as newline-delimited JSON pages. Both commands use an exclusive
journal-position `--after` cursor, so pass a page's `cursor` back as `--after` to
resume without duplicating its last entry. Pages contain the next scanned cursor
even when filters exclude intervening entries.

Journal queries can be narrowed with `--subject-type <type>`, `--subject-id <id>`,
`--event-type <type>` (repeatable), `--session <id>`, and
`--correlation <id>`. `--limit` controls matching entries per page (1–500, default
100). For example:

```sh
mews journal list --session ses_... --event-type user_message_appended
mews journal watch --after 420 --correlation request-...
```

SQLite state tables are authoritative. Each semantic mutation writes its state,
typed journal entries, durable delivery rows, and optional idempotency receipt in
one transaction. The journal supports audit and ordered queries; it is not used
to rebuild state.

`mews-protocol` owns stable IDs, domain DTOs, and the serialized Hub/Host contracts; `mews-store` owns the concrete SQLite schema and persistence invariants; `mews-relay` owns the stateless relay protocol, client, and server; `mews-router` is the separately running local model gateway and owns model adapters, auth storage, and token refresh. On first start, the router creates an owner-only `auth.json` and imports any supported provider keys from its environment; the file moves with Hub. Setup and Agent authoring do not require credentials, but starting a Turn requires a configured model. Use `mews providers login` to select an OAuth provider, or `mews providers set-key openai|anthropic|google` to set an API key directly. Anthropic login opens its browser-based PKCE authorization and listens on `localhost:54545` for the callback. Adding credentials attempts to refresh that provider's cached model catalog. `mews providers models` and `mews providers reasoning` select installation defaults, which are copied into newly created native `mews` Agents. Existing native Agents without an explicit model or reasoning option continue to inherit the current installation default. `mews providers models update` forces a catalog refresh. Optional reasoning values are `none`, `minimal`, `low`, `medium`, `high`, `xhigh`, and `max`, translated to the provider's native controls. Choose Provider default to omit reasoning and use the model's default behavior. `mews agents coder` opens a line-oriented chat; each invocation creates a new Session, while `mews sessions <id>` explicitly resumes one from any machine and routes work to its bound Host.

`mews-client` provides the typed local-daemon API. `mews-channel` is an optional reusable runtime for external-conversation adapters: it persists mappings locally, starts asynchronous Turns through its machine's daemon, and consumes acknowledged durable Hub events. Concrete channel implementations remain outside the core workspace until one is needed.

The development database schema is intentionally migration-free. Run
`mise run mews:reset` after a breaking schema change when using the project-local
development environment. Native Sessions retain ordered rich provider responses and can always replay canonical history; OpenAI response IDs are optional same-provider/model/API optimization cursors. ACP continuation remains separately authoritative through its required ACP Session binding.

For the built-in `mews` Harness, `/model <provider/model>` switches the model for subsequent turns in that Session; `/model default` clears the override and returns to the Agent's configured model or, when absent, the current installation default. The override is durable Session state and does not mutate `agent.toml`. ACP Harnesses currently ignore this override.

## Harnesses and providers

MEWS supports these Harness choices:

- `mews` is the built-in model-and-tool Harness. It supplies the four built-in tools and, under the default `tools = ["*"]` allowlist, also exposes tools registered by the selected Agent's extensions. Its currently bundled model providers are `openai` (API key), `openai-codex` (ChatGPT/Codex OAuth), `anthropic` (OAuth or API key), and `google` (Gemini Developer API key).
- `mews harnesses setup` opens the same Harness selection wizard used by initial setup. `codex` and `claude` are managed ACP Harness recipes; `mews harnesses setup <name>` remains available as a shortcut for one Harness.
- Bring your own ACP Harness by adding a trusted Host-local ACP definition under `<MEWS_HOME>/harnesses/*.toml` and running `mews harnesses setup <name>`. A Harness must support persistent Session continuation before MEWS can run it.

Harness setup, credentials, and executable definitions are Host-local. Agent revisions store only the logical Harness name and its portable options.

The agent stack is deliberately split into brains and hands. `mews-agent` is the complete reusable harness API: streamed model/tool state machine, context/tool/hook contracts, cancellation, progress, queues, and lifecycle events. `mews-runtime` connects the native Harness to durable conversation state, `mews-acp` owns external ACP process/session/MCP behavior, and `mews-host` supplies concrete filesystem, process, resource, extension, tool, and Harness-recipe capabilities. Local and remote Hosts implement the same `AgentCapabilities` interface. Agent revisions default to parallel tools; set `tool_execution = "sequential"` in `agent.toml` when calls must not overlap.

The built-in `mews` Harness discovers Agent Skills from
`<MEWS_HOME>/agents/<agent-slug>/skills/` and ancestor `.agents/skills/`. Project Skills
override same-named Agent Skills. Prompt templates are discovered from
`<MEWS_HOME>/prompts/` and ancestor `.agents/prompts/`. Invoke a prompt with `/name arguments`. Unsandboxed runtime
extensions in `<MEWS_HOME>/agents/<agent-slug>/extensions/*.toml` can register tools and handle the
native Harness lifecycle hooks for that Agent; they hot-reload and run with the Host user's authority. ACP
Harnesses receive turn-scoped extension tools through MCP when they support it. ACP turns also
receive only a snapshot of the selected Agent's skills through `mews_list_skills` and
`mews_read_skill`; project/global skills are not exposed. One private endpoint supports both
legacy MCP through `2025-11-25` and stateless MCP `2026-07-28`, selected from the client's wire
behavior rather than the Harness name.

The local client protocol uses owner-only Unix sockets, so MEWS targets macOS and Linux. Hub setup automatically starts a relay on `0.0.0.0:8787` and advertises a `.local` address derived from the configured Host name. A second machine on the same LAN can join without configuring relay infrastructure:

```sh
# On the Hub machine:
mews hosts invite

# On the joining Host:
mews setup --name mini-pc --join '<printed-offer>'
```

Override the advertised URL with `mews setup --relay <URL>` or `MEWS_RELAY_URL`, and the derived bind address with `--relay-listen <ADDRESS>` or `MEWS_RELAY_LISTEN`. A `ws://` URL derives a local listener automatically. A `wss://` URL requires an explicit listener because the bundled relay serves plain WebSocket behind external TLS termination. The resolved configuration is persisted, so the daemon does not depend on its launch environment.

The relay authenticates admission but only forwards opaque, bounded frames. Hub and Host authenticate each other and encrypt the application stream end to end with Noise XX. Remote CLI calls, cwd attestation, tools, Sessions, and editable Agent replicas then use that stream. Links heartbeat and reconnect, and the Hub restores enrolled Host listeners after restart.

`mews setup` installs and starts one per-user daemon: launchd on macOS and systemd with user lingering on Linux. The initial Hub Host supervises the local relay and model router. When Hub moves, the old relay remains available for a migration grace period: 10 minutes after every enrolled remote Host accepts the new relay candidates, or 10 days otherwise. Hub movement does not require reinstalling the service.

After replacing the MEWS executable, run `mews restart` to restart the installed daemon and wait until it is ready.

`mews relay serve --listen 0.0.0.0:8787` remains available for explicit deployments. Put TLS termination in front of a publicly exposed relay and advertise a `wss://` URL. The relay is not a VPN and does not expose a Host filesystem or shell outside an Agent tool call.

Move Hub from its current machine to any connected Host:

```sh
mews hub move mini-pc
```

The command fences both local and remote requests, transfers a consistent integrity-checked snapshot, promotes one new generation, removes Hub credentials from the old machine, and reconnects that machine as a normal Host. Moving back uses the same command. If the source restarts during activation, it remains fenced and finishes issuing the target's one-time activation before demoting. If the source is permanently lost after arming, run `mews hub recover` on the target; recovery refuses snapshots that were merely prepared.

Automatic TLS and non-Unix local transports are outside the initial product.

## Core invariants

- There is exactly one writable Hub per installation.
- Hub's machine is modeled as an ordinary Host. Local and remote execution use the same serialized Host protocol boundary.
- Every Session is permanently bound to one Agent, Host, and working directory.
- Existing Sessions never fall back to another Host.
- Agent definitions synchronize; project files do not.
- Invocations create new Sessions by default; there is no cross-session memory or implicit continuity.
- Project `AGENTS.md` files provide directory-specific instructions, while `SOUL.md` defines Agent identity.
- Effective tools are the intersection of an Agent allowlist, the built-ins, and that Agent's live extension-tool catalog.
- Clients and adapters may attach bounded JSON metadata; Hub preserves it and Harnesses decide how to present it to models.
- Remote connectivity uses a stateless, end-to-end-encrypted MEWS relay, not a general-purpose VPN.

## Verification

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Tests focus on durable invariants and public end-to-end behavior.

Real provider checks are opt-in so ordinary tests never spend tokens or require secrets:

```sh
MEWS_LIVE_MODEL=openai/gpt-5 cargo test -p mews-router live_provider -- --ignored
```

Use `ANTHROPIC_API_KEY` with an `anthropic/...` model or `OPENAI_API_KEY` with an
`openai/...` model.

See [Architecture and protocols](docs/ARCHITECTURE.md) for storage ownership, extension interfaces, wire boundaries, and security/operations details.
