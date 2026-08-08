# MEWS vision

## The product

MEWS is a personal operating layer for durable AI agents across all of a person's machines.

A user should be able to install MEWS on a laptop, VPS, mini PC, workstation, or other general-purpose computer, connect that machine to their MEWS installation, and make the same agents available there. An agent can then work inside the directory where it is invoked, using the models and tools available through that Host.

MEWS is not an operating system, a hosted agent marketplace, or a large autonomous-agent framework. It is a small set of dependable primitives from which those experiences can be built:

- portable agent identity and configuration;
- durable conversations and execution history;
- execution on an explicitly selected machine and directory;
- replaceable models, harnesses, tools, and user interfaces;
- secure communication between a user's machines;
- one movable source of truth.

The intended feeling is closer to Pi than to a traditional agent platform: small, direct, inspectable, and easy to extend.

## The experience

The first machine is initialized with one command:

```sh
mews setup
```

That machine is both a Host and the initial Hub. There is no separate user or operator object to configure and no special class of “local” machine.

An Agent is created and opened directly:

```sh
mews agents new coder
cd /path/to/project
mews agents coder
```

`mews agents coder` starts a fresh Session for `coder` in the current directory and opens the terminal client. Calling `mews` without arguments shows help. Explicit commands resume existing Sessions when continuity is wanted.

Another machine joins with a short-lived invitation:

```sh
# On the Hub machine
mews hosts invite

# On the joining machine
mews setup --join '<offer>'
```

After joining, that machine is an ordinary Host. Agents can run there using its filesystem, tools, credentials, compute, and operating-system authority. The machine carrying Hub is still an ordinary Host as well.

The Hub can move when the user's topology changes:

```sh
mews hub move mini-pc
```

Moving Hub transfers the canonical database, agent definitions, model credentials, Sessions, cryptographic authority, and other Hub-owned state. The old machine remains enrolled as a Host. Moving Hub back is the same operation.

## Agents are small and portable

An Agent is fundamentally two files:

```text
SOUL.md
agent.toml
```

`SOUL.md` is the Agent's identity: its character, purpose, values, and durable behavioral instructions. `agent.toml` contains mechanical configuration: an explicit Harness, its Harness-owned options, and the tool allowlist.

MEWS keeps the canonical revisions of these files in Hub. Each Host materializes an editable replica when the Agent runs there. Valid edits flow back to Hub and later reach other Hosts. Synchronization is revisioned and conflict-safe rather than last-writer-wins.

The Agent does not own a synchronized workspace. It works in the directory from which it is invoked, just like a coding agent:

- run it from a project and it works in that project;
- run it from the home directory and it works there;
- invoke it through a locationless client and it starts from the selected Host's home directory.

Existing project instruction files such as `AGENTS.md` remain project context. They complement rather than replace `SOUL.md`: the former describes the place in which the Agent is working; the latter describes who the Agent is.

This avoids workspace replication, hidden project copies, and a second filesystem abstraction. Project files stay on their Host.

## Hub owns truth; Hosts own execution

One Hub generation is the canonical authority for an installation. It owns:

- Agent identities and revisions;
- Sessions, Messages, Runs, and durable events;
- enrolled Host records and installation authority;
- model credentials and Hub configuration;
- handoff state needed to move Hub safely.

Every machine is a Host. A Host owns:

- its filesystem and working directories;
- its Agent replicas;
- its available tools and extension executables;
- its operating-system permissions and local resources;
- the local daemon through which clients connect.

This division is strict. Hub does not pretend a remote path is local, copy project workspaces between machines, or silently execute elsewhere when a Host is unavailable. Hosts do not become independent sources of truth for conversation history.

There must be exactly one writable Hub. Hub movement prefers single-writer safety over temporary availability and must remain recoverable across process or machine failure.

## Sessions are explicit execution locations

A Session permanently captures:

- an Agent and its revision;
- one Host;
- one canonical working directory;
- its selected model override, if any;
- ordered Messages and Runs.

New Agent invocations create new Sessions by default. Resuming is explicit. An existing Session always routes back to its bound Host and directory, even when the request originates from another machine or a channel adapter.

MEWS does not build implicit cross-session memory into the core. Memory, summaries, retrieval, or “last ten messages” continuity can be implemented later by a Harness or client using the durable primitives. The base system should not impose one context-management policy on every Agent and interface.

## A minimal, replaceable runtime

The built-in `mews` Harness is intentionally small: it sends the Agent's instructions and Session messages to a model, executes requested tools, returns results, and repeats until the model answers. Its built-in tools are:

- `read`
- `write`
- `edit`
- `bash`

This is the useful default, not a closed world. The default Agent allowlist also permits eligible executable extensions installed by the selected Host.

Models are replaceable at runtime. An Agent has a default model, and a Session may override it without changing the Agent. Provider authentication belongs to Hub state so it follows Hub movement.

Tools are provided by the selected Host. The effective catalog is the intersection of what the Agent permits and what that Host currently offers. Hosts can add, replace, or remove executable extensions while running. MEWS publishes the catalog change without embedding every possible integration into the core.

Harnesses are replaceable as well. A different Harness may implement another model loop, context strategy, approval policy, or memory system while preserving the same Agent, Session, Host, and tool primitives.

## Clients and channels are interchangeable

The Agent runtime must not be coupled to one interface. MEWS should support:

- a terminal client;
- web and desktop clients;
- Slack, Telegram, WhatsApp, and similar channels;
- purpose-built applications and automations.

These are clients of a small protocol, not hardcoded Hub features. A reusable client runtime should handle durable Run submission, event replay, and external-conversation-to-Session mapping. Each concrete channel owns platform-specific behavior and credentials.

Adapters may attach bounded metadata and authenticated source attribution to user messages. A Telegram adapter, for example, can include the sender, chat, thread, reply, and platform message identifiers. The adapter decides what constitutes a conversation and whether it creates or resumes a Session. The Harness decides how relevant metadata is presented to the model. Hub stores it durably without understanding Telegram.

This separation lets interfaces contain the logic they genuinely need without turning MEWS into a collection of channel-specific conditionals.

## Communication between machines

MEWS uses its own minimal communication layer tailored to Hub, Host, and client traffic. It borrows the useful product qualities of systems such as Tailscale—easy enrollment, stable identities, encrypted machine-to-machine communication, and reconnection—but it is not a general-purpose VPN.

The relay is deliberately small and stateless. It authenticates peers that present a valid signed admission, forwards bounded opaque frames, and cannot read Hub-to-Host application traffic. Hub and Host authenticate each other and encrypt their stream end to end. The relay does not expose arbitrary ports, mount filesystems, or grant a general remote shell.

Communication should work behind NAT through a public relay with TLS termination, reconnect automatically, and preserve durable work across temporary disconnections. The design may gain direct peer-to-peer paths later, but correctness must not depend on them.

## Extensibility without framework weight

MEWS exposes a few stable boundaries:

- protocol types for clients and Hosts;
- model providers;
- Harness implementations;
- Host tools and executable extensions;
- channel clients;
- relay transport.

An extension should depend only on the boundary it implements. Independently deployed or dependency-isolated components belong in separate crates; ordinary implementation details do not need package boundaries merely for symmetry.

The system should remain understandable by reading a small number of files and protocols. Extensibility is valuable only when it preserves that simplicity.

## Product principles

### Local-first and user-owned

The user's machines, files, credentials, and SQLite database are the product's center of gravity. A hosted control plane is not required for normal operation. The only shared infrastructure needed for remote connectivity is an untrusted relay.

### Explicit over magical

Hosts, working directories, Session resumption, model selection, and channel mappings are explicit. MEWS should not silently move execution, synchronize projects, invent memory, or reinterpret unavailable resources.

### Durable where it matters

Agent definitions, Sessions, Messages, Runs, client events, enrollment, and Hub movement survive restarts. Transient transport state can be reconstructed.

### Minimal but complete

The default path should work end to end without requiring a plugin ecosystem. The core remains small by providing complete primitives, not incomplete placeholders.

### Secure boundaries, honest authority

MEWS authenticates machines, encrypts remote traffic, bounds untrusted input, protects credentials, and maintains one Hub writer. Agent tools still run with the Host user's authority. MEWS does not pretend that tool execution is sandboxed when it is not.

### No hardcoded ecosystem

Providers, channels, tools, and clients are implementations of protocols. The core may bundle a small set of provider adapters, but should not accumulate special knowledge of every service a user might connect.

## What MEWS is not

MEWS is not intended to be:

- a general mesh VPN or remote-access product;
- a synchronized filesystem or project hosting service;
- a mandatory cloud account or multi-tenant SaaS control plane;
- a container sandbox or permission boundary around tool execution;
- a fixed memory or context-management system;
- a scheduler that silently moves an existing Session to available compute;
- an application that hardcodes every model, tool, or messaging platform;
- a complex multi-agent orchestration language.

Those capabilities can be built beside or on top of MEWS when they have clear product value. They should not enlarge the core by default.

## The finished product

MEWS is successful when a person can:

1. install it on any supported machine in minutes;
2. create an Agent by editing two understandable files;
3. invoke that Agent in any directory with one command;
4. add machines and securely use their distinct files, tools, and compute;
5. resume durable Sessions from another machine or client without changing where they execute;
6. move Hub safely when a laptop is replaced by an always-on mini PC or VPS;
7. switch models, Harnesses, tools, and interfaces without recreating the Agent;
8. build a new channel or tool against a small documented protocol;
9. inspect, back up, and retain ownership of the entire installation;
10. understand the essential architecture without learning a large framework.

At that point, an Agent is no longer tied to one chat window, one project copy, one model provider, or one computer. It has a durable identity and history, while remaining able to work wherever the user deliberately places it.
