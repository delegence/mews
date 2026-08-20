# MEWS

MEWS is an agent construction system.

Create an agent once, run it in any directory, and keep its Sessions after restarts. Connect more computers when you need access to other files, tools, or processing power. You always choose where the work runs.

> MEWS is under active development. Breaking changes can require a clean local state.

## How it works

MEWS has four main concepts:

- **Hub** stores the source of truth for the installation.
- **Host** is a computer that provides files, tools, and agent runtimes.
- **Agent** defines identity, behavior, tools, and runtime settings.
- **Session** connects one Agent to one Host and one working directory.

The computer that runs the Hub is also a Host. You can add more Hosts and move the Hub later.

A Session never moves by itself. If an Agent starts in a project on your laptop, its tools continue to run in that project on that laptop. You can resume the Session from another computer, but MEWS does not copy the project or silently move the work.

## Quick start

MEWS requires macOS or Linux and Rust 1.88 or newer.

```sh
cargo install --path crates/mews
mews setup
mews providers login
mews providers models
mews agents new coder
```

Start the Agent in a project:

```sh
cd /path/to/project
mews agents coder
```

Or send one prompt:

```sh
mews agents coder -p "Summarize this project"
```

Each Agent invocation creates a new Session. List and resume Sessions explicitly:

```sh
mews sessions list
mews sessions <session-id>
```

## Agents

An Agent has two portable files:

```text
$MEWS_HOME/agents/coder/
├── SOUL.md
└── agent.toml
```

`SOUL.md` describes who the Agent is and how it should behave. `agent.toml` selects its Harness, options, and allowed tools.

The built-in `mews` Harness runs a small model and tool loop. It includes these tools:

- `read`
- `write`
- `edit`
- `bash`

MEWS also supports Agent Skills, executable extensions, lifecycle hooks, and external Harnesses through ACP. Tools run with the Host user's operating-system authority. MEWS is not a sandbox.

## Models and Harnesses

The built-in Harness supports OpenAI, OpenAI Codex, Anthropic, and Google models. Configure credentials and select defaults with:

```sh
mews providers login
mews providers set-key <provider>
mews providers models
mews providers reasoning
```

A Harness controls how an Agent talks to models and uses tools. MEWS includes its native Harness and can run supported ACP Harnesses such as Codex and Claude.

```sh
mews harnesses list
mews harnesses setup
```

Harness availability is Host-specific. An Agent keeps only the portable Harness name and options.

## Add another computer

Create a short-lived invitation on the Hub machine:

```sh
mews hosts invite
```

Use the printed invitation on the new computer:

```sh
mews setup --join '<invitation>'
```

MEWS uses an encrypted connection between the Hub and Hosts. Its relay only forwards encrypted messages. It is not a VPN and does not provide general remote shell access.

Useful Host commands:

```sh
mews hosts list
mews hosts remove <host>
mews hub move <host>
```

## What MEWS does not do

MEWS does not:

- synchronize project files between computers
- move a Session when its Host is unavailable
- add hidden memory between separate Sessions
- sandbox Agent tools
- require a cloud account or hosted control plane
- provide a general mesh VPN

It provides small building blocks for durable Agents, explicit execution, replaceable models and Harnesses, and secure access across your own machines.

## Development

[Mise](https://mise.jdx.dev/) keeps development state in this repository:

```sh
mise run mews -- setup
mise run mews -- status
```

Reset development state after a breaking schema change:

```sh
mise run mews:reset
```

Run the checks:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
