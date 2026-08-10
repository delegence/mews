use super::Adapter;

pub(super) const ADAPTER: Adapter = Adapter {
    name: "claude",
    runtime: "claude",
    package: "@agentclientprotocol/claude-agent-acp",
    version: "0.66.0",
    binary: "claude-agent-acp",
    profile_variable: "CLAUDE_CONFIG_DIR",
    auth_args: &["--cli", "auth", "login", "--claudeai"],
};
