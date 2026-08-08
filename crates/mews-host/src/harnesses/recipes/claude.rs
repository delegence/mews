use super::Recipe;

pub(super) const RECIPE: Recipe = Recipe {
    name: "claude",
    runtime: "claude",
    package: "@agentclientprotocol/claude-agent-acp",
    version: "0.64.2",
    binary: "claude-agent-acp",
    profile_variable: "CLAUDE_CONFIG_DIR",
    auth_args: &["--cli", "auth", "login", "--claudeai"],
};
