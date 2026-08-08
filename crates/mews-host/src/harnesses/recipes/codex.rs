use super::Recipe;

pub(super) const RECIPE: Recipe = Recipe {
    name: "codex",
    runtime: "codex",
    package: "@agentclientprotocol/codex-acp",
    version: "1.1.9",
    binary: "codex-acp",
    profile_variable: "CODEX_HOME",
    auth_args: &["login"],
};
