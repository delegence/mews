use super::Adapter;

pub(super) const ADAPTER: Adapter = Adapter {
    name: "codex",
    runtime: "codex",
    package: "@agentclientprotocol/codex-acp",
    version: "1.1.14",
    binary: "codex-acp",
    profile_variable: "CODEX_HOME",
    auth_args: &["login"],
};
