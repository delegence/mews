//! Canonical decoding for typed Hub response variants.

use anyhow::{Result, bail};
use mews_protocol::{
    Agent, AuthStatus, EventBatch, HostHarnessStatus, HostStatus, HubResponse, Installation,
    ModelInfo, ProviderDefaults, Run, Session, SessionEntriesPage, SessionHistoryPage,
    SessionModelConfig,
};

macro_rules! expect {
    ($name:ident, $variant:ident, $ty:ty) => {
        pub fn $name(response: HubResponse) -> Result<$ty> {
            match response {
                HubResponse::$variant(value) => Ok(value),
                other => bail!("unexpected Hub response: {other:?}"),
            }
        }
    };
}

expect!(status, Status, Installation);
expect!(agents, Agents, Vec<Agent>);
expect!(agent, Agent, Agent);
expect!(sessions, Sessions, Vec<Session>);
expect!(session, Session, Session);
expect!(session_history, SessionHistory, SessionHistoryPage);
expect!(session_entries, SessionEntries, SessionEntriesPage);
expect!(session_model_config, SessionModelConfig, SessionModelConfig);
expect!(run, Run, Run);
expect!(events, Events, EventBatch);
expect!(hosts, Hosts, Vec<HostStatus>);
expect!(harnesses, Harnesses, Vec<HostHarnessStatus>);
expect!(auth, Auth, Vec<AuthStatus>);
expect!(models, Models, Vec<ModelInfo>);
expect!(provider_defaults, ProviderDefaults, ProviderDefaults);
expect!(host_invitation, HostInvitation, String);

pub fn ack(response: HubResponse) -> Result<()> {
    match response {
        HubResponse::Ack => Ok(()),
        other => bail!("unexpected Hub response: {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_the_actual_unexpected_variant() {
        let error = status(HubResponse::Ack).unwrap_err();
        assert!(error.to_string().contains("Ack"));
    }
}
