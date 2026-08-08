use anyhow::{Result, bail};
use mews_protocol::{
    Agent, HostHarnessStatus, HostStatus, HubResponse, Installation, ModelInfo, ProviderDefaults,
    Session,
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
expect!(hosts, Hosts, Vec<HostStatus>);
expect!(harnesses, Harnesses, Vec<HostHarnessStatus>);
expect!(models, Models, Vec<ModelInfo>);
expect!(provider_defaults, ProviderDefaults, ProviderDefaults);

pub fn ack(response: HubResponse) -> Result<()> {
    match response {
        HubResponse::Ack => Ok(()),
        other => bail!("unexpected Hub response: {other:?}"),
    }
}
