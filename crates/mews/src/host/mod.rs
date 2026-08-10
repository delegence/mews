//! Joined-Host runtime and Hub/Host connectivity.

mod client_proxy;
mod hub_link;
mod joined;

pub(crate) use hub_link::serve_hub_host;
pub use joined::serve_joined_host;
pub use mews_host::{
    ACP_EVENT_CHANNEL_CAPACITY, AcpBindingWaiters, ConnectedHost, HostControl, HostExecutor,
    RemoteAcpRun, activate_hub_transfer, handle_host_request_streaming,
};
