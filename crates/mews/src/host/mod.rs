mod client_proxy;
mod connection;
mod hub_link;
mod joined;
mod lifecycle;

pub(crate) const ACP_EVENT_CHANNEL_CAPACITY: usize = 64;

pub(crate) use connection::{CancellationRegistryOwner, RemoteAcpRun};
pub use connection::{ConnectedHost, HostControl, HostExecutor};
pub(crate) use hub_link::serve_hub_host;
pub use joined::serve_joined_host;
pub(crate) use lifecycle::AcpBindingWaiters;
pub use lifecycle::activate_hub_transfer;
pub(crate) use lifecycle::handle_host_request_streaming;
