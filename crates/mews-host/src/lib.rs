//! Concrete local-machine execution capabilities for MEWS.

mod connection;
pub mod context;
mod environment;
mod harnesses;
mod lifecycle;
pub mod relay_supervisor;
pub mod resources;
mod rpc;
pub mod tools;

pub use connection::{
    CancellationRegistryOwner, ConnectedHost, HostControl, HostExecutor, RemoteAcpTurn,
    run_host_rpc,
};
pub use environment::LocalEnvironment;
pub use harnesses::{HarnessCatalog, HarnessLaunch, HarnessSetup};
pub use lifecycle::{activate_hub_transfer, handle_host_request_streaming};
pub use rpc::handle_execution_request;
pub use tools::{Tool, ToolRegistry};

pub const ACP_EVENT_CHANNEL_CAPACITY: usize = 64;
pub type AcpBindingWaiters = std::sync::Arc<
    std::sync::Mutex<std::collections::HashMap<String, std::sync::mpsc::Sender<()>>>,
>;
