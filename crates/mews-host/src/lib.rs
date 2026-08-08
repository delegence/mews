//! Concrete local-machine execution capabilities for MEWS.

pub mod context;
mod environment;
mod harnesses;
pub mod resources;
mod rpc;
pub mod tools;

pub use environment::LocalEnvironment;
pub use harnesses::{HarnessCatalog, HarnessLaunch, HarnessSetup};
pub use rpc::handle_execution_request;
pub use tools::{Tool, ToolRegistry};
