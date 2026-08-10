//! MEWS application and Hub composition root.

pub mod app;
pub mod cli;
pub mod enrollment;
pub mod host;
mod machine;
pub mod paths;
pub mod server;

pub use mews_host::relay_supervisor;
pub use mews_protocol::*;
pub use mews_transport as identity;
pub use mews_transport as transport;
