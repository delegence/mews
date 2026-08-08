//! Generic ACP process, session, permission, and extension-tool support.

mod mcp;
mod permissions;
mod process;
mod rpc;
mod session;

pub use permissions::{
    AcpPermissionDecision, AcpPermissionHandler, AcpPermissionOption, AcpPermissionOptionKind,
    AcpPermissionRequest,
};
pub use process::AcpHarnessConfig;
pub use session::{
    AcpProbe, AcpSessionOutcome, AcpSessionRequest, AcpStreamEvent, probe_acp,
    run_acp_session_with_extensions_and_events,
};
