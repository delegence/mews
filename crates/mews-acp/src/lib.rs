//! Generic ACP process, session, permission, and extension-tool support.

mod mcp;
mod permissions;
mod process;
mod rpc;
mod session;
mod updates;

pub use mcp::AcpSkill;
pub use permissions::{
    AcpPermissionDecision, AcpPermissionHandler, AcpPermissionOption, AcpPermissionOptionKind,
    AcpPermissionRequest,
};
pub use process::AcpHarnessConfig;
pub use rpc::{AcpErrorKind, classify_error, is_cancelled};
pub use session::{
    AcpHookMetadata, AcpProbe, AcpProbeTimings, AcpSessionOutcome, AcpSessionRequest,
    AcpStopReason, AcpStreamEvent, AcpTimings, probe_acp,
    run_acp_session_with_extensions_and_events,
};
