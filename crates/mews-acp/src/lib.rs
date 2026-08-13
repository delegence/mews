//! Generic ACP process, session, permission, and extension-tool support.

mod mcp;
mod permissions;
mod process;
mod rpc;
mod session;
mod updates;

pub use mcp::AcpSkill;
pub use mews_protocol::{AcpStopReason, AcpTimings};
pub use process::AcpHarnessConfig;
pub use rpc::{AcpErrorKind, classify_error, is_cancelled};
pub use session::{
    AcpEventSink, AcpHookMetadata, AcpProbe, AcpProbeTimings, AcpSessionOutcome, AcpSessionRequest,
    AcpStreamEvent, AcpTurnRequest, execute_acp_turn, probe_acp,
};
