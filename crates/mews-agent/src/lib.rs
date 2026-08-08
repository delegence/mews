//! Reusable model/tool agent state machine.
//!
//! The embedding runtime supplies persistence, tools, resources, and policy.
//! This crate owns generic turn orchestration, cancellation, streaming,
//! validation, tool scheduling, queues, and lifecycle events.

mod capabilities;
mod loop_run;
mod model;
mod queue;
mod types;

pub use capabilities::*;
pub use loop_run::{run, run_with_config};
pub use mews_protocol::{ReasoningEffort, ToolDefinition};
pub use model::*;
pub use queue::MessageQueue;
pub use types::*;
