//! Reusable model/tool agent state machine.
//!
//! The embedding runtime supplies persistence, tools, resources, and policy.
//! This crate owns generic turn orchestration, cancellation, streaming,
//! validation, tool scheduling, queues, and lifecycle events.

mod capabilities;
mod context_budget;
mod r#loop;
mod model;
mod queue;
mod tools;
mod types;

pub use capabilities::*;
pub use context_budget::{apply_context_budget, context_budget_bytes};
pub use r#loop::{run, run_with_config};
pub use mews_protocol::{ReasoningEffort, ToolDefinition};
pub use model::*;
pub use queue::MessageQueue;
pub use tools::ToolCatalog;
pub use types::*;
