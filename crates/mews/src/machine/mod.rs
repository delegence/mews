//! Local machine setup, daemon integration, and process orchestration.
//!
//! The CLI owns argument parsing, prompting, and presentation; this layer owns
//! machine lifecycle workflows and operating-system integration.

pub mod daemon;
pub mod runtime;
pub mod setup;
