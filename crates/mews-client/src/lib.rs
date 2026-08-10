//! Typed local-daemon client.

mod connection;
pub mod response;

pub use client::MewsClient;
pub use mews_protocol::*;
mod client;
mod events;
