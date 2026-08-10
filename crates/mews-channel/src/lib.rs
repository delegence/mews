//! Runtime for standalone clients backed by external conversations.

mod mapping;
mod runtime;

use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

pub use runtime::{BroadcastMessage, ChannelConfig, ChannelHandle, ChannelRuntime};

pub struct InboundMessage {
    pub external_id: String,
    /// Adapter-defined account/chat/thread identity used as the FIFO lane key.
    pub conversation: String,
    pub text: String,
    pub metadata: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub enum OutboundEvent {
    CompletedMessage { text: String },
    StreamingUpdate { text: String },
    Lifecycle { message: String },
}

pub struct OutboundMessage {
    /// Stable for one Hub event so supporting platforms can suppress duplicates.
    pub idempotency_key: String,
    pub event: OutboundEvent,
    /// Starts at one and lets the adapter own its retry policy without hidden core limits.
    pub attempt: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveryReceipt {
    pub external_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TerminalDeliveryOutcome {
    Delivered(DeliveryReceipt),
    Dropped { reason: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliveryDiagnostic {
    pub idempotency_key: String,
    pub conversation: String,
    pub outcome: TerminalDeliveryOutcome,
}

/// The adapter alone decides whether, when, and how often a delivery is retried.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliveryOutcome {
    Delivered(DeliveryReceipt),
    RetryAfter(Duration),
    Dropped { reason: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ChannelCapability {
    Streaming,
    MessageEdits,
    Attachments,
    Typing,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ChannelSubscription {
    CompletedMessages,
    StreamingUpdates,
    LifecycleEvents,
}

#[async_trait]
pub trait ChannelInbound: Send {
    async fn receive(&mut self) -> Result<InboundMessage>;

    async fn acknowledge_inbound(&mut self, _external_id: &str) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait ChannelOutbound: Send + Sync {
    async fn send(&self, conversation: &str, message: OutboundMessage) -> DeliveryOutcome;
}

/// Splitting platform I/O lets inbound intake continue while outbound work runs.
pub trait Channel: Send + Sized + 'static {
    type Inbound: ChannelInbound;
    type Outbound: ChannelOutbound + 'static;

    fn name(&self) -> &str;
    fn subscriptions(&self) -> &[ChannelSubscription] {
        &[ChannelSubscription::CompletedMessages]
    }
    fn capabilities(&self) -> &[ChannelCapability] {
        &[]
    }
    fn split(self) -> (Self::Inbound, Self::Outbound);
}
