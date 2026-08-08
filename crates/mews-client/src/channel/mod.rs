//! Reusable runtime for clients backed by external conversations.

mod mapping;
mod runtime;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

pub use runtime::{ChannelConfig, ChannelRuntime};

pub struct InboundMessage {
    pub external_id: String,
    pub conversation: String,
    pub text: String,
    pub metadata: Value,
}

pub struct OutboundMessage {
    /// Stable across retries and restarts so supporting platforms can deduplicate delivery.
    pub idempotency_key: String,
    pub text: String,
}

pub struct DeliveryReceipt {
    pub external_id: Option<String>,
}

#[async_trait(?Send)]
pub trait Channel {
    fn name(&self) -> &str;
    async fn receive(&mut self) -> Result<InboundMessage>;
    async fn acknowledge_inbound(&mut self, _external_id: &str) -> Result<()> {
        Ok(())
    }
    async fn send(
        &mut self,
        conversation: &str,
        message: OutboundMessage,
    ) -> Result<DeliveryReceipt>;
}
