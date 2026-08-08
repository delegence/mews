use anyhow::{Result, bail};
use mews_protocol::{ConsumerId, ConsumerKind, EventBatch, HubRequest, MessageSource, SessionId};
use serde_json::Value;

use crate::{MewsClient, response};

impl MewsClient {
    pub async fn send_message(
        &mut self,
        session_id: SessionId,
        prompt: String,
        metadata: Value,
        source: MessageSource,
    ) -> Result<String> {
        let consumer = ConsumerId::new();
        let subscribed = self
            .subscribe_as(
                consumer.clone(),
                session_id.clone(),
                ConsumerKind::Ephemeral,
            )
            .await;
        if let Err(error) = subscribed {
            let _ = self.delete_consumer(consumer).await;
            return Err(error);
        }
        let result = self
            .send_message_subscribed(consumer.clone(), session_id, prompt, metadata, source)
            .await;
        // Cleanup must not replace the Run result when the Hub is disconnecting.
        let _ = self.delete_consumer(consumer).await;
        result
    }

    async fn send_message_subscribed(
        &mut self,
        consumer: ConsumerId,
        session_id: SessionId,
        prompt: String,
        metadata: Value,
        source: MessageSource,
    ) -> Result<String> {
        let run = self
            .start_turn(session_id.clone(), prompt, metadata, source)
            .await?;
        let mut answer = String::new();
        loop {
            let batch = self.poll_events(consumer.clone(), 30_000).await?;
            let mut finished = false;
            let mut failure = None;
            let mut permissions = Vec::new();
            for event in &batch.events {
                match &event.kind {
                    mews_protocol::ClientEventKind::AssistantMessage { message }
                        if message.session_id == session_id =>
                    {
                        if let mews_protocol::MessageContent::Text { text } = &message.content {
                            answer.push_str(text);
                        }
                    }
                    mews_protocol::ClientEventKind::RunCompleted { run_id }
                        if *run_id == run.id =>
                    {
                        finished = true
                    }
                    mews_protocol::ClientEventKind::RunFailed { run_id, error }
                        if *run_id == run.id =>
                    {
                        failure = Some(error.clone())
                    }
                    mews_protocol::ClientEventKind::RunCancelled { run_id }
                        if *run_id == run.id =>
                    {
                        failure = Some("Run cancelled".into())
                    }
                    mews_protocol::ClientEventKind::PermissionRequested { run_id, request }
                        if *run_id == run.id =>
                    {
                        permissions.push(request.clone())
                    }
                    _ => {}
                }
            }
            if batch.advanced {
                self.acknowledge(consumer.clone(), batch.checkpoint).await?;
            }
            if let Some(error) = failure {
                bail!("Run failed: {error}");
            }
            for request in permissions {
                let rejected = request
                    .options
                    .iter()
                    .find(|option| option.kind.starts_with("reject"))
                    .map(|option| option.id.clone());
                self.resolve_permission(request.id, rejected).await?;
            }
            if finished {
                return Ok(answer);
            }
        }
    }

    pub async fn subscribe(
        &mut self,
        consumer_id: ConsumerId,
        session_id: SessionId,
    ) -> Result<()> {
        self.subscribe_as(consumer_id, session_id, ConsumerKind::Durable)
            .await
    }

    pub async fn subscribe_as(
        &mut self,
        consumer_id: ConsumerId,
        session_id: SessionId,
        consumer_kind: ConsumerKind,
    ) -> Result<()> {
        self.expect_ack(HubRequest::SubscribeSession {
            consumer_id,
            session_id,
            consumer_kind,
        })
        .await
    }

    pub async fn delete_consumer(&mut self, consumer_id: ConsumerId) -> Result<()> {
        self.expect_ack(HubRequest::DeleteConsumer { consumer_id })
            .await
    }

    pub async fn unsubscribe(
        &mut self,
        consumer_id: ConsumerId,
        session_id: SessionId,
    ) -> Result<()> {
        self.expect_ack(HubRequest::UnsubscribeSession {
            consumer_id,
            session_id,
        })
        .await
    }

    pub async fn poll_events(
        &mut self,
        consumer_id: ConsumerId,
        wait_ms: u32,
    ) -> Result<EventBatch> {
        response::events(
            self.request(HubRequest::PollEvents {
                consumer_id,
                limit: 100,
                wait_ms,
            })
            .await?,
        )
    }

    pub async fn acknowledge(&mut self, consumer_id: ConsumerId, checkpoint: u64) -> Result<()> {
        self.expect_ack(HubRequest::AcknowledgeEvents {
            consumer_id,
            checkpoint,
        })
        .await
    }
}
