use anyhow::{Result, bail};
use mews_protocol::{ConsumerId, EventBatch, HubRequest, HubResponse, MessageSource, SessionId};
use serde_json::Value;

use crate::MewsClient;

impl MewsClient {
    pub async fn send_message(
        &mut self,
        session_id: SessionId,
        prompt: String,
        metadata: Value,
        source: MessageSource,
    ) -> Result<String> {
        let consumer = ConsumerId::new();
        self.subscribe(consumer.clone(), session_id.clone()).await?;
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
                self.unsubscribe(consumer, session_id).await?;
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
                self.unsubscribe(consumer, session_id).await?;
                return Ok(answer);
            }
        }
    }

    pub async fn subscribe(
        &mut self,
        consumer_id: ConsumerId,
        session_id: SessionId,
    ) -> Result<()> {
        self.expect_ack(HubRequest::SubscribeSession {
            consumer_id,
            session_id,
        })
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
        match self
            .request(HubRequest::PollEvents {
                consumer_id,
                limit: 100,
                wait_ms,
            })
            .await?
        {
            HubResponse::Events(events) => Ok(events),
            response => bail!("unexpected daemon response: {response:?}"),
        }
    }

    pub async fn acknowledge(&mut self, consumer_id: ConsumerId, checkpoint: u64) -> Result<()> {
        self.expect_ack(HubRequest::AcknowledgeEvents {
            consumer_id,
            checkpoint,
        })
        .await
    }
}
