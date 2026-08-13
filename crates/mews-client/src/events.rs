use anyhow::{Context, Result, bail};
use mews_protocol::{
    AssistantResponseBlock, ConsumerId, ConsumerKind, EventBatch, HubRequest, JournalPage,
    JournalQuery, MessageSource, SessionEntry, SessionEntryPayload, SessionId, Turn, TurnId,
    TurnStatus,
};
use serde_json::Value;

use crate::{MewsClient, response};

fn answer_from_entries(entries: Vec<SessionEntry>, turn_id: &TurnId) -> String {
    entries
        .into_iter()
        .filter_map(|entry| match entry.payload {
            SessionEntryPayload::AssistantResponse {
                turn_id: entry_turn,
                response,
            } if &entry_turn == turn_id => Some(response.blocks),
            _ => None,
        })
        .flatten()
        .filter_map(|block| match block {
            AssistantResponseBlock::Text { text } => Some(text),
            _ => None,
        })
        .collect()
}

impl MewsClient {
    pub async fn send_message(
        &mut self,
        session_id: SessionId,
        prompt: String,
        metadata: Value,
        source: MessageSource,
    ) -> Result<String> {
        let turn = self
            .start_turn(session_id, prompt, metadata, source)
            .await?;
        let turn = self.wait_for_turn(turn.id).await?;
        self.terminal_turn_answer(&turn)
            .await?
            .context("terminal Turn has no outcome")
    }

    /// Returns the durable answer for a terminal Turn. Event delivery is only a
    /// wake-up/streaming mechanism and may be compacted before a live observer
    /// sees the terminal event.
    pub async fn terminal_turn_answer(&mut self, turn: &Turn) -> Result<Option<String>> {
        match &turn.status {
            TurnStatus::Running => Ok(None),
            TurnStatus::Completed => Ok(Some(answer_from_entries(
                self.session_entries(turn.session_id.clone()).await?,
                &turn.id,
            ))),
            TurnStatus::Failed => bail!(
                "Turn failed: {}",
                turn.error.as_deref().unwrap_or("unknown error")
            ),
            TurnStatus::Cancelled => bail!("Turn cancelled"),
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

    pub async fn query_journal_entries(&mut self, query: JournalQuery) -> Result<JournalPage> {
        response::journal_entries(
            self.request(HubRequest::QueryJournalEntries { query })
                .await?,
        )
    }

    pub async fn poll_journal_entries(
        &mut self,
        query: JournalQuery,
        wait_ms: u32,
    ) -> Result<JournalPage> {
        response::journal_entries(
            self.request(HubRequest::PollJournalEntries { query, wait_ms })
                .await?,
        )
    }
}

#[cfg(test)]
mod tests {
    use mews_protocol::{AssistantResponse, MessageId};

    use super::*;

    fn assistant_entry(session_id: SessionId, turn_id: TurnId, text: &str) -> SessionEntry {
        SessionEntry {
            id: MessageId::new(),
            session_id,
            sequence: 1,
            parent_id: None,
            payload: SessionEntryPayload::AssistantResponse {
                turn_id,
                response: AssistantResponse {
                    provider: "test".into(),
                    model: "test".into(),
                    api: "test".into(),
                    response_id: None,
                    blocks: vec![AssistantResponseBlock::Text { text: text.into() }],
                    usage: None,
                    stop_reason: None,
                },
            },
            created_at: "2026-01-01T00:00:00Z".parse().unwrap(),
        }
    }

    #[test]
    fn durable_answer_uses_only_entries_from_the_requested_turn() {
        let session_id = SessionId::new();
        let requested_turn = TurnId::new();
        assert_eq!(
            answer_from_entries(
                vec![
                    assistant_entry(session_id.clone(), TurnId::new(), "other"),
                    assistant_entry(session_id, requested_turn.clone(), "answer"),
                ],
                &requested_turn,
            ),
            "answer"
        );
    }
}
