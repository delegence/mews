use std::path::{Path, PathBuf};

use crate::{
    MewsClient,
    channel::{Channel, DeliveryReceipt, InboundMessage, OutboundMessage, mapping::MappingStore},
};
use anyhow::{Context, Result};
use mews_protocol::{
    ClientEventKind, HostId, MessageContent, MessageSource, SessionId, SourceKind,
};

pub struct ChannelConfig {
    pub agent: String,
    pub host: Option<HostId>,
    /// Omit to start on the Hub Host in its user's home directory.
    pub working_directory: Option<PathBuf>,
}

pub struct ChannelRuntime<C> {
    channel: C,
    requests: MewsClient,
    events: MewsClient,
    mappings: MappingStore,
    config: ChannelConfig,
}

impl<C: Channel> ChannelRuntime<C> {
    pub async fn open(
        channel: C,
        mews_root: &Path,
        state_path: &Path,
        config: ChannelConfig,
    ) -> Result<Self> {
        let mut runtime = Self {
            channel,
            requests: MewsClient::connect(mews_root).await?,
            events: MewsClient::connect(mews_root).await?,
            mappings: MappingStore::open(state_path)?,
            config,
        };
        let consumer = runtime.mappings.consumer_id()?;
        for (_, session) in runtime.mappings.mappings()? {
            runtime.events.subscribe(consumer.clone(), session).await?;
        }
        for (session, run_id) in runtime.mappings.active_runs()? {
            let run = runtime
                .requests
                .get_run(run_id.parse().map_err(anyhow::Error::msg)?)
                .await?;
            if run.completed_at.is_some() {
                runtime.mappings.finish_run(&run.id.to_string())?;
                runtime.launch_next(&session).await?;
            }
        }
        Ok(runtime)
    }

    pub async fn run(self) -> Result<()> {
        let (_shutdown_tx, shutdown) = tokio::sync::watch::channel(false);
        self.run_until(shutdown).await
    }

    pub async fn run_until(
        mut self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<()> {
        loop {
            let consumer = self.mappings.consumer_id()?;
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { return Ok(()); }
                }
                inbound = self.channel.receive() => self.handle_inbound(inbound?).await?,
                batch = self.events.poll_events(consumer.clone(), 25_000) => {
                    let batch = batch?;
                    for event in batch.events {
                        if self.mappings.delivered(event.id.as_str())? { continue; }
                        match event.kind {
                            ClientEventKind::AssistantDelta { .. } => {
                                // Channel adapters deliver completed durable messages; interactive
                                // clients may consume deltas directly from the event journal.
                                self.mappings.record_delivery(event.id.as_str(), None)?;
                            }
                            ClientEventKind::AssistantMessage { message } => {
                                if let MessageContent::Text { text } = message.content {
                                    let conversation = self.mappings.conversation(&event.session_id)?
                                        .context("event Session has no Channel conversation mapping")?;
                                    let receipt = self.deliver(event.id.as_str(), &conversation, text).await?;
                                    self.mappings.record_delivery(event.id.as_str(), receipt.external_id.as_deref())?;
                                }
                            }
                            ClientEventKind::RunCompleted { run_id } => {
                                self.mappings.record_delivery(event.id.as_str(), None)?;
                                if let Some(session) = self.mappings.finish_run(&run_id.to_string())? {
                                    self.launch_next(&session).await?;
                                }
                            }
                            ClientEventKind::RunFailed { run_id, error } => {
                                let conversation = self.mappings.conversation(&event.session_id)?
                                    .context("failed Run Session has no Channel mapping")?;
                                let receipt = self.deliver(
                                    event.id.as_str(),
                                    &conversation,
                                    format!("MEWS Run failed: {error}"),
                                ).await?;
                                self.mappings.record_delivery(event.id.as_str(), receipt.external_id.as_deref())?;
                                if let Some(session) = self.mappings.finish_run(&run_id.to_string())? {
                                    self.launch_next(&session).await?;
                                }
                            }
                            ClientEventKind::RunCancelled { run_id } => {
                                self.mappings.record_delivery(event.id.as_str(), None)?;
                                if let Some(session) = self.mappings.finish_run(&run_id.to_string())? {
                                    self.launch_next(&session).await?;
                                }
                            }
                            ClientEventKind::RunStarted { .. }
                            | ClientEventKind::ReasoningDelta { .. }
                            | ClientEventKind::ToolActivity { .. }
                            | ClientEventKind::ToolStarted { .. }
                            | ClientEventKind::ToolCompleted { .. }
                            | ClientEventKind::PermissionResolved { .. } => {
                                self.mappings.record_delivery(event.id.as_str(), None)?;
                            }
                            ClientEventKind::PermissionRequested { request, .. } => {
                                let rejected = request
                                    .options
                                    .iter()
                                    .find(|option| option.kind.starts_with("reject"))
                                    .map(|option| option.id.clone());
                                self.events
                                    .resolve_permission(request.id, rejected)
                                    .await?;
                                self.mappings.record_delivery(event.id.as_str(), None)?;
                            }
                        }
                    }
                    if batch.advanced {
                        self.events.acknowledge(consumer, batch.checkpoint).await?;
                    }
                }
            }
        }
    }

    async fn handle_inbound(&mut self, inbound: InboundMessage) -> Result<()> {
        let consumer = self.mappings.consumer_id()?;
        let session = match self.mappings.session(&inbound.conversation)? {
            Some(session) => session,
            None => {
                let session = match &self.config.host {
                    Some(host) => {
                        let working_directory = self.config.working_directory.clone().context(
                            "a Channel targeting a specific Host requires a working directory",
                        )?;
                        self.requests
                            .start_session_on(
                                self.config.agent.clone(),
                                host.clone(),
                                working_directory,
                            )
                            .await?
                    }
                    None => {
                        self.requests
                            .start_session(
                                self.config.agent.clone(),
                                self.config.working_directory.clone(),
                            )
                            .await?
                    }
                };
                self.mappings.insert(&inbound.conversation, &session.id)?;
                self.events
                    .subscribe(consumer.clone(), session.id.clone())
                    .await?;
                session.id
            }
        };
        self.mappings.enqueue(
            &inbound.external_id,
            &inbound.conversation,
            &inbound.text,
            &inbound.metadata,
        )?;
        self.channel
            .acknowledge_inbound(&inbound.external_id)
            .await?;
        self.launch_next(&session).await
    }

    async fn deliver(
        &mut self,
        event_id: &str,
        conversation: &str,
        text: String,
    ) -> Result<DeliveryReceipt> {
        let mut delay = 1;
        for attempt in 1..=8 {
            match self
                .channel
                .send(
                    conversation,
                    OutboundMessage {
                        idempotency_key: event_id.to_owned(),
                        text: text.clone(),
                    },
                )
                .await
            {
                Ok(receipt) => return Ok(receipt),
                Err(error) if attempt == 8 => {
                    self.mappings.dead_letter(
                        event_id,
                        conversation,
                        &text,
                        &format!("{error:#}"),
                    )?;
                    return Ok(DeliveryReceipt { external_id: None });
                }
                Err(_) => {
                    tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                    delay = (delay * 2).min(30);
                }
            }
        }
        unreachable!()
    }

    async fn launch_next(&mut self, session: &SessionId) -> Result<()> {
        if self.mappings.active(session)? {
            return Ok(());
        }
        let Some((pending_id, external_id, text, metadata)) = self.mappings.next(session)? else {
            return Ok(());
        };
        let run = self
            .requests
            .start_turn_idempotent(
                format!(
                    "channel:{}:{}:{external_id}",
                    self.channel.name(),
                    session.as_str()
                ),
                session.clone(),
                text,
                metadata,
                MessageSource {
                    kind: SourceKind::Channel,
                    id: self.channel.name().to_owned(),
                },
            )
            .await?;
        self.mappings
            .mark_started(pending_id, session, &run.id.to_string())?;
        Ok(())
    }
}
