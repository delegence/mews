use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use futures_util::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use mews_client::MewsClient;
use mews_protocol::{
    ChannelOrigin, ClientEvent, ClientEventKind, HostId, MessageContent, MessageSource, SessionId,
    SourceKind,
};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, watch};

use crate::{
    Channel, ChannelInbound, ChannelOutbound, ChannelSubscription, DeliveryOutcome, InboundMessage,
    InboundRejection, OutboundEvent, OutboundMessage, TerminalDeliveryOutcome,
    mapping::MappingStore,
};

const DEFAULT_DELIVERY_WORKERS: usize = 4;
const DEFAULT_PENDING_DELIVERIES: usize = 256;
const MAX_EXTERNAL_ID_BYTES: usize = 256;

#[derive(Clone, Debug)]
pub struct ChannelConfig {
    pub agent: String,
    pub host: Option<HostId>,
    /// Omit to start on the Hub Host in its user's home directory.
    pub working_directory: Option<PathBuf>,
    /// Maximum simultaneous platform calls in this standalone Channel process.
    pub delivery_workers: usize,
    /// Maximum queued, running, and delayed deliveries before Hub polling applies backpressure.
    pub pending_deliveries: usize,
}

impl ChannelConfig {
    pub fn new(agent: impl Into<String>) -> Self {
        Self {
            agent: agent.into(),
            host: None,
            working_directory: None,
            delivery_workers: DEFAULT_DELIVERY_WORKERS,
            pending_deliveries: DEFAULT_PENDING_DELIVERIES,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.delivery_workers == 0 {
            bail!("Channel delivery_workers must be greater than zero");
        }
        if self.pending_deliveries < self.delivery_workers {
            bail!("Channel pending_deliveries must be at least delivery_workers");
        }
        Ok(())
    }
}

pub struct ChannelRuntime<C: Channel> {
    inbound: C::Inbound,
    outbound: Option<C::Outbound>,
    channel_name: String,
    origin_consumer: mews_protocol::ConsumerId,
    subscriptions: HashSet<ChannelSubscription>,
    requests: MewsClient,
    events: MewsClient,
    mappings: MappingStore,
    config: ChannelConfig,
    delivery_tx: mpsc::Sender<Delivery>,
    delivery_rx: Option<mpsc::Receiver<Delivery>>,
    delivery_capacity: Arc<tokio::sync::Semaphore>,
    outcomes: tokio::sync::broadcast::Sender<crate::DeliveryDiagnostic>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct DestinationKey(String);

struct Delivery {
    event_id: String,
    destination: DestinationKey,
    event: OutboundEvent,
    attempt: u32,
    _capacity: tokio::sync::OwnedSemaphorePermit,
}

struct CompletedDelivery {
    delivery: Delivery,
    outcome: DeliveryOutcome,
}

#[derive(Clone, Debug)]
pub struct BroadcastMessage {
    pub idempotency_key: String,
    pub event: OutboundEvent,
}

/// A cloneable entry point for explicit broadcasts. Broadcast copies share the
/// same destination lanes as ordinary responses, so they cannot overtake them.
#[derive(Clone)]
pub struct ChannelHandle {
    delivery_tx: mpsc::Sender<Delivery>,
    delivery_capacity: Arc<tokio::sync::Semaphore>,
    outcomes: tokio::sync::broadcast::Sender<crate::DeliveryDiagnostic>,
}

impl ChannelHandle {
    pub fn subscribe_outcomes(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::DeliveryDiagnostic> {
        self.outcomes.subscribe()
    }

    pub async fn broadcast(
        &self,
        conversations: impl IntoIterator<Item = String>,
        message: BroadcastMessage,
    ) -> Result<()> {
        for conversation in conversations {
            let permit = self
                .delivery_capacity
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| anyhow::anyhow!("Channel delivery dispatcher stopped"))?;
            self.delivery_tx
                .send(Delivery {
                    event_id: format!("{}:{conversation}", message.idempotency_key),
                    destination: DestinationKey(conversation),
                    event: message.event.clone(),
                    attempt: 1,
                    _capacity: permit,
                })
                .await
                .map_err(|_| anyhow::anyhow!("Channel delivery dispatcher stopped"))?;
        }
        Ok(())
    }
}

impl<C: Channel> ChannelRuntime<C> {
    /// Opens one Channel identity in an owner-only state directory.
    pub async fn open(
        channel: C,
        mews_root: &Path,
        state_directory: &Path,
        config: ChannelConfig,
    ) -> Result<Self> {
        config.validate()?;
        let channel_name = channel.name().to_owned();
        if channel_name.is_empty() || channel_name.len() > 256 {
            bail!("Channel name must contain 1 to 256 bytes");
        }
        let subscriptions = channel.subscriptions().iter().copied().collect();
        validate_negotiation(channel.subscriptions(), channel.capabilities())?;
        let (inbound, outbound) = channel.split();
        let delivery_capacity = Arc::new(tokio::sync::Semaphore::new(config.pending_deliveries));
        let (delivery_tx, delivery_rx) = mpsc::channel(config.pending_deliveries);
        let (outcomes, _) = tokio::sync::broadcast::channel(config.pending_deliveries);
        let mut runtime = Self {
            inbound,
            outbound: Some(outbound),
            channel_name,
            origin_consumer: mews_protocol::ConsumerId::new(),
            subscriptions,
            requests: MewsClient::connect(mews_root).await?,
            events: MewsClient::connect(mews_root).await?,
            mappings: MappingStore::open(state_directory)?,
            config,
            delivery_tx,
            delivery_rx: Some(delivery_rx),
            delivery_capacity,
            outcomes,
        };
        let consumer = runtime.mappings.consumer_id()?;
        runtime.origin_consumer = consumer.clone();
        let mapped_sessions = runtime.mappings.mappings()?;
        for (_, session) in &mapped_sessions {
            runtime
                .events
                .subscribe(consumer.clone(), session.clone())
                .await?;
        }
        for (session, turn_id) in runtime.mappings.active_turns()? {
            let turn = runtime
                .requests
                .get_turn(turn_id.parse().map_err(anyhow::Error::msg)?)
                .await?;
            if turn.completed_at.is_some() {
                runtime.mappings.finish_turn(&turn.id.to_string())?;
                runtime.launch_next(&session).await?;
            }
        }
        for (_, session) in mapped_sessions {
            runtime.launch_next(&session).await?;
        }
        Ok(runtime)
    }

    pub fn handle(&self) -> ChannelHandle {
        ChannelHandle {
            delivery_tx: self.delivery_tx.clone(),
            delivery_capacity: Arc::clone(&self.delivery_capacity),
            outcomes: self.outcomes.clone(),
        }
    }

    /// Attach an existing MEWS Session to a destination in this Channel identity.
    pub async fn attach(&mut self, conversation: &str, session: SessionId) -> Result<()> {
        self.mappings.insert(conversation, &session)?;
        self.events
            .subscribe(self.origin_consumer.clone(), session)
            .await
    }

    pub async fn run(self) -> Result<()> {
        let (_shutdown_tx, shutdown) = watch::channel(false);
        self.run_until(shutdown).await
    }

    pub async fn run_until(mut self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let outbound = self.outbound.take().expect("ChannelRuntime is run once");
        let deliveries_rx = self.delivery_rx.take().expect("ChannelRuntime is run once");
        let worker = tokio::spawn(delivery_dispatcher(
            Arc::new(outbound),
            deliveries_rx,
            self.config.delivery_workers,
            self.outcomes.clone(),
            shutdown.clone(),
        ));

        loop {
            let consumer = self.mappings.consumer_id()?;
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                }
                inbound = self.inbound.receive() => self.handle_inbound(inbound?).await?,
                batch = self.events.poll_events(consumer.clone(), 25_000) => {
                    let batch = batch?;
                    // This is intentionally at-most-once across process crashes: acknowledge the
                    // canonical Hub event before crossing the external platform boundary.
                    if batch.advanced {
                        self.events.acknowledge(consumer.clone(), batch.checkpoint).await?;
                    }
                    for event in batch.events {
                        self.handle_event(event).await?;
                    }
                }
            }
        }

        drop(self.delivery_tx);
        worker
            .await
            .context("Channel delivery dispatcher panicked")?;
        Ok(())
    }

    async fn handle_event(&mut self, event: ClientEvent) -> Result<()> {
        let Some(origin) = event.channel_origin.clone() else {
            return self.handle_turn_state(event, false).await;
        };
        if origin.consumer_id != self.origin_consumer {
            return self.handle_turn_state(event, false).await;
        }
        let outbound = match &event.kind {
            ClientEventKind::AssistantMessage { message, .. }
                if self
                    .subscriptions
                    .contains(&ChannelSubscription::CompletedMessages) =>
            {
                match &message.content {
                    MessageContent::Text { text } => {
                        Some(OutboundEvent::CompletedMessage { text: text.clone() })
                    }
                    _ => None,
                }
            }
            ClientEventKind::AssistantDelta { delta, .. }
                if self
                    .subscriptions
                    .contains(&ChannelSubscription::StreamingUpdates) =>
            {
                Some(OutboundEvent::StreamingUpdate {
                    text: delta.clone(),
                })
            }
            ClientEventKind::TurnFailed { error, .. }
                if self
                    .subscriptions
                    .contains(&ChannelSubscription::LifecycleEvents) =>
            {
                Some(OutboundEvent::Lifecycle {
                    message: format!("MEWS Turn failed: {error}"),
                })
            }
            _ => None,
        };
        if let Some(outbound) = outbound {
            let conversation = origin.conversation;
            let permit = self
                .delivery_capacity
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| anyhow::anyhow!("Channel delivery dispatcher stopped"))?;
            self.delivery_tx
                .send(Delivery {
                    event_id: event.id.to_string(),
                    destination: DestinationKey(conversation),
                    event: outbound,
                    attempt: 1,
                    _capacity: permit,
                })
                .await
                .map_err(|_| anyhow::anyhow!("Channel delivery dispatcher stopped"))?;
        }

        self.handle_turn_state(event, true).await
    }

    async fn handle_turn_state(&mut self, event: ClientEvent, _is_origin: bool) -> Result<()> {
        match event.kind {
            ClientEventKind::TurnCompleted { turn_id }
            | ClientEventKind::TurnCancelled { turn_id } => {
                if let Some(session) = self.mappings.finish_turn(&turn_id.to_string())? {
                    self.launch_next(&session).await?;
                }
            }
            ClientEventKind::TurnFailed { turn_id, .. } => {
                if let Some(session) = self.mappings.finish_turn(&turn_id.to_string())? {
                    self.launch_next(&session).await?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_inbound(&mut self, inbound: InboundMessage) -> Result<()> {
        if let Err(reason) = validate_external_id(&inbound.external_id) {
            return self
                .inbound
                .reject_inbound(
                    &inbound.external_id,
                    InboundRejection::InvalidTurnInput {
                        reason: reason.into(),
                    },
                )
                .await;
        }
        let consumer = self.mappings.consumer_id()?;
        let key = channel_turn_key(
            &self.origin_consumer,
            &inbound.conversation,
            &inbound.external_id,
        );
        let source = channel_source(
            &self.channel_name,
            &self.origin_consumer,
            &inbound.conversation,
        );
        if let Err(error) = mews_protocol::validate_turn_input(
            &key,
            &MessageContent::Text {
                text: inbound.text.clone(),
            },
            &inbound.metadata,
            &source,
        ) {
            return self
                .inbound
                .reject_inbound(
                    &inbound.external_id,
                    InboundRejection::InvalidTurnInput {
                        reason: error.to_string(),
                    },
                )
                .await;
        }
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
                self.events.subscribe(consumer, session.id.clone()).await?;
                session.id
            }
        };
        self.mappings.enqueue(
            &inbound.external_id,
            &inbound.conversation,
            &inbound.text,
            &inbound.metadata,
        )?;
        self.inbound
            .acknowledge_inbound(&inbound.external_id)
            .await?;
        self.launch_next(&session).await
    }

    async fn launch_next(&mut self, session: &SessionId) -> Result<()> {
        if self.mappings.active(session)? {
            return Ok(());
        }
        let conversation = self
            .mappings
            .conversation(session)?
            .context("Channel Session has no conversation mapping")?;
        loop {
            let Some((pending_id, external_id, text, metadata)) = self.mappings.next(session)?
            else {
                return Ok(());
            };
            if validate_external_id(&external_id).is_err() {
                self.mappings.discard_pending(pending_id)?;
                continue;
            }
            let key = channel_turn_key(&self.origin_consumer, &conversation, &external_id);
            let source = channel_source(&self.channel_name, &self.origin_consumer, &conversation);
            if mews_protocol::validate_turn_input(
                &key,
                &MessageContent::Text { text: text.clone() },
                &metadata,
                &source,
            )
            .is_err()
            {
                self.mappings.discard_pending(pending_id)?;
                continue;
            }
            let turn = self
                .requests
                .start_turn_idempotent(key, session.clone(), text, metadata, source)
                .await?;
            // The Hub can return the original terminal Turn when an external
            // platform delivers the same message again. Its terminal event can
            // already be behind this consumer's checkpoint, so do not wait for
            // that event a second time.
            if turn.status != mews_protocol::TurnStatus::Running {
                self.mappings.discard_pending(pending_id)?;
                continue;
            }
            self.mappings
                .mark_started(pending_id, session, &turn.id.to_string())?;
            return Ok(());
        }
    }
}

fn validate_external_id(external_id: &str) -> std::result::Result<(), &'static str> {
    if external_id.is_empty() {
        return Err("external message ID cannot be empty");
    }
    if external_id.len() > MAX_EXTERNAL_ID_BYTES {
        return Err("external message ID exceeds 256 bytes");
    }
    Ok(())
}

fn channel_turn_key(
    consumer: &mews_protocol::ConsumerId,
    conversation: &str,
    external_id: &str,
) -> String {
    let mut hash = Sha256::new();
    for value in [consumer.as_str(), conversation, external_id] {
        hash.update(value.len().to_be_bytes());
        hash.update(value.as_bytes());
    }
    format!("channel:{:x}", hash.finalize())
}

fn channel_source(
    channel_name: &str,
    consumer: &mews_protocol::ConsumerId,
    conversation: &str,
) -> MessageSource {
    MessageSource {
        kind: SourceKind::Channel,
        id: channel_name.to_owned(),
        channel_origin: Some(ChannelOrigin {
            consumer_id: consumer.clone(),
            conversation: conversation.to_owned(),
        }),
    }
}

fn validate_negotiation(
    subscriptions: &[ChannelSubscription],
    capabilities: &[crate::ChannelCapability],
) -> Result<()> {
    if subscriptions.contains(&ChannelSubscription::StreamingUpdates)
        && !capabilities.contains(&crate::ChannelCapability::Streaming)
    {
        bail!("a Channel subscribing to streaming updates must advertise streaming capability");
    }
    Ok(())
}

type SendFuture = BoxFuture<'static, CompletedDelivery>;

async fn delivery_dispatcher<C: ChannelOutbound + 'static>(
    channel: Arc<C>,
    mut incoming: mpsc::Receiver<Delivery>,
    worker_limit: usize,
    outcomes: tokio::sync::broadcast::Sender<crate::DeliveryDiagnostic>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut lanes: HashMap<DestinationKey, VecDeque<Delivery>> = HashMap::new();
    let mut ready = VecDeque::new();
    let mut active = HashSet::new();
    let mut delayed: Vec<(tokio::time::Instant, Delivery)> = Vec::new();
    let mut sends = FuturesUnordered::<SendFuture>::new();
    let mut incoming_open = true;

    loop {
        let now = tokio::time::Instant::now();
        let mut index = 0;
        while index < delayed.len() {
            if delayed[index].0 <= now {
                let (_, delivery) = delayed.swap_remove(index);
                active.remove(&delivery.destination);
                ready.push_back(delivery.destination.clone());
                lanes
                    .entry(delivery.destination.clone())
                    .or_default()
                    .push_front(delivery);
            } else {
                index += 1;
            }
        }
        while sends.len() < worker_limit {
            let Some(key) = ready.pop_front() else {
                break;
            };
            if active.contains(&key) {
                continue;
            }
            let Some(delivery) = lanes.get_mut(&key).and_then(VecDeque::pop_front) else {
                continue;
            };
            active.insert(key);
            let channel = Arc::clone(&channel);
            sends.push(
                async move {
                    let outcome = channel
                        .send(
                            &delivery.destination.0,
                            OutboundMessage {
                                idempotency_key: delivery.event_id.clone(),
                                event: delivery.event.clone(),
                                attempt: delivery.attempt,
                            },
                        )
                        .await;
                    CompletedDelivery { delivery, outcome }
                }
                .boxed(),
            );
        }

        let next_retry = delayed.iter().map(|(when, _)| *when).min();
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return; }
            }
            Some(completed) = sends.next(), if !sends.is_empty() => {
                let key = completed.delivery.destination.clone();
                match completed.outcome {
                    DeliveryOutcome::RetryAfter(delay) => {
                        let mut delivery = completed.delivery;
                        delivery.attempt = delivery.attempt.saturating_add(1);
                        delayed.push((tokio::time::Instant::now() + delay, delivery));
                    }
                    DeliveryOutcome::Delivered(receipt) => {
                        let _ = outcomes.send(crate::DeliveryDiagnostic {
                            idempotency_key: completed.delivery.event_id.clone(),
                            conversation: key.0.clone(),
                            outcome: TerminalDeliveryOutcome::Delivered(receipt),
                        });
                        active.remove(&key);
                        if lanes.get(&key).is_some_and(|lane| !lane.is_empty()) {
                            ready.push_back(key.clone());
                        } else {
                            lanes.remove(&key);
                        }
                    }
                    DeliveryOutcome::Dropped { reason } => {
                        let _ = outcomes.send(crate::DeliveryDiagnostic {
                            idempotency_key: completed.delivery.event_id.clone(),
                            conversation: key.0.clone(),
                            outcome: TerminalDeliveryOutcome::Dropped { reason },
                        });
                        active.remove(&key);
                        if lanes.get(&key).is_some_and(|lane| !lane.is_empty()) {
                            ready.push_back(key.clone());
                        } else {
                            lanes.remove(&key);
                        }
                    }
                }
            }
            delivery = incoming.recv(), if incoming_open => {
                let Some(delivery) = delivery else {
                    incoming_open = false;
                    continue;
                };
                let key = delivery.destination.clone();
                let lane = lanes.entry(key.clone()).or_default();
                lane.push_back(delivery);
                if !active.contains(&key) && lane.len() == 1 { ready.push_back(key); }
            }
            _ = async {
                if let Some(when) = next_retry { tokio::time::sleep_until(when).await }
                else { std::future::pending().await }
            } => {}
        }
        if !incoming_open && sends.is_empty() && delayed.is_empty() && lanes.is_empty() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    struct TestOutbound {
        sent: mpsc::UnboundedSender<(String, String, u32)>,
        concurrent: AtomicUsize,
        maximum: AtomicUsize,
    }

    struct RecoveryChannel;

    struct IdleInbound;

    struct IdleOutbound;

    struct RetainedChannel {
        inbound: RetainedInbound,
    }

    struct RetainedInbound {
        messages: VecDeque<InboundMessage>,
        settlements: mpsc::UnboundedSender<InboundSettlement>,
    }

    #[derive(Debug, PartialEq, Eq)]
    enum InboundSettlement {
        Acknowledged(String),
        Rejected(String, InboundRejection),
    }

    #[async_trait::async_trait]
    impl ChannelInbound for IdleInbound {
        async fn receive(&mut self) -> Result<InboundMessage> {
            std::future::pending().await
        }
    }

    #[async_trait::async_trait]
    impl ChannelInbound for RetainedInbound {
        async fn receive(&mut self) -> Result<InboundMessage> {
            let message = self
                .messages
                .front()
                .context("no retained inbound message")?;
            Ok(InboundMessage {
                external_id: message.external_id.clone(),
                conversation: message.conversation.clone(),
                text: message.text.clone(),
                metadata: message.metadata.clone(),
            })
        }

        async fn acknowledge_inbound(&mut self, external_id: &str) -> Result<()> {
            let message = self.messages.front().context("no inbound to acknowledge")?;
            if message.external_id != external_id {
                bail!("acknowledged a different inbound message");
            }
            self.messages.pop_front();
            self.settlements
                .send(InboundSettlement::Acknowledged(external_id.to_owned()))?;
            Ok(())
        }

        async fn reject_inbound(
            &mut self,
            external_id: &str,
            rejection: InboundRejection,
        ) -> Result<()> {
            let message = self.messages.front().context("no inbound to reject")?;
            if message.external_id != external_id {
                bail!("rejected a different inbound message");
            }
            self.messages.pop_front();
            self.settlements.send(InboundSettlement::Rejected(
                external_id.to_owned(),
                rejection,
            ))?;
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl ChannelOutbound for IdleOutbound {
        async fn send(&self, _conversation: &str, _message: OutboundMessage) -> DeliveryOutcome {
            DeliveryOutcome::Delivered(crate::DeliveryReceipt { external_id: None })
        }
    }

    impl Channel for RecoveryChannel {
        type Inbound = IdleInbound;
        type Outbound = IdleOutbound;

        fn name(&self) -> &str {
            "recovery"
        }

        fn split(self) -> (Self::Inbound, Self::Outbound) {
            (IdleInbound, IdleOutbound)
        }
    }

    impl Channel for RetainedChannel {
        type Inbound = RetainedInbound;
        type Outbound = IdleOutbound;

        fn name(&self) -> &str {
            "retained"
        }

        fn split(self) -> (Self::Inbound, Self::Outbound) {
            (self.inbound, IdleOutbound)
        }
    }

    #[async_trait::async_trait]
    impl ChannelOutbound for TestOutbound {
        async fn send(&self, conversation: &str, message: OutboundMessage) -> DeliveryOutcome {
            let current = self.concurrent.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(current, Ordering::SeqCst);
            let text = match message.event {
                OutboundEvent::CompletedMessage { text } => text,
                _ => String::new(),
            };
            self.sent
                .send((conversation.into(), text.clone(), message.attempt))
                .unwrap();
            if conversation == "blocked" && message.attempt == 1 {
                self.concurrent.fetch_sub(1, Ordering::SeqCst);
                return DeliveryOutcome::RetryAfter(Duration::from_millis(50));
            }
            if conversation == "dropped" {
                self.concurrent.fetch_sub(1, Ordering::SeqCst);
                return DeliveryOutcome::Dropped {
                    reason: "permanent".into(),
                };
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            self.concurrent.fetch_sub(1, Ordering::SeqCst);
            DeliveryOutcome::Delivered(crate::DeliveryReceipt { external_id: None })
        }
    }

    fn delivery(key: &str, text: &str, permit: tokio::sync::OwnedSemaphorePermit) -> Delivery {
        Delivery {
            event_id: text.into(),
            destination: DestinationKey(key.into()),
            event: OutboundEvent::CompletedMessage { text: text.into() },
            attempt: 1,
            _capacity: permit,
        }
    }

    #[tokio::test]
    async fn invalid_inbound_is_terminally_rejected_before_valid_input() {
        let root = tempfile::tempdir().unwrap();
        let state = root.path().join("channel-state");
        let session = mews_protocol::Session {
            id: SessionId::new(),
            agent_id: mews_protocol::AgentId::new(),
            host_id: mews_protocol::HostId::new(),
            working_directory: root.path().to_path_buf(),
            model_override: None,
            leaf_entry_id: None,
            created_at: "2026-01-01T00:00:00Z".parse().unwrap(),
        };
        let session_for_server = session.clone();
        let started_sessions = Arc::new(AtomicUsize::new(0));
        let started_sessions_for_server = Arc::clone(&started_sessions);
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let listener = tokio::net::UnixListener::bind(root.path().join("hub.sock")).unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                let session = session_for_server.clone();
                let started_sessions = Arc::clone(&started_sessions_for_server);
                let started_tx = started_tx.clone();
                tokio::spawn(async move {
                    let (reader, mut writer) = stream.into_split();
                    let mut lines = BufReader::new(reader).lines();
                    while let Some(line) = lines.next_line().await.unwrap() {
                        let request = mews_protocol::decode_hub_frame::<mews_protocol::HubRequest>(
                            line.as_bytes(),
                        )
                        .unwrap();
                        let response = match request.body {
                            mews_protocol::HubRequest::StartSession { .. } => {
                                started_sessions.fetch_add(1, Ordering::SeqCst);
                                mews_protocol::HubResponse::Session(session.clone())
                            }
                            mews_protocol::HubRequest::SubscribeSession { .. } => {
                                mews_protocol::HubResponse::Ack
                            }
                            mews_protocol::HubRequest::StartTurn {
                                session_id, prompt, ..
                            } => {
                                started_tx.send((session_id.clone(), prompt)).unwrap();
                                mews_protocol::HubResponse::Turn(mews_protocol::Turn {
                                    id: mews_protocol::TurnId::new(),
                                    session_id,
                                    agent_revision: 1,
                                    harness: None,
                                    harness_definition_hash: None,
                                    harness_version: None,
                                    status: mews_protocol::TurnStatus::Running,
                                    error: None,
                                    created_at: "2026-01-01T00:00:00Z".parse().unwrap(),
                                    completed_at: None,
                                })
                            }
                            other => panic!("unexpected request: {other:?}"),
                        };
                        let response =
                            mews_protocol::Frame::with_request_id(response, request.request_id);
                        writer
                            .write_all(&mews_protocol::encode_hub_frame(&response).unwrap())
                            .await
                            .unwrap();
                        writer.write_all(b"\n").await.unwrap();
                    }
                });
            }
        });
        let (settlements_tx, mut settlements_rx) = mpsc::unbounded_channel();
        let oversized_external_id = "x".repeat(MAX_EXTERNAL_ID_BYTES + 1);
        let inbound = RetainedInbound {
            messages: VecDeque::from([
                InboundMessage {
                    external_id: String::new(),
                    conversation: "empty-id-conversation".into(),
                    text: "hello".into(),
                    metadata: serde_json::Value::Null,
                },
                InboundMessage {
                    external_id: oversized_external_id.clone(),
                    conversation: "oversized-id-conversation".into(),
                    text: "hello".into(),
                    metadata: serde_json::Value::Null,
                },
                InboundMessage {
                    external_id: "invalid".into(),
                    conversation: "invalid-conversation".into(),
                    text: " ".into(),
                    metadata: serde_json::Value::Null,
                },
                InboundMessage {
                    external_id: "valid".into(),
                    conversation: "valid-conversation".into(),
                    text: "hello".into(),
                    metadata: serde_json::Value::Null,
                },
            ]),
            settlements: settlements_tx,
        };
        let mut runtime = ChannelRuntime::open(
            RetainedChannel { inbound },
            root.path(),
            &state,
            ChannelConfig::new("coder"),
        )
        .await
        .unwrap();

        let empty_id = runtime.inbound.receive().await.unwrap();
        runtime.handle_inbound(empty_id).await.unwrap();
        assert!(matches!(
            settlements_rx.recv().await.unwrap(),
            InboundSettlement::Rejected(id, InboundRejection::InvalidTurnInput { reason })
                if id.is_empty() && reason == "external message ID cannot be empty"
        ));

        let oversized_id = runtime.inbound.receive().await.unwrap();
        runtime.handle_inbound(oversized_id).await.unwrap();
        assert!(matches!(
            settlements_rx.recv().await.unwrap(),
            InboundSettlement::Rejected(id, InboundRejection::InvalidTurnInput { reason })
                if id == oversized_external_id && reason == "external message ID exceeds 256 bytes"
        ));

        let invalid = runtime.inbound.receive().await.unwrap();
        runtime.handle_inbound(invalid).await.unwrap();
        assert!(matches!(
            settlements_rx.recv().await.unwrap(),
            InboundSettlement::Rejected(id, InboundRejection::InvalidTurnInput { reason })
                if id == "invalid" && reason == "message text cannot be empty"
        ));
        assert!(
            runtime
                .mappings
                .session("invalid-conversation")
                .unwrap()
                .is_none()
        );
        assert!(
            runtime
                .mappings
                .session("empty-id-conversation")
                .unwrap()
                .is_none()
        );
        assert!(
            runtime
                .mappings
                .session("oversized-id-conversation")
                .unwrap()
                .is_none()
        );
        let database = rusqlite::Connection::open(state.join("channel.db")).unwrap();
        let rejected_pending: usize = database
            .query_row("SELECT COUNT(*) FROM pending", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rejected_pending, 0);

        let valid = runtime.inbound.receive().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), runtime.handle_inbound(valid))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            settlements_rx.recv().await.unwrap(),
            InboundSettlement::Acknowledged("valid".into())
        );
        assert_eq!(started_sessions.load(Ordering::SeqCst), 1);
        assert_eq!(
            started_rx.recv().await.unwrap(),
            (session.id.clone(), "hello".into())
        );
        assert_eq!(
            runtime.mappings.session("valid-conversation").unwrap(),
            Some(session.id.clone())
        );
        assert!(runtime.mappings.active(&session.id).unwrap());
        assert!(runtime.mappings.next(&session.id).unwrap().is_none());
        server.abort();
    }

    #[tokio::test]
    async fn opening_launches_idle_pending_work_without_new_inbound() {
        let root = tempfile::tempdir().unwrap();
        let state = root.path().join("channel-state");
        let session = SessionId::new();
        let mut mappings = MappingStore::open(&state).unwrap();
        mappings.consumer_id().unwrap();
        mappings.insert("conversation", &session).unwrap();
        mappings
            .enqueue(
                "invalid-message",
                "conversation",
                " ",
                &serde_json::Value::Null,
            )
            .unwrap();
        mappings
            .enqueue(
                "valid-message",
                "conversation",
                "hello",
                &serde_json::Value::Null,
            )
            .unwrap();
        drop(mappings);

        let listener = tokio::net::UnixListener::bind(root.path().join("hub.sock")).unwrap();
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                let started_tx = started_tx.clone();
                tokio::spawn(async move {
                    let (reader, mut writer) = stream.into_split();
                    let mut lines = BufReader::new(reader).lines();
                    while let Some(line) = lines.next_line().await.unwrap() {
                        let request = mews_protocol::decode_hub_frame::<mews_protocol::HubRequest>(
                            line.as_bytes(),
                        )
                        .unwrap();
                        let response = match request.body {
                            mews_protocol::HubRequest::SubscribeSession { .. } => {
                                mews_protocol::HubResponse::Ack
                            }
                            mews_protocol::HubRequest::StartTurn {
                                session_id, prompt, ..
                            } => {
                                started_tx.send((session_id.clone(), prompt)).unwrap();
                                mews_protocol::HubResponse::Turn(mews_protocol::Turn {
                                    id: mews_protocol::TurnId::new(),
                                    session_id,
                                    agent_revision: 1,
                                    harness: None,
                                    harness_definition_hash: None,
                                    harness_version: None,
                                    status: mews_protocol::TurnStatus::Running,
                                    error: None,
                                    created_at: "2026-01-01T00:00:00Z".parse().unwrap(),
                                    completed_at: None,
                                })
                            }
                            other => panic!("unexpected request: {other:?}"),
                        };
                        let response =
                            mews_protocol::Frame::with_request_id(response, request.request_id);
                        writer
                            .write_all(&mews_protocol::encode_hub_frame(&response).unwrap())
                            .await
                            .unwrap();
                        writer.write_all(b"\n").await.unwrap();
                    }
                });
            }
        });

        let runtime = ChannelRuntime::open(
            RecoveryChannel,
            root.path(),
            &state,
            ChannelConfig::new("coder"),
        )
        .await
        .unwrap();

        let started = tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(started, (session.clone(), "hello".into()));
        assert!(runtime.mappings.active(&session).unwrap());
        assert!(runtime.mappings.next(&session).unwrap().is_none());
        server.abort();
    }

    #[tokio::test]
    async fn terminal_turn_replay_does_not_block_the_next_pending_message() {
        let root = tempfile::tempdir().unwrap();
        let state = root.path().join("channel-state");
        let session = SessionId::new();
        let mut mappings = MappingStore::open(&state).unwrap();
        mappings.consumer_id().unwrap();
        mappings.insert("conversation", &session).unwrap();
        mappings
            .enqueue(
                "duplicate-message",
                "conversation",
                "duplicate",
                &serde_json::Value::Null,
            )
            .unwrap();
        mappings
            .enqueue(
                "next-message",
                "conversation",
                "next",
                &serde_json::Value::Null,
            )
            .unwrap();
        drop(mappings);

        let listener = tokio::net::UnixListener::bind(root.path().join("hub.sock")).unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let server_requests = Arc::clone(&requests);
        let completed_turn_id = mews_protocol::TurnId::new();
        let running_turn_id = mews_protocol::TurnId::new();
        let expected_running_turn_id = running_turn_id.clone();
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                let requests = Arc::clone(&server_requests);
                let started_tx = started_tx.clone();
                let completed_turn_id = completed_turn_id.clone();
                let running_turn_id = running_turn_id.clone();
                tokio::spawn(async move {
                    let (reader, mut writer) = stream.into_split();
                    let mut lines = BufReader::new(reader).lines();
                    while let Some(line) = lines.next_line().await.unwrap() {
                        let request = mews_protocol::decode_hub_frame::<mews_protocol::HubRequest>(
                            line.as_bytes(),
                        )
                        .unwrap();
                        let response = match request.body {
                            mews_protocol::HubRequest::SubscribeSession { .. } => {
                                mews_protocol::HubResponse::Ack
                            }
                            mews_protocol::HubRequest::StartTurn {
                                session_id, prompt, ..
                            } => {
                                started_tx.send(prompt).unwrap();
                                let replayed = requests.fetch_add(1, Ordering::SeqCst) == 0;
                                mews_protocol::HubResponse::Turn(mews_protocol::Turn {
                                    id: if replayed {
                                        completed_turn_id.clone()
                                    } else {
                                        running_turn_id.clone()
                                    },
                                    session_id,
                                    agent_revision: 1,
                                    harness: None,
                                    harness_definition_hash: None,
                                    harness_version: None,
                                    status: if replayed {
                                        mews_protocol::TurnStatus::Completed
                                    } else {
                                        mews_protocol::TurnStatus::Running
                                    },
                                    error: None,
                                    created_at: "2026-01-01T00:00:00Z".parse().unwrap(),
                                    completed_at: replayed
                                        .then(|| "2026-01-01T00:00:01Z".parse().unwrap()),
                                })
                            }
                            other => panic!("unexpected request: {other:?}"),
                        };
                        let response =
                            mews_protocol::Frame::with_request_id(response, request.request_id);
                        writer
                            .write_all(&mews_protocol::encode_hub_frame(&response).unwrap())
                            .await
                            .unwrap();
                        writer.write_all(b"\n").await.unwrap();
                    }
                });
            }
        });

        let runtime = ChannelRuntime::open(
            RecoveryChannel,
            root.path(),
            &state,
            ChannelConfig::new("coder"),
        )
        .await
        .unwrap();

        assert_eq!(started_rx.recv().await.unwrap(), "duplicate");
        assert_eq!(started_rx.recv().await.unwrap(), "next");
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        assert_eq!(
            runtime.mappings.active_turns().unwrap(),
            vec![(session.clone(), expected_running_turn_id.to_string())]
        );
        assert!(runtime.mappings.next(&session).unwrap().is_none());
        server.abort();
    }

    #[tokio::test]
    async fn lanes_are_fifo_concurrent_bounded_and_retries_release_workers() {
        let (sent_tx, mut sent_rx) = mpsc::unbounded_channel();
        let outbound = Arc::new(TestOutbound {
            sent: sent_tx,
            concurrent: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let (tx, rx) = mpsc::channel(8);
        let permits = Arc::new(tokio::sync::Semaphore::new(8));
        let (shutdown_tx, shutdown) = watch::channel(false);
        let (outcomes, _) = tokio::sync::broadcast::channel(8);
        let task = tokio::spawn(delivery_dispatcher(
            Arc::clone(&outbound),
            rx,
            2,
            outcomes,
            shutdown,
        ));
        for (key, text) in [
            ("blocked", "a1"),
            ("blocked", "a2"),
            ("free", "b1"),
            ("other", "c1"),
        ] {
            tx.send(delivery(
                key,
                text,
                permits.clone().acquire_owned().await.unwrap(),
            ))
            .await
            .unwrap();
        }
        let mut observed = Vec::new();
        for _ in 0..5 {
            observed.push(sent_rx.recv().await.unwrap());
        }
        assert!(
            observed.iter().position(|item| item.1 == "b1").unwrap()
                < observed.iter().position(|item| item.1 == "a2").unwrap()
        );
        let blocked: Vec<_> = observed
            .iter()
            .filter(|item| item.0 == "blocked")
            .map(|item| (item.1.as_str(), item.2))
            .collect();
        assert_eq!(blocked, [("a1", 1), ("a1", 2), ("a2", 1)]);
        assert!(outbound.maximum.load(Ordering::SeqCst) <= 2);
        shutdown_tx.send(true).unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn broadcast_shares_fifo_with_an_ordinary_delivery() {
        let (sent_tx, mut sent_rx) = mpsc::unbounded_channel();
        let outbound = Arc::new(TestOutbound {
            sent: sent_tx,
            concurrent: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let (delivery_tx, delivery_rx) = mpsc::channel(4);
        let ordinary_tx = delivery_tx.clone();
        let capacity = Arc::new(tokio::sync::Semaphore::new(4));
        let handle = ChannelHandle {
            delivery_tx,
            delivery_capacity: capacity,
            outcomes: tokio::sync::broadcast::channel(4).0,
        };
        let (shutdown_tx, shutdown) = watch::channel(false);
        let task = tokio::spawn(delivery_dispatcher(
            outbound,
            delivery_rx,
            2,
            handle.outcomes.clone(),
            shutdown,
        ));

        ordinary_tx
            .send(delivery(
                "first",
                "ordinary",
                handle
                    .delivery_capacity
                    .clone()
                    .acquire_owned()
                    .await
                    .unwrap(),
            ))
            .await
            .unwrap();
        handle
            .broadcast(
                ["first".into()],
                BroadcastMessage {
                    idempotency_key: "notice-1".into(),
                    event: OutboundEvent::CompletedMessage {
                        text: "notice".into(),
                    },
                },
            )
            .await
            .unwrap();
        let first = sent_rx.recv().await.unwrap();
        let second = sent_rx.recv().await.unwrap();
        assert_eq!((first.0.as_str(), first.1.as_str()), ("first", "ordinary"));
        assert_eq!((second.0.as_str(), second.1.as_str()), ("first", "notice"));
        shutdown_tx.send(true).unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn dispatcher_capacity_includes_delayed_work_and_resumes_after_terminal_outcome() {
        let (sent_tx, mut sent_rx) = mpsc::unbounded_channel();
        let outbound = Arc::new(TestOutbound {
            sent: sent_tx,
            concurrent: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let (delivery_tx, delivery_rx) = mpsc::channel(1);
        let (outcomes, _) = tokio::sync::broadcast::channel(2);
        let handle = ChannelHandle {
            delivery_tx,
            delivery_capacity: Arc::new(tokio::sync::Semaphore::new(1)),
            outcomes: outcomes.clone(),
        };
        let (shutdown_tx, shutdown) = watch::channel(false);
        let task = tokio::spawn(delivery_dispatcher(
            outbound,
            delivery_rx,
            1,
            outcomes,
            shutdown,
        ));
        handle
            .broadcast(
                ["blocked".into()],
                BroadcastMessage {
                    idempotency_key: "first".into(),
                    event: OutboundEvent::CompletedMessage { text: "one".into() },
                },
            )
            .await
            .unwrap();
        sent_rx.recv().await.unwrap();
        let second = tokio::spawn({
            let handle = handle.clone();
            async move {
                handle
                    .broadcast(
                        ["free".into()],
                        BroadcastMessage {
                            idempotency_key: "second".into(),
                            event: OutboundEvent::CompletedMessage { text: "two".into() },
                        },
                    )
                    .await
            }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!second.is_finished());
        tokio::time::timeout(Duration::from_millis(200), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        shutdown_tx.send(true).unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn terminal_delivery_outcomes_are_observable() {
        let (sent_tx, _sent_rx) = mpsc::unbounded_channel();
        let outbound = Arc::new(TestOutbound {
            sent: sent_tx,
            concurrent: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let (delivery_tx, delivery_rx) = mpsc::channel(2);
        let (outcomes, _) = tokio::sync::broadcast::channel(2);
        let handle = ChannelHandle {
            delivery_tx,
            delivery_capacity: Arc::new(tokio::sync::Semaphore::new(2)),
            outcomes: outcomes.clone(),
        };
        let mut observed = handle.subscribe_outcomes();
        let (shutdown_tx, shutdown) = watch::channel(false);
        let task = tokio::spawn(delivery_dispatcher(
            outbound,
            delivery_rx,
            2,
            outcomes,
            shutdown,
        ));
        handle
            .broadcast(
                ["free".into(), "dropped".into()],
                BroadcastMessage {
                    idempotency_key: "terminal".into(),
                    event: OutboundEvent::CompletedMessage {
                        text: "text".into(),
                    },
                },
            )
            .await
            .unwrap();
        let mut terminal = [
            observed.recv().await.unwrap(),
            observed.recv().await.unwrap(),
        ];
        terminal.sort_by(|left, right| left.conversation.cmp(&right.conversation));
        assert!(
            matches!(terminal[0].outcome, TerminalDeliveryOutcome::Dropped { ref reason } if reason == "permanent")
        );
        assert!(matches!(
            terminal[1].outcome,
            TerminalDeliveryOutcome::Delivered(_)
        ));
        shutdown_tx.send(true).unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn closed_input_does_not_starve_a_delayed_retry() {
        let (sent_tx, mut sent_rx) = mpsc::unbounded_channel();
        let outbound = Arc::new(TestOutbound {
            sent: sent_tx,
            concurrent: AtomicUsize::new(0),
            maximum: AtomicUsize::new(0),
        });
        let (delivery_tx, delivery_rx) = mpsc::channel(1);
        let capacity = Arc::new(tokio::sync::Semaphore::new(1));
        let (_shutdown_tx, shutdown) = watch::channel(false);
        let (outcomes, _) = tokio::sync::broadcast::channel(1);
        let task = tokio::spawn(delivery_dispatcher(
            outbound,
            delivery_rx,
            1,
            outcomes,
            shutdown,
        ));
        delivery_tx
            .send(delivery(
                "blocked",
                "retry",
                capacity.acquire_owned().await.unwrap(),
            ))
            .await
            .unwrap();
        drop(delivery_tx);
        assert_eq!(sent_rx.recv().await.unwrap().2, 1);
        assert_eq!(sent_rx.recv().await.unwrap().2, 2);
        tokio::time::timeout(Duration::from_millis(200), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    fn streaming_subscription_requires_the_matching_capability() {
        assert!(validate_negotiation(&[ChannelSubscription::StreamingUpdates], &[]).is_err());
        assert!(
            validate_negotiation(
                &[ChannelSubscription::StreamingUpdates],
                &[crate::ChannelCapability::Streaming],
            )
            .is_ok()
        );
        assert!(validate_negotiation(&[ChannelSubscription::CompletedMessages], &[]).is_ok());
    }

    #[test]
    fn channel_turn_keys_are_fixed_size_and_unambiguous() {
        let consumer = mews_protocol::ConsumerId::new();
        let first = channel_turn_key(&consumer, "ab", "c");
        let second = channel_turn_key(&consumer, "a", "bc");
        let long = channel_turn_key(&consumer, &"x".repeat(10_000), &"y".repeat(10_000));

        assert_eq!(first.len(), "channel:".len() + 64);
        assert_eq!(long.len(), first.len());
        assert_ne!(first, second);
    }
}
