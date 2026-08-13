use super::*;

// Four times the maximum 30-second long poll keeps healthy observers alive
// while bounding retention after a client disappears without cleanup.
const EPHEMERAL_CONSUMER_LEASE_SECONDS: i64 = 120;

fn runtime_payload_from_client(
    event: ClientEventKind,
) -> Option<mews_protocol::RuntimeSignalPayload> {
    match event {
        ClientEventKind::AssistantDelta {
            delta, message_id, ..
        } => Some(mews_protocol::RuntimeSignalPayload::AssistantDelta { delta, message_id }),
        ClientEventKind::ReasoningDelta {
            delta, message_id, ..
        } => Some(mews_protocol::RuntimeSignalPayload::ReasoningDelta { delta, message_id }),
        ClientEventKind::ToolActivity { activity, .. } => {
            Some(mews_protocol::RuntimeSignalPayload::ToolActivity { activity })
        }
        _ => None,
    }
}

pub(crate) fn append_durable_client_event(
    transaction: &rusqlite::Transaction<'_>,
    journal_entry: &mews_protocol::JournalEntry,
    entry_id: Option<&MessageId>,
    kind: &ClientEventKind,
) -> Result<(), StoreError> {
    let session_id = crate::events::journal_session_id(journal_entry)?;
    let origin = channel_origin_json(transaction, kind)?;
    validate_client_event(
        &session_id,
        &journal_entry.id,
        kind,
        origin.as_deref(),
        journal_entry.recorded_at,
    )?;
    transaction.execute(
        "INSERT INTO client_events
         (id, session_id, entry_id, journal_entry_id, journal_position,
          kind_json, channel_origin_json, transient, created_at)
         VALUES (?1, ?2, ?3, ?1, ?4, ?5, ?6, 0, ?7)",
        params![
            journal_entry.id.as_str(),
            session_id.as_str(),
            entry_id.map(MessageId::as_str),
            journal_entry.position,
            json(kind)?,
            origin,
            timestamp(journal_entry.recorded_at)
        ],
    )?;
    Ok(())
}

pub(crate) fn append_runtime_signal_in(
    transaction: &rusqlite::Transaction<'_>,
    signal: mews_protocol::RuntimeSignal,
    idempotency_key: Option<&str>,
) -> Result<(), StoreError> {
    let mews_protocol::RuntimeSignal {
        id,
        session_id,
        turn_id,
        channel_origin,
        emitted_at,
        payload,
    } = signal;
    let kind = match payload {
        mews_protocol::RuntimeSignalPayload::AssistantDelta { delta, message_id } => {
            ClientEventKind::AssistantDelta {
                turn_id,
                delta,
                message_id,
            }
        }
        mews_protocol::RuntimeSignalPayload::ReasoningDelta { delta, message_id } => {
            ClientEventKind::ReasoningDelta {
                turn_id,
                delta,
                message_id,
            }
        }
        mews_protocol::RuntimeSignalPayload::ToolActivity { activity } => {
            ClientEventKind::ToolActivity { turn_id, activity }
        }
    };
    let origin = channel_origin.as_ref().map(json).transpose()?;
    validate_client_event(&session_id, &id, &kind, origin.as_deref(), emitted_at)?;
    if let Some(idempotency_key) = idempotency_key {
        // The key and payload must become visible together. `OR IGNORE` makes a
        // replay race converge on the first committed signal via the unique key.
        transaction.execute(
            "INSERT OR IGNORE INTO client_events
             (id, session_id, idempotency_key, kind_json, channel_origin_json,
              transient, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6)",
            params![
                id.as_str(),
                session_id.as_str(),
                idempotency_key,
                json(&kind)?,
                origin,
                timestamp(emitted_at)
            ],
        )?;
    } else {
        transaction.execute(
            "INSERT INTO client_events
             (id, session_id, kind_json, channel_origin_json, transient, created_at)
             VALUES (?1, ?2, ?3, ?4, 1, ?5)",
            params![
                id.as_str(),
                session_id.as_str(),
                json(&kind)?,
                origin,
                timestamp(emitted_at)
            ],
        )?;
    }
    Ok(())
}

impl Store {
    pub fn emit_runtime_signal(
        &self,
        session_id: &SessionId,
        turn_id: &TurnId,
        payload: mews_protocol::RuntimeSignalPayload,
    ) -> Result<(), StoreError> {
        self.emit_runtime_signal_with_key(session_id, turn_id, payload, None)
    }

    fn emit_runtime_signal_with_key(
        &self,
        session_id: &SessionId,
        turn_id: &TurnId,
        payload: mews_protocol::RuntimeSignalPayload,
        idempotency_key: Option<&str>,
    ) -> Result<(), StoreError> {
        let transaction = rusqlite::Transaction::new_unchecked(
            &self.connection,
            rusqlite::TransactionBehavior::Immediate,
        )?;
        if let Some(idempotency_key) = idempotency_key {
            let exists: bool = transaction.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM client_events
                    WHERE session_id = ?1 AND idempotency_key = ?2
                 )",
                params![session_id.as_str(), idempotency_key],
                |row| row.get(0),
            )?;
            if exists {
                transaction.commit()?;
                return Ok(());
            }
        }
        let channel_origin = transaction
            .query_row(
                "SELECT channel_origin_json FROM turns
                 WHERE id = ?1 AND session_id = ?2 AND completed_at IS NULL",
                params![turn_id.as_str(), session_id.as_str()],
                |row| row.get::<_, Option<String>>(0)?.map(parse_json).transpose(),
            )
            .optional()?
            .ok_or_else(|| {
                StoreError::InvalidData(format!(
                    "Turn {turn_id} is not active in Session {session_id}"
                ))
            })?;
        append_runtime_signal_in(
            &transaction,
            mews_protocol::RuntimeSignal {
                id: EventId::new(),
                session_id: session_id.clone(),
                turn_id: turn_id.clone(),
                channel_origin,
                emitted_at: Utc::now(),
                payload,
            },
            idempotency_key,
        )?;
        transaction.commit()?;
        self.prune_client_events()
    }

    pub fn append_runtime_signal(
        &self,
        signal: mews_protocol::RuntimeSignal,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        append_runtime_signal_in(&transaction, signal, None)?;
        transaction.commit()?;
        self.prune_client_events()
    }

    /// Commit the inspectable ACP observation with its corresponding delivery
    /// event before subscribers can see either one.
    pub fn append_acp_observation_with_client_event(
        &self,
        session_id: &SessionId,
        turn_id: TurnId,
        acp_session_id: Option<String>,
        event_key: Option<mews_protocol::AcpEventKey>,
        observation: AcpObservation,
        event: ClientEventKind,
    ) -> Result<(), StoreError> {
        if matches!(observation, AcpObservation::AssistantDelta { .. }) {
            let payload = runtime_payload_from_client(event).ok_or_else(|| {
                StoreError::InvalidData("ACP delta must use a transient delivery kind".into())
            })?;
            return self.emit_runtime_signal_with_key(
                session_id,
                &turn_id,
                payload,
                event_key.as_deref(),
            );
        }
        let transient = runtime_payload_from_client(event);
        let transaction = self.connection.unchecked_transaction()?;
        crate::sessions::record_acp_observation_transaction(
            &transaction,
            session_id,
            turn_id.clone(),
            acp_session_id,
            event_key.clone(),
            observation,
        )?;
        if let Some(payload) = transient {
            let channel_origin = transaction.query_row(
                "SELECT channel_origin_json FROM turns
                 WHERE id = ?1 AND session_id = ?2 AND completed_at IS NULL",
                params![turn_id.as_str(), session_id.as_str()],
                |row| row.get::<_, Option<String>>(0)?.map(parse_json).transpose(),
            )?;
            append_runtime_signal_in(
                &transaction,
                mews_protocol::RuntimeSignal {
                    id: EventId::new(),
                    session_id: session_id.clone(),
                    turn_id,
                    channel_origin,
                    emitted_at: Utc::now(),
                    payload,
                },
                event_key.as_deref(),
            )?;
        }
        transaction.commit()?;
        self.prune_client_events()
    }

    #[cfg(test)]
    pub(crate) fn append_client_event(
        &self,
        session_id: &SessionId,
        kind: ClientEventKind,
    ) -> Result<(), StoreError> {
        if !kind.is_transient() {
            return Err(StoreError::InvalidData(
                "durable client events must be projected from a JournalEntry".into(),
            ));
        }
        let id = EventId::new();
        let now = Utc::now();
        let origin = channel_origin_json(&self.connection, &kind)?;
        validate_client_event(session_id, &id, &kind, origin.as_deref(), now)?;
        self.connection.execute(
            "INSERT INTO client_events (id, session_id, kind_json, channel_origin_json, transient, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                id.as_str(),
                session_id.as_str(),
                json(&kind)?,
                origin,
                kind.is_transient(),
                timestamp(now)
            ],
        )?;
        self.prune_client_events()?;
        Ok(())
    }

    pub fn subscribe_session(
        &self,
        consumer_id: &ConsumerId,
        session_id: &SessionId,
        kind: ConsumerKind,
    ) -> Result<(), StoreError> {
        self.session(session_id)?;
        let transaction = self.connection.unchecked_transaction()?;
        let changed = transaction.execute(
            "INSERT INTO client_consumers (id, cursor, kind, created_at, last_seen_at)
             VALUES (?1, (SELECT COALESCE(MAX(sequence), 0) FROM client_events), ?2, ?3, ?3)
             ON CONFLICT(id) DO UPDATE SET
                 kind = excluded.kind,
                 last_seen_at = excluded.last_seen_at
             WHERE client_consumers.kind = excluded.kind",
            params![
                consumer_id.as_str(),
                consumer_kind(kind),
                timestamp(Utc::now())
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::InvalidData(
                "an event consumer cannot change durability".into(),
            ));
        }
        transaction.execute(
            "INSERT OR IGNORE INTO client_subscriptions (consumer_id, session_id) VALUES (?1, ?2)",
            params![consumer_id.as_str(), session_id.as_str()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn delete_consumer(&self, consumer_id: &ConsumerId) -> Result<(), StoreError> {
        self.connection.execute(
            "DELETE FROM client_consumers WHERE id = ?1",
            [consumer_id.as_str()],
        )?;
        self.prune_client_events()?;
        Ok(())
    }

    pub fn unsubscribe_session(
        &self,
        consumer_id: &ConsumerId,
        session_id: &SessionId,
    ) -> Result<(), StoreError> {
        self.connection.execute(
            "DELETE FROM client_subscriptions WHERE consumer_id = ?1 AND session_id = ?2",
            params![consumer_id.as_str(), session_id.as_str()],
        )?;
        self.prune_client_events()?;
        Ok(())
    }

    pub fn client_events(
        &self,
        consumer_id: &ConsumerId,
        limit: u16,
    ) -> Result<EventBatch, StoreError> {
        let cursor: u64 = self
            .connection
            .query_row(
                "UPDATE client_consumers SET last_seen_at = ?2 WHERE id = ?1 RETURNING cursor",
                params![consumer_id.as_str(), timestamp(Utc::now())],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound {
                kind: "event consumer",
                id: consumer_id.to_string(),
            })?;
        let mut statement = self.connection.prepare(
            "SELECT e.sequence, e.id, e.session_id, e.kind_json,
                    e.channel_origin_json, e.created_at
             FROM client_events e
             JOIN client_subscriptions s ON s.session_id = e.session_id
             WHERE s.consumer_id = ?1 AND e.sequence > ?2
             ORDER BY e.sequence LIMIT ?3",
        )?;
        let mut rows = statement.query_map(
            params![consumer_id.as_str(), cursor, i64::from(limit)],
            |row| {
                Ok((
                    row.get::<_, u64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, String>(5)?,
                ))
            },
        )?;
        let mut events = Vec::new();
        let mut payload_bytes = 0usize;
        for row in &mut rows {
            let row = row?;
            let event = ClientEvent {
                sequence: row.0,
                id: parse_id(row.1)?,
                session_id: parse_id(row.2)?,
                kind: parse_json(row.3)?,
                channel_origin: row.4.map(parse_json).transpose()?,
                created_at: parse_time(row.5)?,
            };
            let event_bytes = serde_json::to_vec(&event)
                .map_err(|error| StoreError::InvalidData(error.to_string()))?
                .len();
            if event_bytes > mews_protocol::MAX_EVENT_PAGE_PAYLOAD_BYTES {
                return Err(StoreError::InvalidData(
                    "stored client event exceeds the event page limit".into(),
                ));
            }
            if !events.is_empty()
                && payload_bytes + event_bytes > mews_protocol::MAX_EVENT_PAGE_PAYLOAD_BYTES
            {
                break;
            }
            payload_bytes += event_bytes;
            events.push(event);
        }
        let checkpoint = events.last().map_or(cursor, |event| event.sequence);
        Ok(EventBatch {
            events,
            checkpoint,
            advanced: checkpoint > cursor,
        })
    }

    pub fn acknowledge_events(
        &self,
        consumer_id: &ConsumerId,
        checkpoint: u64,
    ) -> Result<(), StoreError> {
        // AUTOINCREMENT's watermark survives pruning, unlike MAX(sequence).
        let max: u64 = self.connection.query_row(
            "SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'client_events'), 0)",
            [],
            |row| row.get(0),
        )?;
        if checkpoint > max {
            return Err(StoreError::InvalidData(
                "event checkpoint is ahead of Hub".into(),
            ));
        }
        let changed = self.connection.execute(
            "UPDATE client_consumers
             SET cursor = MAX(cursor, ?2), last_seen_at = ?3
             WHERE id = ?1",
            params![consumer_id.as_str(), checkpoint, timestamp(Utc::now())],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound {
                kind: "event consumer",
                id: consumer_id.to_string(),
            });
        }
        self.prune_client_events()?;
        Ok(())
    }

    /// Durable consumers define the replay floor. Ephemeral consumers are scoped
    /// observers and must not retain the journal after they disappear or stall.
    pub(crate) fn prune_client_events(&self) -> Result<(), StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        transaction.execute(
            "DELETE FROM client_consumers
             WHERE kind = 'ephemeral' AND julianday(last_seen_at) < julianday(?1)",
            [timestamp(
                Utc::now() - chrono::Duration::seconds(EPHEMERAL_CONSUMER_LEASE_SECONDS),
            )],
        )?;
        transaction.execute(
            "DELETE FROM client_events AS e
             WHERE
                 (e.transient = 1 AND NOT EXISTS (
                     SELECT 1 FROM client_subscriptions s
                     JOIN client_consumers c ON c.id = s.consumer_id
                     WHERE s.session_id = e.session_id AND c.cursor < e.sequence
                 ))
                 OR
                 (e.transient = 0 AND EXISTS (
                     SELECT 1 FROM client_subscriptions s
                     JOIN client_consumers c ON c.id = s.consumer_id
                     WHERE s.session_id = e.session_id AND c.kind = 'durable'
                 ) AND NOT EXISTS (
                     SELECT 1 FROM client_subscriptions s
                     JOIN client_consumers c ON c.id = s.consumer_id
                     WHERE s.session_id = e.session_id AND c.kind = 'durable'
                       AND c.cursor < e.sequence
                 ))
                 OR
                 (e.transient = 0 AND NOT EXISTS (
                     SELECT 1 FROM client_subscriptions s
                     JOIN client_consumers c ON c.id = s.consumer_id
                     WHERE s.session_id = e.session_id AND c.kind = 'durable'
                 ) AND NOT EXISTS (
                     SELECT 1 FROM client_subscriptions s
                     JOIN client_consumers c ON c.id = s.consumer_id
                     WHERE s.session_id = e.session_id AND c.cursor < e.sequence
                 ))",
            [],
        )?;
        transaction.commit()?;
        Ok(())
    }
}

pub(crate) fn channel_origin_json(
    connection: &rusqlite::Connection,
    kind: &ClientEventKind,
) -> Result<Option<String>, StoreError> {
    let Some(turn_id) = kind.turn_id() else {
        return Ok(None);
    };
    Ok(connection
        .query_row(
            "SELECT channel_origin_json FROM turns WHERE id = ?1",
            [turn_id.as_str()],
            |row| row.get(0),
        )
        .optional()?
        .flatten())
}

pub(crate) fn validate_client_event(
    session_id: &SessionId,
    id: &EventId,
    kind: &ClientEventKind,
    channel_origin_json: Option<&str>,
    created_at: chrono::DateTime<Utc>,
) -> Result<(), StoreError> {
    let event = ClientEvent {
        sequence: u64::MAX,
        id: id.clone(),
        session_id: session_id.clone(),
        channel_origin: channel_origin_json
            .map(|value| parse_json(value.to_owned()))
            .transpose()?,
        kind: kind.clone(),
        created_at,
    };
    let bytes = serde_json::to_vec(&event)
        .map_err(|error| StoreError::InvalidData(error.to_string()))?
        .len();
    if bytes > mews_protocol::MAX_EVENT_PAGE_PAYLOAD_BYTES {
        return Err(StoreError::InvalidData(
            "client event exceeds the event page limit".into(),
        ));
    }
    Ok(())
}

fn consumer_kind(kind: ConsumerKind) -> &'static str {
    match kind {
        ConsumerKind::Durable => "durable",
        ConsumerKind::Ephemeral => "ephemeral",
    }
}
