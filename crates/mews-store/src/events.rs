use super::*;

impl Store {
    /// Commit the inspectable ACP observation with its corresponding delivery
    /// event before subscribers can see either one.
    pub fn append_acp_observation_with_client_event(
        &self,
        session_id: &SessionId,
        run_id: RunId,
        acp_session_id: Option<String>,
        event_key: Option<mews_protocol::AcpEventKey>,
        observation: AcpObservation,
        event: ClientEventKind,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.unchecked_transaction()?;
        if matches!(observation, AcpObservation::AssistantDelta { .. }) {
            let event_id = EventId::new();
            let now = Utc::now();
            let origin = channel_origin_json(&transaction, &event)?;
            validate_client_event(session_id, &event_id, &event, origin.as_deref(), now)?;
            transaction.execute(
                "INSERT OR IGNORE INTO client_events
                 (id, session_id, idempotency_key, kind_json, channel_origin_json, transient, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    event_id.as_str(),
                    session_id.as_str(),
                    event_key.as_deref(),
                    json(&event)?,
                    origin,
                    event.is_transient(),
                    timestamp(now)
                ],
            )?;
            transaction.commit()?;
            self.prune_client_events()?;
            return Ok(());
        }
        let leaf: Option<MessageId> = transaction
            .query_row(
                "SELECT leaf_entry_id FROM sessions WHERE id = ?1",
                [session_id.as_str()],
                |row| row.get::<_, Option<String>>(0)?.map(parse_id).transpose(),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound {
                kind: "session",
                id: session_id.to_string(),
            })?;
        let sequence: u64 = transaction.query_row(
            "SELECT COALESCE(MAX(sequence) + 1, 1) FROM session_entries WHERE session_id = ?1",
            [session_id.as_str()],
            |row| row.get(0),
        )?;
        let payload =
            crate::sessions::harness_observation_payload(run_id, acp_session_id, observation)?;
        let entry_kind = crate::sessions::session_entry_kind(&payload);
        crate::sessions::validate_session_item(&payload)?;
        let now = Utc::now();
        let entry_id = MessageId::new();
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO session_entries (id, session_id, sequence, parent_id, kind, contextual, observation_key, payload_json, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7, ?8)",
            params![entry_id.as_str(), session_id.as_str(), sequence,
                leaf.as_ref().map(MessageId::as_str), entry_kind, event_key, json(&payload)?, timestamp(now)],
        )?;
        if inserted == 0 {
            transaction.commit()?;
            return Ok(());
        }
        let event_id = EventId::new();
        let origin = channel_origin_json(&transaction, &event)?;
        validate_client_event(session_id, &event_id, &event, origin.as_deref(), now)?;
        transaction.execute(
            "INSERT INTO client_events (id, session_id, entry_id, kind_json, channel_origin_json, transient, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                event_id.as_str(),
                session_id.as_str(),
                entry_id.as_str(),
                json(&event)?,
                origin,
                event.is_transient(),
                timestamp(now)
            ],
        )?;
        transaction.commit()?;
        self.prune_client_events()?;
        Ok(())
    }

    pub fn append_client_event(
        &self,
        session_id: &SessionId,
        kind: ClientEventKind,
    ) -> Result<(), StoreError> {
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
            "INSERT INTO client_consumers (id, cursor, kind, created_at)
             VALUES (?1, (SELECT COALESCE(MAX(sequence), 0) FROM client_events), ?2, ?3)
             ON CONFLICT(id) DO UPDATE SET kind = excluded.kind
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
                "SELECT cursor FROM client_consumers WHERE id = ?1",
                [consumer_id.as_str()],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound {
                kind: "event consumer",
                id: consumer_id.to_string(),
            })?;
        let mut statement = self.connection.prepare(
            "SELECT e.sequence, e.id, e.session_id, e.kind_json, e.channel_origin_json, e.created_at
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
            "UPDATE client_consumers SET cursor = MAX(cursor, ?2) WHERE id = ?1",
            params![consumer_id.as_str(), checkpoint],
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
        self.connection.execute(
            "DELETE FROM client_events AS e
             WHERE
                 (EXISTS (
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
                 (NOT EXISTS (
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
        Ok(())
    }
}

pub(crate) fn channel_origin_json(
    connection: &rusqlite::Connection,
    kind: &ClientEventKind,
) -> Result<Option<String>, StoreError> {
    let Some(run_id) = kind.run_id() else {
        return Ok(None);
    };
    Ok(connection
        .query_row(
            "SELECT channel_origin_json FROM runs WHERE id = ?1",
            [run_id.as_str()],
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
