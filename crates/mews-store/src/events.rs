use super::*;

impl Store {
    pub fn append_client_event(
        &self,
        session_id: &SessionId,
        kind: ClientEventKind,
    ) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO client_events (id, session_id, kind_json, transient, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                EventId::new().as_str(),
                session_id.as_str(),
                json(&kind)?,
                kind.is_transient(),
                timestamp(Utc::now())
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
            "SELECT e.sequence, e.id, e.session_id, e.kind_json, e.created_at
             FROM client_events e
             JOIN client_subscriptions s ON s.session_id = e.session_id
             WHERE s.consumer_id = ?1 AND e.sequence > ?2
             ORDER BY e.sequence LIMIT ?3",
        )?;
        let rows = statement
            .query_map(
                params![consumer_id.as_str(), cursor, i64::from(limit)],
                |row| {
                    Ok((
                        row.get::<_, u64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;
        let checkpoint = rows.last().map_or(cursor, |row| row.0);
        let events = rows
            .into_iter()
            .map(|row| {
                Ok(ClientEvent {
                    sequence: row.0,
                    id: parse_id(row.1)?,
                    session_id: parse_id(row.2)?,
                    kind: parse_json(row.3)?,
                    created_at: parse_time(row.4)?,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
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
    fn prune_client_events(&self) -> Result<(), StoreError> {
        self.connection.execute(
            "DELETE FROM client_events AS e
             WHERE (
                 e.transient = 0
                 AND EXISTS (
                     SELECT 1 FROM client_subscriptions s
                     JOIN client_consumers c ON c.id = s.consumer_id
                     WHERE s.session_id = e.session_id AND c.kind = 'durable'
                 )
                 AND NOT EXISTS (
                     SELECT 1 FROM client_subscriptions s
                     JOIN client_consumers c ON c.id = s.consumer_id
                     WHERE s.session_id = e.session_id AND c.kind = 'durable'
                       AND c.cursor < e.sequence
                 )
             ) OR (
                 e.transient = 1
                 AND (
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
                     ))
                 )
             )",
            [],
        )?;
        Ok(())
    }
}

fn consumer_kind(kind: ConsumerKind) -> &'static str {
    match kind {
        ConsumerKind::Durable => "durable",
        ConsumerKind::Ephemeral => "ephemeral",
    }
}
