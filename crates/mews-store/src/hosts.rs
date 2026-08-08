use super::*;

impl Store {
    pub fn hosts(&self) -> Result<Vec<Host>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT id, name, public_key, noise_public_key, relay_url, created_at FROM hosts WHERE revoked = 0 ORDER BY name",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(Host {
                id: parse_id(row.get::<_, String>(0)?)?,
                name: row.get(1)?,
                public_key: row.get(2)?,
                noise_public_key: row.get(3)?,
                relay_url: row.get(4)?,
                created_at: parse_time(row.get::<_, String>(5)?)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn host(&self, id: &HostId) -> Result<Host, StoreError> {
        self.connection
            .query_row(
                "SELECT name, public_key, noise_public_key, relay_url, created_at FROM hosts WHERE id = ?1 AND revoked = 0",
                [id.as_str()],
                |row| {
                    Ok(Host {
                        id: id.clone(),
                        name: row.get(0)?,
                        public_key: row.get(1)?,
                        noise_public_key: row.get(2)?,
                        relay_url: row.get(3)?,
                        created_at: parse_time(row.get::<_, String>(4)?)?,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound {
                kind: "Host",
                id: id.to_string(),
            })
    }

    pub fn revoke_host(&self, id: &HostId) -> Result<(), StoreError> {
        let installation = self
            .installation()?
            .ok_or_else(|| StoreError::InvalidData("installation is missing".into()))?;
        if installation.hub_host_id == *id {
            return Err(StoreError::InvalidData(
                "cannot remove the Host currently running Hub".into(),
            ));
        }
        let changed = self.connection.execute(
            "UPDATE hosts SET revoked = 1 WHERE id = ?1 AND revoked = 0",
            [id.as_str()],
        )?;
        if changed == 0 {
            return Err(StoreError::NotFound {
                kind: "Host",
                id: id.to_string(),
            });
        }
        Ok(())
    }

    pub fn create_invitation(
        &self,
        expires_at: DateTime<Utc>,
    ) -> Result<(InvitationId, String), StoreError> {
        if expires_at <= Utc::now() {
            return Err(StoreError::InvalidData(
                "invitation expiry must be in the future".into(),
            ));
        }
        let id = InvitationId::new();
        let mut secret_bytes = [0; 32];
        OsRng.fill_bytes(&mut secret_bytes);
        let secret = URL_SAFE_NO_PAD.encode(secret_bytes);
        self.connection.execute(
            "INSERT INTO invitations (id, secret_hash, expires_at, consumed_at)
             VALUES (?1, ?2, ?3, NULL)",
            params![id.as_str(), secret_hash(&secret), timestamp(expires_at)],
        )?;
        Ok((id, secret))
    }

    pub fn consume_invitation(
        &mut self,
        invitation_id: &InvitationId,
        secret: &str,
        name: &str,
        public_key: &str,
        noise_public_key: &str,
        relay_url: &str,
    ) -> Result<Host, StoreError> {
        validate_name("Host name", name)?;
        let transaction = self.connection.transaction()?;
        let invitation = transaction
            .query_row(
                "SELECT secret_hash, expires_at, consumed_at FROM invitations WHERE id = ?1",
                [invitation_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        parse_time(row.get(1)?)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound {
                kind: "invitation",
                id: invitation_id.to_string(),
            })?;
        if invitation.2.is_some()
            || invitation.1 <= Utc::now()
            || invitation.0 != secret_hash(secret)
        {
            return Err(StoreError::InvalidData(
                "invitation is invalid, expired, or already used".into(),
            ));
        }
        let host = Host {
            id: HostId::new(),
            name: name.to_owned(),
            public_key: public_key.to_owned(),
            noise_public_key: noise_public_key.to_owned(),
            relay_url: Some(relay_url.to_owned()),
            created_at: Utc::now(),
        };
        transaction.execute(
            "INSERT INTO hosts (id, name, public_key, noise_public_key, relay_url, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                host.id.as_str(),
                host.name,
                host.public_key,
                host.noise_public_key,
                host.relay_url,
                timestamp(host.created_at)
            ],
        )?;
        let consumed = transaction.execute(
            "UPDATE invitations SET consumed_at = ?2
             WHERE id = ?1 AND consumed_at IS NULL",
            params![invitation_id.as_str(), timestamp(Utc::now())],
        )?;
        if consumed != 1 {
            return Err(StoreError::InvalidData(
                "invitation was concurrently consumed".into(),
            ));
        }
        transaction.commit()?;
        Ok(host)
    }
}
