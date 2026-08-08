use std::fmt;

use super::*;

pub(super) fn validate_name(label: &str, value: &str) -> Result<(), StoreError> {
    if value.trim().is_empty() || value.len() > 64 {
        return Err(StoreError::InvalidData(format!(
            "{label} must be 1-64 characters"
        )));
    }
    Ok(())
}

pub(super) fn secret_hash(secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"mews-invitation-v1\0");
    hasher.update(secret.as_bytes());
    format!("{:x}", hasher.finalize())
}

pub(super) fn validate_slug(slug: &str) -> Result<(), StoreError> {
    if slug.is_empty()
        || slug.len() > 64
        || slug.split('-').any(|part| {
            part.is_empty()
                || !part
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        })
    {
        return Err(StoreError::InvalidAgent(
            "slug must contain only lowercase letters, digits, and hyphens".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_definition(
    soul: &str,
    config_toml: &str,
) -> Result<AgentConfig, StoreError> {
    if soul.trim().is_empty() || soul.len() > 24 * 1024 {
        return Err(StoreError::InvalidAgent(
            "SOUL.md must contain 1-24576 bytes".into(),
        ));
    }
    if config_toml.len() > 8 * 1024 {
        return Err(StoreError::InvalidAgent(
            "agent.toml must not exceed 8192 bytes".into(),
        ));
    }
    let config = AgentConfig::parse(config_toml)
        .map_err(|error| StoreError::InvalidAgent(error.to_string()))?;
    config.validate().map_err(StoreError::InvalidAgent)?;
    Ok(config)
}

pub(super) fn revision(
    agent_id: &AgentId,
    revision: u64,
    soul: &str,
    config_toml: &str,
    author_host_id: &HostId,
    created_at: DateTime<Utc>,
) -> AgentRevision {
    let mut hasher = Sha256::new();
    hasher.update((soul.len() as u64).to_be_bytes());
    hasher.update(soul.as_bytes());
    hasher.update((config_toml.len() as u64).to_be_bytes());
    hasher.update(config_toml.as_bytes());
    AgentRevision {
        agent_id: agent_id.clone(),
        revision,
        soul: soul.to_owned(),
        config_toml: config_toml.to_owned(),
        content_hash: format!("{:x}", hasher.finalize()),
        author_host_id: author_host_id.clone(),
        created_at,
    }
}

pub(super) fn insert_revision(
    transaction: &rusqlite::Transaction<'_>,
    revision: &AgentRevision,
) -> Result<(), StoreError> {
    transaction.execute(
        "INSERT INTO agent_revisions
         (agent_id, revision, soul, config_toml, content_hash, author_host_id, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            revision.agent_id.as_str(),
            revision.revision,
            revision.soul,
            revision.config_toml,
            revision.content_hash,
            revision.author_host_id.as_str(),
            timestamp(revision.created_at)
        ],
    )?;
    Ok(())
}

pub(super) fn timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339()
}

pub(super) fn parse_time(value: String) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(&value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|error| conversion(error.to_string()))
}

pub(super) fn parse_id<T: FromStr>(value: String) -> rusqlite::Result<T>
where
    T::Err: fmt::Display,
{
    value
        .parse()
        .map_err(|error: T::Err| conversion(error.to_string()))
}

pub(super) fn json(value: &impl serde::Serialize) -> Result<String, StoreError> {
    serde_json::to_string(value).map_err(|error| StoreError::InvalidData(error.to_string()))
}

pub(super) fn parse_json<T: serde::de::DeserializeOwned>(value: String) -> rusqlite::Result<T> {
    serde_json::from_str(&value).map_err(|error| conversion(error.to_string()))
}

pub(super) fn conversion(message: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            message,
        )),
    )
}
