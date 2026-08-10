use super::*;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClientEvent {
    pub id: EventId,
    pub sequence: u64,
    pub session_id: SessionId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_origin: Option<ChannelOrigin>,
    pub kind: ClientEventKind,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientEventKind {
    RunStarted {
        run_id: RunId,
    },
    /// Ephemeral-sized response fragment. The completed assistant message is
    /// still journaled separately as the durable conversation record.
    AssistantDelta {
        run_id: RunId,
        delta: String,
        message_id: Option<String>,
    },
    ReasoningDelta {
        run_id: RunId,
        delta: String,
        message_id: Option<String>,
    },
    ToolActivity {
        run_id: RunId,
        activity: ToolActivity,
    },
    AssistantMessage {
        run_id: RunId,
        message: Message,
    },
    ToolStarted {
        run_id: RunId,
        message: Message,
    },
    ToolCompleted {
        run_id: RunId,
        message: Message,
    },
    RunCompleted {
        run_id: RunId,
    },
    RunFailed {
        run_id: RunId,
        error: String,
    },
    RunCancelled {
        run_id: RunId,
    },
}

impl ClientEventKind {
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::AssistantDelta { .. } | Self::ReasoningDelta { .. } | Self::ToolActivity { .. }
        )
    }

    pub fn run_id(&self) -> Option<&RunId> {
        match self {
            Self::RunStarted { run_id }
            | Self::AssistantDelta { run_id, .. }
            | Self::ReasoningDelta { run_id, .. }
            | Self::ToolActivity { run_id, .. }
            | Self::AssistantMessage { run_id, .. }
            | Self::ToolStarted { run_id, .. }
            | Self::ToolCompleted { run_id, .. }
            | Self::RunCompleted { run_id }
            | Self::RunFailed { run_id, .. }
            | Self::RunCancelled { run_id } => Some(run_id),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerKind {
    #[default]
    Durable,
    Ephemeral,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolActivity {
    pub call_id: String,
    pub title: String,
    pub kind: Option<String>,
    pub status: Option<String>,
    pub input: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventBatch {
    pub events: Vec<ClientEvent>,
    /// Acknowledging this advances past the subscribed events returned by the Hub.
    pub checkpoint: u64,
    pub advanced: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthCredential {
    ApiKey {
        key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        base_url: Option<String>,
    },
    Oauth {
        access: String,
        refresh: String,
        expires: u64,
        #[serde(rename = "accountId")]
        account_id: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthStatus {
    pub provider: String,
    pub kind: String,
}
