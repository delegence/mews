use super::*;

/// One semantic fact committed alongside authoritative Hub state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalEntry {
    pub id: EventId,
    pub position: u64,
    pub subject: JournalSubject,
    pub event_type: JournalEventType,
    pub recorded_at: DateTime<Utc>,
    pub actor: EventActor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    pub payload: JournalEvent,
}

impl JournalEntry {
    /// Rejects corrupt or hand-built envelopes whose stable discriminator does
    /// not describe the typed payload.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.event_type != self.payload.event_type() {
            return Err("event type does not match payload");
        }
        if self.subject.kind != self.payload.subject_type() {
            return Err("journal subject type does not match payload");
        }
        self.subject.validate()?;
        self.payload.validate_subject_id(&self.subject.id)?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalSubject {
    pub kind: JournalSubjectType,
    pub id: String,
}

impl JournalSubject {
    fn validate(&self) -> Result<(), &'static str> {
        let valid = match self.kind {
            JournalSubjectType::Installation => self.id.parse::<InstallationId>().is_ok(),
            JournalSubjectType::Host => self.id.parse::<HostId>().is_ok(),
            JournalSubjectType::Agent => self.id.parse::<AgentId>().is_ok(),
            JournalSubjectType::Session => self.id.parse::<SessionId>().is_ok(),
        };
        valid.then_some(()).ok_or("invalid journal subject ID")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalSubjectType {
    Installation,
    Host,
    Agent,
    Session,
}

impl JournalSubjectType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Installation => "installation",
            Self::Host => "host",
            Self::Agent => "agent",
            Self::Session => "session",
        }
    }
}

impl std::fmt::Display for JournalSubjectType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for JournalSubjectType {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "installation" => Ok(Self::Installation),
            "host" => Ok(Self::Host),
            "agent" => Ok(Self::Agent),
            "session" => Ok(Self::Session),
            _ => Err("unknown journal subject type"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventActorKind {
    System,
    Client,
    Channel,
    Host,
    Harness,
}

impl EventActorKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Client => "client",
            Self::Channel => "channel",
            Self::Host => "host",
            Self::Harness => "harness",
        }
    }
}

impl std::fmt::Display for EventActorKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for EventActorKind {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "system" => Ok(Self::System),
            "client" => Ok(Self::Client),
            "channel" => Ok(Self::Channel),
            "host" => Ok(Self::Host),
            "harness" => Ok(Self::Harness),
            _ => Err("unknown event actor kind"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventActor {
    pub kind: EventActorKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

impl EventActor {
    pub fn system() -> Self {
        Self {
            kind: EventActorKind::System,
            id: None,
        }
    }

    pub fn from_source(source: &MessageSource) -> Self {
        let kind = match source.kind {
            SourceKind::Client => EventActorKind::Client,
            SourceKind::Channel => EventActorKind::Channel,
            SourceKind::Harness => EventActorKind::Harness,
            SourceKind::Host => EventActorKind::Host,
        };
        Self {
            kind,
            id: Some(source.id.clone()),
        }
    }
}

/// Stable names persisted separately from the payload JSON, allowing storage
/// and subscriptions to filter events without decoding their bodies.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalEventType {
    InstallationCreated,
    RelayChanged,
    HubChanged,
    HostEnrolled,
    HostRevoked,
    HostInvitationCreated,
    HostInvitationConsumed,
    AgentCreated,
    AgentRevisionCreated,
    AgentRenamed,
    AgentArchived,
    ProviderDefaultsChanged,
    SessionCreated,
    SessionModelChanged,
    UserMessageAppended,
    SessionLeafChanged,
    TurnAccepted,
    TurnStarted,
    AssistantResponseRecorded,
    ToolCallRequested,
    ToolExecutionCompleted,
    ToolResultRecorded,
    ReasoningRecorded,
    ContextCompacted,
    HarnessObservationRecorded,
    AcpBindingChanged,
    AcpContextDispatched,
    TurnCompleted,
    TurnFailed,
    TurnCancelled,
    TurnInterrupted,
    EffectScheduled,
    EffectStarted,
    EffectSucceeded,
    EffectFailed,
    EffectUncertain,
}

impl JournalEventType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InstallationCreated => "installation_created",
            Self::RelayChanged => "relay_changed",
            Self::HubChanged => "hub_changed",
            Self::HostEnrolled => "host_enrolled",
            Self::HostRevoked => "host_revoked",
            Self::HostInvitationCreated => "host_invitation_created",
            Self::HostInvitationConsumed => "host_invitation_consumed",
            Self::AgentCreated => "agent_created",
            Self::AgentRevisionCreated => "agent_revision_created",
            Self::AgentRenamed => "agent_renamed",
            Self::AgentArchived => "agent_archived",
            Self::ProviderDefaultsChanged => "provider_defaults_changed",
            Self::SessionCreated => "session_created",
            Self::SessionModelChanged => "session_model_changed",
            Self::UserMessageAppended => "user_message_appended",
            Self::SessionLeafChanged => "session_leaf_changed",
            Self::TurnAccepted => "turn_accepted",
            Self::TurnStarted => "turn_started",
            Self::AssistantResponseRecorded => "assistant_response_recorded",
            Self::ToolCallRequested => "tool_call_requested",
            Self::ToolExecutionCompleted => "tool_execution_completed",
            Self::ToolResultRecorded => "tool_result_recorded",
            Self::ReasoningRecorded => "reasoning_recorded",
            Self::ContextCompacted => "context_compacted",
            Self::HarnessObservationRecorded => "harness_observation_recorded",
            Self::AcpBindingChanged => "acp_binding_changed",
            Self::AcpContextDispatched => "acp_context_dispatched",
            Self::TurnCompleted => "turn_completed",
            Self::TurnFailed => "turn_failed",
            Self::TurnCancelled => "turn_cancelled",
            Self::TurnInterrupted => "turn_interrupted",
            Self::EffectScheduled => "effect_scheduled",
            Self::EffectStarted => "effect_started",
            Self::EffectSucceeded => "effect_succeeded",
            Self::EffectFailed => "effect_failed",
            Self::EffectUncertain => "effect_uncertain",
        }
    }
}

impl std::fmt::Display for JournalEventType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for JournalEventType {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "installation_created" => Ok(Self::InstallationCreated),
            "relay_changed" => Ok(Self::RelayChanged),
            "hub_changed" => Ok(Self::HubChanged),
            "host_enrolled" => Ok(Self::HostEnrolled),
            "host_revoked" => Ok(Self::HostRevoked),
            "host_invitation_created" => Ok(Self::HostInvitationCreated),
            "host_invitation_consumed" => Ok(Self::HostInvitationConsumed),
            "agent_created" => Ok(Self::AgentCreated),
            "agent_revision_created" => Ok(Self::AgentRevisionCreated),
            "agent_renamed" => Ok(Self::AgentRenamed),
            "agent_archived" => Ok(Self::AgentArchived),
            "provider_defaults_changed" => Ok(Self::ProviderDefaultsChanged),
            "session_created" => Ok(Self::SessionCreated),
            "session_model_changed" => Ok(Self::SessionModelChanged),
            "user_message_appended" => Ok(Self::UserMessageAppended),
            "session_leaf_changed" => Ok(Self::SessionLeafChanged),
            "turn_accepted" => Ok(Self::TurnAccepted),
            "turn_started" => Ok(Self::TurnStarted),
            "assistant_response_recorded" => Ok(Self::AssistantResponseRecorded),
            "tool_call_requested" => Ok(Self::ToolCallRequested),
            "tool_execution_completed" => Ok(Self::ToolExecutionCompleted),
            "tool_result_recorded" => Ok(Self::ToolResultRecorded),
            "reasoning_recorded" => Ok(Self::ReasoningRecorded),
            "context_compacted" => Ok(Self::ContextCompacted),
            "harness_observation_recorded" => Ok(Self::HarnessObservationRecorded),
            "acp_binding_changed" => Ok(Self::AcpBindingChanged),
            "acp_context_dispatched" => Ok(Self::AcpContextDispatched),
            "turn_completed" => Ok(Self::TurnCompleted),
            "turn_failed" => Ok(Self::TurnFailed),
            "turn_cancelled" => Ok(Self::TurnCancelled),
            "turn_interrupted" => Ok(Self::TurnInterrupted),
            "effect_scheduled" => Ok(Self::EffectScheduled),
            "effect_started" => Ok(Self::EffectStarted),
            "effect_succeeded" => Ok(Self::EffectSucceeded),
            "effect_failed" => Ok(Self::EffectFailed),
            "effect_uncertain" => Ok(Self::EffectUncertain),
            _ => Err("unknown journal event type"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JournalEvent {
    InstallationCreated {
        installation: Installation,
    },
    RelayChanged {
        relay_url: Option<String>,
        /// Present when the Hub Host's advertised relay changes with the
        /// installation relay; absent for movement-time installation fencing.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        host_id: Option<HostId>,
    },
    HubChanged {
        host_id: HostId,
        generation: u64,
    },
    HostEnrolled {
        host: Host,
    },
    HostRevoked {
        host_id: HostId,
    },
    HostInvitationCreated {
        invitation_id: InvitationId,
        expires_at: DateTime<Utc>,
        /// One-way verifier for the bearer secret. The secret itself is never
        /// part of semantic history.
        secret_hash: String,
    },
    HostInvitationConsumed {
        invitation_id: InvitationId,
        host_id: HostId,
    },
    AgentCreated {
        agent: Agent,
        initial_revision: AgentRevision,
    },
    AgentRevisionCreated {
        revision: AgentRevision,
    },
    AgentRenamed {
        slug: String,
    },
    AgentArchived,
    ProviderDefaultsChanged {
        defaults: ProviderDefaults,
    },
    SessionCreated {
        session: Session,
    },
    SessionModelChanged {
        model: Option<String>,
    },
    UserMessageAppended {
        entry_id: MessageId,
        content: MessageContent,
        metadata: Value,
        source: MessageSource,
    },
    SessionLeafChanged {
        leaf_entry_id: Option<MessageId>,
    },
    TurnAccepted {
        turn_id: TurnId,
        agent_revision: u64,
        entry_id: MessageId,
        content: MessageContent,
        metadata: Value,
        source: MessageSource,
    },
    TurnStarted {
        turn_id: TurnId,
        harness: HarnessProvenance,
    },
    AssistantResponseRecorded {
        turn_id: TurnId,
        entry_id: MessageId,
        response: AssistantResponse,
    },
    ToolCallRequested {
        turn_id: TurnId,
        entry_id: MessageId,
        call: ToolCall,
    },
    ToolExecutionCompleted {
        operation_id: OperationId,
        turn_id: TurnId,
        result: ToolResult,
    },
    ToolResultRecorded {
        turn_id: TurnId,
        entry_id: MessageId,
        result: ToolResult,
    },
    ReasoningRecorded {
        turn_id: TurnId,
        entry_id: MessageId,
        text: String,
        visibility: ReasoningVisibility,
        provenance: ReasoningProvenance,
    },
    ContextCompacted {
        entry_id: MessageId,
        summary: String,
        first_kept_entry_id: MessageId,
        tokens_before: u64,
    },
    HarnessObservationRecorded {
        turn_id: TurnId,
        entry_id: MessageId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        harness_session_id: Option<String>,
        kind: String,
        data: Value,
    },
    AcpBindingChanged {
        binding: AcpSessionBinding,
    },
    AcpContextDispatched {
        host_id: HostId,
        harness: String,
        context_version: u32,
        context_hash: String,
        channel: AcpInstructionChannel,
    },
    TurnCompleted {
        turn_id: TurnId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stop_reason: Option<String>,
    },
    TurnFailed {
        turn_id: TurnId,
        error: String,
    },
    TurnCancelled {
        turn_id: TurnId,
    },
    TurnInterrupted {
        turn_id: TurnId,
        reason: String,
    },
    EffectScheduled {
        operation_id: OperationId,
        turn_id: TurnId,
        effect: EffectRequest,
    },
    EffectStarted {
        operation_id: OperationId,
        turn_id: TurnId,
    },
    EffectSucceeded {
        operation_id: OperationId,
        turn_id: TurnId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<Value>,
    },
    EffectFailed {
        operation_id: OperationId,
        turn_id: TurnId,
        error: String,
    },
    EffectUncertain {
        operation_id: OperationId,
        turn_id: TurnId,
        reason: String,
    },
}

impl JournalEvent {
    pub const fn subject_type(&self) -> JournalSubjectType {
        match self {
            Self::InstallationCreated { .. }
            | Self::RelayChanged { .. }
            | Self::HubChanged { .. }
            | Self::HostInvitationCreated { .. }
            | Self::HostInvitationConsumed { .. }
            | Self::ProviderDefaultsChanged { .. } => JournalSubjectType::Installation,
            Self::HostEnrolled { .. } | Self::HostRevoked { .. } => JournalSubjectType::Host,
            Self::AgentCreated { .. }
            | Self::AgentRevisionCreated { .. }
            | Self::AgentRenamed { .. }
            | Self::AgentArchived => JournalSubjectType::Agent,
            Self::SessionCreated { .. }
            | Self::SessionModelChanged { .. }
            | Self::UserMessageAppended { .. }
            | Self::SessionLeafChanged { .. }
            | Self::TurnAccepted { .. }
            | Self::TurnStarted { .. }
            | Self::AssistantResponseRecorded { .. }
            | Self::ToolCallRequested { .. }
            | Self::ToolExecutionCompleted { .. }
            | Self::ToolResultRecorded { .. }
            | Self::ReasoningRecorded { .. }
            | Self::ContextCompacted { .. }
            | Self::HarnessObservationRecorded { .. }
            | Self::AcpBindingChanged { .. }
            | Self::AcpContextDispatched { .. }
            | Self::TurnCompleted { .. }
            | Self::TurnFailed { .. }
            | Self::TurnCancelled { .. }
            | Self::TurnInterrupted { .. }
            | Self::EffectScheduled { .. }
            | Self::EffectStarted { .. }
            | Self::EffectSucceeded { .. }
            | Self::EffectFailed { .. }
            | Self::EffectUncertain { .. } => JournalSubjectType::Session,
        }
    }

    pub const fn event_type(&self) -> JournalEventType {
        match self {
            Self::InstallationCreated { .. } => JournalEventType::InstallationCreated,
            Self::RelayChanged { .. } => JournalEventType::RelayChanged,
            Self::HubChanged { .. } => JournalEventType::HubChanged,
            Self::HostEnrolled { .. } => JournalEventType::HostEnrolled,
            Self::HostRevoked { .. } => JournalEventType::HostRevoked,
            Self::HostInvitationCreated { .. } => JournalEventType::HostInvitationCreated,
            Self::HostInvitationConsumed { .. } => JournalEventType::HostInvitationConsumed,
            Self::AgentCreated { .. } => JournalEventType::AgentCreated,
            Self::AgentRevisionCreated { .. } => JournalEventType::AgentRevisionCreated,
            Self::AgentRenamed { .. } => JournalEventType::AgentRenamed,
            Self::AgentArchived => JournalEventType::AgentArchived,
            Self::ProviderDefaultsChanged { .. } => JournalEventType::ProviderDefaultsChanged,
            Self::SessionCreated { .. } => JournalEventType::SessionCreated,
            Self::SessionModelChanged { .. } => JournalEventType::SessionModelChanged,
            Self::UserMessageAppended { .. } => JournalEventType::UserMessageAppended,
            Self::SessionLeafChanged { .. } => JournalEventType::SessionLeafChanged,
            Self::TurnAccepted { .. } => JournalEventType::TurnAccepted,
            Self::TurnStarted { .. } => JournalEventType::TurnStarted,
            Self::AssistantResponseRecorded { .. } => JournalEventType::AssistantResponseRecorded,
            Self::ToolCallRequested { .. } => JournalEventType::ToolCallRequested,
            Self::ToolExecutionCompleted { .. } => JournalEventType::ToolExecutionCompleted,
            Self::ToolResultRecorded { .. } => JournalEventType::ToolResultRecorded,
            Self::ReasoningRecorded { .. } => JournalEventType::ReasoningRecorded,
            Self::ContextCompacted { .. } => JournalEventType::ContextCompacted,
            Self::HarnessObservationRecorded { .. } => JournalEventType::HarnessObservationRecorded,
            Self::AcpBindingChanged { .. } => JournalEventType::AcpBindingChanged,
            Self::AcpContextDispatched { .. } => JournalEventType::AcpContextDispatched,
            Self::TurnCompleted { .. } => JournalEventType::TurnCompleted,
            Self::TurnFailed { .. } => JournalEventType::TurnFailed,
            Self::TurnCancelled { .. } => JournalEventType::TurnCancelled,
            Self::TurnInterrupted { .. } => JournalEventType::TurnInterrupted,
            Self::EffectScheduled { .. } => JournalEventType::EffectScheduled,
            Self::EffectStarted { .. } => JournalEventType::EffectStarted,
            Self::EffectSucceeded { .. } => JournalEventType::EffectSucceeded,
            Self::EffectFailed { .. } => JournalEventType::EffectFailed,
            Self::EffectUncertain { .. } => JournalEventType::EffectUncertain,
        }
    }

    /// Validates IDs duplicated inside creation or mutation payloads.
    pub fn validate_subject_id(&self, subject_id: &str) -> Result<(), &'static str> {
        match self {
            Self::InstallationCreated { installation }
                if installation.id.as_str() != subject_id =>
            {
                Err("Installation event identity does not match its subject")
            }
            Self::HostEnrolled { host } if host.id.as_str() != subject_id => {
                Err("Host event identity does not match its subject")
            }
            Self::HostRevoked { host_id } if host_id.as_str() != subject_id => {
                Err("Host event identity does not match its subject")
            }
            Self::AgentCreated {
                agent,
                initial_revision,
            } if agent.id.as_str() != subject_id
                || initial_revision.agent_id.as_str() != subject_id =>
            {
                Err("Agent event identity does not match its subject")
            }
            Self::AgentRevisionCreated { revision } if revision.agent_id.as_str() != subject_id => {
                Err("Agent event identity does not match its subject")
            }
            Self::SessionCreated { session } if session.id.as_str() != subject_id => {
                Err("Session payload identity does not match its subject")
            }
            Self::AcpBindingChanged { binding } if binding.session_id.as_str() != subject_id => {
                Err("Session payload identity does not match its subject")
            }
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EffectRequest {
    ProviderCall { provider: String, model: String },
    ToolCall { call: ToolCall },
    AcpPrompt { host_id: HostId, harness: String },
    LifecycleHook { hook: String },
}

/// A live-only signal. It is never written to the journal and carries no
/// recovery authority.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RuntimeSignal {
    pub id: EventId,
    pub session_id: SessionId,
    pub turn_id: TurnId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_origin: Option<ChannelOrigin>,
    pub emitted_at: DateTime<Utc>,
    pub payload: RuntimeSignalPayload,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuntimeSignalPayload {
    AssistantDelta {
        delta: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
    },
    ReasoningDelta {
        delta: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
    },
    ToolActivity {
        activity: ToolActivity,
    },
}

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
    TurnStarted {
        turn_id: TurnId,
    },
    /// Ephemeral-sized response fragment. The completed assistant message is
    /// still journaled separately as the durable conversation record.
    AssistantDelta {
        turn_id: TurnId,
        delta: String,
        message_id: Option<String>,
    },
    ReasoningDelta {
        turn_id: TurnId,
        delta: String,
        message_id: Option<String>,
    },
    ToolActivity {
        turn_id: TurnId,
        activity: ToolActivity,
    },
    AssistantMessage {
        turn_id: TurnId,
        message: Message,
    },
    ToolStarted {
        turn_id: TurnId,
        message: Message,
    },
    ToolCompleted {
        turn_id: TurnId,
        message: Message,
    },
    TurnCompleted {
        turn_id: TurnId,
    },
    TurnFailed {
        turn_id: TurnId,
        error: String,
    },
    TurnCancelled {
        turn_id: TurnId,
    },
}

impl ClientEventKind {
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::AssistantDelta { .. } | Self::ReasoningDelta { .. } | Self::ToolActivity { .. }
        )
    }

    pub fn turn_id(&self) -> Option<&TurnId> {
        match self {
            Self::TurnStarted { turn_id }
            | Self::AssistantDelta { turn_id, .. }
            | Self::ReasoningDelta { turn_id, .. }
            | Self::ToolActivity { turn_id, .. }
            | Self::AssistantMessage { turn_id, .. }
            | Self::ToolStarted { turn_id, .. }
            | Self::ToolCompleted { turn_id, .. }
            | Self::TurnCompleted { turn_id }
            | Self::TurnFailed { turn_id, .. }
            | Self::TurnCancelled { turn_id } => Some(turn_id),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn journal_entry(
        subject_type: JournalSubjectType,
        subject_id: String,
        payload: JournalEvent,
    ) -> JournalEntry {
        JournalEntry {
            id: EventId::new(),
            position: 1,
            subject: JournalSubject {
                kind: subject_type,
                id: subject_id.clone(),
            },
            event_type: payload.event_type(),
            recorded_at: Utc::now(),
            actor: EventActor::system(),
            command_id: None,
            correlation_id: None,
            payload,
        }
    }

    #[test]
    fn journal_entry_round_trips_with_stable_discriminators() {
        let session_id = SessionId::new();
        let payload = JournalEvent::TurnCancelled {
            turn_id: TurnId::new(),
        };
        let event = JournalEntry {
            id: EventId::new(),
            position: 42,
            subject: JournalSubject {
                kind: JournalSubjectType::Session,
                id: session_id.to_string(),
            },
            event_type: payload.event_type(),
            recorded_at: Utc::now(),
            actor: EventActor::system(),
            command_id: Some("channel:conversation:message-1".into()),
            correlation_id: Some("turn-1".into()),
            payload,
        };

        let encoded = serde_json::to_value(&event).unwrap();
        assert_eq!(encoded["subject"]["kind"], "session");
        assert_eq!(encoded["event_type"], "turn_cancelled");
        assert_eq!(encoded["payload"]["type"], "turn_cancelled");
        assert_eq!(encoded["command_id"], "channel:conversation:message-1");

        let decoded: JournalEntry = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, event);
        assert_eq!(decoded.validate(), Ok(()));
    }

    #[test]
    fn envelope_validation_rejects_discriminator_drift() {
        let payload = JournalEvent::AgentArchived;
        let event = JournalEntry {
            id: EventId::new(),
            position: 1,
            subject: JournalSubject {
                kind: JournalSubjectType::Agent,
                id: AgentId::new().to_string(),
            },
            event_type: JournalEventType::AgentRenamed,
            recorded_at: Utc::now(),
            actor: EventActor::system(),
            command_id: None,
            correlation_id: None,
            payload,
        };

        assert_eq!(event.validate(), Err("event type does not match payload"));
    }

    #[test]
    fn envelope_validation_rejects_payload_aggregate_identity_drift() {
        let host = Host {
            id: HostId::new(),
            name: "host".into(),
            public_key: "public".into(),
            noise_public_key: "noise".into(),
            relay_url: None,
            created_at: Utc::now(),
        };
        assert_eq!(
            journal_entry(
                JournalSubjectType::Host,
                HostId::new().to_string(),
                JournalEvent::HostEnrolled { host },
            )
            .validate(),
            Err("Host event identity does not match its subject")
        );

        let installation = Installation {
            id: InstallationId::new(),
            public_key: "public".into(),
            relay_url: None,
            hub_host_id: HostId::new(),
            generation: 1,
            created_at: Utc::now(),
        };
        assert_eq!(
            journal_entry(
                JournalSubjectType::Installation,
                InstallationId::new().to_string(),
                JournalEvent::InstallationCreated { installation },
            )
            .validate(),
            Err("Installation event identity does not match its subject")
        );

        let revision = AgentRevision {
            agent_id: AgentId::new(),
            revision: 2,
            soul: "Soul".into(),
            config_toml: "harness = \"mews\"".into(),
            content_hash: "hash".into(),
            author_host_id: HostId::new(),
            created_at: Utc::now(),
        };
        assert_eq!(
            journal_entry(
                JournalSubjectType::Agent,
                AgentId::new().to_string(),
                JournalEvent::AgentRevisionCreated { revision },
            )
            .validate(),
            Err("Agent event identity does not match its subject")
        );
    }

    #[test]
    fn runtime_signals_cannot_be_mistaken_for_journal_entries() {
        let signal = RuntimeSignal {
            id: EventId::new(),
            session_id: SessionId::new(),
            turn_id: TurnId::new(),
            channel_origin: None,
            emitted_at: Utc::now(),
            payload: RuntimeSignalPayload::AssistantDelta {
                delta: "hello".into(),
                message_id: None,
            },
        };

        let encoded = serde_json::to_value(&signal).unwrap();
        assert_eq!(encoded["payload"]["type"], "assistant_delta");
        assert!(encoded.get("position").is_none());
        assert_eq!(
            serde_json::from_value::<RuntimeSignal>(encoded).unwrap(),
            signal
        );
    }

    #[test]
    fn persisted_enum_names_parse_without_serde() {
        assert_eq!(
            "session".parse::<JournalSubjectType>(),
            Ok(JournalSubjectType::Session)
        );
        assert_eq!(
            "host_invitation_consumed".parse::<JournalEventType>(),
            Ok(JournalEventType::HostInvitationConsumed)
        );
        assert_eq!(
            "channel".parse::<EventActorKind>(),
            Ok(EventActorKind::Channel)
        );
    }
}
