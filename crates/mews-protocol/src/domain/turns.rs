use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Turn {
    pub id: TurnId,
    pub session_id: SessionId,
    /// Exact Agent revision selected atomically when this Turn was accepted.
    pub agent_revision: u64,
    /// Exact Host-local Harness definition selected for this Turn. These are
    /// filled before execution begins and remain stable even if the Host
    /// catalog changes while the Turn is active.
    pub harness: Option<String>,
    pub harness_definition_hash: Option<String>,
    pub harness_version: Option<String>,
    pub status: TurnStatus,
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// Serializable tool metadata shared by model requests and Host catalogs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub schema: Value,
    /// Present only in Host catalogs. Model requests contain tools already
    /// filtered to the selected Agent and clear this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
}

/// The wire-level protocol used by a Host Harness. Agent configuration keeps
/// the logical Harness name; Hosts keep the executable details private.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessProtocol {
    Mews,
    Acp,
}

/// Readiness of one independently observable part of a Harness installation.
/// A descriptor is ready for execution only when all applicable parts are
/// ready. `NotApplicable` is used by the native Harness for adapter and auth.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessReadiness {
    NotApplicable,
    Ready,
    Missing,
    Required,
    Stale,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessAvailability {
    pub runtime: HarnessReadiness,
    pub adapter: HarnessReadiness,
    pub authentication: HarnessReadiness,
    pub catalog: HarnessReadiness,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl HarnessAvailability {
    pub fn ready(&self) -> bool {
        [
            self.runtime,
            self.adapter,
            self.authentication,
            self.catalog,
        ]
        .into_iter()
        .all(|state| {
            matches!(
                state,
                HarnessReadiness::Ready | HarnessReadiness::NotApplicable
            )
        })
    }
}

/// Opaque model configuration advertised by an external Harness. Its IDs are
/// intentionally strings: Hub must preserve, not reinterpret, provider values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessModelCapability {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reasoning: Vec<String>,
}

/// A bounded, Host-published description of one logical Harness. It never
/// includes paths, launch arguments, credentials, or other Host authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessDescriptor {
    pub name: String,
    pub protocol: HarnessProtocol,
    pub definition_hash: String,
    pub availability: HarnessAvailability,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable_version: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub native_tools: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub modes: Vec<String>,
    pub supports_mcp: bool,
    pub supports_continuation: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<HarnessModelCapability>,
    /// Bounded, Harness-owned ACP Session Config Options. The Hub preserves
    /// these verbatim rather than assigning provider-specific core fields.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub config_options: Vec<Value>,
    /// Host-local Unix timestamp of the cached ACP probe, if this descriptor
    /// was discovered by starting the adapter rather than static detection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probed_at: Option<i64>,
}
