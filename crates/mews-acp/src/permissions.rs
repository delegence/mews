use anyhow::Result;
use async_trait::async_trait;
use mews_agent::CancellationToken;
use serde::Deserialize;
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AcpPermissionRequest {
    pub session_id: String,
    pub tool_call: Value,
    pub options: Vec<AcpPermissionOption>,
    #[serde(rename = "_meta", default)]
    pub metadata: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AcpPermissionOption {
    pub option_id: String,
    pub name: String,
    pub kind: AcpPermissionOptionKind,
    #[serde(rename = "_meta", default)]
    pub metadata: Option<Value>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AcpPermissionOptionKind {
    AllowOnce,
    AllowAlways,
    RejectOnce,
    RejectAlways,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AcpPermissionDecision {
    Selected(String),
    Cancelled,
}

#[async_trait]
pub trait AcpPermissionHandler: Send + Sync {
    async fn request_permission(
        &self,
        request: &AcpPermissionRequest,
        cancellation: &CancellationToken,
    ) -> Result<AcpPermissionDecision>;
}

#[derive(Debug)]
pub(crate) struct RejectPermissions;

#[async_trait]
impl AcpPermissionHandler for RejectPermissions {
    async fn request_permission(
        &self,
        request: &AcpPermissionRequest,
        _: &CancellationToken,
    ) -> Result<AcpPermissionDecision> {
        Ok(request
            .options
            .iter()
            .find(|option| {
                matches!(
                    option.kind,
                    AcpPermissionOptionKind::RejectOnce | AcpPermissionOptionKind::RejectAlways
                )
            })
            .map_or(AcpPermissionDecision::Cancelled, |option| {
                AcpPermissionDecision::Selected(option.option_id.clone())
            }))
    }
}
