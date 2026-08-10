use anyhow::Result;
use async_trait::async_trait;
use mews_agent::CancellationToken;
use serde::Deserialize;
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AcpPermissionRequest {
    pub session_id: String,
    pub tool_call: Value,
    pub options: Vec<AcpPermissionOption>,
    #[serde(rename = "_meta", default)]
    pub metadata: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AcpPermissionOption {
    pub option_id: String,
    pub name: String,
    pub kind: AcpPermissionOptionKind,
    #[serde(rename = "_meta", default)]
    pub metadata: Option<Value>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AcpPermissionOptionKind {
    AllowOnce,
    AllowAlways,
    RejectOnce,
    RejectAlways,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AcpPermissionDecision {
    Selected(String),
    Cancelled,
}

#[async_trait]
pub(crate) trait AcpPermissionHandler: Send + Sync {
    async fn request_permission(
        &self,
        request: &AcpPermissionRequest,
        cancellation: &CancellationToken,
    ) -> Result<AcpPermissionDecision>;
}

#[derive(Debug)]
pub(crate) struct AllowPermissions;

#[async_trait]
impl AcpPermissionHandler for AllowPermissions {
    async fn request_permission(
        &self,
        request: &AcpPermissionRequest,
        _: &CancellationToken,
    ) -> Result<AcpPermissionDecision> {
        Ok(request
            .options
            .iter()
            .find(|option| matches!(option.kind, AcpPermissionOptionKind::AllowAlways))
            .or_else(|| {
                request
                    .options
                    .iter()
                    .find(|option| matches!(option.kind, AcpPermissionOptionKind::AllowOnce))
            })
            .map_or(AcpPermissionDecision::Cancelled, |option| {
                AcpPermissionDecision::Selected(option.option_id.clone())
            }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn permissions_prefer_persistent_then_one_time_approval() {
        let handler = AllowPermissions;
        let request = |options| AcpPermissionRequest {
            session_id: "session".into(),
            tool_call: Value::Null,
            options,
            metadata: None,
        };
        let option = |id: &str, kind| AcpPermissionOption {
            option_id: id.into(),
            name: id.into(),
            kind,
            metadata: None,
        };

        let decision = handler
            .request_permission(
                &request(vec![
                    option("once", AcpPermissionOptionKind::AllowOnce),
                    option("always", AcpPermissionOptionKind::AllowAlways),
                ]),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(decision, AcpPermissionDecision::Selected("always".into()));

        let decision = handler
            .request_permission(
                &request(vec![option("once", AcpPermissionOptionKind::AllowOnce)]),
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(decision, AcpPermissionDecision::Selected("once".into()));
    }
}
