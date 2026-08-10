use std::{collections::BTreeMap, path::Path};

use anyhow::Result;
use mews_protocol::{HostId, HubRequest, HubResponse, MessageSource, Run, Session, SessionId};
use serde_json::Value;

use crate::{connection::LocalConnection, response};

pub struct MewsClient {
    connection: LocalConnection,
}

impl MewsClient {
    pub async fn connect(root: &Path) -> Result<Self> {
        Ok(Self {
            connection: LocalConnection::connect(root).await?,
        })
    }

    pub async fn request(&mut self, request: HubRequest) -> Result<HubResponse> {
        self.connection.request(request).await
    }

    pub async fn status(&mut self) -> Result<mews_protocol::Installation> {
        response::status(self.request(HubRequest::Status).await?)
    }

    pub async fn agents(&mut self) -> Result<Vec<mews_protocol::Agent>> {
        response::agents(self.request(HubRequest::ListAgents).await?)
    }

    pub async fn create_agent(
        &mut self,
        slug: String,
        harness: Option<String>,
        harness_options: BTreeMap<String, String>,
    ) -> Result<mews_protocol::Agent> {
        response::agent(
            self.request(HubRequest::CreateAgent {
                slug,
                harness,
                harness_options,
            })
            .await?,
        )
    }

    pub async fn rename_agent(
        &mut self,
        slug: String,
        new_slug: String,
    ) -> Result<mews_protocol::Agent> {
        response::agent(
            self.request(HubRequest::RenameAgent { slug, new_slug })
                .await?,
        )
    }

    pub async fn archive_agent(&mut self, slug: String) -> Result<()> {
        self.expect_ack(HubRequest::ArchiveAgent { slug }).await
    }

    pub async fn sessions(&mut self) -> Result<Vec<Session>> {
        response::sessions(self.request(HubRequest::ListSessions).await?)
    }

    pub async fn session(&mut self, id: SessionId) -> Result<Session> {
        response::session(self.request(HubRequest::GetSession { id }).await?)
    }

    pub async fn session_history(&mut self, id: SessionId) -> Result<Vec<mews_protocol::Message>> {
        let mut after = None;
        let mut messages = Vec::new();
        loop {
            let page = response::session_history(
                self.request(HubRequest::GetSessionHistory {
                    id: id.clone(),
                    after,
                    limit: 100,
                })
                .await?,
            )?;
            messages.extend(page.messages);
            let Some(next) = page.next else {
                return Ok(messages);
            };
            after = Some(next);
        }
    }

    pub async fn session_entries(
        &mut self,
        id: SessionId,
    ) -> Result<Vec<mews_protocol::SessionEntry>> {
        let mut after = None;
        let mut entries = Vec::new();
        loop {
            let page = response::session_entries(
                self.request(HubRequest::GetSessionEntries {
                    id: id.clone(),
                    after,
                    limit: 100,
                })
                .await?,
            )?;
            entries.extend(page.entries);
            let Some(next) = page.next else {
                return Ok(entries);
            };
            after = Some(next);
        }
    }

    pub async fn session_model_config(
        &mut self,
        id: SessionId,
    ) -> Result<mews_protocol::SessionModelConfig> {
        response::session_model_config(
            self.request(HubRequest::GetSessionModelConfig { id })
                .await?,
        )
    }

    pub async fn set_session_model(
        &mut self,
        id: SessionId,
        model: Option<String>,
    ) -> Result<Session> {
        response::session(
            self.request(HubRequest::SetSessionModel { id, model })
                .await?,
        )
    }

    pub async fn hosts(&mut self) -> Result<Vec<mews_protocol::HostStatus>> {
        response::hosts(self.request(HubRequest::ListHosts).await?)
    }

    pub async fn harnesses(&mut self) -> Result<Vec<mews_protocol::HostHarnessStatus>> {
        response::harnesses(self.request(HubRequest::ListHarnesses).await?)
    }

    pub async fn refresh_harnesses(&mut self) -> Result<Vec<mews_protocol::HostHarnessStatus>> {
        response::harnesses(self.request(HubRequest::RefreshHarnesses).await?)
    }

    pub async fn provider_defaults(&mut self) -> Result<mews_protocol::ProviderDefaults> {
        response::provider_defaults(self.request(HubRequest::GetProviderDefaults).await?)
    }

    pub async fn models(&mut self) -> Result<Vec<mews_protocol::ModelInfo>> {
        response::models(self.request(HubRequest::ListModels).await?)
    }

    pub async fn refresh_models(&mut self) -> Result<Vec<mews_protocol::ModelInfo>> {
        response::models(self.request(HubRequest::RefreshModels).await?)
    }

    pub async fn set_default_model(&mut self, model: String) -> Result<()> {
        self.expect_ack(HubRequest::SetDefaultModel { model }).await
    }

    pub async fn set_default_reasoning(
        &mut self,
        reasoning: Option<mews_protocol::ReasoningEffort>,
    ) -> Result<()> {
        self.expect_ack(HubRequest::SetDefaultReasoning { reasoning })
            .await
    }

    pub async fn remove_host(&mut self, id: HostId) -> Result<()> {
        self.expect_ack(HubRequest::RemoveHost { id }).await
    }

    pub async fn create_host_invitation(&mut self, relay_url: Option<String>) -> Result<String> {
        response::host_invitation(
            self.request(HubRequest::CreateHostInvitation { relay_url })
                .await?,
        )
    }

    pub async fn move_hub(&mut self, host: String) -> Result<()> {
        self.expect_ack(HubRequest::MoveHub { host }).await
    }

    pub async fn shutdown_daemon(&mut self) -> Result<()> {
        self.expect_ack(HubRequest::Shutdown).await
    }

    pub async fn auth_status(&mut self) -> Result<Vec<mews_protocol::AuthStatus>> {
        response::auth(self.request(HubRequest::ListAuth).await?)
    }

    pub async fn set_api_key(&mut self, provider: String, key: String) -> Result<()> {
        self.set_auth(
            provider,
            mews_protocol::AuthCredential::ApiKey {
                key,
                base_url: None,
            },
        )
        .await
    }

    pub async fn set_auth(
        &mut self,
        provider: String,
        credential: mews_protocol::AuthCredential,
    ) -> Result<()> {
        self.expect_ack(HubRequest::SetAuth {
            provider,
            credential,
        })
        .await
    }

    pub async fn remove_auth(&mut self, provider: String) -> Result<()> {
        self.expect_ack(HubRequest::RemoveAuth { provider }).await
    }

    pub async fn start_session(
        &mut self,
        agent: impl Into<String>,
        cwd: Option<std::path::PathBuf>,
    ) -> Result<Session> {
        response::session(
            self.request(HubRequest::StartSession {
                slug: agent.into(),
                working_directory: cwd,
            })
            .await?,
        )
    }

    pub async fn start_session_on(
        &mut self,
        agent: impl Into<String>,
        host_id: HostId,
        cwd: std::path::PathBuf,
    ) -> Result<Session> {
        response::session(
            self.request(HubRequest::StartSessionOn {
                slug: agent.into(),
                host_id,
                working_directory: cwd,
            })
            .await?,
        )
    }

    pub async fn start_turn(
        &mut self,
        session_id: SessionId,
        prompt: String,
        metadata: Value,
        source: MessageSource,
    ) -> Result<Run> {
        self.start_turn_idempotent(
            uuid::Uuid::now_v7().to_string(),
            session_id,
            prompt,
            metadata,
            source,
        )
        .await
    }

    pub async fn start_turn_idempotent(
        &mut self,
        idempotency_key: String,
        session_id: SessionId,
        prompt: String,
        metadata: Value,
        source: MessageSource,
    ) -> Result<Run> {
        response::run(
            self.request(HubRequest::StartTurn {
                idempotency_key,
                session_id,
                prompt,
                metadata,
                source: Some(source),
            })
            .await?,
        )
    }

    pub async fn get_run(&mut self, id: mews_protocol::RunId) -> Result<Run> {
        response::run(self.request(HubRequest::GetRun { id }).await?)
    }

    pub async fn cancel_run(&mut self, id: mews_protocol::RunId) -> Result<()> {
        self.expect_ack(HubRequest::CancelRun { id }).await
    }

    pub async fn resolve_permission(
        &mut self,
        request_id: String,
        option_id: Option<String>,
    ) -> Result<()> {
        self.expect_ack(HubRequest::ResolvePermission {
            request_id,
            outcome: option_id.map_or(mews_protocol::PermissionOutcome::Cancelled, |option_id| {
                mews_protocol::PermissionOutcome::Selected { option_id }
            }),
        })
        .await
    }

    pub async fn wait_for_run(&mut self, id: mews_protocol::RunId) -> Result<Run> {
        let mut delay = std::time::Duration::from_millis(100);
        loop {
            let run = self.get_run(id.clone()).await?;
            if run.completed_at.is_some() {
                return Ok(run);
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(std::time::Duration::from_secs(1));
        }
    }

    pub(crate) async fn expect_ack(&mut self, request: HubRequest) -> Result<()> {
        response::ack(self.request(request).await?)
    }
}
