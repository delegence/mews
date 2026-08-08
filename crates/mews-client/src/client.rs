use std::{collections::BTreeMap, path::Path};

use anyhow::{Result, bail};
use mews_protocol::{HostId, HubRequest, HubResponse, MessageSource, Run, Session, SessionId};
use serde_json::Value;

use crate::connection::LocalConnection;

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
        match self.request(HubRequest::Status).await? {
            HubResponse::Status(value) => Ok(value),
            response => bail!("unexpected daemon response: {response:?}"),
        }
    }

    pub async fn agents(&mut self) -> Result<Vec<mews_protocol::Agent>> {
        match self.request(HubRequest::ListAgents).await? {
            HubResponse::Agents(value) => Ok(value),
            response => bail!("unexpected daemon response: {response:?}"),
        }
    }

    pub async fn create_agent(
        &mut self,
        slug: String,
        harness: Option<String>,
        harness_options: BTreeMap<String, String>,
    ) -> Result<mews_protocol::Agent> {
        match self
            .request(HubRequest::CreateAgent {
                slug,
                harness,
                harness_options,
            })
            .await?
        {
            HubResponse::Agent(value) => Ok(value),
            response => bail!("unexpected daemon response: {response:?}"),
        }
    }

    pub async fn rename_agent(
        &mut self,
        slug: String,
        new_slug: String,
    ) -> Result<mews_protocol::Agent> {
        match self
            .request(HubRequest::RenameAgent { slug, new_slug })
            .await?
        {
            HubResponse::Agent(value) => Ok(value),
            response => bail!("unexpected daemon response: {response:?}"),
        }
    }

    pub async fn archive_agent(&mut self, slug: String) -> Result<()> {
        self.expect_ack(HubRequest::ArchiveAgent { slug }).await
    }

    pub async fn sessions(&mut self) -> Result<Vec<Session>> {
        match self.request(HubRequest::ListSessions).await? {
            HubResponse::Sessions(value) => Ok(value),
            response => bail!("unexpected daemon response: {response:?}"),
        }
    }

    pub async fn session(&mut self, id: SessionId) -> Result<Session> {
        match self.request(HubRequest::GetSession { id }).await? {
            HubResponse::Session(value) => Ok(value),
            response => bail!("unexpected daemon response: {response:?}"),
        }
    }

    pub async fn session_model_config(
        &mut self,
        id: SessionId,
    ) -> Result<mews_protocol::SessionModelConfig> {
        match self
            .request(HubRequest::GetSessionModelConfig { id })
            .await?
        {
            HubResponse::SessionModelConfig(value) => Ok(value),
            response => bail!("unexpected daemon response: {response:?}"),
        }
    }

    pub async fn set_session_model(
        &mut self,
        id: SessionId,
        model: Option<String>,
    ) -> Result<Session> {
        match self
            .request(HubRequest::SetSessionModel { id, model })
            .await?
        {
            HubResponse::Session(value) => Ok(value),
            response => bail!("unexpected daemon response: {response:?}"),
        }
    }

    pub async fn hosts(&mut self) -> Result<Vec<mews_protocol::HostStatus>> {
        match self.request(HubRequest::ListHosts).await? {
            HubResponse::Hosts(value) => Ok(value),
            response => bail!("unexpected daemon response: {response:?}"),
        }
    }

    pub async fn provider_defaults(&mut self) -> Result<mews_protocol::ProviderDefaults> {
        match self.request(HubRequest::GetProviderDefaults).await? {
            HubResponse::ProviderDefaults(value) => Ok(value),
            response => bail!("unexpected daemon response: {response:?}"),
        }
    }

    pub async fn remove_host(&mut self, id: HostId) -> Result<()> {
        self.expect_ack(HubRequest::RemoveHost { id }).await
    }

    pub async fn create_host_invitation(&mut self, relay_url: Option<String>) -> Result<String> {
        match self
            .request(HubRequest::CreateHostInvitation { relay_url })
            .await?
        {
            HubResponse::HostInvitation(value) => Ok(value),
            response => bail!("unexpected daemon response: {response:?}"),
        }
    }

    pub async fn move_hub(&mut self, host: String) -> Result<()> {
        self.expect_ack(HubRequest::MoveHub { host }).await
    }

    pub async fn shutdown_daemon(&mut self) -> Result<()> {
        self.expect_ack(HubRequest::Shutdown).await
    }

    pub async fn auth_status(&mut self) -> Result<Vec<mews_protocol::AuthStatus>> {
        match self.request(HubRequest::ListAuth).await? {
            HubResponse::Auth(value) => Ok(value),
            response => bail!("unexpected daemon response: {response:?}"),
        }
    }

    pub async fn set_api_key(&mut self, provider: String, key: String) -> Result<()> {
        self.expect_ack(HubRequest::SetApiKey { provider, key })
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
        match self
            .request(HubRequest::StartSession {
                slug: agent.into(),
                working_directory: cwd,
            })
            .await?
        {
            HubResponse::Session(session) => Ok(session),
            response => bail!("unexpected daemon response: {response:?}"),
        }
    }

    pub async fn start_session_on(
        &mut self,
        agent: impl Into<String>,
        host_id: HostId,
        cwd: std::path::PathBuf,
    ) -> Result<Session> {
        match self
            .request(HubRequest::StartSessionOn {
                slug: agent.into(),
                host_id,
                working_directory: cwd,
            })
            .await?
        {
            HubResponse::Session(session) => Ok(session),
            response => bail!("unexpected daemon response: {response:?}"),
        }
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
        match self
            .request(HubRequest::StartTurn {
                idempotency_key,
                session_id,
                prompt,
                metadata,
                source: Some(source),
            })
            .await?
        {
            HubResponse::Run(run) => Ok(run),
            response => bail!("unexpected daemon response: {response:?}"),
        }
    }

    pub async fn get_run(&mut self, id: mews_protocol::RunId) -> Result<Run> {
        match self.request(HubRequest::GetRun { id }).await? {
            HubResponse::Run(run) => Ok(run),
            response => bail!("unexpected daemon response: {response:?}"),
        }
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
            option_id,
        })
        .await
    }

    pub async fn wait_for_run(&mut self, id: mews_protocol::RunId) -> Result<Run> {
        loop {
            let run = self.get_run(id.clone()).await?;
            if run.completed_at.is_some() {
                return Ok(run);
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    pub(crate) async fn expect_ack(&mut self, request: HubRequest) -> Result<()> {
        match self.request(request).await? {
            HubResponse::Ack => Ok(()),
            response => bail!("unexpected daemon response: {response:?}"),
        }
    }
}
