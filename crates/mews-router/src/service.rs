#[cfg(not(unix))]
compile_error!("mews-router requires Unix sockets");

use std::{
    collections::BTreeMap,
    fs,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
};

use crate::{
    AuthCredential, AuthStatus, BrowserAuthorization, DeviceAuthorization, ModelInfo, ModelRequest,
    ModelResponse, ModelStream, ModelStreamEvent, Provider, ProviderError, ProviderInfo,
    ProviderResult, registry::ProviderRegistry,
};

const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
const ROUTER_PROTOCOL_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
struct RouterFrame<T> {
    version: u32,
    body: T,
}

#[derive(Serialize, Deserialize)]
enum RouterRequest {
    Generate(ModelRequest),
    GenerateStream(ModelRequest),
    Providers,
    Models,
    RefreshModels {
        provider: Option<String>,
    },
    SetAuth {
        provider: String,
        credential: AuthCredential,
    },
    RemoveAuth {
        provider: String,
    },
    ListAuth,
    LoginOpenAi,
    LoginAnthropic,
    Shutdown,
}

#[derive(Serialize, Deserialize)]
enum RouterResponse {
    Generated(ProviderResult<ModelResponse>),
    StreamEvent(ProviderResult<ModelStreamEvent>),
    StreamEnd,
    Providers(Vec<ProviderInfo>),
    Models(ProviderResult<Vec<ModelInfo>>),
    Auth(ProviderResult<Vec<AuthStatus>>),
    Ack(ProviderResult<()>),
    DeviceAuthorization(DeviceAuthorization),
    BrowserAuthorization(BrowserAuthorization),
    Login(ProviderResult<AuthCredential>),
}

#[derive(Clone)]
pub struct RouterClient {
    socket: PathBuf,
}

impl RouterClient {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            socket: socket_path(root.as_ref()),
        }
    }

    pub async fn ready(&self) -> bool {
        UnixStream::connect(&self.socket).await.is_ok()
    }

    pub async fn providers(&self) -> ProviderResult<Vec<ProviderInfo>> {
        match self.call(RouterRequest::Providers).await? {
            RouterResponse::Providers(providers) => Ok(providers),
            _ => Err(ProviderError::InvalidResponse(
                "unexpected router response".into(),
            )),
        }
    }

    pub async fn models(&self) -> ProviderResult<Vec<ModelInfo>> {
        match self.call(RouterRequest::Models).await? {
            RouterResponse::Models(models) => models,
            _ => Err(ProviderError::InvalidResponse(
                "unexpected router response".into(),
            )),
        }
    }

    pub async fn refresh_models(&self, provider: Option<String>) -> ProviderResult<Vec<ModelInfo>> {
        match self.call(RouterRequest::RefreshModels { provider }).await? {
            RouterResponse::Models(models) => models,
            _ => Err(unexpected()),
        }
    }

    pub async fn set_api_key(&self, provider: String, key: String) -> ProviderResult<()> {
        self.set_auth(
            provider,
            AuthCredential::ApiKey {
                key,
                base_url: None,
            },
        )
        .await
    }

    pub async fn set_auth(
        &self,
        provider: String,
        credential: AuthCredential,
    ) -> ProviderResult<()> {
        match self
            .call(RouterRequest::SetAuth {
                provider,
                credential,
            })
            .await?
        {
            RouterResponse::Ack(result) => result,
            _ => Err(unexpected()),
        }
    }

    pub async fn remove_auth(&self, provider: String) -> ProviderResult<()> {
        match self.call(RouterRequest::RemoveAuth { provider }).await? {
            RouterResponse::Ack(result) => result,
            _ => Err(unexpected()),
        }
    }

    pub async fn auth_statuses(&self) -> ProviderResult<Vec<AuthStatus>> {
        match self.call(RouterRequest::ListAuth).await? {
            RouterResponse::Auth(result) => result,
            _ => Err(unexpected()),
        }
    }

    pub async fn login_openai(
        &self,
        notify: impl FnOnce(DeviceAuthorization),
    ) -> ProviderResult<AuthCredential> {
        let mut stream = self.connect().await?;
        write_frame(&mut stream, &RouterRequest::LoginOpenAi).await?;
        match read_frame(&mut stream).await? {
            RouterResponse::DeviceAuthorization(device) => notify(device),
            _ => return Err(unexpected()),
        }
        match read_frame(&mut stream).await? {
            RouterResponse::Login(result) => result,
            _ => Err(unexpected()),
        }
    }

    pub async fn login_anthropic(
        &self,
        notify: impl FnOnce(BrowserAuthorization),
    ) -> ProviderResult<AuthCredential> {
        let mut stream = self.connect().await?;
        write_frame(&mut stream, &RouterRequest::LoginAnthropic).await?;
        match read_frame(&mut stream).await? {
            RouterResponse::BrowserAuthorization(authorization) => notify(authorization),
            _ => return Err(unexpected()),
        }
        match read_frame(&mut stream).await? {
            RouterResponse::Login(result) => result,
            _ => Err(unexpected()),
        }
    }

    pub async fn shutdown(&self) -> ProviderResult<()> {
        match self.call(RouterRequest::Shutdown).await? {
            RouterResponse::Ack(result) => result,
            _ => Err(unexpected()),
        }
    }

    async fn call(&self, request: RouterRequest) -> ProviderResult<RouterResponse> {
        let mut stream = self.connect().await?;
        write_frame(&mut stream, &request).await?;
        read_frame(&mut stream).await
    }

    async fn connect(&self) -> ProviderResult<UnixStream> {
        UnixStream::connect(&self.socket)
            .await
            .map_err(|error| ProviderError::Http(format!("connect to mews-router: {error}")))
    }
}

#[async_trait]
impl Provider for RouterClient {
    fn continuation_capability(&self, model: &str) -> mews_agent::ContinuationCapability {
        match model.split_once('/').map(|(provider, _)| provider) {
            Some(provider @ ("openai" | "openai-codex")) => {
                mews_agent::ContinuationCapability::ResponseId {
                    provider: provider.into(),
                    api: "responses".into(),
                }
            }
            _ => mews_agent::ContinuationCapability::None,
        }
    }

    async fn generate(&self, request: ModelRequest) -> ProviderResult<ModelResponse> {
        match self.call(RouterRequest::Generate(request)).await? {
            RouterResponse::Generated(response) => response,
            _ => Err(ProviderError::InvalidResponse(
                "unexpected router response".into(),
            )),
        }
    }

    async fn stream(&self, request: ModelRequest) -> ProviderResult<ModelStream> {
        let mut socket = self.connect().await?;
        write_frame(&mut socket, &RouterRequest::GenerateStream(request)).await?;
        Ok(Box::pin(stream::unfold(Some(socket), |state| async move {
            let mut socket = state?;
            match read_frame::<RouterResponse>(&mut socket).await {
                Ok(RouterResponse::StreamEvent(event)) => Some((event, Some(socket))),
                Ok(RouterResponse::StreamEnd) => None,
                Ok(_) => Some((Err(unexpected()), None)),
                Err(error) => Some((Err(error), None)),
            }
        })))
    }
}

pub fn socket_path(root: &Path) -> PathBuf {
    root.join("router.sock")
}

pub async fn serve(root: PathBuf) -> anyhow::Result<()> {
    crate::AuthStore::initialize(&root)?;
    let socket = socket_path(&root);
    if socket.exists() {
        if UnixStream::connect(&socket).await.is_ok() {
            anyhow::bail!("mews-router is already running");
        }
        fs::remove_file(&socket)?;
    }
    let listener = UnixListener::bind(&socket)?;
    fs::set_permissions(&socket, fs::Permissions::from_mode(0o600))?;
    let _cleanup = SocketCleanup(socket);
    let registry = Arc::new(ProviderRegistry::new(root));
    if !registry.root.join("models.json").exists() {
        let _ = refresh_models(&registry, None).await;
    }
    let shutdown = Arc::new(tokio::sync::Notify::new());
    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => Some(accepted?),
            () = shutdown.notified() => None,
        };
        let Some((stream, _)) = accepted else {
            return Ok(());
        };
        let registry = Arc::clone(&registry);
        let shutdown = Arc::clone(&shutdown);
        tokio::spawn(async move {
            let _ = handle(stream, registry, shutdown).await;
        });
    }
}

async fn handle(
    mut stream: UnixStream,
    registry: Arc<ProviderRegistry>,
    shutdown: Arc<tokio::sync::Notify>,
) -> ProviderResult<()> {
    let request: RouterRequest = read_frame(&mut stream).await?;
    if matches!(request, RouterRequest::LoginOpenAi) {
        return handle_openai_login(stream).await;
    }
    if matches!(request, RouterRequest::LoginAnthropic) {
        return handle_anthropic_login(stream).await;
    }
    if let RouterRequest::GenerateStream(request) = request {
        let mut events = match registry.stream(request).await {
            Ok(events) => events,
            Err(error) => {
                write_frame(&mut stream, &RouterResponse::StreamEvent(Err(error))).await?;
                write_frame(&mut stream, &RouterResponse::StreamEnd).await?;
                return Ok(());
            }
        };
        while let Some(event) = events.next().await {
            write_frame(&mut stream, &RouterResponse::StreamEvent(event)).await?;
        }
        write_frame(&mut stream, &RouterResponse::StreamEnd).await?;
        return Ok(());
    }
    let response = match request {
        RouterRequest::Generate(request) => {
            RouterResponse::Generated(registry.generate(request).await)
        }
        RouterRequest::GenerateStream(_) => unreachable!(),
        RouterRequest::Providers => RouterResponse::Providers(crate::implemented_providers()),
        RouterRequest::Models => RouterResponse::Models(load_models_locked(&registry).await),
        RouterRequest::RefreshModels { provider } => {
            RouterResponse::Models(refresh_models(&registry, provider.as_deref()).await)
        }
        RouterRequest::SetAuth {
            provider,
            credential,
        } => {
            let saved =
                crate::AuthStore::set(&registry.root, &provider, &credential).map_err(auth_error);
            if saved.is_ok() {
                let _ = refresh_models(&registry, Some(&provider)).await;
            }
            RouterResponse::Ack(saved)
        }
        RouterRequest::RemoveAuth { provider } => {
            let removed = crate::AuthStore::remove(&registry.root, &provider).map_err(auth_error);
            if removed.is_ok() {
                let _ = remove_cached_models(&registry, &provider).await;
            }
            RouterResponse::Ack(removed)
        }
        RouterRequest::ListAuth => RouterResponse::Auth(
            crate::AuthStore::load(&registry.root)
                .map(|store| store.statuses())
                .map_err(auth_error),
        ),
        RouterRequest::LoginOpenAi => unreachable!(),
        RouterRequest::LoginAnthropic => unreachable!(),
        RouterRequest::Shutdown => {
            write_frame(&mut stream, &RouterResponse::Ack(Ok(()))).await?;
            shutdown.notify_one();
            return Ok(());
        }
    };
    write_frame(&mut stream, &response).await
}

async fn handle_openai_login(mut stream: UnixStream) -> ProviderResult<()> {
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut login = tokio::spawn(async move {
        crate::login_openai(|device| {
            let _ = sender.send(device);
        })
        .await
        .map_err(|error| ProviderError::Authentication(error.to_string()))
    });
    let device = receiver
        .recv()
        .await
        .ok_or_else(|| ProviderError::InvalidResponse("device login did not start".into()))?;
    write_frame(&mut stream, &RouterResponse::DeviceAuthorization(device)).await?;
    let result = tokio::select! {
        result = &mut login => result.map_err(|error| ProviderError::Http(error.to_string()))?,
        _ = stream.read_u8() => {
            login.abort();
            return Err(ProviderError::Cancelled);
        }
    };
    write_frame(&mut stream, &RouterResponse::Login(result)).await
}

async fn handle_anthropic_login(mut stream: UnixStream) -> ProviderResult<()> {
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut login = tokio::spawn(async move {
        crate::login_anthropic(|authorization| {
            let _ = sender.send(authorization);
        })
        .await
        .map_err(|error| ProviderError::Authentication(error.to_string()))
    });
    let authorization = receiver.recv().await.ok_or_else(|| {
        ProviderError::InvalidResponse("Anthropic OAuth login did not start".into())
    })?;
    write_frame(
        &mut stream,
        &RouterResponse::BrowserAuthorization(authorization),
    )
    .await?;
    let result = tokio::select! {
        result = &mut login => result.map_err(|error| ProviderError::Http(error.to_string()))?,
        _ = stream.read_u8() => {
            login.abort();
            return Err(ProviderError::Cancelled);
        }
    };
    write_frame(&mut stream, &RouterResponse::Login(result)).await
}

pub(crate) fn load_models(root: &Path) -> ProviderResult<Vec<ModelInfo>> {
    let path = root.join("models.json");
    let catalog: ModelCatalog = if path.exists() {
        serde_json::from_slice(
            &fs::read(path).map_err(|error| ProviderError::InvalidRequest(error.to_string()))?,
        )
        .map_err(|error| ProviderError::InvalidRequest(format!("invalid models.json: {error}")))?
    } else {
        ModelCatalog::default()
    };
    let mut models = catalog
        .providers
        .into_values()
        .flat_map(|entry| entry.models)
        .collect::<Vec<_>>();
    if crate::registry::test_provider_enabled(root)
        && !models.iter().any(|model: &ModelInfo| model.id == "test")
    {
        models.insert(0, crate::registry::test_model());
    }
    Ok(models)
}

#[derive(Default, Serialize, Deserialize)]
struct ModelCatalog {
    #[serde(default)]
    providers: BTreeMap<String, CachedModels>,
}

#[derive(Serialize, Deserialize)]
struct CachedModels {
    models: Vec<ModelInfo>,
}

async fn refresh_models(
    registry: &ProviderRegistry,
    provider: Option<&str>,
) -> ProviderResult<Vec<ModelInfo>> {
    let providers = if let Some(provider) = provider {
        vec![provider.to_owned()]
    } else {
        crate::AuthStore::load(&registry.root)
            .map_err(auth_error)?
            .statuses()
            .into_iter()
            .map(|status| status.provider)
            .collect()
    };
    let mut failures = Vec::new();
    let mut discovered = Vec::new();
    for provider in providers {
        match registry.discover_models(&provider).await {
            Ok(models) => discovered.push((provider, models)),
            Err(error) => failures.push(error.to_string()),
        }
    }
    update_catalog(registry, |catalog| {
        for (provider, models) in discovered {
            catalog.providers.insert(provider, CachedModels { models });
        }
    })
    .await?;
    if !failures.is_empty() {
        return Err(ProviderError::Http(failures.join("; ")));
    }
    load_models_locked(registry).await
}

async fn load_models_locked(registry: &ProviderRegistry) -> ProviderResult<Vec<ModelInfo>> {
    let _guard = registry.catalog_lock.lock().await;
    load_models(&registry.root)
}

async fn update_catalog(
    registry: &ProviderRegistry,
    update: impl FnOnce(&mut ModelCatalog),
) -> ProviderResult<()> {
    let _guard = registry.catalog_lock.lock().await;
    let path = registry.root.join("models.json");
    let mut catalog = if path.exists() {
        serde_json::from_slice(&fs::read(path).map_err(io_error)?)
            .map_err(|error| ProviderError::InvalidResponse(error.to_string()))?
    } else {
        ModelCatalog::default()
    };
    update(&mut catalog);
    save_catalog(&registry.root, &catalog)
}

fn save_catalog(root: &Path, catalog: &ModelCatalog) -> ProviderResult<()> {
    let path = root.join("models.json");
    let temporary = root.join(format!(".models-{}.tmp", uuid::Uuid::now_v7()));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true).mode(0o600);
    use std::io::Write;
    let mut file = options.open(&temporary).map_err(io_error)?;
    file.write_all(
        &serde_json::to_vec_pretty(catalog)
            .map_err(|e| ProviderError::InvalidResponse(e.to_string()))?,
    )
    .map_err(io_error)?;
    file.sync_all().map_err(io_error)?;
    fs::rename(temporary, path).map_err(io_error)?;
    Ok(())
}

async fn remove_cached_models(registry: &ProviderRegistry, provider: &str) -> ProviderResult<()> {
    update_catalog(registry, |catalog| {
        catalog.providers.remove(provider);
    })
    .await
}

async fn write_frame<T: Serialize>(stream: &mut UnixStream, value: &T) -> ProviderResult<()> {
    let bytes = serde_json::to_vec(&RouterFrame {
        version: ROUTER_PROTOCOL_VERSION,
        body: value,
    })
    .map_err(|error| ProviderError::InvalidRequest(error.to_string()))?;
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(ProviderError::InvalidRequest(
            "router frame is too large".into(),
        ));
    }
    stream
        .write_u32(bytes.len() as u32)
        .await
        .map_err(io_error)?;
    stream.write_all(&bytes).await.map_err(io_error)
}

async fn read_frame<T: for<'de> Deserialize<'de>>(stream: &mut UnixStream) -> ProviderResult<T> {
    let len = stream.read_u32().await.map_err(io_error)? as usize;
    if len > MAX_FRAME_BYTES {
        return Err(ProviderError::InvalidRequest(
            "router frame is too large".into(),
        ));
    }
    let mut bytes = vec![0; len];
    stream.read_exact(&mut bytes).await.map_err(io_error)?;
    let frame: RouterFrame<T> = serde_json::from_slice(&bytes)
        .map_err(|error| ProviderError::InvalidResponse(error.to_string()))?;
    validate_version(frame.version)?;
    Ok(frame.body)
}

fn validate_version(version: u32) -> ProviderResult<()> {
    if version == ROUTER_PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ProviderError::InvalidResponse(format!(
            "unsupported router protocol version {version}"
        )))
    }
}

fn unexpected() -> ProviderError {
    ProviderError::InvalidResponse("unexpected router response".into())
}

fn auth_error(error: anyhow::Error) -> ProviderError {
    ProviderError::InvalidRequest(error.to_string())
}

fn io_error(error: std::io::Error) -> ProviderError {
    ProviderError::Http(error.to_string())
}

struct SocketCleanup(PathBuf);

impl Drop for SocketCleanup {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn client_calls_router_over_unix_socket() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join(".test-provider"), []).unwrap();
        crate::AuthStore::initialize(root.path()).unwrap();
        let task = tokio::spawn(serve(root.path().to_path_buf()));
        let client = RouterClient::new(root.path());
        for _ in 0..50 {
            if client.ready().await {
                break;
            }
            tokio::task::yield_now().await;
        }
        let response = client
            .generate(ModelRequest {
                model: "test".into(),
                reasoning: None,
                system: String::new(),
                messages: vec![],
                tools: vec![],
                continuation: None,
            })
            .await
            .unwrap();
        assert_eq!(
            response.parts,
            vec![crate::ModelPart::Text {
                text: " [test]".into()
            }]
        );
        let streamed = client
            .stream(ModelRequest {
                model: "test".into(),
                reasoning: None,
                system: String::new(),
                messages: vec![],
                tools: vec![],
                continuation: None,
            })
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<ProviderResult<Vec<_>>>()
            .unwrap();
        assert_eq!(
            streamed,
            vec![
                ModelStreamEvent::Start,
                ModelStreamEvent::ResponseMetadata {
                    provider: "test".into(),
                    model: "test".into(),
                    api: "test".into(),
                    response_id: None,
                },
                ModelStreamEvent::TextDelta(" [test]".into()),
                ModelStreamEvent::ResponseCompleted {
                    usage: None,
                    stop_reason: Some("stop".into())
                },
                ModelStreamEvent::Done,
            ]
        );
        client
            .set_api_key("openai".into(), "secret".into())
            .await
            .unwrap();
        assert!(
            client
                .auth_statuses()
                .await
                .unwrap()
                .iter()
                .any(|status| status.provider == "openai")
        );
        task.abort();
    }

    #[test]
    fn rejects_an_incompatible_protocol_version() {
        assert!(validate_version(ROUTER_PROTOCOL_VERSION + 1).is_err());
    }

    #[tokio::test]
    async fn cached_catalog_is_provider_scoped_and_survives_removal() {
        let root = tempfile::tempdir().unwrap();
        let catalog = ModelCatalog {
            providers: BTreeMap::from([
                (
                    "openai".into(),
                    CachedModels {
                        models: vec![ModelInfo {
                            id: "openai/test".into(),
                            display_name: None,
                            reasoning: vec![],
                            default_reasoning: None,
                        }],
                    },
                ),
                (
                    "anthropic".into(),
                    CachedModels {
                        models: vec![ModelInfo {
                            id: "anthropic/test".into(),
                            display_name: None,
                            reasoning: vec![],
                            default_reasoning: None,
                        }],
                    },
                ),
            ]),
        };
        save_catalog(root.path(), &catalog).unwrap();
        let serialized = std::fs::read_to_string(root.path().join("models.json")).unwrap();
        assert!(!serialized.contains("fetched_at"));
        let registry = ProviderRegistry::new(root.path().to_owned());
        remove_cached_models(&registry, "openai").await.unwrap();
        let ids = load_models(root.path())
            .unwrap()
            .into_iter()
            .map(|model| model.id)
            .collect::<Vec<_>>();
        assert_eq!(ids, ["anthropic/test"]);
    }

    #[tokio::test]
    async fn concurrent_catalog_updates_preserve_each_provider() {
        let root = tempfile::tempdir().unwrap();
        let registry = Arc::new(ProviderRegistry::new(root.path().to_owned()));
        let update = |provider: &'static str, registry: Arc<ProviderRegistry>| async move {
            update_catalog(&registry, |catalog| {
                catalog.providers.insert(
                    provider.into(),
                    CachedModels {
                        models: vec![ModelInfo {
                            id: format!("{provider}/test"),
                            display_name: None,
                            reasoning: vec![],
                            default_reasoning: None,
                        }],
                    },
                );
            })
            .await
            .unwrap();
        };
        tokio::join!(
            update("openai", Arc::clone(&registry)),
            update("anthropic", Arc::clone(&registry))
        );

        let mut ids = load_models(root.path())
            .unwrap()
            .into_iter()
            .map(|model| model.id)
            .collect::<Vec<_>>();
        ids.sort();
        assert_eq!(ids, ["anthropic/test", "openai/test"]);
    }
}
