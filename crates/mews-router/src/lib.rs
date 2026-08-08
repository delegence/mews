//! Provider-independent model protocol, credentials, and bundled adapters.

mod auth;
mod http;
mod providers;
mod registry;
mod service;

pub use auth::AuthStore;
pub use mews_agent::{
    MessageContent, MessageRole, ModelMessage, ModelPart, ModelRequest, ModelResponse, ModelStream,
    ModelStreamEvent, Provider, ProviderError, ProviderResult, ReasoningEffort, ToolDefinition,
};
pub use mews_protocol::{AuthCredential, AuthStatus, ModelInfo};
pub use providers::anthropic::{BrowserAuthorization, login_anthropic};
pub use providers::openai::{DeviceAuthorization, login_openai, login_openai_cancellable};
pub use registry::{ProviderInfo, implemented_providers};
pub use service::{RouterClient, serve, socket_path};
