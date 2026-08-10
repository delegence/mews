use std::{
    path::Path,
    process::{Command, Stdio},
};

use anyhow::{Context, Result, bail};
use inquire::Select;
use mews_client::MewsClient;
use mews_protocol::ModelInfo;
use mews_router::RouterClient;

use super::{
    command::{ProviderCommand, ProviderModelsCommand},
    prompt,
};

pub async fn run(root: &Path, command: Option<ProviderCommand>) -> Result<()> {
    let mut client = MewsClient::connect(root).await?;
    let command = match command {
        Some(command) => command,
        None => {
            let Some(choice) = prompt(
                Select::new(
                    "How would you like to authenticate?",
                    vec!["Sign in with an account", "Sign in with an API key"],
                )
                .prompt(),
            )?
            else {
                return Ok(());
            };
            match choice {
                "Sign in with an account" => ProviderCommand::Login { provider: None },
                "Sign in with an API key" => ProviderCommand::SetKey { provider: None },
                _ => unreachable!("authentication choice came from a fixed list"),
            }
        }
    };
    match command {
        ProviderCommand::Status => match client.auth_status().await? {
            entries if entries.is_empty() => {
                println!("No providers authenticated.")
            }
            entries => {
                for entry in entries {
                    println!("{}  {}", entry.provider, entry.kind);
                }
            }
        },
        ProviderCommand::Login { provider } => login(root, &mut client, provider).await?,
        ProviderCommand::SetKey { provider } => set_key(&mut client, provider).await?,
        ProviderCommand::Logout { provider } => {
            client.remove_auth(provider.clone()).await?;
            println!("Removed {provider} credential.");
        }
        ProviderCommand::Models {
            command: Some(ProviderModelsCommand::Update),
        } => {
            let models = client.refresh_models().await?;
            println!("Updated model catalog ({} models).", models.len());
        }
        ProviderCommand::Models { command: None } => {
            let models = client.models().await?;
            if models.is_empty() {
                bail!(
                    "no models are available; configure a provider with `mews providers login` or `mews providers set-key <provider>`"
                );
            }
            let Some(choice) = prompt(
                Select::new(
                    "Select the default model:",
                    models.iter().map(model_label).collect(),
                )
                .prompt(),
            )?
            else {
                return Ok(());
            };
            let model = models
                .iter()
                .find(|model| model_label(model) == choice)
                .context("model choice disappeared")?;
            client.set_default_model(model.id.clone()).await?;
            println!("Default model: {}", model.id);
        }
        ProviderCommand::Reasoning => {
            let defaults = client.provider_defaults().await?;
            let models = client.models().await?;
            let default_model = defaults
                .model
                .context("no default model is configured; run `mews providers models`")?;
            let model = models
                .iter()
                .find(|model| model.id == default_model)
                .with_context(|| {
                    format!(
                        "default model {} is absent from the catalog; run `mews providers models`",
                        default_model
                    )
                })?;
            let mut values = vec![("Provider default".to_owned(), None)];
            values.extend(
                model
                    .reasoning
                    .iter()
                    .copied()
                    .filter(|effort| *effort != mews_protocol::ReasoningEffort::Auto)
                    .map(|effort| (format!("{effort:?}"), Some(effort))),
            );
            let Some(choice) = prompt(
                Select::new(
                    "Select the default reasoning effort:",
                    values.iter().map(|(label, _)| label.clone()).collect(),
                )
                .prompt(),
            )?
            else {
                return Ok(());
            };
            let reasoning = values
                .into_iter()
                .find(|(label, _)| label == &choice)
                .map(|(_, value)| value)
                .context("reasoning choice disappeared")?;
            client.set_default_reasoning(reasoning).await?;
            println!("Default reasoning: {choice}");
        }
    }
    Ok(())
}

async fn login(root: &Path, client: &mut MewsClient, id: Option<String>) -> Result<()> {
    let providers = mews_router::implemented_providers()
        .into_iter()
        .filter(|provider| provider.auth.contains("oauth"))
        .collect::<Vec<_>>();
    let provider = match id {
        Some(id) => providers
            .iter()
            .find(|provider| provider.id == id)
            .with_context(|| format!("provider {id} does not support account sign-in"))?,
        None => {
            let Some(choice) = prompt(
                Select::new(
                    "Select a provider:",
                    providers
                        .iter()
                        .map(|provider| provider.id.clone())
                        .collect(),
                )
                .prompt(),
            )?
            else {
                return Ok(());
            };
            providers
                .iter()
                .find(|provider| provider.id == choice)
                .context("provider choice disappeared")?
        }
    };
    let credential = match provider.auth.as_str() {
        "oauth" if provider.id == "openai-codex" => {
            RouterClient::new(root)
                .login_openai(|device| {
                    println!(
                        "Open {} and enter code {}",
                        device.verification_uri, device.user_code
                    )
                })
                .await?
        }
        "oauth_or_api_key" if provider.id == "anthropic" => {
            RouterClient::new(root)
                .login_anthropic(|authorization| {
                    println!("Open {}", authorization.authorization_uri);
                    open_browser(&authorization.authorization_uri);
                })
                .await?
        }
        auth => bail!("{} does not support login ({auth})", provider.id),
    };
    client.set_auth(provider.id.clone(), credential).await?;
    println!("Authenticated {}.", provider.id);
    Ok(())
}

async fn set_key(client: &mut MewsClient, id: Option<String>) -> Result<()> {
    let providers = mews_router::implemented_providers()
        .into_iter()
        .filter(|provider| provider.auth.contains("api_key"))
        .collect::<Vec<_>>();
    let provider = match id {
        Some(id) => providers
            .iter()
            .find(|provider| provider.id == id)
            .with_context(|| format!("provider {id} does not support API keys"))?,
        None => {
            let Some(choice) = prompt(
                Select::new(
                    "Select a provider:",
                    providers
                        .iter()
                        .map(|provider| provider.id.clone())
                        .collect(),
                )
                .prompt(),
            )?
            else {
                return Ok(());
            };
            providers
                .iter()
                .find(|provider| provider.id == choice)
                .context("provider choice disappeared")?
        }
    };
    let key = rpassword::prompt_password(format!("{} API key: ", provider.id))?;
    client.set_api_key(provider.id.clone(), key).await?;
    println!("Saved {} credential in Hub auth.json.", provider.id);
    Ok(())
}

fn open_browser(url: &str) {
    #[cfg(target_os = "macos")]
    let command = "open";
    #[cfg(target_os = "linux")]
    let command = "xdg-open";
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    return;
    let _ = Command::new(command)
        .arg(url)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
}

fn model_label(model: &ModelInfo) -> String {
    model
        .display_name
        .as_ref()
        .map(|name| format!("{name} · {}", model.id))
        .unwrap_or_else(|| model.id.clone())
}
