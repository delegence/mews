use std::{collections::BTreeMap, env, io::IsTerminal, path::Path};

use anyhow::{Context, Result, bail};
use clap::CommandFactory;
use inquire::{Confirm, Select, Text};
use mews_client::MewsClient;
use mews_protocol::{MessageSource, Session, SourceKind};

use super::{command::Cli, prompt};

pub async fn agents(root: &Path, args: Vec<String>) -> Result<()> {
    if args.is_empty() {
        return print_command_help("agents");
    }
    if matches!(args.as_slice(), [command, help] if command == "new" && matches!(help.as_str(), "--help" | "-h"))
    {
        println!(
            "Create an agent using the interactive setup wizard.\n\nUsage:\n  mews agents new [NAME] [--harness <NAME>] [--option <KEY=VALUE>]...\n\nThe wizard selects the Harness, model, and reasoning level from available Hosts.\n\nExamples:\n  mews agents new\n  mews agents new researcher\n  mews agents new coder --harness codex"
        );
        return Ok(());
    }
    let mut client = MewsClient::connect(root).await?;
    match args.as_slice() {
        [command] if command == "list" => {
            for agent in client.agents().await? {
                println!("{}  {}  r{}", agent.id, agent.slug, agent.current_revision);
            }
        }
        [command, create_args @ ..] if command == "new" => {
            let mut creation = parse_create_agent_args(create_args)?;
            let interactive = std::io::stdin().is_terminal() && std::io::stdout().is_terminal();
            if interactive {
                complete_creation_wizard(root, &mut client, &mut creation).await?;
            }
            let slug = creation.slug.context(
                "agent name is required outside an interactive terminal; usage: mews agents new <name>",
            )?;
            let agent = client
                .create_agent(slug, creation.harness.clone(), creation.harness_options)
                .await?;
            println!(
                "Created {} ({}) with {} Harness.",
                agent.slug,
                agent.id,
                creation
                    .harness
                    .as_deref()
                    .unwrap_or(mews_runtime::MEWS_HARNESS)
            );
        }
        [command, slug, new_slug] if command == "rename" => {
            let agent = client.rename_agent(slug.clone(), new_slug.clone()).await?;
            println!("Renamed Agent to {}.", agent.slug);
        }
        [command, slug] if command == "delete" => {
            client.archive_agent(slug.clone()).await?;
            println!("Deleted Agent {slug}.");
        }
        [slug, command, prompt @ ..] if command == "ask" && !prompt.is_empty() => {
            let session = start_session(&mut client, slug).await?;
            let answer = send(&mut client, &session, prompt.join(" ")).await?;
            println!("{answer}\n\nsession: {}", session.id);
        }
        [slug] => {
            let session = start_session(&mut client, slug).await?;
            super::interactive::chat(&mut client, session).await?;
        }
        _ => bail!(
            "usage: mews agents list | mews agents new [name] [--harness <name>] [--option <key=value>]... | mews agents rename <slug> <new-slug> | mews agents delete <slug> | mews agents <slug> [ask <message>]"
        ),
    }
    Ok(())
}

async fn complete_creation_wizard(
    root: &Path,
    client: &mut MewsClient,
    creation: &mut CreateAgentArgs,
) -> Result<()> {
    if creation.slug.is_none() {
        let slug =
            prompt(Text::new("Name this Agent").prompt())?.context("agent creation cancelled")?;
        let slug = slug.trim();
        if slug.is_empty() {
            bail!("Agent name cannot be empty");
        }
        creation.slug = Some(slug.to_owned());
    }
    let mut harnesses = client.harnesses().await?;
    if creation.harness.is_none() {
        let mut names = harnesses
            .iter()
            .map(|entry| entry.descriptor.name.clone())
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        names.sort_by_key(|name| (name != mews_runtime::MEWS_HARNESS, name.clone()));
        let choices = names
            .into_iter()
            .map(|name| {
                let mut ready_hosts = harnesses
                    .iter()
                    .filter(|entry| {
                        entry.descriptor.name == name
                            && entry.descriptor.availability.ready()
                            && (name == mews_runtime::MEWS_HARNESS
                                || entry.descriptor.supports_continuation)
                    })
                    .map(|entry| entry.host.name.clone())
                    .collect::<Vec<_>>();
                ready_hosts.sort();
                ready_hosts.dedup();
                let label = if name == mews_runtime::MEWS_HARNESS {
                    format!("{name} — built in")
                } else if ready_hosts.is_empty() {
                    format!("{name} — setup required")
                } else {
                    format!("{name} — {}", ready_hosts.join(", "))
                };
                (label, name)
            })
            .collect::<Vec<_>>();
        let selected = prompt(
            Select::new(
                "Choose a Harness",
                choices.iter().map(|(label, _)| label.clone()).collect(),
            )
            .prompt(),
        )?
        .context("agent creation cancelled")?;
        creation.harness = choices
            .into_iter()
            .find(|(label, _)| label == &selected)
            .map(|(_, name)| name);
    }
    let harness = creation
        .harness
        .as_deref()
        .expect("wizard always selects a Harness");
    if harness == mews_runtime::MEWS_HARNESS {
        return select_mews_options(client, creation).await;
    }
    let mut descriptors = ready_descriptors(&harnesses, harness);
    if descriptors.is_empty()
        && prompt(
            Confirm::new(&format!(
                "{harness} is not ready on any Host. Set it up on this Host now?"
            ))
            .with_default(true)
            .prompt(),
        )?
        .unwrap_or(false)
    {
        mews_host::HarnessCatalog::setup(root, harness).await?;
        harnesses = client.refresh_harnesses().await?;
        descriptors = ready_descriptors(&harnesses, harness);
    }
    if descriptors.is_empty() {
        bail!(
            "Harness {harness:?} is not ready on any enrolled Host; run `mews harnesses setup {harness}` on the Host that should run it"
        );
    }
    select_external_option(creation, &descriptors, "model", "Choose a model")?;
    select_external_option(
        creation,
        &descriptors,
        "reasoning",
        "Choose reasoning effort",
    )?;
    Ok(())
}

fn ready_descriptors<'a>(
    harnesses: &'a [mews_protocol::HostHarnessStatus],
    harness: &str,
) -> Vec<(&'a mews_protocol::HarnessDescriptor, &'a str)> {
    harnesses
        .iter()
        .filter(|entry| {
            entry.descriptor.name == harness
                && entry.descriptor.availability.ready()
                && entry.descriptor.supports_continuation
        })
        .map(|entry| (&entry.descriptor, entry.host.name.as_str()))
        .collect()
}

async fn select_mews_options(
    client: &mut MewsClient,
    creation: &mut CreateAgentArgs,
) -> Result<()> {
    let defaults = client.provider_defaults().await?;
    let models = client.models().await?;
    if !creation.harness_options.contains_key("model") && !models.is_empty() {
        let mut choices = vec![("Provider default".to_owned(), None)];
        choices.extend(
            models
                .iter()
                .map(|model| (model.id.clone(), Some(model.id.clone()))),
        );
        let labels = choices.iter().map(|(label, _)| label.clone()).collect();
        let selected = prompt(Select::new("Choose a model", labels).prompt())?
            .context("agent creation cancelled")?;
        if let Some((_, Some(value))) = choices.into_iter().find(|(label, _)| label == &selected) {
            creation.harness_options.insert("model".into(), value);
        }
    }
    let selected_model = creation
        .harness_options
        .get("model")
        .or(defaults.model.as_ref());
    let reasoning = selected_model
        .and_then(|id| models.iter().find(|model| &model.id == id))
        .map(|model| model.reasoning.as_slice())
        .unwrap_or_default();
    if !creation.harness_options.contains_key("reasoning") && !reasoning.is_empty() {
        let mut choices = vec![("Provider default".to_owned(), None)];
        choices.extend(reasoning.iter().map(|value| {
            let value = format!("{value:?}").to_lowercase();
            (value.clone(), Some(value))
        }));
        let labels = choices.iter().map(|(label, _)| label.clone()).collect();
        let selected = prompt(Select::new("Choose reasoning effort", labels).prompt())?
            .context("agent creation cancelled")?;
        if let Some((_, Some(value))) = choices.into_iter().find(|(label, _)| label == &selected) {
            creation.harness_options.insert("reasoning".into(), value);
        }
    }
    Ok(())
}

fn select_external_option(
    creation: &mut CreateAgentArgs,
    descriptors: &[(&mews_protocol::HarnessDescriptor, &str)],
    option_hint: &str,
    title: &str,
) -> Result<()> {
    if creation
        .harness_options
        .keys()
        .any(|key| option_id_matches(key, option_hint))
    {
        return Ok(());
    }
    let options = descriptors
        .iter()
        .flat_map(|(descriptor, _)| descriptor.config_options.iter())
        .filter(|option| {
            option
                .get("id")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|id| option_id_matches(id, option_hint))
        })
        .collect::<Vec<_>>();
    let Some(option) = options.first() else {
        return Ok(());
    };
    let Some(config_id) = option.get("id").and_then(serde_json::Value::as_str) else {
        return Ok(());
    };
    let values = external_option_choices(descriptors, config_id);
    let labels = values.iter().map(|choice| choice.label.clone()).collect();
    let selected =
        prompt(Select::new(title, labels).prompt())?.context("agent creation cancelled")?;
    if selected != "Harness default" {
        let value = values
            .into_iter()
            .find(|choice| choice.label == selected)
            .expect("selected option exists")
            .value;
        creation.harness_options.insert(config_id.into(), value);
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
struct OptionChoice {
    label: String,
    value: String,
}

fn external_option_choices(
    descriptors: &[(&mews_protocol::HarnessDescriptor, &str)],
    config_id: &str,
) -> Vec<OptionChoice> {
    let mut choices = BTreeMap::<String, (Option<String>, Vec<String>)>::new();
    for (descriptor, host) in descriptors {
        for option in descriptor.config_options.iter().filter(|option| {
            option.get("id").and_then(serde_json::Value::as_str) == Some(config_id)
        }) {
            collect_option_entries(option.get("options"), &mut |value, name| {
                let entry = choices.entry(value.to_owned()).or_default();
                if entry.0.is_none() {
                    entry.0 = name.map(str::to_owned);
                }
                entry.1.push((*host).to_owned());
            });
        }
    }
    let mut result = vec![OptionChoice {
        label: "Harness default".into(),
        value: String::new(),
    }];
    result.extend(choices.into_iter().map(|(value, (name, mut hosts))| {
        hosts.sort();
        hosts.dedup();
        let display = name
            .filter(|name| name != &value)
            .map_or_else(|| value.clone(), |name| format!("{name} ({value})"));
        OptionChoice {
            label: format!("{display}  [{}]", hosts.join(", ")),
            value,
        }
    }));
    result
}

fn option_id_matches(id: &str, hint: &str) -> bool {
    id == hint
        || id.contains(hint)
        || (hint == "reasoning" && (id.contains("reason") || id.contains("effort")))
}

fn collect_option_entries(
    entries: Option<&serde_json::Value>,
    add: &mut impl FnMut(&str, Option<&str>),
) {
    let Some(entries) = entries.and_then(serde_json::Value::as_array) else {
        return;
    };
    for entry in entries {
        if let Some(nested) = entry.get("options") {
            collect_option_entries(Some(nested), add);
        } else if let Some(value) = entry.get("value").and_then(serde_json::Value::as_str) {
            add(value, entry.get("name").and_then(serde_json::Value::as_str));
        }
    }
}

struct CreateAgentArgs {
    slug: Option<String>,
    harness: Option<String>,
    harness_options: BTreeMap<String, String>,
}

fn parse_create_agent_args(args: &[String]) -> Result<CreateAgentArgs> {
    let slug = args
        .first()
        .filter(|value| !value.starts_with('-'))
        .cloned();

    let mut harness = None;
    let mut harness_options = BTreeMap::new();
    let mut index = usize::from(slug.is_some());
    while index < args.len() {
        let argument = &args[index];
        if argument == "--harness" {
            index += 1;
            let Some(value) = args.get(index) else {
                bail!("--harness requires a Harness name");
            };
            if value.starts_with('-') {
                bail!("--harness requires a Harness name");
            }
            if harness.replace(value.clone()).is_some() {
                bail!("--harness may only be provided once");
            }
        } else if let Some(value) = argument.strip_prefix("--harness=") {
            if value.is_empty() {
                bail!("--harness requires a Harness name");
            }
            if harness.replace(value.into()).is_some() {
                bail!("--harness may only be provided once");
            }
        } else if argument == "--option" {
            index += 1;
            let Some(value) = args.get(index) else {
                bail!("--option requires key=value");
            };
            insert_harness_option(&mut harness_options, value)?;
        } else if let Some(value) = argument.strip_prefix("--option=") {
            insert_harness_option(&mut harness_options, value)?;
        } else {
            bail!("unexpected argument for agent creation: {argument}");
        }
        index += 1;
    }
    Ok(CreateAgentArgs {
        slug,
        harness,
        harness_options,
    })
}

fn insert_harness_option(options: &mut BTreeMap<String, String>, value: &str) -> Result<()> {
    let Some((name, option_value)) = value.split_once('=') else {
        bail!("--option requires key=value");
    };
    if name.is_empty() || option_value.is_empty() {
        bail!("--option requires a non-empty key and value");
    }
    if options.insert(name.into(), option_value.into()).is_some() {
        bail!("Harness option {name:?} was provided more than once");
    }
    Ok(())
}

pub async fn sessions(root: &Path, id: Option<String>, args: Vec<String>) -> Result<()> {
    let Some(id) = id else {
        return print_command_help("sessions");
    };
    let mut client = MewsClient::connect(root).await?;
    if id == "list" {
        if !args.is_empty() {
            bail!("usage: mews sessions list");
        }
        for session in client.sessions().await? {
            println!(
                "{}  {}  {}",
                session.id,
                session.host_id,
                session.working_directory.display()
            );
        }
        return Ok(());
    }
    let session = client
        .session(id.parse().map_err(anyhow::Error::msg)?)
        .await?;
    if let [command, prompt @ ..] = args.as_slice()
        && command == "ask"
        && !prompt.is_empty()
    {
        println!("{}", send(&mut client, &session, prompt.join(" ")).await?);
    } else if args.is_empty() {
        super::interactive::chat(&mut client, session).await?;
    } else {
        bail!("usage: mews sessions list | mews sessions <id> [ask <message>]");
    }
    Ok(())
}

fn print_command_help(name: &str) -> Result<()> {
    let mut command = Cli::command();
    let subcommand = command
        .find_subcommand_mut(name)
        .expect("CLI subcommand exists");
    subcommand.set_bin_name(format!("mews {name}"));
    subcommand.print_help()?;
    println!();
    Ok(())
}

async fn start_session(client: &mut MewsClient, slug: &str) -> Result<Session> {
    client
        .start_session(slug, Some(env::current_dir()?.canonicalize()?))
        .await
}

async fn send(client: &mut MewsClient, session: &Session, prompt: String) -> Result<String> {
    client
        .send_message(
            session.id.clone(),
            prompt,
            serde_json::Value::Null,
            MessageSource {
                kind: SourceKind::Client,
                id: "cli".into(),
            },
        )
        .await
}

#[cfg(test)]
mod tests {
    use mews_protocol::{
        HarnessAvailability, HarnessDescriptor, HarnessProtocol, HarnessReadiness,
    };

    use super::{external_option_choices, parse_create_agent_args};

    #[test]
    fn parses_a_harness_and_opaque_options() {
        let args = [
            "coder".into(),
            "--harness=codex".into(),
            "--option".into(),
            "model=gpt-5.6-codex".into(),
            "--option=approval=never".into(),
        ];

        let parsed = parse_create_agent_args(&args).unwrap();

        assert_eq!(parsed.slug.as_deref(), Some("coder"));
        assert_eq!(parsed.harness.as_deref(), Some("codex"));
        assert_eq!(
            parsed.harness_options.get("model").map(String::as_str),
            Some("gpt-5.6-codex")
        );
        assert_eq!(
            parsed.harness_options.get("approval").map(String::as_str),
            Some("never")
        );
    }

    #[test]
    fn defaults_the_harness_when_it_is_not_supplied() {
        let parsed = parse_create_agent_args(&["coder".into()]).unwrap();

        assert_eq!(parsed.harness, None);
        assert!(parsed.harness_options.is_empty());
    }

    #[test]
    fn permits_an_omitted_name_for_the_interactive_wizard() {
        let parsed = parse_create_agent_args(&["--harness=codex".into()]).unwrap();

        assert_eq!(parsed.slug, None);
        assert_eq!(parsed.harness.as_deref(), Some("codex"));
    }

    #[test]
    fn flattens_grouped_acp_options_and_annotates_hosts() {
        let mut descriptor = HarnessDescriptor {
            name: "codex".into(),
            protocol: HarnessProtocol::Acp,
            definition_hash: "test".into(),
            availability: HarnessAvailability {
                runtime: HarnessReadiness::Ready,
                adapter: HarnessReadiness::Ready,
                authentication: HarnessReadiness::Ready,
                catalog: HarnessReadiness::Ready,
                detail: None,
            },
            executable_version: None,
            native_tools: Vec::new(),
            modes: Vec::new(),
            supports_mcp: true,
            supports_continuation: false,
            models: Vec::new(),
            config_options: Vec::new(),
            probed_at: None,
        };
        descriptor.config_options = vec![serde_json::json!({
            "id": "model",
            "options": [{
                "group": "OpenAI",
                "options": [{"name": "GPT Test", "value": "gpt-test"}]
            }]
        })];

        let choices = external_option_choices(&[(&descriptor, "laptop")], "model");

        assert_eq!(choices[0].label, "Harness default");
        assert_eq!(choices[1].label, "GPT Test (gpt-test)  [laptop]");
        assert_eq!(choices[1].value, "gpt-test");
    }

    #[test]
    fn rejects_malformed_or_duplicate_options() {
        assert!(parse_create_agent_args(&["coder".into(), "--option=model".into()]).is_err());
        assert!(
            parse_create_agent_args(&[
                "coder".into(),
                "--option=model=first".into(),
                "--option=model=second".into(),
            ])
            .is_err()
        );
    }
}
