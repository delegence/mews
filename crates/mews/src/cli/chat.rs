use std::{collections::BTreeMap, env, fmt::Write, io::IsTerminal, path::Path};

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
    if matches!(args.as_slice(), [command, help] if command == "inspect" && matches!(help.as_str(), "--help" | "-h"))
    {
        println!(
            "Inspect canonical Agent configuration and live Host resolution.\n\nUsage:\n  mews agents inspect <SLUG>"
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
        [command, slug] if command == "inspect" => {
            let inspection = client.inspect_agent(slug.clone(), None, None).await?;
            let mut output = format_agent_inspection(&inspection);
            let mut hosts = client.hosts().await?;
            hosts.sort_by(|left, right| left.host.name.cmp(&right.host.name));
            for status in hosts {
                let mut after_tool = None;
                let mut resolved: Option<mews_protocol::AgentHostInspection> = None;
                loop {
                    let page = client
                        .inspect_agent(slug.clone(), Some(status.host.id.clone()), after_tool)
                        .await?;
                    ensure_same_agent_snapshot(&inspection, &page)?;
                    let Some(mut host) = page.host else {
                        break;
                    };
                    let next = host.tools.next;
                    if let Some(resolved) = &mut resolved {
                        resolved.tools.tools.append(&mut host.tools.tools);
                        resolved.tools.next = next;
                    } else {
                        resolved = Some(host);
                    }
                    let Some(next) = next else {
                        break;
                    };
                    after_tool = Some(next);
                }
                if let Some(host) = &resolved {
                    format_agent_host(&mut output, host);
                }
            }
            print!("{output}");
        }
        [command, ..] if command == "inspect" => {
            bail!("usage: mews agents inspect <slug>");
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
        [slug, prompt_args @ ..] if !prompt_args.is_empty() => {
            let prompt = parse_prompt_args(prompt_args)?;
            let session = start_session(&mut client, slug).await?;
            if prompt.detach {
                let turn = start_detached(&mut client, &session, prompt.message).await?;
                println!("session: {}\nturn: {}", session.id, turn.id);
            } else {
                let answer = send(&mut client, &session, prompt.message).await?;
                println!("{answer}\n\nsession: {}", session.id);
            }
        }
        [slug] => {
            let session = start_session(&mut client, slug).await?;
            mews_tui::chat(&mut client, session).await?;
        }
        _ => bail!(
            "usage: mews agents list | mews agents inspect <slug> | mews agents new [name] [--harness <name>] [--option <key=value>]... | mews agents rename <slug> <new-slug> | mews agents delete <slug> | mews agents <slug> [-p <message> [--detach]]"
        ),
    }
    Ok(())
}

fn ensure_same_agent_snapshot(
    expected: &mews_protocol::AgentInspection,
    actual: &mews_protocol::AgentInspection,
) -> Result<()> {
    if expected.agent != actual.agent
        || expected.revision_hash != actual.revision_hash
        || expected.author_host_id != actual.author_host_id
        || expected.config != actual.config
    {
        bail!("Agent changed during inspection; retry the command");
    }
    Ok(())
}

fn format_agent_inspection(inspection: &mews_protocol::AgentInspection) -> String {
    let mut output = String::new();
    let _ = writeln!(
        output,
        "agent: {} ({})\nrevision: r{} {}\nauthor host: {}",
        inspection.agent.slug,
        inspection.agent.id,
        inspection.agent.current_revision,
        inspection.revision_hash,
        inspection.author_host_id
    );
    let _ = writeln!(output, "\nconfiguration:");
    let _ = writeln!(output, "  harness: {}", inspection.config.harness);
    let _ = writeln!(
        output,
        "  tool execution: {}",
        match inspection.config.tool_execution {
            mews_protocol::ToolExecutionMode::Sequential => "sequential",
            mews_protocol::ToolExecutionMode::Parallel => "parallel",
        }
    );
    let _ = writeln!(
        output,
        "  tool allowlist: {}",
        inspection.config.tools.join(", ")
    );
    if inspection.config.harness_options.is_empty() {
        let _ = writeln!(output, "  harness options: (none)");
    } else {
        let _ = writeln!(output, "  harness options:");
        for (name, value) in &inspection.config.harness_options {
            let _ = writeln!(output, "    {name}: {value}");
        }
    }
    let _ = writeln!(output, "\nhost resolution:");
    if let Some(host) = &inspection.host {
        format_agent_host(&mut output, host);
    }
    output
}

fn format_agent_host(output: &mut String, host: &mews_protocol::AgentHostInspection) {
    let status = if host.connected {
        "connected"
    } else {
        "offline"
    };
    let _ = writeln!(output, "  {} ({})  {status}", host.host.name, host.host.id);
    if let Some(harness) = &host.harness {
        let readiness = if harness.availability.ready() {
            "ready"
        } else {
            "not ready"
        };
        let _ = writeln!(output, "    harness: {} ({readiness})", harness.name);
        if let Some(detail) = &harness.availability.detail {
            let _ = writeln!(output, "    detail: {detail}");
        }
    } else {
        let _ = writeln!(output, "    harness: unavailable");
    }
    match host.harness_native_authority {
        mews_protocol::HarnessNativeAuthority::NotApplicable => {}
        mews_protocol::HarnessNativeAuthority::KnownUncontrolled => {
            let _ = writeln!(output, "    Harness-native authority: uncontrolled");
        }
        mews_protocol::HarnessNativeAuthority::UnknownUncontrolled => {
            let _ = writeln!(
                output,
                "    Harness-native authority: unknown and uncontrolled"
            );
        }
    }
    match host.acp_skill_tools.state {
        mews_protocol::AcpSkillToolsState::NotApplicable => {}
        mews_protocol::AcpSkillToolsState::NoneKnown => {
            let _ = writeln!(output, "    ACP skill tools: none known");
        }
        mews_protocol::AcpSkillToolsState::Conditional => {
            let _ = writeln!(
                output,
                "    ACP skill tools: {} (conditional on Agent skills at Turn start)",
                host.acp_skill_tools.names.join(", ")
            );
        }
        mews_protocol::AcpSkillToolsState::Exposed => {
            let _ = writeln!(
                output,
                "    ACP skill tools: {} (exposed)",
                host.acp_skill_tools.names.join(", ")
            );
        }
        mews_protocol::AcpSkillToolsState::HarnessUnavailable => {
            let _ = writeln!(
                output,
                "    ACP skill tools: {} (Harness unavailable)",
                host.acp_skill_tools.names.join(", ")
            );
        }
        mews_protocol::AcpSkillToolsState::UnsupportedTransport => {
            let _ = writeln!(
                output,
                "    ACP skill tools: {} (HTTP MCP unsupported)",
                host.acp_skill_tools.names.join(", ")
            );
        }
    }
    if !host.connected {
        return;
    }
    if let Some(generation) = host.tool_catalog_generation {
        let _ = writeln!(output, "    tool catalog: generation {generation}");
    }
    for (label, exposure) in [
        ("exposed tools", mews_protocol::AgentToolExposure::Exposed),
        (
            "excluded by allowlist",
            mews_protocol::AgentToolExposure::ExcludedByAllowlist,
        ),
        (
            "Harness unavailable",
            mews_protocol::AgentToolExposure::HarnessUnavailable,
        ),
        (
            "HTTP MCP unsupported",
            mews_protocol::AgentToolExposure::UnsupportedTransport,
        ),
        (
            "Harness-controlled tools",
            mews_protocol::AgentToolExposure::HarnessControlled,
        ),
    ] {
        write_inspected_tools(
            output,
            label,
            host.tools
                .tools
                .iter()
                .filter(|tool| tool.exposure == exposure),
        );
    }
}

fn write_inspected_tools<'a>(
    output: &mut String,
    label: &str,
    tools: impl Iterator<Item = &'a mews_protocol::AgentToolInspection>,
) {
    let tools = tools
        .map(|tool| {
            let source = match tool.source {
                mews_protocol::AgentToolSource::MewsNative => "mews native",
                mews_protocol::AgentToolSource::HarnessNative => "Harness native",
                mews_protocol::AgentToolSource::AgentExtension => "Agent extension",
            };
            format!("{} ({source})", tool.name)
        })
        .collect::<Vec<_>>();
    let _ = writeln!(
        output,
        "    {label}: {}",
        if tools.is_empty() {
            "(none)".to_owned()
        } else {
            tools.join(", ")
        }
    );
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
                    format!("{name} | built in")
                } else if ready_hosts.is_empty() {
                    format!("{name} | setup required")
                } else {
                    format!("{name} | {}", ready_hosts.join(", "))
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
        super::harnesses::setup(root, harness).await?;
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
        choices.extend(reasoning.iter().filter_map(|value| {
            if *value == mews_protocol::ReasoningEffort::Auto {
                return None;
            }
            let value = format!("{value:?}").to_lowercase();
            Some((value.clone(), Some(value)))
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
    let mut choices = Vec::<(String, Option<String>, Vec<String>)>::new();
    for (descriptor, host) in descriptors {
        for option in descriptor.config_options.iter().filter(|option| {
            option.get("id").and_then(serde_json::Value::as_str) == Some(config_id)
        }) {
            collect_option_entries(option.get("options"), &mut |value, name| {
                if let Some((_, existing_name, hosts)) = choices
                    .iter_mut()
                    .find(|(existing, _, _)| existing == value)
                {
                    if existing_name.is_none() {
                        *existing_name = name.map(str::to_owned);
                    }
                    hosts.push((*host).to_owned());
                } else {
                    choices.push((
                        value.to_owned(),
                        name.map(str::to_owned),
                        vec![(*host).to_owned()],
                    ));
                }
            });
        }
    }
    let mut result = vec![OptionChoice {
        label: "Harness default".into(),
        value: String::new(),
    }];
    result.extend(choices.into_iter().map(|(value, name, mut hosts)| {
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
    if args.is_empty() {
        mews_tui::chat(&mut client, session).await?;
    } else {
        let prompt = parse_prompt_args(&args)?;
        if prompt.detach {
            let turn = start_detached(&mut client, &session, prompt.message).await?;
            println!("session: {}\nturn: {}", session.id, turn.id);
        } else {
            println!("{}", send(&mut client, &session, prompt.message).await?);
        }
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
                channel_origin: None,
            },
        )
        .await
}

async fn start_detached(
    client: &mut MewsClient,
    session: &Session,
    prompt: String,
) -> Result<mews_protocol::Turn> {
    client
        .start_turn(
            session.id.clone(),
            prompt,
            serde_json::Value::Null,
            MessageSource {
                kind: SourceKind::Client,
                id: "cli".into(),
                channel_origin: None,
            },
        )
        .await
}

#[derive(Debug, PartialEq, Eq)]
struct PromptArgs {
    message: String,
    detach: bool,
}

fn parse_prompt_args(args: &[String]) -> Result<PromptArgs> {
    let Some((option, rest)) = args.split_first() else {
        bail!("-p requires a message");
    };
    if option != "-p" {
        bail!("expected -p <message> [--detach]");
    }
    let detach = rest.iter().any(|argument| argument == "--detach");
    let message = rest
        .iter()
        .filter(|argument| argument.as_str() != "--detach")
        .cloned()
        .collect::<Vec<_>>()
        .join(" ");
    if message.is_empty() {
        bail!("-p requires a message");
    }
    Ok(PromptArgs { message, detach })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::Utc;
    use mews_protocol::{
        AcpSkillToolsInspection, AcpSkillToolsState, Agent, AgentConfig, AgentHostInspection,
        AgentInspection, AgentToolExposure, AgentToolInspection, AgentToolInspectionPage,
        AgentToolSource, HarnessAvailability, HarnessDescriptor, HarnessNativeAuthority,
        HarnessProtocol, HarnessReadiness, Host, ToolExecutionMode,
    };

    use super::{
        ensure_same_agent_snapshot, external_option_choices, format_agent_inspection,
        parse_create_agent_args, parse_prompt_args,
    };

    #[test]
    fn parses_prompt_and_optional_detach_flag() {
        let attached = parse_prompt_args(&["-p".into(), "do".into(), "work".into()]).unwrap();
        assert_eq!(attached.message, "do work");
        assert!(!attached.detach);

        let detached =
            parse_prompt_args(&["-p".into(), "do".into(), "work".into(), "--detach".into()])
                .unwrap();
        assert_eq!(detached.message, "do work");
        assert!(detached.detach);
        assert!(parse_prompt_args(&["ask".into(), "do work".into()]).is_err());
    }

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
    fn inspection_keeps_connected_host_tools_when_the_harness_is_unavailable() {
        let agent_id = mews_protocol::AgentId::new();
        let host_id = mews_protocol::HostId::new();
        let inspection = AgentInspection {
            agent: Agent {
                id: agent_id,
                slug: "coder".into(),
                current_revision: 1,
                archived: false,
                created_at: Utc::now(),
            },
            revision_hash: "revision".into(),
            author_host_id: host_id.clone(),
            config: AgentConfig {
                harness: "missing".into(),
                harness_options: BTreeMap::new(),
                tools: vec!["lookup".into()],
                tool_execution: ToolExecutionMode::Sequential,
            },
            host: Some(AgentHostInspection {
                host: Host {
                    id: host_id,
                    name: "laptop".into(),
                    public_key: "key".into(),
                    noise_public_key: "noise".into(),
                    relay_url: None,
                    created_at: Utc::now(),
                },
                connected: true,
                harness: None,
                harness_native_authority: HarnessNativeAuthority::UnknownUncontrolled,
                acp_skill_tools: AcpSkillToolsInspection {
                    names: vec!["mews_list_skills".into(), "mews_read_skill".into()],
                    state: AcpSkillToolsState::HarnessUnavailable,
                },
                tool_catalog_generation: Some(7),
                tools: AgentToolInspectionPage {
                    tools: vec![AgentToolInspection {
                        name: "lookup".into(),
                        source: AgentToolSource::AgentExtension,
                        allowlist_match: true,
                        exposure: AgentToolExposure::HarnessUnavailable,
                    }],
                    next: None,
                },
            }),
        };

        let output = format_agent_inspection(&inspection);

        assert!(output.contains("harness: unavailable"));
        assert!(output.contains("tool catalog: generation 7"));
        assert!(output.contains("Harness unavailable: lookup (Agent extension)"));
        assert!(output.contains("Harness-native authority: unknown and uncontrolled"));
    }

    #[test]
    fn inspection_rejects_host_pages_from_another_agent_revision() {
        let agent_id = mews_protocol::AgentId::new();
        let author_host_id = mews_protocol::HostId::new();
        let inspection = AgentInspection {
            agent: Agent {
                id: agent_id,
                slug: "coder".into(),
                current_revision: 1,
                archived: false,
                created_at: Utc::now(),
            },
            revision_hash: "first".into(),
            author_host_id,
            config: AgentConfig {
                harness: "mews".into(),
                harness_options: BTreeMap::new(),
                tools: vec!["read".into()],
                tool_execution: ToolExecutionMode::Sequential,
            },
            host: None,
        };
        let mut changed = inspection.clone();
        changed.agent.current_revision = 2;
        changed.revision_hash = "second".into();
        changed.config.tools.push("write".into());

        ensure_same_agent_snapshot(&inspection, &inspection).unwrap();
        assert!(
            ensure_same_agent_snapshot(&inspection, &changed)
                .unwrap_err()
                .to_string()
                .contains("Agent changed during inspection")
        );
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
            supports_http_mcp: true,
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
    fn preserves_acp_option_order_while_merging_hosts() {
        let first = HarnessDescriptor {
            name: "codex".into(),
            protocol: HarnessProtocol::Acp,
            definition_hash: "first".into(),
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
            supports_http_mcp: true,
            supports_continuation: false,
            models: Vec::new(),
            config_options: vec![serde_json::json!({
                "id": "reasoning_effort",
                "options": [
                    { "value": "low" },
                    { "value": "medium" },
                    { "value": "high" },
                    { "value": "xhigh" }
                ]
            })],
            probed_at: None,
        };
        let mut second = first.clone();
        second.definition_hash = "second".into();
        second.config_options = vec![serde_json::json!({
            "id": "reasoning_effort",
            "options": [{ "value": "high" }, { "value": "max" }]
        })];

        let choices = external_option_choices(
            &[(&first, "laptop"), (&second, "desktop")],
            "reasoning_effort",
        );

        assert_eq!(
            choices
                .iter()
                .map(|choice| choice.value.as_str())
                .collect::<Vec<_>>(),
            ["", "low", "medium", "high", "xhigh", "max"]
        );
        assert_eq!(choices[3].label, "high  [desktop, laptop]");
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
