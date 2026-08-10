use std::{
    io::IsTerminal,
    net::SocketAddr,
    path::Path,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use console::style;
use indicatif::{ProgressBar, ProgressStyle};
use inquire::{MultiSelect, Password, Select, Text};
use mews_client::MewsClient;

use super::{
    defaults::{
        concrete_relay_listen, default_host_name, default_relay_url, default_relay_url_for,
        derive_relay_listen,
    },
    prompt,
};

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct RestartFailure(String);

pub async fn run(
    root: &Path,
    name: Option<String>,
    join: Option<String>,
    relay: Option<String>,
    relay_listen: Option<SocketAddr>,
    no_daemon: bool,
) -> Result<()> {
    let explicit =
        name.is_some() || join.is_some() || relay.is_some() || relay_listen.is_some() || no_daemon;
    let options = if !explicit && std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
    {
        let Some(options) = prompt_options(root)? else {
            println!("Setup cancelled.");
            return Ok(());
        };
        options
    } else {
        SetupOptions {
            mode: join.map_or(SetupMode::Create, |offer| SetupMode::Join { offer }),
            name: name.unwrap_or_else(default_host_name),
            relay,
            relay_listen,
            no_daemon,
        }
    };
    let local_host_name = options.name.clone();
    match options.mode {
        SetupMode::Create => {
            let relay_url = options.relay.unwrap_or_else(default_relay_url);
            let listen = concrete_relay_listen(
                options
                    .relay_listen
                    .or(derive_relay_listen(&relay_url)?)
                    .context("an external relay requires --relay-listen to be hosted locally")?,
            )?;
            crate::machine::setup::create(root, &options.name, relay_url, listen, options.no_daemon)
                .await?
        }
        SetupMode::Join { offer } => {
            let relay_url = options.relay.unwrap_or_else(default_relay_url);
            let listen = concrete_relay_listen(
                options
                    .relay_listen
                    .or(derive_relay_listen(&relay_url)?)
                    .context("a Host relay requires --relay-listen")?,
            )?;
            let host = crate::machine::setup::join(
                root,
                &options.name,
                &offer,
                relay_url,
                listen,
                options.no_daemon,
            )
            .await?;
            println!("MEWS Host enrolled: {}", host.id);
        }
    }
    if std::io::stdin().is_terminal() && std::io::stdout().is_terminal() {
        offer_harness_setup(root, &local_host_name).await?;
    }
    Ok(())
}

pub(crate) async fn offer_harness_setup(root: &Path, host_name: &str) -> Result<()> {
    let choices = super::harnesses::discover(root)?
        .into_iter()
        .filter(|descriptor| descriptor.protocol == mews_protocol::HarnessProtocol::Acp)
        .map(|descriptor| {
            let state = if descriptor.availability.adapter == mews_protocol::HarnessReadiness::Ready
            {
                "managed adapter installed"
            } else if descriptor.availability.runtime == mews_protocol::HarnessReadiness::Ready {
                "provider CLI detected — managed adapter available"
            } else {
                "managed install available"
            };
            (format!("{} — {state}", descriptor.name), descriptor.name)
        })
        .collect::<Vec<_>>();
    println!("{} Set up Harnesses on {host_name}", style(">").green());
    println!(
        "  {} mews — built-in default Harness, ready",
        style("✓").green()
    );
    if choices.is_empty() {
        println!("\nNo additional supported Harnesses were detected.");
        println!("\nMEWS is ready!");
        return Ok(());
    }
    let selected = prompt(
        MultiSelect::new(
            "Select additional Harnesses (nothing is installed unless selected).",
            choices.iter().map(|(label, _)| label.clone()).collect(),
        )
        .with_formatter(&|_| String::new())
        .prompt(),
    )?
    .unwrap_or_default();
    let mut changed_catalog = false;
    for selection in selected {
        let name = choices
            .iter()
            .find(|(label, _)| label == &selection)
            .map(|(_, name)| name)
            .expect("selected Harness exists");
        println!("  setting up {name} ...");
        match super::harnesses::setup(root, name).await {
            Ok(setup) if setup.descriptor.availability.ready() => {
                println!("  {} {name} is ready.", style("✓").green());
                changed_catalog = true;
            }
            Ok(_) => {
                changed_catalog = true;
                println!("{name} needs authentication; rerun `mews harnesses setup {name}`.");
            }
            Err(error) => eprintln!("{name} setup failed: {error:#}"),
        }
    }
    // Initial setup starts the Hub before this interactive install loop. Refresh
    // its live Host catalog so the just-installed adapter is immediately usable.
    if changed_catalog && let Ok(mut client) = MewsClient::connect(root).await {
        client.refresh_harnesses().await?;
    }
    println!("\nMEWS is ready!");
    Ok(())
}

enum SetupMode {
    Create,
    Join { offer: String },
}
struct SetupOptions {
    mode: SetupMode,
    name: String,
    relay: Option<String>,
    relay_listen: Option<SocketAddr>,
    no_daemon: bool,
}

fn prompt_options(root: &Path) -> Result<Option<SetupOptions>> {
    crate::machine::setup::ensure_available(root)?;
    println!("---------\n M E W S\n---------\n\nSet up MEWS\n");
    let Some(action) = prompt(
        Select::new(
            "What would you like to do?",
            vec!["Create a new MEWS", "Join an existing MEWS"],
        )
        .without_filtering()
        .with_help_message("↑↓ to move, Enter to select")
        .prompt(),
    )?
    else {
        return Ok(None);
    };
    let offer = if action == "Join an existing MEWS" {
        println!("\nOn an existing machine, run `mews hosts invite`, then paste the invitation.");
        let Some(encoded) = prompt(
            Password::new("Invitation")
                .without_confirmation()
                .with_help_message("The invitation is hidden because it is a single-use secret")
                .prompt(),
        )?
        else {
            return Ok(None);
        };
        crate::machine::setup::validate_invitation(encoded.trim()).context("invalid invitation")?;
        Some(encoded.trim().to_owned())
    } else {
        None
    };
    let default_name = default_host_name();
    let Some(name) = prompt(
        Text::new("Name this machine")
            .with_default(&default_name)
            .with_help_message("Press Enter to use the default")
            .prompt(),
    )?
    else {
        return Ok(None);
    };
    let name = name.trim();
    if name.is_empty() {
        bail!("Host name cannot be empty");
    }
    let default_relay = default_relay_url_for(name);
    let label = if offer.is_some() {
        "Address this machine advertises"
    } else {
        "Relay address"
    };
    let Some(relay) = prompt(
        Text::new(label)
            .with_default(&default_relay)
            .with_help_message("Press Enter to use the default")
            .prompt(),
    )?
    else {
        return Ok(None);
    };
    derive_relay_listen(relay.trim())?;
    Ok(Some(SetupOptions {
        mode: offer.map_or(SetupMode::Create, |offer| SetupMode::Join { offer }),
        name: name.to_owned(),
        relay: Some(relay.trim().to_owned()),
        relay_listen: None,
        no_daemon: false,
    }))
}

pub(super) async fn restart(root: &Path) -> Result<()> {
    let started = Instant::now();
    println!("Restarting MEWS...\n");

    let progress = spinner("Restarting daemon");
    crate::machine::daemon::restart()?;
    finish_progress(progress, "\u{2713} Daemon ready");

    let progress = spinner("Waiting for Hub");
    let (mut client, installation) = match connect_to_hub(root).await {
        Ok(ready) => ready,
        Err(last_error) => {
            progress.abandon();
            return Err(RestartFailure(restart_failure(root, &last_error)).into());
        }
    };
    let hosts = client.hosts().await.unwrap_or_default();
    let hub = hosts
        .iter()
        .find(|status| status.host.id == installation.hub_host_id)
        .map(|status| status.host.name.clone())
        .unwrap_or_else(|| installation.hub_host_id.to_string());
    finish_progress(progress, format!("\u{2713} Hub {hub} ready"));

    let progress = spinner("Connecting to Hosts");
    let connected_hosts = hosts.iter().filter(|status| status.connected).count();
    finish_progress(
        progress,
        format!("\u{2713} Hosts {connected_hosts}/{} connected", hosts.len()),
    );

    let progress = spinner("Checking Harnesses");
    let mut harnesses = client
        .harnesses()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|status| {
            status.host.id == installation.hub_host_id && status.descriptor.availability.ready()
        })
        .map(|status| status.descriptor.name)
        .collect::<Vec<_>>();
    harnesses.sort();
    harnesses.dedup();
    finish_progress(
        progress,
        format!(
            "\u{2713} Harnesses {} available on this host",
            harnesses.len()
        ),
    );

    let progress = spinner("Checking Agents");
    let agents = client
        .agents()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|agent| !agent.archived)
        .count();
    finish_progress(progress, format!("\u{2713} Agents {agents} available"));

    println!("\nMEWS is ready in {}ms.", started.elapsed().as_millis());
    Ok(())
}

fn spinner(message: &'static str) -> ProgressBar {
    let progress = ProgressBar::new_spinner();
    progress.set_style(
        ProgressStyle::with_template("{spinner} {msg}")
            .expect("static spinner template is valid")
            .tick_strings(&[
                "\u{280b}", "\u{2819}", "\u{2839}", "\u{2838}", "\u{283c}", "\u{2834}", "\u{2826}",
                "\u{2827}", "\u{2807}", "\u{280f}",
            ]),
    );
    progress.set_message(message);
    progress.enable_steady_tick(Duration::from_millis(80));
    progress
}

fn finish_progress(progress: ProgressBar, message: impl Into<String>) {
    let message = message.into();
    if std::io::stderr().is_terminal() {
        progress.finish_with_message(message);
    } else {
        progress.finish_and_clear();
        println!("{message}");
    }
}

async fn connect_to_hub(
    root: &Path,
) -> std::result::Result<(MewsClient, mews_protocol::Installation), String> {
    let mut last_error = "Hub did not respond".to_owned();
    for _ in 0..200 {
        match MewsClient::connect(root).await {
            Ok(mut client) => match client.status().await {
                Ok(installation) => return Ok((client, installation)),
                Err(error) => last_error = format!("{error:#}"),
            },
            Err(error) => last_error = format!("{error:#}"),
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Err(last_error)
}

fn restart_failure(root: &Path, last_error: &str) -> String {
    let context = crate::machine::runtime::restart_failure_context(root, last_error);
    format!(
        "MEWS failed to restart\n\nDaemon restarted\nHub unavailable after 5s\nSocket: {}\nLog: {}\n\nLast error:\n{}",
        context.socket.display(),
        context.log.display(),
        context.last_error
    )
}
