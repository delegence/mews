use std::{
    env,
    fs::OpenOptions,
    io::IsTerminal,
    net::SocketAddr,
    path::Path,
    process::{Command, Stdio},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use inquire::{MultiSelect, Password, Select, Text};
use mews::{
    enrollment::JoinOffer,
    enrollment::relay::{JoinedHostState, join_host},
    identity::{HostIdentity, NoiseIdentity},
    relay_supervisor::{RelayConfig, RelayRole},
    service::Mews,
};
use mews_client::MewsClient;
use mews_protocol::{HubRequest, HubResponse};

use super::{
    defaults::{
        concrete_relay_listen, default_host_name, default_relay_url, default_relay_url_for,
        derive_relay_listen,
    },
    prompt,
};

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
            create(
                root,
                &options.name,
                options.relay,
                options.relay_listen,
                options.no_daemon,
            )
            .await?
        }
        SetupMode::Join { offer } => {
            join_existing(
                root,
                &options.name,
                &offer,
                options.relay,
                options.relay_listen,
                options.no_daemon,
            )
            .await?
        }
    }
    if std::io::stdin().is_terminal() && std::io::stdout().is_terminal() {
        offer_harness_setup(root, &local_host_name).await?;
    }
    Ok(())
}

async fn offer_harness_setup(root: &Path, host_name: &str) -> Result<()> {
    let choices = mews_host::HarnessCatalog::discover(Some(root))?
        .descriptors()
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
    println!("\nSet up Harnesses on {host_name}");
    println!("✓ mews — built-in default Harness, ready");
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
    for selection in selected {
        let name = choices
            .iter()
            .find(|(label, _)| label == &selection)
            .map(|(_, name)| name)
            .expect("selected Harness exists");
        println!("setting up {name} ...");
        match mews_host::HarnessCatalog::setup(root, name).await {
            Ok(setup) if setup.descriptor.availability.ready() => {
                println!("✓ {name} is ready.");
            }
            Ok(_) => println!("{name} needs authentication; rerun `mews harnesses setup {name}`."),
            Err(error) => eprintln!("{name} setup failed: {error:#}"),
        }
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
    ensure_empty(root)?;
    println!("Set up MEWS\n");
    let Some(action) = prompt(
        Select::new(
            "What would you like to do?",
            vec!["Create a new MEWS", "Join an existing MEWS"],
        )
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
        JoinOffer::decode(encoded.trim()).context("invalid invitation")?;
        Some(encoded.trim().to_owned())
    } else {
        None
    };
    let default_name = default_host_name();
    let Some(name) = prompt(
        Text::new("Name this machine")
            .with_default(&default_name)
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

async fn create(
    root: &Path,
    name: &str,
    relay_url: Option<String>,
    relay_listen: Option<SocketAddr>,
    no_daemon: bool,
) -> Result<()> {
    let relay_url = relay_url.unwrap_or_else(default_relay_url);
    let listen = concrete_relay_listen(
        relay_listen
            .or(derive_relay_listen(&relay_url)?)
            .context("an external relay requires --relay-listen to be hosted locally")?,
    )?;
    let config = RelayConfig {
        listen,
        url: relay_url,
        role: RelayRole::Active,
    };
    let mews = Mews::setup(root, name)?;
    mews.set_relay_url(&config.url)?;
    mews::relay_supervisor::write(root, &config)?;
    drop(mews);
    if !no_daemon {
        mews::daemon::install(root, &env::current_exe()?)?;
        return wait_for_hub_ready(root, false).await;
    }
    spawn_and_wait(root, "hub.log", &["hub", "serve"], "Hub").await
}

async fn join_existing(
    root: &Path,
    name: &str,
    encoded: &str,
    relay_url: Option<String>,
    relay_listen: Option<SocketAddr>,
    no_daemon: bool,
) -> Result<()> {
    let offer = JoinOffer::decode(encoded)?;
    ensure_empty(root)?;
    std::fs::create_dir_all(root)?;
    secure(root, 0o700)?;
    mews::paths::ensure_directories(root)?;
    let identity = HostIdentity::load_or_create(&root.join("secrets/host.key"))?;
    let noise = NoiseIdentity::load_or_create(&root.join("secrets/host-noise.key"))?;
    let relay_url = relay_url.unwrap_or_else(default_relay_url);
    let listen = concrete_relay_listen(
        relay_listen
            .or(derive_relay_listen(&relay_url)?)
            .context("a Host relay requires --relay-listen")?,
    )?;
    mews::relay_supervisor::write(
        root,
        &RelayConfig {
            url: relay_url.clone(),
            listen,
            role: RelayRole::Disabled,
        },
    )?;
    let accepted = join_host(&offer, name, &identity, &noise, &relay_url).await?;
    let state_path = root.join("hub.json");
    std::fs::write(
        &state_path,
        serde_json::to_vec_pretty(&JoinedHostState {
            offer,
            relay_urls: accepted.relay_urls.clone(),
            accepted: accepted.clone(),
        })?,
    )?;
    secure(&state_path, 0o600)?;
    println!("MEWS Host enrolled: {}", accepted.host.id);
    if no_daemon {
        spawn_and_wait(root, "host.log", &["daemon"], "Host").await
    } else {
        mews::daemon::install(root, &env::current_exe()?)?;
        wait_for_hub_ready(root, false).await
    }
}

fn ensure_empty(root: &Path) -> Result<()> {
    if root.join("mews.db").exists() || root.join("hub.json").exists() {
        bail!("MEWS state already exists at {}", root.display());
    }
    Ok(())
}

#[cfg(unix)]
fn secure(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))?;
    Ok(())
}
#[cfg(not(unix))]
fn secure(_: &Path, _: u32) -> Result<()> {
    Ok(())
}

async fn spawn_and_wait(root: &Path, log_name: &str, args: &[&str], name: &str) -> Result<()> {
    mews::paths::ensure_directories(root)?;
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(mews::paths::log(root, log_name))?;
    let mut child = Command::new(env::current_exe()?)
        .arg("--root")
        .arg(root)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .spawn()
        .with_context(|| format!("start {name}"))?;
    for _ in 0..200 {
        if let Ok(mut client) = MewsClient::connect(root).await
            && let Ok(HubResponse::Status(installation)) = client.request(HubRequest::Status).await
        {
            let _ = installation;
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let _ = child.kill();
    let _ = child.wait();
    bail!(
        "{name} did not become ready; inspect {}",
        mews::paths::log(root, log_name).display()
    )
}

pub(super) async fn wait_for_hub(root: &Path) -> Result<()> {
    wait_for_hub_ready(root, true).await
}

async fn wait_for_hub_ready(root: &Path, announce: bool) -> Result<()> {
    for _ in 0..200 {
        if let Ok(mut client) = MewsClient::connect(root).await
            && let Ok(HubResponse::Status(installation)) = client.request(HubRequest::Status).await
        {
            if announce {
                print_hub_ready(&mut client, &installation).await;
            }
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    bail!(
        "MEWS daemon did not become ready; inspect {}",
        mews::paths::log(root, "daemon.log").display()
    )
}

async fn print_hub_ready(client: &mut MewsClient, installation: &mews_protocol::Installation) {
    let name = match client.request(HubRequest::ListHosts).await {
        Ok(HubResponse::Hosts(hosts)) => hosts
            .into_iter()
            .find(|status| status.host.id == installation.hub_host_id)
            .map(|status| status.host.name),
        _ => None,
    };
    println!(
        "MEWS is ready. Hub Host: {}",
        name.unwrap_or_else(|| installation.hub_host_id.to_string())
    );
}
