use std::path::Path;

use anyhow::{Context, Result};
use clap::CommandFactory;
use mews_client::MewsClient;

use super::command::{Cli, HostsCommand};

pub async fn run(root: &Path, command: Option<HostsCommand>) -> Result<()> {
    let Some(command) = command else {
        let mut cli = Cli::command();
        let hosts = cli
            .find_subcommand_mut("hosts")
            .expect("hosts command exists");
        hosts.set_bin_name("mews hosts");
        hosts.print_help()?;
        println!();
        return Ok(());
    };
    let mut client = MewsClient::connect(root).await?;
    match command {
        HostsCommand::List => {
            let installation = client.status().await?;
            for status in client.hosts().await? {
                let role = if status.host.id == installation.hub_host_id {
                    "hub+host"
                } else {
                    "host"
                };
                let connection = if status.connected {
                    "online"
                } else {
                    "offline"
                };
                println!(
                    "{}  {}  {role}  {connection}",
                    status.host.id, status.host.name
                );
            }
        }
        HostsCommand::Invite { relay } => {
            println!("{}", client.create_host_invitation(relay).await?)
        }
        HostsCommand::Remove { host } => {
            let target = client
                .hosts()
                .await?
                .into_iter()
                .map(|status| status.host)
                .find(|candidate| candidate.name == host || candidate.id.to_string() == host)
                .context("Host not found")?;
            client.remove_host(target.id).await?;
            println!("Removed Host {host}.");
        }
    }
    Ok(())
}
