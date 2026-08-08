use std::path::Path;

use anyhow::{Context, Result, bail};
use clap::CommandFactory;
use mews_client::MewsClient;
use mews_protocol::{HubRequest, HubResponse};

use super::{
    command::{Cli, HostsCommand},
    response,
};

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
            let installation = response::status(client.request(HubRequest::Status).await?)?;
            for status in response::hosts(client.request(HubRequest::ListHosts).await?)? {
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
        HostsCommand::Invite { relay } => match client
            .request(HubRequest::CreateHostInvitation { relay_url: relay })
            .await?
        {
            HubResponse::HostInvitation(offer) => println!("{offer}"),
            other => bail!("unexpected Hub response: {other:?}"),
        },
        HostsCommand::Remove { host } => {
            let target = response::hosts(client.request(HubRequest::ListHosts).await?)?
                .into_iter()
                .map(|status| status.host)
                .find(|candidate| candidate.name == host || candidate.id.to_string() == host)
                .context("Host not found")?;
            response::ack(
                client
                    .request(HubRequest::RemoveHost { id: target.id })
                    .await?,
            )?;
            println!("Removed Host {host}.");
        }
    }
    Ok(())
}
