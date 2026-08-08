use std::{io::IsTerminal, path::Path};

use anyhow::Result;
use clap::CommandFactory;
use mews_client::MewsClient;
use mews_protocol::HubRequest;

use super::{
    command::{Cli, HarnessesCommand},
    response,
};

pub async fn run(root: &Path, command: Option<HarnessesCommand>) -> Result<()> {
    let Some(command) = command else {
        let mut cli = Cli::command();
        let harnesses = cli
            .find_subcommand_mut("harnesses")
            .expect("harnesses command exists");
        harnesses.set_bin_name("mews harnesses");
        harnesses.print_help()?;
        println!();
        return Ok(());
    };
    if let HarnessesCommand::Setup { name } = command {
        let setup = mews_host::HarnessCatalog::setup(root, &name).await?;
        if let Some(profile) = setup.managed_profile {
            let action = if setup.profile_created {
                "created"
            } else {
                "already exists"
            };
            println!(
                "{} profile {action}: {}",
                setup.descriptor.name,
                profile.display()
            );
        } else {
            println!(
                "{} is ready; no managed profile is needed",
                setup.descriptor.name
            );
        }
        if let Ok(mut client) = MewsClient::connect(root).await {
            let _ = client.request(HubRequest::RefreshHarnesses).await;
        }
        if setup.descriptor.availability.authentication == mews_protocol::HarnessReadiness::Required
            && !std::io::stdin().is_terminal()
        {
            anyhow::bail!(
                "{} authentication is required; rerun `mews harnesses setup {}` from a terminal",
                setup.descriptor.name,
                setup.descriptor.name
            );
        }
        return Ok(());
    }
    let request = match command {
        HarnessesCommand::List => HubRequest::ListHarnesses,
        HarnessesCommand::Refresh => HubRequest::RefreshHarnesses,
        HarnessesCommand::Setup { .. } => unreachable!("handled before connecting to Hub"),
    };
    let mut client = MewsClient::connect(root).await?;
    for entry in response::harnesses(client.request(request).await?)? {
        let availability = &entry.descriptor.availability;
        println!(
            "{}  {}  {}  runtime={:?} adapter={:?} auth={:?} catalog={:?}",
            entry.descriptor.name,
            entry.host.name,
            format!("{:?}", entry.descriptor.protocol).to_lowercase(),
            availability.runtime,
            availability.adapter,
            availability.authentication,
            availability.catalog,
        );
    }
    Ok(())
}
