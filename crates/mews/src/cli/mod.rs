mod chat;
mod command;
mod defaults;
mod harnesses;
mod hosts;
mod providers;
mod response;
mod runtime;
mod setup;

use anyhow::Result;
use clap::{CommandFactory, Parser};
use inquire::InquireError;
use mews_client::MewsClient;
use mews_protocol::HubRequest;

use command::{Cli, Command, HubCommand, RelayCommand};

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let Some(command) = cli.command else {
        Cli::command().print_help()?;
        println!();
        return Ok(());
    };
    match command {
        Command::Setup {
            name,
            join,
            relay,
            relay_listen,
            no_daemon,
        } => setup::run(&cli.root, name, join, relay, relay_listen, no_daemon).await?,
        Command::Hub {
            command: HubCommand::Serve,
        } => runtime::serve_machine(cli.root, false).await?,
        Command::Hub {
            command: HubCommand::Stop,
        } => {
            let mut client = MewsClient::connect(&cli.root).await?;
            response::ack(client.request(HubRequest::Shutdown).await?)?;
        }
        Command::Hub {
            command: HubCommand::Move { host },
        } => {
            let mut client = MewsClient::connect(&cli.root).await?;
            response::ack(client.request(HubRequest::MoveHub { host }).await?)?;
            println!("Hub moved successfully.");
        }
        Command::Hub {
            command: HubCommand::Recover,
        } => {
            mews::host::activate_hub_transfer(&cli.root)?;
            mews::hub::serve(cli.root).await?;
        }
        Command::Relay {
            command: RelayCommand::Serve { listen },
        } => mews_relay::serve(listen).await?,
        Command::Providers { command } => providers::run(&cli.root, command).await?,
        Command::Daemon => runtime::serve_machine(cli.root, true).await?,
        Command::Router => mews_router::serve(cli.root).await?,
        Command::Restart => {
            mews::daemon::restart()?;
            setup::wait_for_hub(&cli.root).await?;
        }
        Command::Agents { args } => chat::agents(&cli.root, args).await?,
        Command::Sessions { id, args } => chat::sessions(&cli.root, id, args).await?,
        Command::Hosts { command } => hosts::run(&cli.root, command).await?,
        Command::Harnesses { command } => harnesses::run(&cli.root, command).await?,
        Command::Status => {
            let mut client = MewsClient::connect(&cli.root).await?;
            let installation = response::status(client.request(HubRequest::Status).await?)?;
            println!(
                "installation: {}\nhub host: {}\ngeneration: {}",
                installation.id, installation.hub_host_id, installation.generation
            );
        }
    }
    Ok(())
}

fn prompt<T>(result: std::result::Result<T, InquireError>) -> Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(InquireError::OperationCanceled | InquireError::OperationInterrupted) => Ok(None),
        Err(error) => Err(error.into()),
    }
}
