mod chat;
mod command;
mod defaults;
mod harnesses;
mod hosts;
mod providers;
mod setup;

use anyhow::Result;
use clap::{CommandFactory, Parser};
use inquire::InquireError;
use mews_client::MewsClient;

use command::{Cli, Command, HubCommand, RelayCommand};
pub use setup::RestartFailure;

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
        } => crate::machine::runtime::serve_machine(cli.root, false).await?,
        Command::Hub {
            command: HubCommand::Stop,
        } => {
            let mut client = MewsClient::connect(&cli.root).await?;
            client.shutdown_daemon().await?;
        }
        Command::Hub {
            command: HubCommand::Move { host },
        } => {
            let mut client = MewsClient::connect(&cli.root).await?;
            client.move_hub(host).await?;
            println!("Hub moved successfully.");
        }
        Command::Hub {
            command: HubCommand::Recover,
        } => crate::machine::runtime::recover_hub(cli.root).await?,
        Command::Relay {
            command: RelayCommand::Serve { listen },
        } => crate::machine::runtime::serve_relay(listen).await?,
        Command::Providers { command } => providers::run(&cli.root, command).await?,
        Command::Daemon => crate::machine::runtime::serve_machine(cli.root, true).await?,
        Command::Router => crate::machine::runtime::serve_router(cli.root).await?,
        Command::Restart => {
            setup::restart(&cli.root).await?;
        }
        Command::Agents { args } => chat::agents(&cli.root, args).await?,
        Command::Sessions { id, args } => chat::sessions(&cli.root, id, args).await?,
        Command::Hosts { command } => hosts::run(&cli.root, command).await?,
        Command::Harnesses { command } => harnesses::run(&cli.root, command).await?,
        Command::Status => {
            let mut client = MewsClient::connect(&cli.root).await?;
            let installation = client.status().await?;
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
