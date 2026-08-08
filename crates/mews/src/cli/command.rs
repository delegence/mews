use std::{net::SocketAddr, path::PathBuf};

use clap::{Parser, Subcommand};

use super::defaults::default_root;

#[derive(Parser)]
#[command(
    name = "mews",
    version,
    about = "Build and run durable agents on your machines"
)]
pub struct Cli {
    #[arg(long, global = true, env = "MEWS_HOME", default_value_os_t = default_root())]
    pub root: PathBuf,
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand)]
pub enum Command {
    /// Configure this machine or join an existing MEWS installation
    Setup {
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        join: Option<String>,
        #[arg(long, env = "MEWS_RELAY_URL")]
        relay: Option<String>,
        #[arg(long, env = "MEWS_RELAY_LISTEN")]
        relay_listen: Option<SocketAddr>,
        #[arg(long, hide = true)]
        no_daemon: bool,
    },
    /// Create, manage, and chat with agents
    #[command(
        after_help = "Forms:\n  mews agents list\n  mews agents new [name] [--harness <name>] [--option <key=value>]...\n  mews agents rename <slug> <new-slug>\n  mews agents delete <slug>\n  mews agents <slug>\n  mews agents <slug> ask <message>"
    )]
    Agents {
        #[arg(
            trailing_var_arg = true,
            value_name = "new [name] | <slug> [ask <message>]"
        )]
        args: Vec<String>,
    },
    /// List and continue agent sessions
    #[command(
        after_help = "Forms:\n  mews sessions list\n  mews sessions <id>\n  mews sessions <id> ask <message>"
    )]
    Sessions {
        id: Option<String>,
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
    /// List and manage enrolled hosts
    Hosts {
        #[command(subcommand)]
        command: Option<HostsCommand>,
    },
    /// Inspect Harness availability on connected Hosts
    Harnesses {
        #[command(subcommand)]
        command: Option<HarnessesCommand>,
    },
    /// Manage the hub running the MEWS installation
    Hub {
        #[command(subcommand)]
        command: HubCommand,
    },
    /// Run the relay server
    Relay {
        #[command(subcommand)]
        command: RelayCommand,
    },
    /// Configure model providers and their credentials
    Providers {
        #[command(subcommand)]
        command: Option<ProviderCommand>,
    },
    /// Run the machine service as a background daemon
    #[command(hide = true)]
    Daemon,
    /// Run the model-provider router service
    #[command(hide = true)]
    Router,
    /// Restart the installed MEWS daemon and wait until it is ready
    Restart,
    /// Show the current MEWS installation status
    Status,
}

#[derive(Subcommand)]
pub enum HubCommand {
    /// Run the hub server in the foreground
    #[command(hide = true)]
    Serve,
    /// Stop the running hub server
    #[command(hide = true)]
    Stop,
    /// Transfer the hub role to another host
    Move { host: String },
    /// Recover this host after a hub transfer
    Recover,
}

#[derive(Subcommand)]
pub enum HostsCommand {
    /// List enrolled Hosts and their connection status
    List,
    /// Create an invitation for a new Host
    Invite {
        #[arg(long, env = "MEWS_RELAY_URL")]
        relay: Option<String>,
    },
    /// Revoke and remove a Host
    Remove { host: String },
}

#[derive(Subcommand)]
pub enum HarnessesCommand {
    /// List Harnesses published by connected Hosts
    List,
    /// Refresh Harness discovery on every connected Host
    Refresh,
    /// Prepare this Host's isolated profile for a trusted Harness definition
    Setup { name: String },
}

#[derive(Subcommand)]
pub enum RelayCommand {
    /// Start accepting relay connections
    Serve {
        #[arg(long, default_value = "0.0.0.0:8787")]
        listen: SocketAddr,
    },
}

#[derive(Subcommand)]
pub enum ProviderCommand {
    /// List authenticated providers
    Status,
    /// Sign in to a provider account
    Login { provider: Option<String> },
    /// Save an API key for a provider
    SetKey { provider: Option<String> },
    /// Remove a provider credential
    Logout { provider: String },
    /// Select the default model or manage the model catalog
    Models {
        #[command(subcommand)]
        command: Option<ProviderModelsCommand>,
    },
    /// Select the default reasoning effort
    Reasoning,
}

#[derive(Subcommand)]
pub enum ProviderModelsCommand {
    /// Refresh the available model catalog
    Update,
}

#[cfg(test)]
mod tests {
    use super::{Cli, Command};
    use clap::{CommandFactory, Parser};

    #[test]
    fn all_commands_have_descriptions() {
        fn check(command: &clap::Command) {
            for subcommand in command.get_subcommands() {
                check(subcommand);
                assert!(
                    subcommand.get_about().is_some(),
                    "{} is missing a description",
                    subcommand.get_name()
                );
            }
        }

        check(&Cli::command());
    }

    #[test]
    fn provider_commands_use_the_providers_namespace() {
        assert!(Cli::try_parse_from(["mews", "providers", "status"]).is_ok());
        assert!(Cli::try_parse_from(["mews", "providers"]).is_ok());
        assert!(Cli::try_parse_from(["mews", "providers", "login"]).is_ok());
        assert!(Cli::try_parse_from(["mews", "providers", "login", "openai"]).is_ok());
        assert!(Cli::try_parse_from(["mews", "providers", "set-key"]).is_ok());
        assert!(Cli::try_parse_from(["mews", "providers", "set-key", "google"]).is_ok());
        assert!(Cli::try_parse_from(["mews", "providers", "models"]).is_ok());
        assert!(Cli::try_parse_from(["mews", "providers", "models", "update"]).is_ok());
        assert!(Cli::try_parse_from(["mews", "providers", "reasoning"]).is_ok());
        assert!(Cli::try_parse_from(["mews", "auth", "status"]).is_err());
    }

    #[test]
    fn hosts_list_is_an_explicit_subcommand() {
        assert!(Cli::try_parse_from(["mews", "hosts", "list"]).is_ok());
    }

    #[test]
    fn restart_is_a_top_level_command() {
        assert!(matches!(
            Cli::try_parse_from(["mews", "restart"]).unwrap().command,
            Some(Command::Restart)
        ));
    }

    #[test]
    fn setup_preserves_interactive_and_explicit_host_name_forms() {
        let interactive = Cli::try_parse_from(["mews", "setup"]).unwrap();
        assert!(matches!(
            interactive.command,
            Some(Command::Setup { name: None, .. })
        ));
        let explicit = Cli::try_parse_from(["mews", "setup", "--name", "mini-pc"]).unwrap();
        assert!(
            matches!(explicit.command, Some(Command::Setup { name: Some(name), .. }) if name == "mini-pc")
        );
    }
}
