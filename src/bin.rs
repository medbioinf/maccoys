// 3rd party imports
use anyhow::Result;
use clap::{Parser, Subcommand};
use indicatif::ProgressStyle;
use tracing::{error, info, Level};
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

// internal imports
use maccoys::web::server::start as start_web_server;

#[derive(Debug, Subcommand)]
enum Commands {
    Web {
        database_url: String,
        interface: String,
        port: u16,
    },
}

#[derive(Debug, Parser)]
#[command(name = "maccoys")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::from_default_env()
        .add_directive(Level::DEBUG.into())
        .add_directive("scylla=info".parse().unwrap())
        .add_directive("tokio_postgres=info".parse().unwrap());

    let indicatif_layer = IndicatifLayer::new().with_progress_style(
        ProgressStyle::with_template(
            "{spinner:.cyan} {span_child_prefix}{span_name}{{{span_fields}}} {wide_msg} {elapsed}",
        )
        .unwrap()
    ).with_span_child_prefix_symbol("↳ ").with_span_child_prefix_indent(" ");

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(indicatif_layer.get_stderr_writer()))
        .with(indicatif_layer)
        .with(filter)
        .init();

    info!("Welcome to MaCoyS - The `S` is silent!");

    let args = Cli::parse();

    match args.command {
        Commands::Web {
            database_url,
            interface,
            port,
        } => {
            if database_url.starts_with("scylla://") {
                let plain_database_url = database_url[9..].to_string();
                let database_hosts = plain_database_url
                    .split(",")
                    .map(|x| x.to_string())
                    .collect::<Vec<String>>();
                start_web_server(database_hosts, interface, port).await?;
            } else {
                error!("Unsupported database protocol: {}", database_url);
            }
        }
    };

    Ok(())
}
