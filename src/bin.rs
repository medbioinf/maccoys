// 3rd party imports
use anyhow::Result;
use clap::{Parser, Subcommand};
use indicatif::ProgressStyle;
use tracing::{error, info, Level};
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

// internal imports
use maccoys::database::database_build::DatabaseBuild;
use maccoys::web::server::start as start_web_server;

#[derive(Debug, Subcommand)]
enum Commands {
    Web {
        database_url: String,
        database: String,
        interface: String,
        port: u16,
    },
    /// Build a MaCPepDB database for decoy caching.
    ///
    DatabaseBuild {
        /// Comma separated list of ScyllaDB nodes
        database_url: String,
        /// Database/keyspace name
        database: String,
        /// Path or http(s)-URL to a MaCPepDB configuration JSON file. If a URL is given, the file will be downloaded and parsed.
        /// If you have no working MaCPepDB do download one, you can use the default from the MaCcoyS repo.
        configuration_resource: String,
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
            database,
            interface,
            port,
        } => {
            if database_url.starts_with("scylla://") {
                let plain_database_url = database_url[9..].to_string();
                let database_hosts = plain_database_url
                    .split(",")
                    .map(|x| x.to_string())
                    .collect::<Vec<String>>();
                start_web_server(database_hosts, database, interface, port).await?;
            } else {
                error!("Unsupported database protocol: {}", database_url);
            }
        }
        Commands::DatabaseBuild {
            database_url,
            database,
            configuration_resource,
        } => {
            let plain_database_url = database_url[9..].to_string();
            let database_hosts = plain_database_url
                .split(",")
                .map(|x| x.to_string())
                .collect::<Vec<String>>();
            let build = DatabaseBuild::new(database_hosts, database);
            build.build(&configuration_resource).await?;
            info!("Database build finished");
        }
    };

    Ok(())
}
