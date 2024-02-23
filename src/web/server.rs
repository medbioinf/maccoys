// std imports
use std::sync::Arc;

// 3rd party imports
use anyhow::Result;
use axum::routing::post;
use axum::Router;
use macpepdb::database::configuration_table::ConfigurationTable as ConfigurationTableTrait;
use macpepdb::database::generic_client::GenericClient;
use macpepdb::database::scylla::client::Client;
use macpepdb::database::scylla::configuration_table::ConfigurationTable;
use macpepdb::entities::configuration::Configuration;

// internal imports
use crate::web::decoy_controller::insert_decoys;

/// Starts the MaCcoyS API addition to MaCPepDB web server on the given interface and port.
///
/// # Arguments
/// * `database_url` - Database URL of the format scylla://host1,host2,host3/keyspace
/// * `interface` - Interface to listen on
/// * `port` - Port to listen on
///
pub async fn start(database_url: &str, interface: String, port: u16) -> Result<()> {
    // Create a database client
    // Session maintains it own connection pool internally: https://github.com/scylladb/scylla-rust-driver/issues/724
    // A single client with a session should be sufficient for the entire application
    let db_client = Client::new(&database_url).await?;
    let db_client = Arc::new(db_client);

    // Load configuration
    let configuration: Configuration = ConfigurationTable::select(db_client.as_ref()).await?;
    let configuration = Arc::new(configuration);

    tracing::info!("Start MaCcoyS addition to MaCPepDB web server");

    // Build our application with route
    let app = Router::new()
        // Peptide routes
        .route("/api/decoys/insert", post(insert_decoys))
        .with_state((db_client.clone(), configuration.clone()));

    // Run our app with hyper
    let listener = tokio::net::TcpListener::bind(format!("{}:{}", interface, port)).await?;
    tracing::info!("ready for connections, listening on {}", interface);
    axum::serve(listener, app).await.unwrap();

    Ok(())
}
