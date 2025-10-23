use std::sync::Arc;

use macpepdb::{
    database::{
        generic_client::GenericClient,
        scylla::{client::Client as DbClient, configuration_table::ConfigurationTable},
    },
    entities::configuration::Configuration,
};
use reqwest::Client as HttpClient;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Unsupported protocol in URL: {0}")]
    UnsupportedProtocol(String),
    #[error("Database error: {0}")]
    DatabaseError(anyhow::Error),
    #[error("HTTP error: {0}")]
    HttpError(anyhow::Error),
}

/// Enum holding either a database client or an HTTP client for MaCPepDB like databases
/// It does not provide any methods itself, but is used by other structs to abstract over the two access methods
///
pub enum MaCPepDBClient {
    Db(Arc<DbClient>, Configuration),
    Http(HttpClient),
}

impl MaCPepDBClient {
    /// Create a new MaCPepDBClient from a URL
    /// The URL can either be an HTTP URL (http:// or https://) for the web API or a ScyllaDB URL (scylla://...)
    pub async fn from_url(url: &str) -> Result<Self, Error> {
        if url.starts_with("http") || url.starts_with("https") {
            Ok(Self::Http(HttpClient::new()))
        } else if url.starts_with("scylla") {
            let db_client = DbClient::new(url).await.map_err(Error::DatabaseError)?;
            let configuration = ConfigurationTable::select(&db_client)
                .await
                .map_err(Error::DatabaseError)?;
            Ok(Self::Db(Arc::new(db_client), configuration))
        } else {
            Err(Error::UnsupportedProtocol(url.to_string()))
        }
    }
}
