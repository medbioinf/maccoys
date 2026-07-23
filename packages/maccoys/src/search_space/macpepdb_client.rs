use std::sync::Arc;

use macpepdb::{
    blob_table::BlobTable, client::Client as DbClient, configuration::RuntimeConfiguration,
    peptide_search::PeptideSearchType,
};
use reqwest::Client as HttpClient;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Database error: {0}")]
    BlobTable(macpepdb::blob_table::Error),
    #[error("Database error: {0}")]
    Database(macpepdb::client::Error),
    #[error("HTTP error: {0}")]
    HttpError(reqwest::Error),
    #[error("Missing MaCPepDB runtime configuration in database")]
    MissingMaCpepDBRuntimeConfiguration,
    #[error("Unsupported protocol in URL: {0}")]
    UnsupportedProtocol(String),
}

/// Enum holding either a database client or an HTTP client for MaCPepDB like databases
/// It does not provide any methods itself, but is used by other structs to abstract over the two access methods
///
pub enum MaCPepDBClient {
    Db(Arc<DbClient>, Arc<RuntimeConfiguration>, PeptideSearchType),
    Http(HttpClient),
}

impl MaCPepDBClient {
    /// Create a new MaCPepDBClient from a URL
    /// The URL can either be an HTTP URL (http:// or https://) for the web API or a ScyllaDB URL (scylla://...)
    pub async fn from_url(url: &str, search_type: PeptideSearchType) -> Result<Self, Error> {
        if url.starts_with("http") || url.starts_with("https") {
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(300))
                .user_agent("maccoys")
                .http2_prior_knowledge()
                .build()
                .map_err(Error::HttpError)?;

            Ok(Self::Http(client))
        } else if url.starts_with("postgresql") {
            let db_client = DbClient::new(url).await.map_err(Error::Database)?;
            let configuration = BlobTable::select::<RuntimeConfiguration>(
                &db_client,
                RuntimeConfiguration::BLOB_KEY,
            )
            .await
            .map_err(Error::BlobTable)?
            .ok_or(Error::MissingMaCpepDBRuntimeConfiguration)?;
            Ok(Self::Db(
                Arc::new(db_client),
                Arc::new(configuration),
                search_type,
            ))
        } else {
            Err(Error::UnsupportedProtocol(url.to_string()))
        }
    }
}
