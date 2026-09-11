use std::{sync::Arc, time::Duration};

use http::StatusCode;
use macpepdb::{
    blob_table::BlobTable, configuration::RuntimeConfiguration, peptide_table::PeptideTable,
    sequence::PeptideSequence,
};
use thiserror::Error;

/// Path for MaCPepDB web API, with `:sequence:` as placeholder for the sequence
const MACPEPDB_WEB_API_PATH: &str = "/api/peptides/:sequence:/exists";

/// Path for MaCPepDB BloomFilter web API, with `:sequence:` as placeholder for the sequence
const MACPEPDB_BLOOM_FILTER_WEB_API_PATH: &str = "/lookup/everywhere/:sequence:";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Blob table error in target lookup: {0}")]
    BlobTable(Box<macpepdb::blob_table::Error>),
    #[error("Database client error in target lookup: {0}")]
    DatabaseClient(Box<macpepdb::client::Error>),
    #[error("Invalid target lookup URL `{0}`. Allowed is `http://<macpepdb-api-base>`, `bloom+http://<macpepdb-bloomfilter-base>` or `postgresql://<postgresql-connection-url>`")]
    InvalidUrl(String),
    #[error("HTTP request error in target lookup: {0}")]
    HttpRequest(Box<reqwest::Error>),
    #[error("Missing configuration in database for target lookup")]
    MissingConfiguration,
    #[error("Peptide seqeunce error in target lookup")]
    PeptideSequence(Box<macpepdb::sequence::Error>),
    #[error("Peptide table error in target lookup: {0}")]
    PeptideTable(Box<macpepdb::peptide_table::Error>),
    #[error("Unexpected status code `{1}` in target lookup at `{0}` (content type `{2}`): {3}")]
    UnexpectedStatusCode(String, u16, String, String),
    #[error("Unable to build HTTP client: {0}")]
    WebClientBuild(Box<reqwest::Error>),
}

/// Checks if a sequence is a target using the database or the web API
///
pub enum TargetLookup {
    Database(Arc<macpepdb::client::Client>, Arc<RuntimeConfiguration>),
    Api(reqwest::Client, String),
    BloomFilter(reqwest::Client, String),
}

impl TargetLookup {
    /// Creates a new `TargetLookup` from a URL. The URL can be:
    ///
    /// # Arguments
    /// * `url` - A string slice that holds the URL. It can be:
    ///   * `http://<macpepdb-api-base>` or `https://<macpepdb-api-base>` for the MaCPepDB web API
    ///   * `bloom+http://<macpepdb-bloomfilter-base>` or `bloom+https://<macpepdb-bloomfilter-base>` for the MaCPepDB BloomFilter web API
    ///   * `postgresql://<postgresql-connection-url>` for the MaCPepDB database
    pub async fn try_from_url(url: &str) -> Result<Self, Error> {
        if url.starts_with("http") || url.starts_with("https") {
            let client = Self::build_http_client(url)?;
            Ok(Self::Api(
                client,
                format!("{}{}", url, MACPEPDB_WEB_API_PATH),
            ))
        } else if url.starts_with("bloom+http") || url.starts_with("bloom+https") {
            let client = Self::build_http_client(&url[6..])?;
            Ok(Self::BloomFilter(
                client,
                format!("{}{}", &url[6..], MACPEPDB_BLOOM_FILTER_WEB_API_PATH),
            ))
        } else if url.starts_with("postgresql") {
            let client = macpepdb::client::Client::new(url)
                .await
                .map(Arc::new)
                .map_err(|e| Error::DatabaseClient(Box::new(e)))?;
            let configuration = BlobTable::select::<RuntimeConfiguration>(
                client.as_ref(),
                RuntimeConfiguration::BLOB_KEY,
            )
            .await
            .map_err(|err| Error::BlobTable(Box::new(err)))?
            .map(Arc::new)
            .ok_or(Error::MissingConfiguration)?;

            Ok(Self::Database(client, configuration))
        } else {
            Err(Error::InvalidUrl(url.to_string()))
        }
    }

    /// Builds a reqwest HTTP client with a 60 seconds timeout and HTTP/2 support for plaintext connections
    ///
    /// # Arguments
    /// * `url` - A string slice that holds the URL. It can be:
    ///
    fn build_http_client(url: &str) -> Result<reqwest::Client, Error> {
        let mut client_builder = reqwest::Client::builder().timeout(Duration::from_secs(60));
        if url.starts_with("http://") {
            client_builder = client_builder
                .http2_prior_knowledge() // force h2 without TLS ALPN (plaintext h2c) — rare, needs server support
                .http2_keep_alive_interval(Duration::from_secs(20))
                .http2_keep_alive_timeout(Duration::from_secs(10))
                .http2_keep_alive_while_idle(true)
        }
        client_builder
            .build()
            .map_err(|e| Error::WebClientBuild(Box::new(e)))
    }

    /// Checks if a sequence is a target using the database or the web API
    ///
    /// # Arguments
    /// * `sequence` - A string slice that holds the sequence to check
    ///
    pub async fn is_target(&self, sequence: &str) -> Result<bool, Error> {
        match self {
            Self::Database(client, configuration) => {
                let sequence = PeptideSequence::try_from(sequence)
                    .map_err(|err| Error::PeptideSequence(Box::new(err)))?;

                PeptideTable::new(client.clone())
                    .exists(configuration.as_ref(), sequence)
                    .await
                    .map_err(|err| Error::PeptideTable(Box::new(err)))
            }
            Self::Api(client, url) | Self::BloomFilter(client, url) => {
                let request_url = url.replace(":sequence:", sequence);
                let response = client
                    .get(&request_url)
                    .send()
                    .await
                    .map_err(|err| Error::HttpRequest(Box::new(err)))?;

                match response.status() {
                    StatusCode::OK => Ok(true),
                    StatusCode::NOT_FOUND => Ok(false),
                    status => {
                        let content_type = response
                            .headers()
                            .get("content-type")
                            .and_then(|ct| ct.to_str().ok())
                            .unwrap_or("unknown content type")
                            .to_string();
                        let body = response
                            .text()
                            .await
                            .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));
                        Err(Error::UnexpectedStatusCode(
                            request_url,
                            status.as_u16(),
                            content_type,
                            body,
                        ))
                    }
                }
            }
        }
    }
}
