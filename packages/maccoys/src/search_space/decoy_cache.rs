use std::{sync::Arc, time::Duration};

use anyhow::Result;
use macpepdb::{
    blob_table::BlobTable, configuration::RuntimeConfiguration, sequence::PeptideSequence,
};
use thiserror::Error;

/// Path to MaCCoyS web API for decoy caching
const MACCOYS_WEB_API_PATH: &str = "/api/decoys/insert";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Blob table error in target lookup: {0}")]
    BlobTable(Box<macpepdb::blob_table::Error>),
    #[error("Database client error in target lookup: {0}")]
    DatabaseClient(Box<macpepdb::client::Error>),
    #[error("Invalid target lookup URL. Allowed is `http://<macpepdb-api-base>`, `bloom+http://<macpepdb-bloomfilter-base>` or `postgresql://<postgresql-connection-url>`")]
    InvalidUrl,
    #[error("HTTP request error in target lookup: {0}")]
    HttpRequest(Box<reqwest::Error>),
    #[error("Missing configuration in database for target lookup")]
    MissingConfiguration,
    #[error("Peptide seqeunce error in target lookup")]
    PeptideSequence(Box<macpepdb::sequence::Error>),
    #[error("Peptide table error in target lookup: {0}")]
    PeptideTable(Box<macpepdb::peptide_table::Error>),
    #[error("Unexpected status code in target lookup: {0}")]
    UnexpectedStatusCode(u16),
    #[error("Unable to build HTTP client: {0}")]
    WebClientBuild(Box<reqwest::Error>),
}

/// Insert generated decoys into the cache database using the web API or directly database access
///
pub enum DecoyCache {
    Database(Arc<macpepdb::client::Client>, Arc<RuntimeConfiguration>),
    Api(reqwest::Client, String),
}

impl DecoyCache {
    pub async fn try_from_url(url: &str) -> Result<Self, Error> {
        if url.starts_with("http") || url.starts_with("https") {
            let client = Self::build_http_client(url)?;
            Ok(Self::Api(
                client,
                format!("{}{}", url, MACCOYS_WEB_API_PATH),
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
            Err(Error::InvalidUrl)
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

    pub async fn cache(&self, _decoys: Vec<(PeptideSequence, i16)>) -> Result<(), Error> {
        // Decoys might be removed anywys
        unimplemented!()
        // match &self.client {
        //     MaCPepDBClient::Db(_client, _configuration, _) => {
        //         let _decoys = decoys
        //             .into_iter()
        //             .map(|(seq, _)| Peptide::new(seq, vec![], vec![], vec![], false, false))
        //             .collect::<Vec<_>>();

        //         DecoyTable::bulk_upsert(client, decoys.iter()).await?;
        //     }
        //     MaCPepDBClient::Http(_client) => {
        //         // TODO: Decoys might be removed anyways
        //         unimplemented!()
        //         client
        //             .post(&self.url)
        //             .json(&json!(
        //                 {
        //                     "decoys": decoys
        //                 }
        //             ))
        //             .send()
        //             .await?;
        //     }
        // }
        // Ok(())
    }
}
