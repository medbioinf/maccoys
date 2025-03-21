// 3rd party imports
use anyhow::{anyhow, bail, Result};
use macpepdb::{
    database::{
        generic_client::GenericClient,
        scylla::{
            client::Client as DbClient, configuration_table::ConfigurationTable,
            peptide_table::PeptideTable,
        },
    },
    entities::configuration::Configuration,
};
use reqwest::Client as HttpClient;

/// Path for MaCPepDB web API, with `:sequence:` as placeholder for the sequence
const MACPEPDB_WEB_API_PATH: &str = "/api/peptides/:sequence:/exists";

/// Path for MaCPepDB BloomFilter web API, with `:sequence:` as placeholder for the sequence
const MACPEPDB_BLOOM_FILTER_WEB_API_PATH: &str = "/lookup/everywhere/:sequence:";

/// Enum for storing the client for the target lookup
///
enum Client {
    Db(DbClient, Configuration), // no ARC needed as the client is only used in this threads
    Http(HttpClient),
}

/// Checks if a sequence is a target using the database or the web API
///
pub struct TargetLookup {
    url: String,
    client: Client,
}

impl TargetLookup {
    /// Create a new WebApiTargetLookup
    ///
    /// # Arguments
    /// * `url` - URL to the web API endpoint, e.g. `http://...` or `bloom+http://...` or directly to the database `scylla://host1,host2,host3/keyspace`
    pub async fn new(url: &str) -> Result<Self> {
        let (url, client) = if url.starts_with("http") {
            (
                format!("{}{}", url, MACPEPDB_WEB_API_PATH),
                Client::Http(HttpClient::new()),
            )
        } else if url.starts_with("bloom+http") {
            (
                format!("{}{}", &url[6..], MACPEPDB_BLOOM_FILTER_WEB_API_PATH),
                Client::Http(HttpClient::new()),
            )
        } else if url.starts_with("scylla") {
            let db_client = DbClient::new(url).await?;
            let configuration = ConfigurationTable::select(&db_client).await?;
            (url.to_string(), Client::Db(db_client, configuration))
        } else {
            bail!("Unsupported URL protocol: {}", url)
        };

        Ok(Self { url, client })
    }

    /// Check if a sequence is a target
    ///
    /// # Arguments
    /// * `sequence` - Sequence to be checked
    ///
    pub async fn is_target(&self, sequence: &str) -> Result<bool> {
        match &self.client {
            Client::Db(client, configuration) => {
                Ok(PeptideTable::exists_by_sequence(client, sequence, configuration).await?)
            }
            Client::Http(client) => {
                let response = client
                    .get(self.url.replace(":sequence:", sequence))
                    .send()
                    .await?;

                match response.status().as_u16() {
                    200 => Ok(true),
                    404 => Ok(false),
                    _ => Err(anyhow!("Unexpected response status: {}", response.status())),
                }
            }
        }
    }
}
