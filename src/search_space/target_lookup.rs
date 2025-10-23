// 3rd party imports
use crate::search_space::macpepdb_client::MaCPepDBClient;
use anyhow::{anyhow, Result};
use macpepdb::database::scylla::peptide_table::PeptideTable;

/// Path for MaCPepDB web API, with `:sequence:` as placeholder for the sequence
const MACPEPDB_WEB_API_PATH: &str = "/api/peptides/:sequence:/exists";

/// Path for MaCPepDB BloomFilter web API, with `:sequence:` as placeholder for the sequence
const MACPEPDB_BLOOM_FILTER_WEB_API_PATH: &str = "/lookup/everywhere/:sequence:";

/// Checks if a sequence is a target using the database or the web API
///
pub struct TargetLookup {
    url: String,
    client: MaCPepDBClient,
}

impl TargetLookup {
    /// Create a new WebApiTargetLookup
    ///
    /// # Arguments
    /// * `url` - URL to the web API endpoint, e.g. `http://...` or `bloom+http://...` or directly to the database `scylla://host1,host2,host3/keyspace`
    pub async fn new(url: &str) -> Result<Self> {
        let url = if url.starts_with("http") || url.starts_with("https") {
            format!("{}{}", url, MACPEPDB_WEB_API_PATH)
        } else if url.starts_with("bloom+http") || url.starts_with("bloom+https") {
            format!("{}{}", &url[6..], MACPEPDB_BLOOM_FILTER_WEB_API_PATH)
        } else {
            url.to_string()
        };

        let client = MaCPepDBClient::from_url(&url).await?;

        Ok(Self { url, client })
    }

    /// Check if a sequence is a target
    ///
    /// # Arguments
    /// * `sequence` - Sequence to be checked
    ///
    pub async fn is_target(&self, sequence: &str) -> Result<bool> {
        match &self.client {
            MaCPepDBClient::Db(client, configuration) => {
                Ok(PeptideTable::exists_by_sequence(client, sequence, configuration).await?)
            }
            MaCPepDBClient::Http(client) => {
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
