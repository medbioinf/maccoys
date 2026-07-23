// 3rd party imports
use crate::search_space::macpepdb_client::MaCPepDBClient;
use anyhow::{anyhow, Result};
use futures::StreamExt;
use macpepdb::{
    peptide::Peptide,
    peptide_search::PeptideSearchType,
    peptide_table::PeptideTable,
    sequence::{IsSimpleSequence, PeptideSequence},
};
use tokio_postgres::types::ToSql;

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

        let client = MaCPepDBClient::from_url(&url, PeptideSearchType::MultiTask).await?;

        Ok(Self { url, client })
    }

    /// Check if a sequence is a target
    ///
    /// # Arguments
    /// * `sequence` - Sequence to be checked
    ///
    pub async fn is_target(&self, sequence: &str) -> Result<bool> {
        match &self.client {
            MaCPepDBClient::Db(client, configuration, _) => {
                let sequence = PeptideSequence::try_from(sequence)
                    .map_err(|e| anyhow!("Invalid sequence: {}", e))?;
                let mass =
                    Peptide::peptide_mass_from_amino_acid_bits(sequence.amino_acid_bit_codes());
                let partitions = configuration
                    .mass_partitioning()
                    .partition_by_mass(mass)
                    .map(|(_, mass)| mass)
                    .collect::<Vec<_>>();

                let params: Vec<Box<dyn ToSql + Sync + Send>> = vec![
                    Box::new(partitions) as Box<dyn ToSql + Sync + Send>,
                    Box::new(mass) as Box<dyn ToSql + Sync + Send>,
                    Box::new(sequence) as Box<dyn ToSql + Sync + Send>,
                ];

                // TODO: Implement exist query on peptide table
                let peptide = PeptideTable::new(client.clone())
                    .select(
                        "WHERE partitions = ANY($1) AND mass = $2 AND sequence = $3 LIMIT 1",
                        params,
                    )
                    .await
                    .map_err(|e| anyhow!("Unable to query peptides: {}", e))?
                    .next()
                    .await;

                match peptide {
                    Some(Ok(_)) => Ok(true),
                    Some(Err(e)) => Err(anyhow!("Error querying peptides: {}", e)),
                    None => Ok(false),
                }
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
