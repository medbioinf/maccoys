// 3rd party imports
use anyhow::Result;
use macpepdb::{
    chemistry::amino_acid::calc_sequence_mass,
    database::scylla::client::{Client as DbClient, GenericClient},
    database::{
        configuration_table::ConfigurationTable as ConfigurationTableTrait,
        scylla::{
            configuration_table::ConfigurationTable,
            peptide_table::{PeptideTable, UPDATE_SET_PLACEHOLDER},
        },
        table::Table,
    },
    entities::{configuration::Configuration, peptide::Peptide},
    tools::peptide_partitioner::get_mass_partition,
};
use reqwest::Client as HttpClient;
use serde_json::json;

/// Trait for storing decoys in a MaCPepDB-like cache database
///
pub trait DecoyCache: Sized {
    /// Create a new DecoyCache
    ///
    /// # Arguments
    /// * `cache_url` - URL of the cache (database or web url)
    async fn new(cache_url: &str) -> Result<Self>;

    /// Put a list od decoys into the cache
    ///
    /// # Arguments
    /// * `decoys` - List of decoys to be cached
    ///
    async fn cache(&self, decoys: Vec<(String, i16)>) -> Result<()>;
}

/// DecoyCache implementation with direct accedd to a MaCPepDB-like cache database
///
pub struct WebApiDecoyCache {
    cache_url: String,
    client: HttpClient,
}

impl DecoyCache for WebApiDecoyCache {
    async fn new(cache_url: &str) -> Result<Self> {
        Ok(WebApiDecoyCache {
            cache_url: format!("{}/api/decoys/insert", cache_url),
            client: HttpClient::new(),
        })
    }

    async fn cache(&self, decoys: Vec<(String, i16)>) -> Result<()> {
        self.client
            .post(&self.cache_url)
            .json(&json!(
                {
                    "decoys": decoys
                }
            ))
            .send()
            .await?;
        Ok(())
    }
}

/// DecoyCache implementation with direct access to a MaCPepDB-like cache database
///
pub struct DatabaseDecoyCache {
    client: DbClient,
    macpepdb_conf: Configuration,
}

impl DecoyCache for DatabaseDecoyCache {
    async fn new(cache_url: &str) -> Result<Self> {
        let client = DbClient::new(cache_url).await?;

        let configuration = ConfigurationTable::select(&client).await?;

        Ok(DatabaseDecoyCache {
            client: client,
            macpepdb_conf: configuration,
        })
    }

    async fn cache(&self, decoys: Vec<(String, i16)>) -> Result<()> {
        let decoys: Vec<Peptide> = decoys
            .into_iter()
            .map(|(seq, missed_cleavages)| {
                let mass = calc_sequence_mass(seq.as_str())?;
                let partition =
                    get_mass_partition(self.macpepdb_conf.get_partition_limits(), mass)?;
                Peptide::new(
                    partition as i64,
                    mass,
                    seq,
                    missed_cleavages,
                    Vec::new(),
                    false,
                    false,
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                )
            })
            .collect::<Result<Vec<Peptide>>>()?;

        let statement = format!(
            "UPDATE {}.{} SET {}, is_metadata_updated = false WHERE partition = ? and mass = ? and sequence = ?",
            self.client.get_database(),
            PeptideTable::table_name(),
            UPDATE_SET_PLACEHOLDER.as_str()
        );

        let prepared = self.client.get_session().prepare(statement).await?;
        PeptideTable::bulk_insert(&self.client, decoys.iter(), &prepared).await?;

        Ok(())
    }
}
