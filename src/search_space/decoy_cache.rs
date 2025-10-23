// 3rd party imports
use anyhow::Result;
use macpepdb::{
    chemistry::amino_acid::calc_sequence_mass_int, database::scylla::peptide_table::PeptideTable,
    entities::peptide::Peptide, tools::peptide_partitioner::get_mass_partition,
};
use serde_json::json;

use crate::search_space::macpepdb_client::MaCPepDBClient;

/// Path to MaCCoyS web API for decoy caching
const MACCOYS_WEB_API_PATH: &str = "/api/decoys/insert";

/// Insert generated decoys into the cache database using the web API or directly database access
///
pub struct DecoyCache {
    url: String,
    client: MaCPepDBClient,
}

impl DecoyCache {
    pub async fn new(url: &str) -> Result<Self> {
        let url = if url.starts_with("http") || url.starts_with("https") {
            format!("{}{}", url, MACCOYS_WEB_API_PATH)
        } else {
            url.to_string()
        };

        let client = MaCPepDBClient::from_url(&url).await?;

        Ok(Self { url, client })
    }

    pub async fn cache(&self, decoys: Vec<(String, i16)>) -> Result<()> {
        match &self.client {
            MaCPepDBClient::Db(client, configuration) => {
                let decoys: Vec<Peptide> = decoys
                    .into_iter()
                    .map(|(seq, missed_cleavages)| {
                        let mass = calc_sequence_mass_int(seq.as_str())?;
                        let partition =
                            get_mass_partition(configuration.get_partition_limits(), mass)?;
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

                PeptideTable::bulk_upsert(client, decoys.iter()).await?;
            }
            MaCPepDBClient::Http(client) => {
                client
                    .post(&self.url)
                    .json(&json!(
                        {
                            "decoys": decoys
                        }
                    ))
                    .send()
                    .await?;
            }
        }
        Ok(())
    }
}
