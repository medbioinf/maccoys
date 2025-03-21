// 3rd party imports
use anyhow::{bail, Result};
use macpepdb::{
    chemistry::amino_acid::calc_sequence_mass_int,
    database::{
        generic_client::GenericClient,
        scylla::{
            client::Client as DbClient, configuration_table::ConfigurationTable,
            peptide_table::PeptideTable,
        },
    },
    entities::{configuration::Configuration, peptide::Peptide},
    tools::peptide_partitioner::get_mass_partition,
};
use reqwest::Client as HttpClient;
use serde_json::json;

/// Path to MaCCoyS web API for decoy caching
const MACCOYS_WEB_API_PATH: &str = "/api/decoys/insert";

/// Path for MaCPepDB web API, with `:sequence:` as placeholder for the sequence
///
enum Client {
    Db(DbClient, Configuration), // no ARC needed as the client is only used in this threads
    Http(HttpClient),
}

/// Insert generated decoys into the cache database using the web API or directly database access
///
pub struct DecoyCache {
    url: String,
    client: Client,
}

impl DecoyCache {
    pub async fn new(url: &str) -> Result<Self> {
        let (url, client) = if url.starts_with("http") {
            (
                format!("{}{}", url, MACCOYS_WEB_API_PATH),
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

    pub async fn cache(&self, decoys: Vec<(String, i16)>) -> Result<()> {
        match &self.client {
            Client::Db(client, configuration) => {
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
            Client::Http(client) => {
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
