// 3rd party imports
use anyhow::Result;
use macpepdb::sequence::PeptideSequence;

use crate::search_space::macpepdb_client::MaCPepDBClient;

/// Path to MaCCoyS web API for decoy caching
const MACCOYS_WEB_API_PATH: &str = "/api/decoys/insert";

/// Insert generated decoys into the cache database using the web API or directly database access
///
pub struct DecoyCache {
    _url: String,
    _client: MaCPepDBClient,
}

impl DecoyCache {
    pub async fn new(url: &str) -> Result<Self> {
        let url = if url.starts_with("http") || url.starts_with("https") {
            format!("{}{}", url, MACCOYS_WEB_API_PATH)
        } else {
            url.to_string()
        };

        let client =
            MaCPepDBClient::from_url(&url, macpepdb::peptide_search::PeptideSearchType::MultiTask)
                .await?;

        Ok(Self {
            _url: url,
            _client: client,
        })
    }

    pub async fn cache(&self, _decoys: Vec<(PeptideSequence, i16)>) -> Result<()> {
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
