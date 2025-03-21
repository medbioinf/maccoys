// std imports
use std::sync::Arc;

// 3rd party imports
use anyhow::Result;
use axum::extract::{Json, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use macpepdb::chemistry::amino_acid::calc_sequence_mass_int;
use macpepdb::database::scylla::client::Client;
use macpepdb::database::scylla::peptide_table::PeptideTable;
use macpepdb::entities::{configuration::Configuration, peptide::Peptide as Decoy};
use macpepdb::tools::peptide_partitioner::get_mass_partition;
use macpepdb::web::web_error::WebError;
use serde::Deserialize;

/// Simple struct to deserialize the request body for decoy insertion
///
#[derive(Deserialize)]
pub struct InsertDecoyRequest {
    decoys: Vec<(String, i16)>,
}

impl InsertDecoyRequest {
    /// Returns the decoys
    ///
    /// # Arguments
    /// * `configuration` - MaCPepDB configuration
    ///
    pub fn get_decoys(&self, configuration: &Configuration) -> Result<Vec<Decoy>> {
        let mut decoys: Vec<Decoy> = Vec::with_capacity(self.decoys.len());
        for (sequence, missed_cleavages) in self.decoys.iter() {
            let mass = calc_sequence_mass_int(sequence)?;
            let partition = get_mass_partition(configuration.get_partition_limits(), mass)?;
            decoys.push(Decoy::new(
                partition as i64,
                mass,
                sequence.clone(),
                *missed_cleavages,
                Vec::new(),
                false,
                false,
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )?);
        }
        Ok(decoys)
    }
}

pub struct DecoyController;

impl DecoyController {
    /// Inserts decoys into the database.
    ///
    /// # Arguments
    /// * `db_client` - The database client
    /// * `configuration` - MaCPepDB configuration
    /// * `accession` - Protein accession extracted from URL path
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/decoys/insert`
    /// * Method: `POST`
    ///
    /// * Body:
    ///     ```json
    ///     {
    ///         # Array of decoy tuples (sequence, missed_cleavages)
    ///         "decoys": [
    ///             ["peptide_seq_0", missed_cleavages_0],
    ///             ["peptide_seq_1", missed_cleavages_1],
    ///            ...
    ///         ],
    ///     }
    ///     ```
    ///     Deserialized into [InsertDecoyRequest](InsertDecoyRequest)
    ///
    /// ## Response
    /// Response will be empty
    ///
    pub async fn insert_decoys(
        State((db_client, configuration)): State<(Arc<Client>, Arc<Configuration>)>,
        Json(payload): Json<InsertDecoyRequest>,
    ) -> Result<Response, WebError> {
        let decoys = payload.get_decoys(configuration.as_ref())?;

        let db_client = db_client.as_ref();
        PeptideTable::bulk_upsert(db_client, decoys.iter()).await?;

        Ok((StatusCode::OK, "").into_response())
    }
}
