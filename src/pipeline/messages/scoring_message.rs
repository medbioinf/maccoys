use std::{fmt::Debug, path::PathBuf};

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::pipeline::errors::pipeline_error::PipelineError;

use super::{
    error_message::ErrorMessage, is_message::IsMessage, publication_message::PublicationMessage,
};

/// Scoring message
///
#[derive(Serialize, Deserialize)]
pub struct ScoringMessage {
    /// Search uuid
    uuid: String,
    /// MS run name
    ms_run_name: String,
    /// Spectrum ID
    spectrum_id: String,
    /// Precursor (m/z, charge) used for generating the search space
    precursor: (f64, u8),
    /// PSMs
    psms: DataFrame,
}

impl ScoringMessage {
    /// Create a new search space generation message
    ///
    pub fn new(
        uuid: String,
        ms_run_name: String,
        spectrum_id: String,
        precursor: (f64, u8),
        psms: DataFrame,
    ) -> Self {
        Self {
            uuid,
            ms_run_name,
            spectrum_id,
            precursor,
            psms,
        }
    }

    /// Get the search uuid
    ///
    pub fn uuid(&self) -> &String {
        &self.uuid
    }

    /// Get the MS run name
    ///
    pub fn ms_run_name(&self) -> &String {
        &self.ms_run_name
    }

    /// Get the spectrum ID
    pub fn spectrum_id(&self) -> &String {
        &self.spectrum_id
    }

    /// Get the precursor (m/z, charge) used for generating the search space
    ///
    pub fn precursor(&self) -> &(f64, u8) {
        &self.precursor
    }

    /// Get the PSMs
    ///
    pub fn psms(&self) -> &DataFrame {
        &self.psms
    }

    pub fn psms_mut(&mut self) -> &mut DataFrame {
        &mut self.psms
    }

    /// Get the PSMs as a CSV
    ///
    /// # Arguments
    /// * `file_path` - The relative path to the file to write content to
    pub fn into_publication_message(
        mut self,
        file_path: PathBuf,
    ) -> Result<PublicationMessage, Box<IntoPublicationMessageError>> {
        // Lets assume the CSV has roughly the same size as the PSMs dataframe
        let mut content: Vec<u8> =
            Vec::with_capacity(self.psms.estimated_size() * std::mem::size_of::<u8>());

        match CsvWriter::new(&mut content)
            .include_header(true)
            .with_separator(b'\t')
            .finish(&mut self.psms)
        {
            Ok(_) => (),
            Err(e) => {
                return Err(Box::new(IntoPublicationMessageError::CsvWriteError(
                    e, self,
                )));
            }
        }

        Ok(PublicationMessage::new(
            self.uuid,
            self.ms_run_name,
            self.spectrum_id,
            file_path,
            content,
        ))
    }
}

impl IsMessage for ScoringMessage {
    fn to_error_message(&self, error: PipelineError) -> ErrorMessage {
        ErrorMessage::new(
            self.uuid.clone(),
            self.ms_run_name.clone(),
            Some(self.spectrum_id.clone()),
            Some(self.precursor),
            error,
        )
    }
}

impl Debug for ScoringMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScoringMessage")
            .field("uuid", &self.uuid)
            .field("ms_run_name", &self.ms_run_name)
            .field("spectrum_id", &self.spectrum_id)
            .field("precursor", &self.precursor)
            .finish()
    }
}

#[derive(Debug, Error)]
pub enum IntoPublicationMessageError {
    #[error("Error writing PSMs to CSV:\n\t{0}")]
    CsvWriteError(PolarsError, ScoringMessage),
}
