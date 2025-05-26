use std::path::{Path, PathBuf};

use polars::frame::DataFrame;
use serde::{Deserialize, Serialize};
use tokio::fs::write;

use crate::pipeline::errors::pipeline_error::PipelineError;

use super::{
    error_message::ErrorMessage, is_message::IsMessage, publication_message::PublicationMessage,
    scoring_message::ScoringMessage,
};

const ID_PREFIX: &str = "maccoys_itentification_message";

/// Indexing message
///
#[derive(Serialize, Deserialize)]
pub struct IdentificationMessage {
    /// Search uuid
    uuid: String,
    /// MS run name
    ms_run_name: String,
    /// Spectrum ID
    spectrum_id: String,
    /// mzML containing only one MS/MS spectrum and the related MS spectrum
    mzml: Vec<u8>,
    /// fasta file
    fasta: Vec<u8>,
    /// Precursor (m/z, charge) used for generating the search space
    precursor: (f64, u8),
}

impl IdentificationMessage {
    /// Create a new search space generation message
    ///
    pub fn new(
        uuid: String,
        ms_run_name: String,
        spectrum_id: String,
        mzml: Vec<u8>,
        fasta: Vec<u8>,
        precursor: (f64, u8),
    ) -> Self {
        Self {
            uuid,
            ms_run_name,
            spectrum_id,
            mzml,
            fasta,
            precursor,
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

    /// Get the spectrum mzML
    ///
    pub fn mzml(&self) -> &Vec<u8> {
        &self.mzml
    }

    /// Get the fasta file
    ///
    pub fn fasta(&self) -> &Vec<u8> {
        &self.fasta
    }

    /// Get the precursor (m/z, charge) used for generating the search space
    ///
    pub fn precursor(&self) -> &(f64, u8) {
        &self.precursor
    }

    /// Writes the mzML to the given path and removes it from the message
    /// to free up some memory.
    ///
    /// # Arguments
    /// * `path` - The path to write the mzML file to
    ///
    pub async fn write_spectrum_mzml_file(&mut self, path: &Path) -> std::io::Result<()> {
        let contents = std::mem::take(&mut self.mzml);
        write(path, contents).await
    }

    /// Writes the mzML to the given path and removes it from the message
    /// to free up some memory.
    ///
    /// # Arguments
    /// * `path` - The path to write the mzML file to
    ///
    pub async fn write_fasta_file(&mut self, path: &Path) -> std::io::Result<()> {
        let contents = std::mem::take(&mut self.fasta);
        write(path, contents).await
    }

    pub fn into_publication_message(
        &self,
        file_path: PathBuf,
        content: Vec<u8>,
    ) -> PublicationMessage {
        PublicationMessage::new(
            self.uuid.clone(),
            self.ms_run_name.clone(),
            self.spectrum_id.clone(),
            file_path,
            false,
            content,
        )
    }

    /// Converts the message into a scoring message
    ///  
    /// # Arguments
    /// * `psms` - The PSMs to be used for scoring
    ///
    pub fn into_scoring_message(&self, psms: DataFrame) -> ScoringMessage {
        ScoringMessage::new(
            self.uuid.clone(),
            self.ms_run_name.clone(),
            self.spectrum_id.clone(),
            self.precursor,
            psms,
        )
    }
}

impl IsMessage for IdentificationMessage {
    fn to_error_message(&self, error: PipelineError) -> ErrorMessage {
        ErrorMessage::new(
            self.uuid.clone(),
            Some(self.ms_run_name.clone()),
            Some(self.spectrum_id.clone()),
            Some(self.precursor),
            error,
        )
    }

    fn get_id(&self) -> String {
        format!(
            "{}_{}_{}_{}_{}_{}",
            ID_PREFIX,
            self.uuid,
            self.ms_run_name,
            self.spectrum_id,
            self.precursor.0,
            self.precursor.1,
        )
    }
}
