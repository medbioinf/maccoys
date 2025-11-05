use std::path::PathBuf;

use ndarray::Array1;
use serde::{Deserialize, Serialize};

use crate::{
    peptide_spectrum_match::PeptideSpectrumMatchCollection,
    pipeline::errors::{identification_error::IdentificationError, pipeline_error::PipelineError},
    precursor::Precursor,
};

use super::{
    error_message::ErrorMessage, is_message::IsMessage, publication_message::PublicationMessage,
    scoring_message::ScoringMessage,
};

const ID_PREFIX: &str = "maccoys_itentification_message";

/// Indexing message
///
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IdentificationMessage {
    /// Search uuid
    uuid: String,
    /// MS run name
    ms_run_name: String,
    /// Spectrum ID
    spectrum_id: String,
    /// m/z
    mz_list: Array1<f64>,
    /// Intensity
    intensity_list: Array1<f64>,
    /// Precursor (m/z, charge) used for generating the search space
    precursor: Precursor,
    /// peptide candidates in ProForma format
    peptides: Vec<String>,
}

impl IdentificationMessage {
    /// Create a new search space generation message
    ///
    pub fn new(
        uuid: String,
        ms_run_name: String,
        spectrum_id: String,
        mz_list: Array1<f64>,
        intensity_list: Array1<f64>,
        precursor: Precursor,
        peptides: Vec<String>,
    ) -> Self {
        Self {
            uuid,
            ms_run_name,
            spectrum_id,
            mz_list,
            intensity_list,
            precursor,
            peptides,
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

    /// Get the m/z list
    ///
    pub fn mz_list(&self) -> &Array1<f64> {
        &self.mz_list
    }

    /// Get the intensity list
    ///
    pub fn intensity_list(&self) -> &Array1<f64> {
        &self.intensity_list
    }

    /// Get the precursor (m/z, charge) used for generating the search space
    ///
    pub fn precursor(&self) -> &Precursor {
        &self.precursor
    }

    /// Get the peptide candidates in ProForma format
    ///
    pub fn peptides(&self) -> &Vec<String> {
        &self.peptides
    }

    /// Takes and returns peptides, leaving an empty vector in its place
    ///
    pub fn take_peptides(&mut self) -> Vec<String> {
        std::mem::take(&mut self.peptides)
    }

    /// Creates a publication message for non result publication
    ///
    /// # Arguments
    /// * `file_path` - The relative path to the file to write content to
    /// * `content` - The content of the CSV file
    ///
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
    pub fn into_scoring_message(
        &self,
        psms: PeptideSpectrumMatchCollection,
    ) -> Result<ScoringMessage, IdentificationError> {
        Ok(ScoringMessage::new(
            self.uuid.clone(),
            self.ms_run_name.clone(),
            self.spectrum_id.clone(),
            self.precursor.clone(),
            psms.try_into()
                .map_err(IdentificationError::PsmCollectionToDataFrameError)?,
        ))
    }
}

impl IsMessage for IdentificationMessage {
    fn to_error_message(&self, error: PipelineError) -> ErrorMessage {
        ErrorMessage::new(
            self.uuid.clone(),
            Some(self.ms_run_name.clone()),
            Some(self.spectrum_id.clone()),
            Some(self.precursor.clone()),
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
            self.precursor.mz(),
            self.precursor.charge(),
        )
    }
}
