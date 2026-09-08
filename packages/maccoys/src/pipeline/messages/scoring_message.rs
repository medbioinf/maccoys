use std::{fmt::Debug, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::{
    peptide_spectrum_match::PeptideSpectrumMatchCollection,
    pipeline::errors::{pipeline_error::PipelineError, scoring_error::IntoPublicationMessageError},
    precursor::Precursor,
};

use super::{
    error_message::ErrorMessage, is_message::IsMessage, publication_message::PublicationMessage,
};

const ID_PREFIX: &str = "maccoys_scoring_message";

/// Scoring message
///
#[derive(PartialEq, Serialize, Deserialize)]
pub struct ScoringMessage {
    /// Search uuid
    uuid: String,
    /// MS run name
    ms_run_name: String,
    /// Spectrum ID
    spectrum_id: String,
    /// Precursor (m/z, charge) used for generating the search space
    precursor: Precursor,
    /// PSMs
    psms: PeptideSpectrumMatchCollection,
}

impl ScoringMessage {
    /// Create a new search space generation message
    ///
    pub fn new(
        uuid: String,
        ms_run_name: String,
        spectrum_id: String,
        precursor: Precursor,
        psms: PeptideSpectrumMatchCollection,
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
    pub fn precursor(&self) -> &Precursor {
        &self.precursor
    }

    /// Get the PSMs
    ///
    pub fn psms(&self) -> &PeptideSpectrumMatchCollection {
        &self.psms
    }

    /// get PSMs mutable
    ///
    pub fn psms_mut(&mut self) -> &mut PeptideSpectrumMatchCollection {
        &mut self.psms
    }

    /// Create publication message for the PSMs
    ///
    /// # Arguments
    /// * `file_path` - The relative path to the file to write content to
    ///
    pub async fn into_final_publication_message(
        self,
        file_path: PathBuf,
    ) -> Result<PublicationMessage, (Self, IntoPublicationMessageError)> {
        // Lets assume the Parquet file has roughly half the same size as the PSMs due to compression
        let mut content: Vec<u8> = Vec::with_capacity(self.psms.size() / 2);

        match self.psms().write_parquet(&mut content).await {
            Ok(_) => (),
            Err(err) => {
                return Err((
                    self,
                    IntoPublicationMessageError::ParquetConversionError(Box::new(err)),
                ))
            }
        }

        Ok(PublicationMessage::new(
            self.uuid,
            self.ms_run_name,
            self.spectrum_id,
            file_path,
            true,
            content,
        ))
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
}

impl IsMessage for ScoringMessage {
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
