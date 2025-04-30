use serde::{Deserialize, Serialize};

use crate::pipeline::errors::pipeline_error::PipelineError;

use super::{
    error_message::ErrorMessage, identification_message::IdentificationMessage,
    is_message::IsMessage,
};

/// Search space generation message
///
#[derive(Serialize, Deserialize)]
pub struct SearchSpaceGenerationMessage {
    /// Search uuid
    uuid: String,
    /// MS run name
    ms_run_name: String,
    /// Spectrum ID
    spectrum_id: String,
    /// mzML containing only one MS/MS spectrum and the related MS spectrum
    mzml: Vec<u8>,
}

impl SearchSpaceGenerationMessage {
    /// Create a new search space generation message
    ///
    pub fn new(uuid: String, ms_run_name: String, spectrum_id: String, mzml: Vec<u8>) -> Self {
        Self {
            uuid,
            ms_run_name,
            spectrum_id,
            mzml,
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

    /// Converts this into a IdenitificationMessage
    ///
    /// # Arguments
    /// * `fasta` - Fasta file
    /// * `precursor` - Precursor (m/z, charge) used for generating the search space
    ///
    pub fn into_identification_message(
        &self,
        fasta: Vec<u8>,
        precursor: (f64, u8),
    ) -> IdentificationMessage {
        IdentificationMessage::new(
            self.uuid.clone(),
            self.ms_run_name.clone(),
            self.spectrum_id.clone(),
            self.mzml.clone(),
            fasta,
            precursor,
        )
    }

    /// Converts this into a Error message with precursor
    ///
    /// # Arguments
    /// * `error` - Error
    /// * `precursor` - Precursor (m/z, charge) used for generating the search space
    ///
    pub fn to_error_message_with_precursor(
        &self,
        error: PipelineError,
        precursor: (f64, u8),
    ) -> ErrorMessage {
        ErrorMessage::new(
            self.uuid.clone(),
            self.ms_run_name.clone(),
            Some(self.spectrum_id.clone()),
            Some(precursor),
            error,
        )
    }
}

impl IsMessage for SearchSpaceGenerationMessage {
    fn to_error_message(&self, error: PipelineError) -> ErrorMessage {
        ErrorMessage::new(
            self.uuid.clone(),
            self.ms_run_name.clone(),
            Some(self.spectrum_id.clone()),
            None,
            error,
        )
    }
}
