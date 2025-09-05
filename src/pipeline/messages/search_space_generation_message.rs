use std::io::Cursor;

use ndarray::Array1;
use serde::{Deserialize, Serialize};

use crate::{pipeline::errors::pipeline_error::PipelineError, precursor::Precursor};

use super::{
    error_message::ErrorMessage, identification_message::IdentificationMessage,
    is_message::IsMessage,
};

const ID_PREFIX: &str = "maccoys_search_space_generation_message";

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
    /// m/z
    mz_list: Array1<f64>,
    /// Intensity
    intensity_list: Array1<f64>,
    /// Precursor Vec<(m/z, charge)> used for generating the search space
    precursors: Vec<Precursor>,
}

impl SearchSpaceGenerationMessage {
    /// Create a new search space generation message
    ///
    pub fn new(
        uuid: String,
        ms_run_name: String,
        spectrum_id: String,
        mz_list: Array1<f64>,
        intensity_list: Array1<f64>,
        precursors: Vec<Precursor>,
    ) -> Self {
        Self {
            uuid,
            ms_run_name,
            spectrum_id,
            mz_list,
            intensity_list,
            precursors,
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

    /// Get the precursors (m/z, charge) used for generating the search space
    ///
    pub fn precursors(&self) -> &[Precursor] {
        &self.precursors
    }

    /// Converts this into a IdenitificationMessage
    ///
    /// # Arguments
    /// * `precursor` - Precursor (m/z, charge) used for generating the search space
    /// * `peptides` - peptide candidates in ProForma format
    ///
    pub fn into_identification_message(
        &self,
        precursor: Precursor,
        peptides: Vec<String>,
    ) -> IdentificationMessage {
        IdentificationMessage::new(
            self.uuid.clone(),
            self.ms_run_name.clone(),
            self.spectrum_id.clone(),
            self.mz_list.clone(),
            self.intensity_list.clone(),
            precursor,
            peptides,
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
        precursor: Precursor,
    ) -> ErrorMessage {
        ErrorMessage::new(
            self.uuid.clone(),
            Some(self.ms_run_name.clone()),
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
            Some(self.ms_run_name.clone()),
            Some(self.spectrum_id.clone()),
            None,
            error,
        )
    }

    fn get_id(&self) -> String {
        let spectrum_characteristics_string = format!(
            "{}|{}|{}",
            self.mz_list()
                .iter()
                .map(|x| format!("{x}"))
                .collect::<Vec<String>>()
                .join(","),
            self.intensity_list()
                .iter()
                .map(|x| format!("{x}"))
                .collect::<Vec<String>>()
                .join(","),
            self.precursors()
                .iter()
                .map(|p| format!("{}:{}", p.mz(), p.charge()))
                .collect::<Vec<String>>()
                .join(","),
        );

        let mut cursor = Cursor::new(&spectrum_characteristics_string);
        format!(
            "{}_{}_{}_{}_{}",
            ID_PREFIX,
            self.uuid,
            self.ms_run_name,
            self.spectrum_id,
            murmur3::murmur3_x64_128(&mut cursor, 0).unwrap_or(self.uuid.len() as u128),
        )
    }
}
