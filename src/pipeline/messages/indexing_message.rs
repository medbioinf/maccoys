use dihardts_omicstools::proteomics::io::mzml::elements::spectrum::Spectrum;
use ndarray::Array1;
use serde::{Deserialize, Serialize};

use crate::{
    pipeline::errors::{indexing_error::IndexingError, pipeline_error::PipelineError},
    precursor::Precursor,
};

use super::{
    error_message::ErrorMessage, is_message::IsMessage,
    search_space_generation_message::SearchSpaceGenerationMessage,
};

const ID_PREFIX: &str = "maccoys_indexing_message";

/// Indexing message
///
#[derive(Serialize, Deserialize)]
pub struct IndexingMessage {
    /// Search uuid
    uuid: String,
    /// MS run names
    ms_run_names: Vec<String>,
}

impl IndexingMessage {
    /// Create a new indexing message
    pub fn new(uuid: String, ms_run_names: Vec<String>) -> Self {
        Self { uuid, ms_run_names }
    }

    /// Get the search uuid
    pub fn uuid(&self) -> &String {
        &self.uuid
    }

    /// Get the MS run name
    pub fn ms_run_names(&self) -> &Vec<String> {
        &self.ms_run_names
    }

    /// Converts this message into a search space generation message which would be sent to the search space generation module
    ///
    /// # Arguments
    /// * `ms_run_name` - The name of the MS run
    /// * `spectrum_id` - The spectrum ID
    /// * `mzml` - Content of mzML with only one MS/MS spectrum and the related MS spectrum
    ///
    pub fn into_search_space_generation_message(
        &self,
        ms_run_name: String,
        spectrum_id: String,
        spectrum: Spectrum,
        precursors: Vec<Precursor>,
    ) -> Result<SearchSpaceGenerationMessage, IndexingError> {
        let mz_list = spectrum
            .binary_data_array_list
            .get_mz_array()
            .map_err(IndexingError::MzListReadError)?
            .deflate_data()
            .map_err(IndexingError::MzListReadError)?;

        let intensity_list = spectrum
            .binary_data_array_list
            .get_intensity_array()
            .map_err(IndexingError::IntensityListReadError)?
            .deflate_data()
            .map_err(IndexingError::IntensityListReadError)?;

        Ok(SearchSpaceGenerationMessage::new(
            self.uuid.clone(),
            ms_run_name,
            spectrum_id,
            Array1::from(mz_list),
            Array1::from(intensity_list),
            precursors,
        ))
    }
}

impl IsMessage for IndexingMessage {
    fn to_error_message(&self, error: PipelineError) -> ErrorMessage {
        ErrorMessage::new(self.uuid.clone(), None, None, None, error)
    }

    fn get_id(&self) -> String {
        format!("{}_{}_indexing", ID_PREFIX, self.uuid,)
    }
}
