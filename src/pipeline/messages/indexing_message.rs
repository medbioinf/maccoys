use serde::{Deserialize, Serialize};

use crate::pipeline::errors::pipeline_error::PipelineError;

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
        mzml: String,
    ) -> SearchSpaceGenerationMessage {
        SearchSpaceGenerationMessage::new(self.uuid.clone(), ms_run_name, spectrum_id, mzml.into())
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
