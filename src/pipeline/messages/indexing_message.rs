use serde::{Deserialize, Serialize};

use crate::pipeline::errors::pipeline_error::PipelineError;

use super::{
    error_message::ErrorMessage, is_message::IsMessage,
    search_space_generation_message::SearchSpaceGenerationMessage,
};

/// Indexing message
///
#[derive(Serialize, Deserialize)]
pub struct IndexingMessage {
    /// Search uuid
    uuid: String,
    /// MS run name
    ms_run_name: String,
}

impl IndexingMessage {
    /// Create a new indexing message
    pub fn new(uuid: String, ms_run_name: String) -> Self {
        Self { uuid, ms_run_name }
    }

    /// Get the search uuid
    pub fn uuid(&self) -> &String {
        &self.uuid
    }

    /// Get the MS run name
    pub fn ms_run_name(&self) -> &String {
        &self.ms_run_name
    }

    /// Converts this message into a search space generation message which would be sent to the search space generation module
    ///
    /// # Arguments
    /// * `spectrum_id` - The spectrum ID
    /// * `mzml` - Content of mzML with only one MS/MS spectrum and the related MS spectrum
    ///
    pub fn into_search_space_generation_message(
        &self,
        spectrum_id: String,
        mzml: String,
    ) -> SearchSpaceGenerationMessage {
        SearchSpaceGenerationMessage::new(
            self.uuid.clone(),
            self.ms_run_name.clone(),
            spectrum_id,
            mzml.into(),
        )
    }
}

impl IsMessage for IndexingMessage {
    fn to_error_message(&self, error: PipelineError) -> ErrorMessage {
        ErrorMessage::new(
            self.uuid.clone(),
            self.ms_run_name.clone(),
            None,
            None,
            error,
        )
    }
}
