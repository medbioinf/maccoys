use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::pipeline::errors::pipeline_error::PipelineError;

use super::{error_message::ErrorMessage, is_message::IsMessage};

/// Publication message
///
#[derive(Serialize, Deserialize)]
pub struct PublicationMessage {
    /// Search uuid
    uuid: String,
    /// MS run name
    ms_run_name: String,
    /// Spectrum ID
    spectrum_id: String,
    /// Relative target file path
    file_path: PathBuf,
    /// File content
    content: Vec<u8>,
}

impl PublicationMessage {
    /// Create a new search space generation message
    ///
    pub fn new(
        uuid: String,
        ms_run_name: String,
        spectrum_id: String,
        file_path: PathBuf,
        content: Vec<u8>,
    ) -> Self {
        Self {
            uuid,
            ms_run_name,
            spectrum_id,
            file_path,
            content,
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

    /// Get the rleative target file path
    ///
    pub fn file_path(&self) -> &PathBuf {
        &self.file_path
    }

    /// Get the target file content
    ///
    pub fn content(&self) -> &Vec<u8> {
        &self.content
    }

    /// Takes the content and replaces it with an empty vector
    ///
    pub fn take_content(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.content)
    }
}

impl IsMessage for PublicationMessage {
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
