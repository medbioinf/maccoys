use std::{io::Cursor, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::pipeline::errors::pipeline_error::PipelineError;

use super::{error_message::ErrorMessage, is_message::IsMessage};

const ID_PREFIX: &str = "maccoys_publication_message";

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
    /// If true the spectrum count is updated in the storage
    update_spectrum_count: bool,
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
        update_spectrum_count: bool,
        content: Vec<u8>,
    ) -> Self {
        Self {
            uuid,
            ms_run_name,
            spectrum_id,
            file_path,
            update_spectrum_count,
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

    /// Is the publication successful
    ///
    pub fn update_spectrum_count(&self) -> bool {
        self.update_spectrum_count
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
            Some(self.ms_run_name.clone()),
            Some(self.spectrum_id.clone()),
            None,
            error,
        )
    }

    fn get_id(&self) -> String {
        let lossy_file_path = self.file_path.to_string_lossy();
        let mut path_cursor = Cursor::new(lossy_file_path.as_bytes());
        let mut content_cursor = Cursor::new(&self.content);

        format!(
            "{}_{}_{}_{}_{}_{}",
            ID_PREFIX,
            self.uuid,
            self.ms_run_name,
            self.spectrum_id,
            murmur3::murmur3_x64_128(&mut path_cursor, 0).unwrap_or(lossy_file_path.len() as u128),
            murmur3::murmur3_x64_128(&mut content_cursor, 0).unwrap_or(self.content.len() as u128),
        )
    }
}
