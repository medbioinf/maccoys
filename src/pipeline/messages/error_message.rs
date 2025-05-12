use std::{fmt::Display, io::Cursor};

use serde::{Deserialize, Serialize};

use crate::pipeline::errors::pipeline_error::PipelineError;

use super::is_message::IsMessage;

const ID_PREFIX: &str = "maccoys_error_message";

#[derive(Serialize, Deserialize)]
pub struct ErrorMessage {
    /// Search uuid
    uuid: String,
    /// MS run
    ms_run_name: String,
    /// spectrum id
    spectrum_id: Option<String>,
    /// Precursor (m/z, charge)
    precursor: Option<(f64, u8)>,
    /// Error message
    error: String,
}

impl ErrorMessage {
    pub fn new(
        uuid: String,
        ms_run_name: String,
        spectrum_id: Option<String>,
        precursor: Option<(f64, u8)>,
        error: PipelineError,
    ) -> Self {
        let error = format!("{}", error);
        Self {
            uuid,
            ms_run_name,
            spectrum_id,
            precursor,
            error,
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
    ///
    pub fn spectrum_id(&self) -> &Option<String> {
        &self.spectrum_id
    }

    /// Get the precursor
    ///
    pub fn precursor(&self) -> &Option<(f64, u8)> {
        &self.precursor
    }

    /// Get the error message
    ///
    pub fn error(&self) -> &String {
        &self.error
    }
}

impl IsMessage for ErrorMessage {
    fn to_error_message(&self, error: PipelineError) -> Self {
        Self::new(
            self.uuid.clone(),
            self.ms_run_name.clone(),
            self.spectrum_id.clone(),
            self.precursor,
            error,
        )
    }

    fn get_id(&self) -> String {
        let mut cursor = Cursor::new(self.error.as_bytes());
        format!(
            "{}_{}_{}_{}_{}_{}",
            ID_PREFIX,
            self.uuid,
            self.ms_run_name,
            self.spectrum_id.as_deref().unwrap_or(""),
            self.precursor
                .map(|(mz, charge)| format!("{}_{}", mz, charge))
                .unwrap_or_default(),
            murmur3::murmur3_x64_128(&mut cursor, 0).unwrap_or(self.error.len() as u128),
        )
    }
}

impl Display for ErrorMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut location = format!("{} / {}", &self.uuid, &self.ms_run_name);
        if let Some(spectrum_id) = &self.spectrum_id {
            location.push_str(&format!(" / {}", spectrum_id));
        }
        if let Some(precursor) = &self.precursor {
            location.push_str(&format!(" / ({}, {})", precursor.0, precursor.1));
        }
        write!(f, "[{}] {}", location, self.error)
    }
}
