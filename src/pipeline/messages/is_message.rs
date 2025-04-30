use serde::{de::DeserializeOwned, Serialize};

use crate::pipeline::errors::pipeline_error::PipelineError;

/// Trait to sum up the common properties of all messages
///
pub trait IsMessage: Sized + Send + Sync + Serialize + DeserializeOwned {
    /// Converts message to an error message with the given error
    ///
    /// # Arguments
    /// * `error` - Error to be converted to an error message
    ///
    fn to_error_message(&self, error: PipelineError) -> super::error_message::ErrorMessage;
}
