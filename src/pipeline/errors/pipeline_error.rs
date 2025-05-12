use thiserror::Error;

use super::{
    identification_error::IdentificationError, indexing_error::IndexingError,
    publication_error::PublicationError, queue_error::QueueError, scoring_error::ScoringError,
    search_space_generation_error::SearchSpaceGenerationError, storage_error::StorageError,
};

/// Merges all task errors into a single error type
/// including some shared errors
///
#[derive(Error, Debug)]
pub enum PipelineError {
    // Taks errors
    #[error("Indexing error: {0}")]
    IndexingError(#[from] IndexingError),
    #[error("Search space generation error: {0}")]
    SearchSpaceGenerationError(#[from] SearchSpaceGenerationError),
    #[error("Identification error: {0}")]
    IdentificationError(#[from] IdentificationError),
    #[error("Scoring error: {0}")]
    ScoringError(#[from] ScoringError),
    #[error("Publication error: {0}")]
    PublicationError(#[from] PublicationError),
    // Queue errors
    #[error("Queueiung error: {0}")]
    QueueError(#[from] QueueError),
    // Common errors
    #[error("Unable to create directory")]
    DirectoryCreationError(std::io::Error),
    #[error("Unable to read file: `{0}`:\n\t{1}")]
    FileReadError(String, std::io::Error),
    #[error("Unable to write file: `{0}`:\n\t{1}")]
    ConfigDeserializationError(String, toml::de::Error),
    #[error("Unable to join task: {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("Unable to register signal handler: {0}")]
    SignalHandlerError(std::io::Error),
    // Storage errors
    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),
    // Errors in error handler
    #[error("Unable to open log file")]
    OpenLogError(std::io::Error),
    #[error("Unable to write to log file")]
    WriteLogError(std::io::Error),
}
