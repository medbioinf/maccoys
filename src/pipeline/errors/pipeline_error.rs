use thiserror::Error;

use super::{
    identification_error::IdentificationError, indexing_error::IndexingError,
    publication_error::PublicationError, scoring_error::ScoringError,
    search_space_generation_error::SearchSpaceGenerationError,
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
    // Common errors
    #[error("Unable to push message to queue")]
    MessageEnqueueError(),
    #[error("Unable to create directory")]
    DirectoryCreationError(std::io::Error),
    // Errors in error handler
    #[error("Unable to open log file")]
    OpenLogError(std::io::Error),
    #[error("Unable to write to log file")]
    WriteLogError(std::io::Error),
}
