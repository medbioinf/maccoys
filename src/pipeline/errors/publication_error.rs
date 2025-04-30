use thiserror::Error;

#[derive(Error, Debug)]
pub enum PublicationError {
    #[error("Error while publishing the file")]
    FilePublishingError(std::io::Error),
}
