use thiserror::Error;

#[derive(Error, Debug)]
pub enum MessageError {
    #[error("Failed to (de-)serialize message: {0}")]
    DeSerializationError(#[from] postcard::Error),
    #[error("Gzip compression error: {0}")]
    CompressionError(#[from] std::io::Error),
}
