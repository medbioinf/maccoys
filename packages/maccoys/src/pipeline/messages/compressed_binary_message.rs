use flate2::Compression;
use flate2::{read::GzDecoder, write::GzEncoder};
use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};
use std::io::prelude::*;

use crate::pipeline::{errors::message_error::MessageError, messages::is_message::IsMessage};

/// Message contains the actual message serialized as postcard and gzip compressed
/// for efficient storing and transmission
///  
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CompressedBinaryMessage<T>
where
    T: IsMessage,
{
    id: String,
    bytes: Vec<u8>,
    message_type: std::marker::PhantomData<T>,
}

impl<T> CompressedBinaryMessage<T>
where
    T: IsMessage,
{
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn bytes(&self) -> &Vec<u8> {
        &self.bytes
    }

    /// Create a compressed binary message from a message
    ///
    /// # Arguments
    /// * `message` - Message to be compressed and serialized
    ///
    pub fn try_from_message(message: &T) -> Result<Self, MessageError> {
        let bytes = to_allocvec(message).map_err(MessageError::DeSerializationError)?;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
        encoder
            .write_all(&bytes)
            .map_err(MessageError::CompressionError)?;
        Ok(CompressedBinaryMessage {
            id: message.get_id(),
            bytes: encoder.finish().map_err(MessageError::CompressionError)?,
            message_type: std::marker::PhantomData,
        })
    }

    /// Decompress and deserialize the message into the original message type
    ///
    /// # Returns
    /// * `T` - The original message type
    ///
    pub fn try_into_message(&self) -> Result<T, MessageError> {
        let mut bytes = Vec::with_capacity(self.bytes.len());
        let mut decoder = GzDecoder::new(self.bytes.as_slice());
        let _ = decoder
            .read_to_end(&mut bytes)
            .map_err(MessageError::CompressionError)?;

        from_bytes::<T>(&bytes).map_err(MessageError::DeSerializationError)
    }

    pub fn to_postcard(&self) -> Result<Vec<u8>, MessageError> {
        to_allocvec(self).map_err(MessageError::DeSerializationError)
    }

    pub fn from_postcard(data: &[u8]) -> Result<Self, MessageError> {
        from_bytes::<CompressedBinaryMessage<T>>(data).map_err(MessageError::DeSerializationError)
    }
}
