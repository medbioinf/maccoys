use rustis::Error as RedisError;
use thiserror::Error;

use crate::pipeline::errors::message_error::MessageError;

#[derive(Error, Debug)]
pub enum QueueError {
    #[error("{0}")]
    LocalQueueError(#[from] LocalQueueError),
    #[error("{0}")]
    RedisQueueError(#[from] RedisQueueError),
    #[error("{0}")]
    HttpQueueError(#[from] HttpQueueError),
    // Commone Error
    #[error("[Queue] Queue is full")]
    QueueFullError,
    #[error("[Queue] Queue is missing")]
    QueueMissingError,
}

#[derive(Error, Debug)]
pub enum LocalQueueError {
    #[error("[LocalStorage] Count `{0}` not found in storage.")]
    CountNotFoundError(String),
    /// Error when RwLock is poisoned. String is a hint to the operation being done when it happend
    #[error("[LocalStorage] Poisened storage lock while `{0}`")]
    PoisenedStorageLock(String),
}

#[derive(Error, Debug)]
pub enum RedisQueueError {
    #[error("[RedisQueue / {0}] Redis URL is missing in config")]
    RedisUrlMissing(String),
    #[error("[RedisQueue / {0}] Error in Redis config/URL:\n\t{1}")]
    ConfigError(String, rustis::Error),
    #[error("[RedisQueue / {0}] Connection error:\n\t{1}")]
    ConnectionError(String, rustis::Error),
    #[error("[RedisQueue / {0}] Error when {1}:\n\t{2}")]
    RedisError(String, &'static str, RedisError),
    #[error("[RedisQueue / {0}] Unable to deserialize when {1}:\n\t{2}")]
    DeserializationError(String, &'static str, serde_json::Error),
    #[error("[RedisQueue / {0}] Unable to serialize when {1}:\n\t{2}")]
    SerializationError(String, &'static str, serde_json::Error),
}

#[derive(Error, Debug)]
pub enum HttpQueueError {
    #[error("[HttpQueue / {0}] Base URL is missing in config")]
    BaseUrlMissing(String),
    #[error("[HttpQueue / {0}] Error building HTTP client:\n\t{1}")]
    BuildError(String, reqwest::Error),
    #[error("[HttpQueue / {0}] HTTP request error when {1}:\n\t{2:?}")]
    RequestError(String, &'static str, reqwest::Error),
    #[error("[HttpQueue / {0}] Non-success status code `{1} - {2}` when {3}")]
    UnsuccessStatusCode(String, reqwest::StatusCode, String, &'static str),
    #[error("[HttpQueue / {0}] Unable to deserialize response when {1}:\n\t{2}")]
    DeserializationError(String, &'static str, reqwest::Error),
    #[error("[HttpQueue / {0}] Message error {1}:\n\t{2}")]
    MessageError(String, &'static str, MessageError),
}
