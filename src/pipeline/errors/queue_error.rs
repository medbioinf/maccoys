use rustis::Error as RedisError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum QueueError {
    #[error("{0}")]
    LocalQueueError(#[from] LocalQueueError),
    #[error("{0}")]
    RedisQueueError(#[from] RedisQueueError),
    // Commone Error
    #[error("[Queue] Queue is full")]
    QueueFullError,
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
