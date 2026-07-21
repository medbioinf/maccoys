use rustis::Error as RedisError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("{0}")]
    LocalStorageError(#[from] LocalStorageError),
    #[error("{0}")]
    RedisStorageError(#[from] RedisStorageError),
}

#[derive(Error, Debug)]
pub enum LocalStorageError {
    #[error("[LocalStorage] Count `{0}` not found in storage.")]
    CountNotFoundError(String),
    /// Error when RwLock is poisoned. String is a hint to the operation being done when it happend
    #[error("[LocalStorage] Poisened storage lock while `{0}`")]
    PoisenedStorageLock(String),
}

#[derive(Error, Debug)]
pub enum RedisStorageError {
    /// Redis error with a hint to the operation being done when it happend
    #[error("[RedisStorage] Error when `{0}`:\n\t{1}")]
    RedisError(&'static str, RedisError),
    /// Error when deserialization happens including a hint to overall operation when it happened
    #[error("[RedisStorage] Unable to deserialize when {0}:\n\t{1}")]
    DeserializationError(String, serde_json::Error),
    /// Error when serialization happens including a hint to overall operation when it happened
    #[error("[RedisStorage] Unable to serialize when {0}:\n\t{1}")]
    SerializationError(String, serde_json::Error),
    /// Missing Redis URL in config
    #[error("[RedisStorage] Redis URL is missing in config")]
    RedisUrlMissing,
    /// Configuration error
    #[error("[RedisStorage] Error in Redis config/URL:\n\t{0}")]
    ConfigError(RedisError),
    /// Connection error
    #[error("[RedisStorage] Connection error:\n\t{0}")]
    ConnectionError(RedisError),
}
