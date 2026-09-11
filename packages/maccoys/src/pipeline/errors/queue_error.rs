use thiserror::Error;

use crate::pipeline::errors::{
    http_queue_server_error::HttpQueueServerError, message_error::MessageError,
};

#[derive(Error, Debug)]
pub enum QueueError {
    #[error("{0}")]
    LocalQueueError(#[from] LocalQueueError),
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
    #[error("[LocalQueue / {0}] Not constructible from TaskConfiguration alone; use LocalPipelineQueue::new_at")]
    NotConstructibleFromTaskConfiguration(String),
    #[error("[LocalQueue] {0}")]
    MessageError(#[from] MessageError),
    #[error("[LocalQueue] Disk spool error: {0}")]
    DiskSpoolError(#[from] HttpQueueServerError),
}

#[derive(Error, Debug)]
pub enum HttpQueueError {
    #[error("[HttpQueue / {0}] Base URL is missing in config")]
    BaseUrlMissing(String),
    #[error("[HttpQueue / {0}] Error building HTTP client:\n\t{1:?}")]
    BuildError(String, reqwest::Error),
    #[error("[HttpQueue / {0}] HTTP request error when {2} (url: {1}):\n\t{3:?}")]
    RequestError(String, String, String, reqwest::Error),
    #[error("[HttpQueue / {0}] Non-success status code `{2} - {3}` when {4} (url: {1})")]
    UnsuccessStatusCode(String, String, reqwest::StatusCode, String, &'static str),
    #[error("[HttpQueue / {0}] Unable to deserialize response when {1}:\n\t{2:?}")]
    DeserializationError(String, &'static str, reqwest::Error),
    #[error("[HttpQueue / {0}] Message error {1}:\n\t{2}")]
    MessageError(String, &'static str, MessageError),
}
