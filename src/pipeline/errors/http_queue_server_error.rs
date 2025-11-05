use axum::{
    body::Body,
    response::{IntoResponse, Response},
};
use http::StatusCode;
use thiserror::Error;
use tracing::error;

use crate::pipeline::errors::message_error::MessageError;

#[derive(Error, Debug)]
pub enum HttpQueueServerError {
    #[error("[HttpQueueServer] Queue is full")]
    QueueFullError,
    #[error("[HttpQueueServer] Queue is empty")]
    QueueEmptyError,
    #[error("[HttpQueueServer] Key `{0}` not found in store")]
    KeyNotFoundError(String),
    #[error("[HttpQueueServer] Unreadabale queue")]
    UnreadableQueueError,
    #[error("[HttpQueueServer] Unwritable queue")]
    UnwritableQueueError,
    #[error("[HttpQueueServer] Unable to acquire TCP listener: {0}")]
    TcpListenerError(std::io::Error),
    #[error("[HttpQueueServer] Unreadable Hub state")]
    UnreadableHubStateError,
    #[error("[HttpQueueServer] Unwritable Hub state")]
    UnwritableHubStateError,
    #[error("[HttpQueueServer] Missing capacity header for new queue")]
    MissingCapacityHeader,
    #[error(
        "[HttpQueueServer] Invaid capacity header for new queue. Should be a positive integer"
    )]
    InvaidCapacityHeader,
    #[error("[HttpQueueServer]  Unknown queue error")]
    UnknownQueueError,
    #[error("[HttpQueueServer] `{0}` not found")]
    RouteNotFoundError(String),
    #[error("[HttpQueueServer] Failed to read state file")]
    CacheFileReadError,
    #[error("[HttpQueueServer] Failed to write state file")]
    CacheFileWriteError,
    #[error("[HttpQueueServer] Failed to serialize state")]
    StateSerializationError,
    #[error("[HttpQueueServer] Failed to deserialize state")]
    StateDeserializationError,
    #[error("[HttpQueueServer] Could not start the server: {0}")]
    ServerStartError(std::io::Error),
    #[error("[HttpQueueServer] Could not send the shutdown signal to the server")]
    ServerShutdownError,
    #[error("[HttpQueueServer] Server error: {0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[error("Unable to register signal handler: {0}")]
    SignalHandlerError(std::io::Error),
    #[error("[HttpQueueServer] {0}")]
    MessageError(#[from] MessageError),
}

impl IntoResponse for HttpQueueServerError {
    fn into_response(self) -> Response<Body> {
        let status_code = match &self {
            HttpQueueServerError::QueueFullError => StatusCode::INSUFFICIENT_STORAGE,
            HttpQueueServerError::QueueEmptyError => StatusCode::NO_CONTENT,
            HttpQueueServerError::KeyNotFoundError(_) => StatusCode::NOT_FOUND,
            HttpQueueServerError::MissingCapacityHeader => StatusCode::BAD_REQUEST,
            HttpQueueServerError::InvaidCapacityHeader => StatusCode::BAD_REQUEST,
            HttpQueueServerError::UnknownQueueError => StatusCode::NOT_FOUND,
            HttpQueueServerError::RouteNotFoundError(_) => StatusCode::NOT_FOUND,
            HttpQueueServerError::MessageError(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        // error!("{}, Status Code: {}", self, status_code);

        (status_code, self.to_string()).into_response()
    }
}
