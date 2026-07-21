use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

/// Struct converting anyhow error to axum response.
///
pub struct AnyhowWebError(anyhow::Error);

// Tell axum how to convert `AnyhowWebError` into a response.
impl IntoResponse for AnyhowWebError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

impl<E> From<E> for AnyhowWebError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}
