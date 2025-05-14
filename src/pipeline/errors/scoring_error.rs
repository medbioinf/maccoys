use polars::{error::PolarsError, frame::DataFrame};
use pyo3::PyErr;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;

#[derive(Error, Debug)]
pub enum ScoringError {
    #[error("Error in Python:\n\t{0}")]
    PythonError(#[from] PyErr),
    #[error("Error sending data to Python enviornment:\n\t{0}")]
    PythonToRustSendError(#[from] SendError<Result<DataFrame, PyErr>>),
    #[error("Clould not write PSMs to CSV:\n\t{0}")]
    IntoPublicationMessageError(PolarsError),
    #[error("Error sending data to Python enviornment:\n\t{0}")]
    RustToPythonSendError(#[from] SendError<DataFrame>),
    #[error("Python thread unexpectedly closed.")]
    PythonThreadUnexpectedlyClosedError(),
    #[error("Error in Polars:\n\t{0}")]
    PolarsError(#[from] PolarsError),
}
