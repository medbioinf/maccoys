use polars::error::PolarsError;
use pyo3::PyErr;
use thiserror::Error;
use tokio::sync::mpsc::error::SendError;

#[derive(Error, Debug)]
pub enum ScoringError {
    #[error("Error in Python:\n\t{0}")]
    PythonError(#[from] PyErr),
    #[error("Error sending data to Python enviornment:\n\t{0}")]
    PythonToRustSendError(#[from] SendError<Result<Vec<f64>, PyErr>>),
    #[error("Error calculating local outlier probabilities:\n\t{0}")]
    LoOPError(PyErr),
    #[error("Clould not write PSMs to CSV:\n\t{0}")]
    IntoPublicationMessageError(PolarsError),
    #[error("Expecting score `{0}` in PSMs:\n\t{1}")]
    MissingScoreError(String, PolarsError),
    #[error("Unable to parse score to f64:\n\t{0}")]
    ScoreToF64VecError(PolarsError),
    #[error("Error sending data to Python enviornment:\n\t{0}")]
    RustToPythonSendError(#[from] SendError<Vec<f64>>),
    #[error("Python thread unexpectedly closed.")]
    PythonThreadUnexpectedlyClosedError(),
    #[error("Error in Python thread:\n\t{0}")]
    AddingScoreToPSMError(PolarsError),
}
