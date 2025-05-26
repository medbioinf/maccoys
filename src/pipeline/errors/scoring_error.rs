use polars::error::PolarsError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ScoringError {
    #[error("Clould not write PSMs to CSV:\n\t{0}")]
    IntoPublicationMessageError(PolarsError),
    #[error("Error in Polars:\n\t{0}")]
    PolarsError(#[from] PolarsError),
    #[error("Error in LoOP calculation:\n\t{0}")]
    LoOPError(#[from] local_outlier_probabilities::error::Error),
}
