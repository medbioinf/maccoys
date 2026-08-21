use thiserror::Error;

#[derive(Error, Debug)]
pub enum ScoringError {
    #[error("Error in LoOP calculation:\n\t{0}")]
    LoOPError(#[from] local_outlier_probabilities::error::Error),
    #[error("Error converting into publication message:\n\t{0}")]
    IntoPublicationMessageError(Box<IntoPublicationMessageError>),
}

#[derive(Debug, Error)]
pub enum IntoPublicationMessageError {
    #[error("Unable to convert PSMs to parquet file: {0}")]
    ParquetConversionError(Box<crate::peptide_spectrum_match::Error>),
}
