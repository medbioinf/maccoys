use thiserror::Error;

use super::storage_error::StorageError;

#[derive(Error, Debug)]
pub enum SearchSpaceGenerationError {
    #[error("Search parameters not found in storage.")]
    SearchParametersNotFoundError(),
    #[error("Could not get search parameters:\n\t{0}")]
    GetSearchParametersError(StorageError),
    #[error("PTMs not found in storage.")]
    PTMsNotFoundError(),
    #[error("Could not get PTMS:\n\t{0}")]
    GetPTMsError(StorageError),
    #[error("Reading and indexing spectrum mzML:\n\t{0}")]
    ReadingSpectrumMzMlError(anyhow::Error),
    #[error("Error while reading the spectrum `{0}`:\n\t{1}")]
    SpectrumReadError(String, anyhow::Error),
    #[error("No precusor list in spectrum `{0}`")]
    NoPrecursorListError(String),
    #[error("No selected ions in precursor in spectrum `{0}`")]
    NoSelectedIonError(String),
    #[error("Missing m/z in sected ion in spectrum `{0}`")]
    MissingSelectedIonMzError(String),
    #[error("Could not parse m/z to f64 in specturm `{0}`\n\t{1}")]
    MzParsingError(String, std::num::ParseFloatError),
    #[error("Could not parse charge to u8 in specturm `{0}`\n\t{1}")]
    ChargeParsingError(String, std::num::ParseIntError),
    #[error("Error generating FASTA:\n\t{0}")]
    FastaGenerationError(anyhow::Error),
    #[error("Error when flushing FASTA:\n\t{0}")]
    FastaFlushError(std::io::Error),
}
