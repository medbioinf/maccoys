use thiserror::Error;

use crate::pipeline::errors::storage_error::StorageError;

#[derive(Error, Debug)]
pub enum IndexingError {
    #[error("Search parameters not found in storage.")]
    SearchParametersNotFoundError(),
    #[error("Could not get search parameters:\n\t{0}")]
    GetSearchParametersError(StorageError),
    #[error("Error while opening the MS run mzML file:\n\t{0}")]
    MsRunMzMlIoError(std::io::Error),
    #[error("Error while creating the index:\n\t{0}")]
    IndexCreationError(anyhow::Error),
    #[error("Error while serializing the index:\n\t{0}")]
    IndexToJsonError(anyhow::Error),
    #[error("Error while writing the index file\n\t{0}")]
    IndexWriteError(std::io::Error),
    #[error("Error while opening the mzML file witht the created index:\n\t{0}")]
    OpenMzMlWithIndexError(anyhow::Error),
    #[error("Error while reading the spectrum `{0}`:\n\t{1}")]
    SpectrumReadError(String, anyhow::Error),
    #[error("No precusor list in spectrum `{0}`")]
    NoPrecursorListError(String),
    #[error("No selected ions in precursor in spectrum `{0}`")]
    NoSelectedIonError(String),
    #[error("Missing m/z in selcted ion in spectrum `{0}`")]
    MissingSelectedIonMzError(String),
    #[error("Could not parse m/z to f64 in specturm `{0}`\n\t{1}")]
    MzParsingError(String, std::num::ParseFloatError),
    #[error("Could not parse charge to u8 in specturm `{0}`\n\t{1}")]
    ChargeParsingError(String, std::num::ParseIntError),
    #[error("Error reading the m/z list file:\n\t{0}")]
    MzListReadError(anyhow::Error),
    #[error("Error reading the intensity list file:\n\t{0}")]
    IntensityListReadError(anyhow::Error),
}
