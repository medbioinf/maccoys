use thiserror::Error;
use xcorrrs::error::Error as XcorrError;

use super::storage_error::StorageError;

#[derive(Error, Debug)]
pub enum IdentificationError {
    #[error("No comet configuration in storage")]
    NoCometCongigurationInStorageError(),
    #[error("Unable to get comet configuration from storage:\n\t{0}")]
    UnableToGetXcorrConfigurationFromStorageError(StorageError),
    #[error("Unable to set comet configuration attribute `{0}`:\n\t{1}")]
    UnableToSetXcorrConfigurationAttributeError(String, anyhow::Error),
    #[error("Search parameters not found in storage.")]
    SearchParametersNotFoundError(),
    #[error("Could not get search parameters:\n\t{0}")]
    GetSearchParametersError(StorageError),
    #[error("Could not write spectrum mzML file:\n\t{0}")]
    WriteSpectrumMzMLFileError(std::io::Error),
    #[error("Could not write FASTA file:\n\t{0}")]
    WriteFastaFileError(std::io::Error),
    #[error("Could not set Comet attribute `{0}`:\n\t{1}")]
    CometConfigAttributeError(String, anyhow::Error),
    #[error("Could not write Comet config file:\n\t{0}")]
    WriteCometConfigFileError(std::io::Error),
    #[error("Could not convert FASTA to UTF-8, invalid bytes found:\n\t{0}")]
    FastaToUtf8Error(std::string::FromUtf8Error),
    #[error("Could not convert mzML to UTF-8, invalid bytes found:\n\t{0}")]
    MzMlToUtf8Error(std::string::FromUtf8Error),
    #[error("Could not create thread pool for identification:\n\t{0}")]
    CreateIdentificationThreadPoolError(rayon::ThreadPoolBuildError),
    #[error("Could not get {0} from mutex.")]
    MutexLockError(String),
    #[error("Could not get {0} from arc.")]
    ArcIntoError(String),
    // Saving the error as string avoid some send+sync issues with boxed dyn errors
    #[error("Error during xcorring:\n\t{0}")]
    XcorrError(String),
    #[error("Cannot convert PSM collection to DataFrame:\n\t{0}")]
    PsmCollectionToDataFrameError(polars::error::PolarsError),
}

impl From<XcorrError> for IdentificationError {
    fn from(e: XcorrError) -> Self {
        IdentificationError::XcorrError(e.to_string())
    }
}
