use thiserror::Error;

use super::storage_error::StorageError;

#[derive(Error, Debug)]
pub enum IdentificationError {
    #[error("No comet configuration in storage")]
    NoCometCongigurationInStorageError(),
    #[error("Unable to get comet configuration from storage:\n\t{0}")]
    UnableToGetCometConfigurationFromStorageError(StorageError),
    #[error("Unable to set comet configuration attribute `{0}`:\n\t{1}")]
    UnableToSetCometConfigurationAttributeError(String, anyhow::Error),
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
    #[error("Error running Comet:\n\t{0}")]
    CometError(anyhow::Error),
}
