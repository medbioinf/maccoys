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
    #[error("Error generating FASTA:\n\t{0}")]
    FastaGenerationError(anyhow::Error),
    #[error("Error when flushing FASTA:\n\t{0}")]
    FastaFlushError(std::io::Error),
    #[error("Could not increased total spectrum count:\n\t{0}")]
    IncreaseTotalSpectrumCountError(StorageError),
    #[error("Could not get {0} from mutex.")]
    MutexLockError(String),
    #[error("Could not get {0} from arc.")]
    ArcIntoError(String),
}
