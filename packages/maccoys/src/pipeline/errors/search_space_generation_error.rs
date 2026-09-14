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
    #[error("Search space generator error in search space generation task:\n\t{0}")]
    SearchSpaceGenerator(Box<crate::search_space::search_space_generator::Error>),
    #[error("Error when flushing FASTA:\n\t{0}")]
    FastaFlushError(std::io::Error),
    #[error("Could not increased total spectrum count:\n\t{0}")]
    IncreaseTotalSpectrumCountError(StorageError),
    #[error("Could not get {0} from mutex.")]
    MutexLockError(String),
    #[error("Could not get {0} from arc.")]
    ArcIntoError(String),
    #[error("Sending request to search peptides failed:\n\t{0}")]
    PeptideSearchRequestSendError(reqwest::Error),
    #[error("Error in peptide search response:\n\tStatus: {0}; Content-Type: {1}; Body: {2}")]
    PeptideSearchRequestResponseError(reqwest::StatusCode, String, String),
    #[error("Error in peptide search response:\n\t{0}")]
    PTMCollection(Box<macpepdb::post_translational_modification::Error>),
    #[error("Target lookup error in search space generation: {0}")]
    TargetLookup(crate::search_space::target_lookup::Error),
}

into_thiserror_boxed!(
    crate::search_space::search_space_generator::Error,
    SearchSpaceGenerationError,
    SearchSpaceGenerator
);
