use thiserror::Error;

#[derive(Error, Debug)]
pub enum IndexingError {
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
    #[error("Error while creating the index:\n\t{0}")]
    SpectrumExtractionError(anyhow::Error),
}
