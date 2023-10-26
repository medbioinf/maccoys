use std::fmt;
use std::io;

/// Represents errors which can occure during the mzml handling.
#[derive(Debug)]
pub enum Error {
    IoError(io::Error),
    SpectrumNotFound(String),
    FmtError(fmt::Error),
    IndexNotMatchingFile(String)
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Self::IoError(ref message) => write!(f, "{}", message),
            Self::SpectrumNotFound(ref message) => write!(f, "{}", message),
            Self::FmtError(ref message) => write!(f, "{}", message),
            Self::IndexNotMatchingFile(ref message) => write!(f, "{}", message)
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        return Error::IoError(err)
    }
}

impl From<fmt::Error> for Error {
    fn from(err: fmt::Error) -> Self {
        return Error::FmtError(err)
    }
}