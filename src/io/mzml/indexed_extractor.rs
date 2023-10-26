// std
use core::fmt::Write;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::path::Path;

// 3rd party
use anyhow::{bail, Result};
use sha1::{Digest, Sha1};

// internal
use crate::io::mzml::error::Error as MzMlError;
use crate::io::mzml::index::Index;

/// Indention level for spectrum tags
const SPECTRUM_INDENTION_LEVEL: u8 = 4;
/// Indention level for spectrum list tags
const SPECTRUM_LIST_INDENTION_LEVEL: u8 = 3;
/// Indention level for run tags
const RUN_INDENTION_LEVEL: u8 = 2;
/// Indentation level for mzML tags
const MZML_INDENTION_LEVEL: u8 = 1;
/// Indentation level for index list tags
const INDEX_LIST_INDENT_LEVEL: u8 = 1;
/// Indentation level for index tags
const INDEX_INDENTION_LEVEL: u8 = 2;
/// Indentation level for offset tags
const OFFSET_INDENTION_LEVEL: u8 = 3;

/// Extracts a spectrum vom an mzML which are previously indexed using [`Indexer`](crate::proteomics::io::mzml::indexer::Indexer)
/// and writes it into a new indexed mzML file.
///
pub struct IndexedExtractor<'a> {
    index: &'a Index,
    file: File,
}

impl<'a> IndexedExtractor<'a> {
    /// Creates a new reader for the given file and index.
    ///
    /// # Arguments
    /// * `file_path` - The path to the mzML file.
    /// * `index` - The index of the mzML file.
    ///
    pub fn new(file_path: &'a Path, index: &'a Index) -> Result<Self> {
        if file_path != index.get_file_path() {
            bail!(MzMlError::IndexNotMatchingFile(
                format!(
                    "File of reader and index not matching, '{}' != '{}'",
                    file_path.to_string_lossy(),
                    index.get_file_path().to_string_lossy()
                )
                .to_owned(),
            ));
        }
        Ok(Self {
            index,
            file: File::open(file_path)?,
        })
    }

    /// Returns the index of the mzML file.
    ///
    pub fn index(&self) -> &Index {
        &self.index
    }

    /// Returns the raw spectrum for the given spectrum id as XML
    ///
    /// # Arguments
    /// * `spectrum_id` - The spectrum id for which the raw spectrum should be returned.
    ///
    pub fn get_raw_spectrum(&mut self, spectrum_id: &str) -> Result<Vec<u8>> {
        if let Some(offset) = self.index.get_spectra().get(spectrum_id) {
            self.file.seek(io::SeekFrom::Start(offset.0 as u64))?;
            let spectrum_len = offset.1 - offset.0;
            let mut raw_spectrum: Vec<u8> = vec![0; spectrum_len];
            self.file.read_exact(raw_spectrum.as_mut_slice())?;
            return Ok(raw_spectrum);
        }
        bail!(MzMlError::SpectrumNotFound(spectrum_id.to_owned()));
    }

    pub fn get_general_information(&mut self) -> Result<Vec<u8>> {
        self.file.seek(io::SeekFrom::Start(0))?;
        let mut general_information: Vec<u8> = vec![0; self.index.get_general_information_len()];
        self.file.read_exact(general_information.as_mut_slice())?;
        return Ok(general_information);
    }

    /// Returns a valid MzML file for the given spectrum id.
    ///
    /// # Arguments
    /// * `spectrum_id` - The spectrum id for which the mzml should be returned.
    ///
    pub fn extract_spectrum(&mut self, spectrum_id: &str) -> Result<Vec<u8>> {
        let mut mzml: Vec<u8> = Vec::new();
        // start mzml
        let mut general_information = self.get_general_information()?;
        mzml.append(&mut general_information);
        mzml.extend_from_slice(b"count=\"1\" defaultDataProcessingRef=\"");
        mzml.extend_from_slice(self.index.get_default_data_processing_ref().as_bytes());
        mzml.extend_from_slice(b"\">\n");
        mzml.extend_from_slice(
            self.index
                .get_indention()
                .repeat(SPECTRUM_INDENTION_LEVEL as usize)
                .as_bytes(),
        );
        let spectrum_offset = mzml.len().to_string();
        // add spectrum
        let mut raw_spectrum = self.get_raw_spectrum(spectrum_id)?;
        mzml.append(&mut raw_spectrum);
        mzml.extend_from_slice(b"\n");
        mzml.extend_from_slice(
            self.index
                .get_indention()
                .repeat(SPECTRUM_LIST_INDENTION_LEVEL as usize)
                .as_bytes(),
        );
        mzml.extend_from_slice(b"</spectrumList>\n");
        mzml.extend_from_slice(
            self.index
                .get_indention()
                .repeat(RUN_INDENTION_LEVEL as usize)
                .as_bytes(),
        );
        mzml.extend_from_slice(b"</run>\n");
        mzml.extend_from_slice(
            self.index
                .get_indention()
                .repeat(MZML_INDENTION_LEVEL as usize)
                .as_bytes(),
        );
        mzml.extend_from_slice(b"</mzML>\n");
        mzml.extend_from_slice(
            self.index
                .get_indention()
                .repeat(INDEX_LIST_INDENT_LEVEL as usize)
                .as_bytes(),
        );
        let index_list_offset = mzml.len().to_string();
        mzml.extend_from_slice(b"<indexList count=\"1\">\n");
        mzml.extend_from_slice(
            self.index
                .get_indention()
                .repeat(INDEX_INDENTION_LEVEL as usize)
                .as_bytes(),
        );
        mzml.extend_from_slice(b"<index name=\"spectrum\">\n");
        mzml.extend_from_slice(
            self.index
                .get_indention()
                .repeat(OFFSET_INDENTION_LEVEL as usize)
                .as_bytes(),
        );
        mzml.extend_from_slice(b"<offset idRef=\"");
        mzml.extend_from_slice(spectrum_id.as_bytes());
        mzml.extend_from_slice(b"\">");
        mzml.extend_from_slice(spectrum_offset.as_bytes());
        mzml.extend_from_slice(b"</offset>\n");
        mzml.extend_from_slice(
            self.index
                .get_indention()
                .repeat(INDEX_INDENTION_LEVEL as usize)
                .as_bytes(),
        );
        mzml.extend_from_slice(b"</index>\n");
        mzml.extend_from_slice(
            self.index
                .get_indention()
                .repeat(INDEX_LIST_INDENT_LEVEL as usize)
                .as_bytes(),
        );
        mzml.extend_from_slice(b"</indexList>\n");
        mzml.extend_from_slice(
            self.index
                .get_indention()
                .repeat(INDEX_LIST_INDENT_LEVEL as usize)
                .as_bytes(),
        );
        mzml.extend_from_slice(b"<indexListOffset>");
        mzml.extend_from_slice(index_list_offset.as_bytes());
        mzml.extend_from_slice(b"</indexListOffset>\n");
        mzml.extend_from_slice(self.index.get_indention().as_bytes());
        mzml.extend_from_slice(b"<fileChecksum>");

        let mut hasher = Sha1::new();
        hasher.update(mzml.as_slice());
        let hash_result = hasher.finalize();

        // hex conversion
        let mut hex_hash = String::with_capacity(2 * hash_result.len());
        for byte in hash_result {
            write!(hex_hash, "{:02x}", byte)?;
        }

        // mzml.extend_from_slice(hash_result.encode_hex::<String>().as_bytes());
        mzml.extend_from_slice(hex_hash.as_bytes());
        mzml.extend_from_slice(b"</fileChecksum>\n</indexedmzML>");
        return Ok(mzml);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::io::mzml::indexer::Indexer;

    #[test]
    fn test_spectra_reading() {
        let file_path = Path::new("./test_files/spectra_small.mzML");

        let index = Indexer::create_index(&file_path, None).unwrap();

        let mut reader = IndexedExtractor::new(file_path, &index).unwrap();

        let spec = reader
            .extract_spectrum("controllerType=0 controllerNumber=1 scan=2814")
            .unwrap();
        println!("{}", String::from_utf8_lossy(spec.as_slice()));
    }

    #[test]
    fn test_file_paths_not_matching() {
        let file_path = Path::new("./test_files/spectra_small.mzML");
        let wrong_file_path = Path::new("./test_files/not_existing.mzML");

        let index = Indexer::create_index(&file_path, None).unwrap();

        let reader_result = IndexedExtractor::new(wrong_file_path, &index);

        assert!(reader_result.is_err());
    }
}
