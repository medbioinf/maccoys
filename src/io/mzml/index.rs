// std imports
use std::collections::HashMap;
use std::path::PathBuf;

// 3rd party crates
use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Index for a mzML file.   
/// Stores length of the the general information (everything before the spectrumList) ends,
/// each spectrum's start and end offset and the default data processing reference and indention
/// (for extraction of a single spectrum into a separate valid mzML file).
/// TODO: Index chromatograms
///
#[derive(Serialize, Deserialize)]
pub struct Index {
    file_path: PathBuf,
    indention: String,
    default_data_processing_ref: String,
    general_information_len: usize,
    spectra: HashMap<String, (usize, usize)>,
    chromatograms: HashMap<String, (usize, usize)>,
}

impl Index {
    pub fn new(
        file_path: PathBuf,
        indention: String,
        default_data_processing_ref: String,
        general_information_len: usize,
        spectra: HashMap<String, (usize, usize)>,
        chromatograms: HashMap<String, (usize, usize)>,
    ) -> Self {
        Self {
            file_path,
            indention,
            default_data_processing_ref,
            general_information_len,
            spectra,
            chromatograms,
        }
    }

    pub fn get_file_path(&self) -> &PathBuf {
        &self.file_path
    }

    pub fn get_indention(&self) -> &String {
        &self.indention
    }

    pub fn get_default_data_processing_ref(&self) -> &String {
        &self.default_data_processing_ref
    }

    pub fn get_general_information_len(&self) -> usize {
        self.general_information_len
    }

    pub fn get_spectra(&self) -> &HashMap<String, (usize, usize)> {
        &self.spectra
    }

    pub fn get_chromatograms(&self) -> &HashMap<String, (usize, usize)> {
        &self.chromatograms
    }

    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string(&self)?)
    }

    pub fn from_json(json: &str) -> Result<Self> {
        Ok(serde_json::from_str(json)?)
    }
}

#[cfg(test)]
mod test {
    // std
    use std::path::Path;

    // internal
    use super::*;
    use crate::io::mzml::indexer::Indexer;

    #[test]
    fn test_serialization_deserialization() {
        let file_path = Path::new("./test_files/spectra_small.mzML");

        let index = Indexer::create_index(&file_path, None).unwrap();

        let serialized = index.to_json().unwrap();
        let deserialized_index: Index = Index::from_json(&serialized).unwrap();

        assert_eq!(index.get_file_path(), deserialized_index.get_file_path());
        assert_eq!(index.get_indention(), deserialized_index.get_indention());
        assert_eq!(
            index.get_default_data_processing_ref(),
            deserialized_index.get_default_data_processing_ref()
        );
        assert_eq!(
            index.get_general_information_len(),
            deserialized_index.get_general_information_len()
        );
        for (spec_id, expected_start_end_offset) in index.get_spectra().iter() {
            assert!(deserialized_index.get_spectra().contains_key(spec_id));
            if let Some(start_end_offset) = deserialized_index.get_spectra().get(spec_id) {
                assert_eq!(expected_start_end_offset.0, start_end_offset.0);
                assert_eq!(expected_start_end_offset.1, start_end_offset.1)
            }
        }
        // TODO: add chromatogram assertion when they get added
    }
}
