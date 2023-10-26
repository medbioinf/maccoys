// std imports
use std::cmp::min;
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::Path;

// 3rd party imports
use anyhow::bail;
use anyhow::Result;

// internal imports
use crate::io::mzml::index::Index;

/// Start of spectrum list tag
const SPECTRUM_LIST_START_TAG: &'static [u8] = b"<spectrumList ";

/// End of spectrum list tag
const SPECTRUM_LIST_END_TAG: &'static [u8] = b"</spectrumList>";

/// Start of spectrum tag
const SPECTRUM_START_TAG: &'static [u8] = b"<spectrum ";

/// End of spectrum tag
const SPECTRUM_END_TAG: &'static [u8] = b"</spectrum>";

/// Start of spectrum ID tag
const SPECTRUM_ID_START: &'static [u8] = b"id=\"";

/// Attribute end tag
const ATTRIBUTE_END: &'static [u8] = b"\"";

/// Start of default data processing ref tag
const DEFAULT_DATA_PROCESSING_REF_ATTRIBUTE: &'static [u8] = b"defaultDataProcessingRef=\"";

/// Tab ASCII value
const TAB: &'static [u8] = b"\t";

/// Whitespace ASCII value
const WHITESPACE: &'static [u8] = b" ";

/// Default chunk size to read from file (1MB)
const DEFAULT_CHUNK_SIZE: usize = 1024;

/// Creates an index of the given mzML file by
/// finding the length of the general information (everything before the spectrumList),
/// each spectrum's start and end offset adn the default data processing reference and indention
/// (for extraction of a single spectrum into a separate valid mzML file).
/// TODO: Index chromatograms
///
pub struct Indexer<'a> {
    file_path: &'a Path,
    reader: BufReader<File>,
    chunk_size: usize,
    buffer: Vec<u8>,
    read_bytes: usize,
    general_information_offset: usize,
    default_data_processing_ref: String,
    indention: Vec<u8>,
    is_eof: bool,
}

impl<'a> Indexer<'a> {
    pub fn new(file_path: &'a Path, chunk_size: Option<usize>) -> Self {
        let chunk_size = chunk_size.unwrap_or(DEFAULT_CHUNK_SIZE);
        let reader = BufReader::with_capacity(chunk_size, File::open(file_path).unwrap());
        Self {
            file_path,
            reader,
            chunk_size,
            buffer: Vec::with_capacity(chunk_size),
            read_bytes: 0,
            general_information_offset: 0,
            default_data_processing_ref: String::new(),
            indention: Vec::new(),
            is_eof: false,
        }
    }

    /// Reads bytes form the open file.
    ///
    fn read_chunk(&mut self) -> Result<()> {
        if self.is_eof {
            return Ok(());
        }
        let mut chunk = Vec::with_capacity(self.chunk_size);
        match self
            .reader
            .by_ref()
            .take(self.chunk_size as u64)
            .read_to_end(&mut chunk)
        {
            Ok(bytes_read) => {
                if bytes_read > 0 {
                    self.read_bytes += bytes_read;
                    self.buffer.extend(chunk);
                } else {
                    self.is_eof = true;
                }
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    self.is_eof = true;
                } else {
                    bail!("{}", e);
                }
            }
        }
        Ok(())
    }

    fn remove_n_first_bytes_from_buffer(&mut self, n: usize) {
        // Remove searched buffer by rotating the processed bytes to the end, and truncate the buffer. This does not have any effect on the allocated buffer size.
        self.buffer.rotate_left(n);
        self.buffer.truncate(self.buffer.len() - n);
    }

    /// Searches and sets general information from the beginning of the file
    /// including the opening spectrum list tag and one space '<spectrumList '.
    ///
    fn find_general_information_offset(&mut self) -> Result<()> {
        let mut search_offset: usize = 0;
        loop {
            // Search for spectrum list
            let position = self.buffer[search_offset..]
                .windows(SPECTRUM_LIST_START_TAG.len())
                .position(|window| window == SPECTRUM_LIST_START_TAG);
            match position {
                None => {
                    if self.is_eof {
                        bail!("Spectrum list start tag not found but reached EOF.")
                    }
                    // Begin with next search before the end of the current chunk, incase the tag overlaps two chunks
                    search_offset = self.buffer.len() - SPECTRUM_LIST_START_TAG.len() - 1;
                    self.read_chunk()?
                }
                Some(position) => {
                    self.general_information_offset =
                        search_offset + position + SPECTRUM_LIST_START_TAG.len();
                    return Ok(());
                }
            }
        }
    }

    /// Searches and sets default data processing reference attribute.
    ///
    fn get_default_data_processing_ref(&mut self) -> Result<()> {
        let mut search_offset: usize = 0;
        let mut search_for = DEFAULT_DATA_PROCESSING_REF_ATTRIBUTE;
        let mut default_data_processing_ref_start: Option<usize> = None;
        loop {
            // Search for start of attribute
            let position = self.buffer[search_offset..]
                .windows(search_for.len())
                .position(|window| window == search_for);
            match position {
                None => {
                    if self.is_eof {
                        bail!("Default data processing reference attribute not found but reached EOF.")
                    }
                    // Read the next chunk, but begin search before the end of the old one in case the attribute is overlapping two chunks
                    search_offset = self.buffer.len() - search_for.len() - 1;
                    self.read_chunk()?;
                }
                Some(position) => {
                    if default_data_processing_ref_start.is_none() {
                        default_data_processing_ref_start =
                            Some(search_offset + position + search_for.len());
                        search_offset = default_data_processing_ref_start.unwrap();
                        search_for = ATTRIBUTE_END; // search for end tag
                    } else {
                        self.default_data_processing_ref = String::from_utf8_lossy(
                            &self.buffer[default_data_processing_ref_start.unwrap()
                                ..search_offset + position],
                        )
                        .to_string();
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Try to guess indention character and width.
    /// Make sure
    ///
    fn get_indention(&mut self) -> Result<()> {
        if self.general_information_offset == 0 {
            bail!(
                "General information offset not set. Call find_general_information_offset() first."
            );
        }
        let mut cursor_pos: usize = 0;

        // Will stop when newline was found in buffer
        loop {
            cursor_pos = match self.buffer[cursor_pos..]
                .windows(1)
                .position(|windows| windows == b"\n")
            {
                Some(position) => cursor_pos + position,
                None => return Ok(()), // if there is no newline in the buffer by this time. It might be a one line file?
            };

            // Check if char behind newline is whitespace or tab
            cursor_pos += 1;
            match &self.buffer[cursor_pos..=cursor_pos] {
                WHITESPACE => self.indention = Vec::from(WHITESPACE),
                TAB => self.indention = Vec::from(TAB),
                _ => continue, // If byte behind newline is not whitespace or tab search for next newline
            }

            // Count indention bytes to get indention list
            let mut indention_width: usize = 0;
            while self.buffer[cursor_pos..=cursor_pos] == self.indention {
                indention_width += 1;
                cursor_pos += 1;
            }
            self.indention = self.indention.repeat(indention_width);
            break;
        }
        Ok(())
    }

    /// Searches and sets the spectrum offsets.
    ///
    /// # Arguments
    /// * `plain_spectrum` - Plain spectrum string
    ///
    fn get_spectrum_id(plain_spectrum: &[u8]) -> Result<String> {
        let spec_id_start_idx: usize = match plain_spectrum
            .windows(SPECTRUM_ID_START.len())
            .position(|window| window == SPECTRUM_ID_START)
        {
            Some(position) => position + SPECTRUM_ID_START.len(),
            None => bail!("Spectrum ID attribute start not found."),
        };
        let spec_id_end_idx: usize = match plain_spectrum[spec_id_start_idx..]
            .windows(ATTRIBUTE_END.len())
            .position(|window| window == ATTRIBUTE_END)
        {
            Some(position) => spec_id_start_idx + position,
            None => bail!("Spectrum ID attribute end not found."),
        };
        Ok(
            String::from_utf8_lossy(&plain_spectrum[spec_id_start_idx..spec_id_end_idx])
                .to_string(),
        )
    }

    /// Returns the spectrum offsets, beginning with '<spectrum ' and ending with </spectrum>.
    ///
    fn get_spectra_offsets(&mut self) -> Result<HashMap<String, (usize, usize)>> {
        let mut spectra_offsets: HashMap<String, (usize, usize)> = HashMap::new();
        let mut spectrum_start_tag_idx: Option<usize> = None;
        let mut search_offset: usize = 0;
        // First search for spectrum start tag
        let mut search_for = SPECTRUM_START_TAG;
        loop {
            // Find match
            match self.buffer[search_offset..]
                .windows(search_for.len())
                .position(|window| window == search_for)
            {
                None => {
                    if self.is_eof && search_for == SPECTRUM_START_TAG {
                        break;
                    }
                    // If no match found move search offset to the end - the length of the search string or to the start of the buffer
                    search_offset = if self.buffer.len() > (search_offset + search_for.len()) {
                        self.buffer.len() - (search_offset + search_for.len()) - 1
                    } else {
                        0
                    };
                    self.read_chunk()?;
                }
                Some(position) => {
                    if spectrum_start_tag_idx.is_none() {
                        spectrum_start_tag_idx = Some(search_offset + position);
                        // Continue search for the end tag
                        search_offset = min(
                            self.buffer.len(),
                            spectrum_start_tag_idx.unwrap() + search_for.len(),
                        );
                        search_for = SPECTRUM_END_TAG;
                    } else {
                        let spectrum_end_tag_idx = search_offset + position + search_for.len();
                        let spec_id = Self::get_spectrum_id(
                            &self.buffer[spectrum_start_tag_idx.unwrap()..spectrum_end_tag_idx],
                        )?;
                        let spec_offset = (
                            self.read_bytes - self.buffer.len() + spectrum_start_tag_idx.unwrap(),
                            self.read_bytes - self.buffer.len() + spectrum_end_tag_idx,
                        );
                        spectra_offsets.insert(spec_id, spec_offset);
                        // Remove searched buffer by rotating the processed bytes to the end, and truncate the buffer. This does not have any effect on the allocated buffer size.
                        self.remove_n_first_bytes_from_buffer(spectrum_end_tag_idx);
                        // reset search
                        spectrum_start_tag_idx = None;
                        search_offset = 0;
                        search_for = SPECTRUM_START_TAG;
                        // Check if spectrum list ends within next few bytes
                        // TODO: Remove when chromatogram support is added
                        let search_end = min(self.buffer.len(), SPECTRUM_LIST_END_TAG.len() * 3);
                        if self.buffer[..search_end]
                            .windows(SPECTRUM_LIST_END_TAG.len())
                            .position(|window| window == SPECTRUM_LIST_END_TAG)
                            .is_some()
                        {
                            // Do not set is_eof to the value SPECTRUM_LIST_END_TAG search as it might return it to false again after set by read_chunk()
                            self.is_eof = true;
                        }
                    }
                }
            }
        }
        Ok(spectra_offsets)
    }

    /// Creates the mzml file index.
    ///
    fn create_idx(&mut self) -> Result<Index> {
        // init buffer
        self.read_chunk()?;
        // find general information & indention
        self.find_general_information_offset()?;
        self.get_indention()?;
        // remove general information from buffer
        self.remove_n_first_bytes_from_buffer(self.general_information_offset);
        self.get_default_data_processing_ref()?;
        let spectra_offsets = self.get_spectra_offsets()?;
        Ok(Index::new(
            self.file_path.to_path_buf(),
            String::from_utf8_lossy(self.indention.as_slice()).to_string(),
            self.default_data_processing_ref.to_string(),
            self.general_information_offset,
            spectra_offsets,
            HashMap::new(), // TODO: add chromatogram offsets
        ))
    }

    /// Creates a file index for the given mzML file.
    ///
    /// # Arguments
    /// * `file_path` - Path to the mzML file.
    /// * `chunk_size` - Size of the chunks to read from the file.
    pub fn create_index(file_path: &'a Path, chunk_size: Option<usize>) -> Result<Index> {
        let mut indexer = Self::new(file_path, chunk_size);
        Ok(indexer.create_idx()?)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const EXPECTED_SPECTRA: [(&'static str, (usize, usize)); 11] = [
        (
            "controllerType=0 controllerNumber=1 scan=4051",
            (59212, 67042),
        ),
        (
            "controllerType=0 controllerNumber=1 scan=4052",
            (67051, 75575),
        ),
        (
            "controllerType=0 controllerNumber=1 scan=4053",
            (75584, 86661),
        ),
        (
            "controllerType=0 controllerNumber=1 scan=4050",
            (50455, 59203),
        ),
        (
            "controllerType=0 controllerNumber=1 scan=3865",
            (22071, 28215),
        ),
        (
            "controllerType=0 controllerNumber=1 scan=4045",
            (28224, 35295),
        ),
        (
            "controllerType=0 controllerNumber=1 scan=3224",
            (15920, 22062),
        ),
        (
            "controllerType=0 controllerNumber=1 scan=2814",
            (4072, 9932),
        ),
        (
            "controllerType=0 controllerNumber=1 scan=4046",
            (35304, 41934),
        ),
        (
            "controllerType=0 controllerNumber=1 scan=4049",
            (41943, 50446),
        ),
        (
            "controllerType=0 controllerNumber=1 scan=2958",
            (9941, 15911),
        ),
    ];

    const EXPECTED_DEFAULT_DATA_PROCESSING_REF: &'static str = "pwiz_Reader_Thermo_conversion";
    const EXPECTED_CONTENT_BEFORE_SPECTRA_LIST: usize = 3992;
    const EXPECTED_INDENTION: &'static str = "  ";

    #[test]
    fn test_index_creation() {
        let file_path = Path::new("./test_files/spectra_small.mzML");

        let index = Indexer::create_index(&file_path, None).unwrap();

        assert_eq!(index.get_file_path(), file_path);
        assert_eq!(
            index.get_general_information_len(),
            EXPECTED_CONTENT_BEFORE_SPECTRA_LIST
        );
        assert_eq!(
            index.get_default_data_processing_ref(),
            EXPECTED_DEFAULT_DATA_PROCESSING_REF
        );
        assert_eq!(index.get_indention(), EXPECTED_INDENTION);
        for (spec_id, expected_start_end) in EXPECTED_SPECTRA {
            assert!(index.get_spectra().contains_key(spec_id));
            if let Some(start_end) = index.get_spectra().get(spec_id) {
                assert_eq!(expected_start_end.0, start_end.0);
                assert_eq!(expected_start_end.1, start_end.1);
            }
        }

        // TODO: add chromatogram assertion when they get added
    }
}
