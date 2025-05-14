// std imports
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::{BufReader, SeekFrom};
use std::path::Path;

// 3rd party imports
use anyhow::{bail, Context, Result};
use polars::prelude::*;

// internal imports
use crate::constants::{COMET_HEADER_ROW, COMET_SEPARATOR};

pub struct PeptideSpectrumMatchTsv;

impl PeptideSpectrumMatchTsv {
    /// Read a Comet PSM file into a dataframe
    ///
    /// # Arguments
    /// * `psm_file_path` - Path to the Comet PSM file
    ///
    pub fn read(psm_file_path: &Path) -> Result<Option<DataFrame>> {
        let reader = match CsvReadOptions::default()
            .with_parse_options(
                CsvParseOptions::default().with_separator(COMET_SEPARATOR.as_bytes()[0]),
            )
            .with_has_header(true)
            .with_skip_lines(COMET_HEADER_ROW as usize)
            .try_into_reader_with_file_path(Some(psm_file_path.to_path_buf()))
        {
            Ok(reader) => reader,
            Err(err) => {
                return Self::handle_polars_error(err)
                    .context("Error when opening Comet PSM file for reading");
            }
        };

        match reader.finish() {
            Ok(df) => Ok(Some(df)),
            Err(err) => Self::handle_polars_error(err)
                .context("Error when parsing Comet PSM file to dataframe"),
        }
    }

    fn handle_polars_error(error: PolarsError) -> Result<Option<DataFrame>> {
        match error {
            PolarsError::NoData(_) => Ok(None),
            _ => bail!(error),
        }
    }

    /// Overwrite a Comet PSM file with the given dataframe
    ///
    /// # Arguments
    /// * `psms` - Dataframe containing the new PSMs
    ///
    pub fn overwrite(psms: DataFrame, psms_file_path: &Path) -> Result<()> {
        let mut psms = psms;

        // open the file for reading and writing
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .open(psms_file_path)
            .context("Error when opening Comet PSM file for overwriting with new scores")?;

        // read the revision number
        let mut reader = BufReader::new(&file);
        let mut revision = String::new();
        reader
            .read_line(&mut revision)
            .context("Error when reading Comet revision")?;
        drop(reader);

        // move cursor back to start of file
        file.seek(SeekFrom::Start(0))?;
        // write the revision number
        file.write(revision.as_bytes())
            .context("Error when writing Comet revision")?;
        // write the dataframe
        CsvWriter::new(&mut file)
            .include_header(true)
            .with_separator(COMET_SEPARATOR.as_bytes()[0])
            .finish(&mut psms)
            .context("Error when writing dataframe")?;
        Ok(())
    }
}
