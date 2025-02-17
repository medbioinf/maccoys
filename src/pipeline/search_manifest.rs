// std imports
use std::{
    fs::File,
    io::{BufRead, Cursor},
    path::{Path, PathBuf},
};

// external imports
use anyhow::{Context, Result};
use polars::{frame::DataFrame, io::SerWriter, prelude::CsvWriter};

// local imports
use crate::{functions::sanatize_string, goodness_of_fit_record::GoodnessOfFitRecord};

/// Manifest for a search, storing the current state of the search
/// and serving as the message between the different tasks
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchManifest {
    /// Search UUID
    pub uuid: String,

    /// MS run path
    pub ms_run_name: String,

    /// Spectrum ID of the spectrum to be searched
    pub spectrum_id: String,

    /// Compressed mzML with the spectrum to be searched
    /// use the relared functions to get or store the uncompressed version
    spectrum_mzml: Vec<u8>,

    /// Precursors for the spectrum (mz, charge)
    pub precursors: Vec<(f64, u8)>,

    /// Compressed FASTA files, one for each precursor
    fastas: Vec<Vec<u8>>,

    /// Comet parameter files for each precursor
    comet_configs: Vec<Vec<u8>>,

    /// PSMs dataframe
    pub psms_dataframes: Vec<DataFrame>,

    /// Goodness of fit test results for each precursor
    pub goodness: Vec<Vec<GoodnessOfFitRecord>>,
}

impl SearchManifest {
    /// Create a new search manifest
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where on folder per MS run is created
    /// * `ms_run_mzml_path` - Path to the original mzML file containing the MS run
    ///
    pub fn new(uuid: String, mzml_file_name: &str) -> Self {
        let ms_run_name = sanatize_string(
            PathBuf::from(mzml_file_name)
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap(),
        );

        Self {
            uuid,
            ms_run_name,
            spectrum_id: String::new(),
            spectrum_mzml: Vec::with_capacity(0),
            precursors: Vec::with_capacity(0),
            fastas: Vec::with_capacity(0),
            comet_configs: Vec::with_capacity(0),
            psms_dataframes: Vec::with_capacity(0),
            goodness: Vec::with_capacity(0),
        }
    }

    /// Returns the search directory
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    ///
    pub fn get_search_dir(&self, work_dir: &Path) -> PathBuf {
        work_dir.join(&self.uuid)
    }

    /// Returns the path to the MS run directory
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    ///
    pub fn get_ms_run_dir_path(&self, work_dir: &Path) -> PathBuf {
        self.get_search_dir(work_dir).join(&self.ms_run_name)
    }

    /// Returns the path to the MS run mzML
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    ///
    pub fn get_ms_run_mzml_path(&self, work_dir: &Path) -> PathBuf {
        self.get_ms_run_dir_path(work_dir).join("run.mzML")
    }

    /// Returns the path to the mzML index JSON
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    ///
    pub fn get_index_path(&self, work_dir: &Path) -> PathBuf {
        self.get_ms_run_dir_path(work_dir).join("index.json")
    }

    /// Retuns the path of the spectrum directory wihtin the MS run directory
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    ///
    pub fn get_spectrum_dir_path(&self, work_dir: &Path) -> PathBuf {
        self.get_ms_run_dir_path(work_dir)
            .join(sanatize_string(&self.spectrum_id))
    }

    /// Retuns the path of the spectrum mzML
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    ///
    pub fn get_spectrum_mzml_path(&self, work_dir: &Path) -> PathBuf {
        self.get_spectrum_dir_path(work_dir).join("spectrum.mzML")
    }

    /// Returns the path to the fasta file for the spectrum's precursor
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    /// * `precursor_mz` - Precursor mass to charge ratio
    /// * `precursor_charge` - Precursor charge
    ///
    pub fn get_fasta_file_path(
        &self,
        work_dir: &Path,
        precursor_mz: f64,
        precursor_charge: u8,
    ) -> PathBuf {
        self.get_spectrum_dir_path(work_dir)
            .join(format!("{}_{}.fasta", precursor_mz, precursor_charge))
    }

    /// Returns the path to the Comet parameter file for the spectrum's precursor
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    /// * `precursor_mz` - Precursor mass to charge ratio
    /// * `precursor_charge` - Precursor charge
    ///
    pub fn get_comet_params_path(
        &self,
        work_dir: &Path,
        precursor_mz: f64,
        precursor_charge: u8,
    ) -> PathBuf {
        self.get_spectrum_dir_path(work_dir)
            .join(format!("{}_{}.comet.param", precursor_mz, precursor_charge))
    }

    /// Returns the path to the PSM file for the spectrum's precursor
    /// This alredy has the TSV file extension while Comet writes it with the extension .txt
    /// It is renamed after the search
    ///
    /// # Argumentss
    /// * `work_dir` - Work directory where the results are stored
    /// * `precursor_mz` - Precursor mass to charge ratio
    /// * `precursor_charge` - Precursor charge
    ///
    pub fn get_psms_file_path(
        &self,
        work_dir: &Path,
        precursor_mz: f64,
        precursor_charge: u8,
    ) -> PathBuf {
        self.get_spectrum_dir_path(work_dir)
            .join(format!("{}_{}.psms.tsv", precursor_mz, precursor_charge))
    }

    /// Returns the path to the goodness TSV file for the spectrum's precursor
    ///
    /// # Argumentss
    /// * `work_dir` - Work directory where the results are stored
    /// * `precursor_mz` - Precursor mass to charge ratio
    /// * `precursor_charge` - Precursor charge
    ///
    pub fn get_goodness_file_path(
        &self,
        work_dir: &Path,
        precursor_mz: f64,
        precursor_charge: u8,
    ) -> PathBuf {
        self.get_spectrum_dir_path(work_dir).join(format!(
            "{}_{}.goodness.tsv",
            precursor_mz, precursor_charge
        ))
    }

    /// Returns the path to the spectrum ID file
    ///
    /// # Argumentss
    /// * `work_dir` - Work directory where the results are stored
    ///
    pub fn get_spectrum_id_path(&self, work_dir: &Path) -> PathBuf {
        self.get_spectrum_dir_path(work_dir).join("spectrum_id.txt")
    }

    /// Compress a given file
    ///
    /// # Arguments
    /// * `file` - File to be compressed
    ///
    fn compress_file(mut file: impl BufRead) -> Result<Vec<u8>> {
        let mut compressed_file = Vec::new();
        let mut compressed_file_cursor = Cursor::new(&mut compressed_file);
        lzma_rs::lzma2_compress(&mut file, &mut compressed_file_cursor)?;

        Ok(compressed_file)
    }

    fn decompress_file(file: &Vec<u8>) -> Result<Vec<u8>> {
        let mut file_cursor = Cursor::new(file);
        let mut uncompressed_file = Vec::with_capacity(file.len());
        let mut uncompressed_file_cursor = Cursor::new(&mut uncompressed_file);
        lzma_rs::lzma2_decompress(&mut file_cursor, &mut uncompressed_file_cursor)?;

        Ok(uncompressed_file)
    }

    /// Set the spectrum mzML
    ///
    /// # Arguments
    /// * `spectrum_mzml` - Spectrum mzML as a BufRead
    ///
    pub fn set_spectrum_mzml(&mut self, spectrum_mzml: impl BufRead) -> Result<()> {
        self.spectrum_mzml = Self::compress_file(spectrum_mzml)?;

        Ok(())
    }

    /// Get the spectrum mzML
    ///
    pub fn get_spectrum_mzml(&self) -> Result<Vec<u8>> {
        Self::decompress_file(&self.spectrum_mzml)
    }

    /// Get the spectrum mzML and write it to a file
    ///
    /// # Arguments
    /// * `mzml_file_path` - Path to the file where the spectrum mzML will be written
    ///
    pub async fn spectrum_mzml_to_file(&self, mzml_file_path: &PathBuf) -> Result<()> {
        let spectrum_mzml = self.get_spectrum_mzml()?;
        tokio::fs::write(mzml_file_path, spectrum_mzml).await?;
        Ok(())
    }

    /// Unset the spectrum mzML
    ///
    pub fn unset_spectrum_mzml(&mut self) {
        self.spectrum_mzml = Vec::with_capacity(0);
    }

    /// Check if the spectrum mzML is set
    ///
    pub fn is_spectrum_mzml_set(&self) -> bool {
        !self.spectrum_mzml.is_empty()
    }

    /// Set the FASTA
    ///
    /// # Arguments
    /// * `fasta` - FASTA as a BufRead
    ///
    pub fn push_fasta(&mut self, fasta: impl BufRead) -> Result<()> {
        self.fastas.push(Self::compress_file(fasta)?);
        Ok(())
    }

    /// Get the FASTA
    ///
    pub fn pop_fasta(&mut self) -> Result<Vec<u8>> {
        match self.fastas.pop() {
            Some(fasta) => Ok(Self::decompress_file(&fasta)?),
            None => Err(anyhow::anyhow!("No FASTA files available")),
        }
    }

    /// Get FASTA and write it to a file. If you do not pop
    ///
    /// # Arguments
    /// * `fasta_file_path` - Path to the file where the FASTA will be written
    ///
    pub async fn pop_fasta_to_file(&mut self, fasta_file_path: &PathBuf) -> Result<()> {
        let fasta = self.pop_fasta()?;
        tokio::fs::write(fasta_file_path, fasta).await?;
        Ok(())
    }

    /// Set the Comet parameter file
    ///
    /// # Arguments
    /// * `comet_config` - Comet parameter file as a BufRead
    ///
    pub fn push_comet_config(&mut self, comet_config: impl BufRead) -> Result<()> {
        self.comet_configs.push(Self::compress_file(comet_config)?);
        Ok(())
    }

    /// Get the Comet parameter file
    ///
    pub fn pop_comet_config(&mut self) -> Result<Vec<u8>> {
        match self.comet_configs.pop() {
            Some(comet_config) => Ok(Self::decompress_file(&comet_config)?),
            None => Err(anyhow::anyhow!("No Comet parameter files available")),
        }
    }

    /// Get Comet parameter file and write it to a file. If you do not pop
    ///
    /// # Arguments
    /// * `comet_parameter_content_file_path` - Path to the file where the Comet parameter file will be written
    ///
    pub async fn pop_comet_config_to_file(
        &mut self,
        comet_config_file_path: &PathBuf,
    ) -> Result<()> {
        let comet_parameter_content = self.pop_comet_config()?;
        tokio::fs::write(comet_config_file_path, comet_parameter_content).await?;
        Ok(())
    }

    /// Get fasta at position `idx`
    ///
    /// # Arguments
    /// * `idx` - Index of the fasta file
    ///
    pub fn get_fasta(&self, idx: usize) -> Result<Vec<u8>> {
        Self::decompress_file(&self.fastas[idx])
    }

    /// Get fasta at position `idx` and write it to a file
    ///
    /// # Arguments
    /// * `idx` - Index of the fasta file
    /// * `fasta_file_path` - Path to the file where the FASTA will be written
    ///
    pub async fn get_fasta_to_file(&self, idx: usize, fasta_file_path: &PathBuf) -> Result<()> {
        let fasta = self.get_fasta(idx)?;
        tokio::fs::write(fasta_file_path, fasta).await?;
        Ok(())
    }

    /// Unset the FASTA
    ///
    pub fn unset_fasta(&mut self) {
        self.fastas = Vec::with_capacity(0);
    }

    /// Check if the FASTA is set
    ///
    pub fn is_fasta_set(&self) -> bool {
        !self.fastas.is_empty()
    }

    /// Get the number of FASTA files
    ///
    pub fn fastas_len(&self) -> usize {
        self.fastas.len()
    }

    /// Get FASTA and write it to a file. If you do not pop
    ///
    /// # Arguments
    /// * `fasta_file_path` - Path to the file where the FASTA will be written
    ///
    pub async fn pop_psms_to_file(&mut self, psms_file_path: &PathBuf) -> Result<()> {
        let mut psms = match self.psms_dataframes.pop() {
            Some(psms) => psms,
            None => return Err(anyhow::anyhow!("No PSMs available")),
        };

        let mut psms_file = std::fs::File::create(psms_file_path)?;

        CsvWriter::new(&mut psms_file)
            .include_header(true)
            .with_separator("\t".as_bytes()[0])
            .finish(&mut psms)
            .context("Error when writing PSM dataframe")?;

        Ok(())
    }

    /// Get goodness of fit and write it to a file
    ///
    /// # Arguments
    /// * `goodness_file_path` - Path to the file where the goodness of fit will be written
    ///
    pub fn pop_goodness_of_fit_to_file(&mut self, goodness_file_path: &PathBuf) -> Result<()> {
        let goodness_of_fits = match self.goodness.pop() {
            Some(goodness) => goodness,
            None => return Err(anyhow::anyhow!("No goodness of fit available")),
        };
        let goodness_of_fit_file = File::create(goodness_file_path)?;

        let mut writer = csv::WriterBuilder::new()
            .delimiter(b'\t')
            .has_headers(true)
            .from_writer(goodness_of_fit_file);

        for goodness_row in goodness_of_fits {
            writer.serialize(goodness_row)?;
        }

        Ok(())
    }
}
