// std imports
use std::path::PathBuf;

// local imports
use crate::functions::sanatize_string;

/// Manifest for a search, storing the current state of the search
/// and serving as the message between the different tasks
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchManifest {
    /// Search UUID
    pub uuid: String,

    /// MS run path
    pub ms_run_name: String,

    /// Path to the original mzML file containing the MS run
    pub ms_run_mzml_path: PathBuf,

    /// Spectrum ID of the spectrum to be searched
    pub spectrum_id: String,

    /// mzML with the spectrum to be searched
    pub spectrum_mzml: Vec<u8>,

    /// Precursors for the spectrum (mz, charge)
    pub precursors: Vec<(f64, Vec<u8>)>,

    /// Flag indicating if the indexing has been done
    pub is_indexing_done: bool,

    /// Flag indicating if the preparation has been done
    pub is_preparation_done: bool,

    /// Flag indicating if the search space has been generated
    pub is_search_space_generated: bool,

    /// Flag indicating if the Comet search has been performed
    pub is_comet_search_done: bool,

    /// Flag indicating if the goodness and rescoring has been performed
    pub is_goodness_and_rescoring_done: bool,
}

impl SearchManifest {
    /// Create a new search manifest
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where on folder per MS run is created
    /// * `ms_run_mzml_path` - Path to the original mzML file containing the MS run
    ///
    pub fn new(uuid: String, ms_run_mzml_path: PathBuf) -> Self {
        let ms_run_name = sanatize_string(ms_run_mzml_path.file_stem().unwrap().to_str().unwrap());

        Self {
            uuid,
            ms_run_name,
            ms_run_mzml_path,
            spectrum_id: String::new(),
            spectrum_mzml: Vec::new(),
            precursors: Vec::new(),
            is_indexing_done: true,
            is_preparation_done: true,
            is_search_space_generated: true,
            is_comet_search_done: true,
            is_goodness_and_rescoring_done: true,
        }
    }

    /// Returns the path to the MS run directory
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    ///
    pub fn get_ms_run_dir_path(&self, work_dir: &PathBuf) -> PathBuf {
        work_dir.join(&self.ms_run_name)
    }

    /// Retuns the path of the spectrum directory wihtin the MS run directory
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    ///
    pub fn get_spectrum_dir_path(&self, work_dir: &PathBuf) -> PathBuf {
        self.get_ms_run_dir_path(work_dir)
            .join(sanatize_string(&self.spectrum_id))
    }

    /// Retuns the path of the spectrum mzML
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    ///
    pub fn get_spectrum_mzml_path(&self, work_dir: &PathBuf) -> PathBuf {
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
        work_dir: &PathBuf,
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
        work_dir: &PathBuf,
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
        work_dir: &PathBuf,
        precursor_mz: f64,
        precursor_charge: u8,
    ) -> PathBuf {
        self.get_spectrum_dir_path(work_dir)
            .join(format!("{}_{}.tsv", precursor_mz, precursor_charge))
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
        work_dir: &PathBuf,
        precursor_mz: f64,
        precursor_charge: u8,
    ) -> PathBuf {
        self.get_spectrum_dir_path(work_dir).join(format!(
            "{}_{}.goodness.tsv",
            precursor_mz, precursor_charge
        ))
    }
}
