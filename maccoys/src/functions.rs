// std imports
use std::cmp::{max, min};
use std::fs::{read_to_string, write as write_file};
use std::path::Path;

// 3rd party imports
use anyhow::{bail, Context, Result};
use dihardts_omicstools::mass_spectrometry::spectrum::{
    MsNSpectrum as MsNSpectrumTrait, Precursor as PrecursorTrait,
};
use dihardts_omicstools::mass_spectrometry::unit_conversions::mass_to_charge_to_dalton;
use dihardts_omicstools::proteomics::{
    io::mzml::{
        index::Index,
        indexed_reader::IndexedReader,
        reader::{Reader as MzmlReader, Spectrum},
    },
    post_translational_modifications::PostTranslationalModification,
};
use fancy_regex::Regex;
use lazy_static::lazy_static;
use macpepdb::functions::post_translational_modification::validate_ptm_vec;
use macpepdb::mass::convert::to_int as mass_to_int;
use polars::prelude::*;
use tokio::process::Command;
use tracing::{error, info};

use crate::constants::{
    COMET_DIST_BASE_SCORE, COMET_EXP_BASE_SCORE, COMET_MAX_PSMS, DIST_SCORE_NAME, EXP_SCORE_NAME,
    FASTA_DECOY_ENTRY_PREFIX,
};
// internal imports
use crate::io::comet::peptide_spectrum_match_tsv::{
    overwrite as overwrite_comet_tsv, read as read_comet_tsv,
};
use crate::{
    constants::FASTA_SEQUENCE_LINE_LENGTH,
    io::comet::configuration::Configuration as CometConfiguration,
    search_space::search_space_generator::SearchSpaceGenerator,
};

lazy_static! {
    /// Regex for finding non word characters
    ///
    static ref NON_WORD_CHAR_REGEX: Regex = fancy_regex::Regex::new(r"\W").unwrap();
}

/// Creates a FASTA entry for the given sequence.
///
/// # Arguments
/// * `sequence` - Sequence
/// * `index` - Sequence index
/// * `entry_prefix` - Entry prefix
/// * `entry_name_prefix` - Entry name prefix
///
pub fn gen_fasta_entry(
    sequence: &str,
    index: usize,
    entry_prefix: &str,
    entry_name_prefix: &str,
) -> String {
    let mut entry = format!(
        ">{}|{entry_name_prefix}{index}|{entry_name_prefix}{index}\n",
        entry_prefix,
        entry_name_prefix = entry_name_prefix,
        index = index
    );
    for start in (0..sequence.len()).step_by(FASTA_SEQUENCE_LINE_LENGTH) {
        let stop = min(start + FASTA_SEQUENCE_LINE_LENGTH, sequence.len());
        entry.push_str(&sequence[start..stop]);
        entry.push('\n');
    }
    return entry;
}

/// Creates a search space for the given mass, mass tolerance and PTMs.
/// Returns the number of targets and decoys.
///
/// # Arguments
/// * `fasta_file_path` - Path to search space FASTA file.
/// * `mass` - Mass
/// * `ptm_file_path` - Path to PTM file
/// * `lower_mass_tolerance_ppm` - Lower mass tolerance in PPM
/// * `upper_mass_tolerance_ppm` - Upper mass tolerance in PPM
/// * `max_variable_modifications` - Maximal number of variable modifications
/// * `decoys_per_peptide` - Amount of decoys to generate
/// * `target_url` - URL for fetching targets, can be URL for the database (`scylla://host1,host2,host3/keyspace`) or base url for MaCPepDB web API.
/// * `decoy_url` - URL for decoys targets, can be URL for the database (`scylla://host1,host2,host3/keyspace`) or base url for MaCPepDB web API.
/// * `target_lookup_url` - Optional URL for checking generated decoy against targets.
/// * `decoy_cache_url` - URL for caching decoys, can be URL for the database (`scylla://host1,host2,host3/keyspace`) or base url for MaCPepDB web API.
///
pub async fn create_search_space(
    fasta_file_path: &Path,
    ptms: &Vec<PostTranslationalModification>,
    mass: i64,
    lower_mass_tolerance_ppm: i64,
    upper_mass_tolerance_ppm: i64,
    max_variable_modifications: i8,
    decoys_per_peptide: usize,
    target_url: String,
    decoy_url: Option<String>,
    target_lookup_url: Option<String>,
    decoy_cache_url: Option<String>,
) -> Result<(usize, usize)> {
    validate_ptm_vec(&ptms)?;
    let search_space_generator = SearchSpaceGenerator::new(
        target_url.as_str(),
        decoy_url,
        target_lookup_url,
        decoy_cache_url,
    )
    .await?;
    Ok(search_space_generator
        .create(
            Path::new(&fasta_file_path),
            mass,
            lower_mass_tolerance_ppm,
            upper_mass_tolerance_ppm,
            max_variable_modifications,
            ptms.clone(),
            decoys_per_peptide,
        )
        .await?)
}

/// Prepares the search by extracting the spectrum from the original spectrum file and creates a search space and comet params file for each charge state.
///
/// # Arguments
/// * `original_spectrum_file_path` - Path to the original spectrum file
/// * `index_file_path` - Path to the index file
/// * `spectrum_id` - Spectrum ID
/// * `work_dir` - Working directory to store all files
/// * `default_comet_file_path` - Path to the default Comet configuration file
/// * `ptms` - PTMs
/// * `lower_mass_tolerance_ppm` - Lower mass tolerance in PPM
/// * `upper_mass_tolerance_ppm` - Upper mass tolerance in PPM
/// * `max_variable_modifications` - Maximal number of variable modifications
/// * `decoys_per_peptide` - Amount of decoys to generate
/// * `target_url` - URL for fetching targets, can be URL for the database (`scylla://host1,host2,host3/keyspace`) or base url for MaCPepDB web API.
/// * `decoy_url` - URL for decoys targets, can be URL for the database (`scylla://host1,host2,host3/keyspace`) or base url for MaCPepDB web API.
/// * `target_lookup_url` - Optional URL for checking generated decoy against targets.
/// * `decoy_cache_url` - URL for caching decoys, can be URL for the database (`scylla://host1,host2,host3/keyspace`) or base url for MaCPepDB web API.
///
pub async fn search_preparation(
    original_spectrum_file_path: &Path,
    index_file_path: &Path,
    spectrum_id: &str,
    work_dir: &Path,
    default_comet_file_path: &Path,
    ptms: &Vec<PostTranslationalModification>,
    lower_mass_tolerance_ppm: i64,
    upper_mass_tolerance_ppm: i64,
    max_variable_modifications: i8,
    fragment_tolerance: f64,
    fragment_bin_offset: f64,
    max_charge: u8,
    decoys_per_peptide: usize,
    target_url: &str,
    decoy_url: Option<String>,
    target_lookup_url: Option<String>,
    decoy_cache_url: Option<String>,
) -> Result<()> {
    let extracted_spectrum_file_path = work_dir.join("extracted.mzML");
    // Extract the spectrum from the original spectrum file
    let index = Index::from_json(&read_to_string(&index_file_path)?)?;
    let mut extractor = IndexedReader::new(Path::new(&original_spectrum_file_path), &index)?;
    write_file(
        &extracted_spectrum_file_path,
        extractor.extract_spectrum(&spectrum_id)?,
    )
    .context("Could not write extracted spectrum.")?;
    let spectrum =
        match MzmlReader::parse_spectrum_xml(extractor.get_raw_spectrum(&spectrum_id)?.as_slice())?
        {
            Spectrum::MsNSpectrum(spec) => spec,
            _ => {
                bail!("Extracted spectrum is not a MS2 spectrum");
            }
        };
    let mut comet_config = CometConfiguration::new(Path::new(&default_comet_file_path))?;

    // Set general information
    comet_config.set_mass_tolerance(max(lower_mass_tolerance_ppm, upper_mass_tolerance_ppm))?;
    comet_config.set_max_variable_mods(max_variable_modifications)?;
    comet_config.set_ptms(&ptms, max_variable_modifications)?;
    comet_config.set_num_results(COMET_MAX_PSMS)?;
    comet_config.set_fragment_bin_tolerance(fragment_tolerance)?;
    comet_config.set_fragment_bin_offset(fragment_bin_offset)?;

    for precursor in spectrum.get_precursors() {
        for (precursor_mz, precursor_charges) in precursor.get_ions() {
            // If the spectrum precursor charges are not set, use the given charges
            let precursor_charges = if precursor_charges.is_empty() {
                (2..=max_charge).collect()
            } else {
                precursor_charges.clone()
            };

            for precursor_charge in precursor_charges {
                let file_base_name = format!("{}", precursor_charge);
                let mass = mass_to_int(mass_to_charge_to_dalton(*precursor_mz, precursor_charge));
                let fasta_file_path = work_dir.join(format!("{}.fasta", file_base_name));
                let comet_config_path = work_dir.join(format!("{}.comet.params", file_base_name));
                comet_config.set_charge(precursor_charge)?;
                comet_config
                    .to_file(&comet_config_path)
                    .context("Could not write adjusted Comet parameter file.")?;
                create_search_space(
                    &fasta_file_path,
                    ptms,
                    mass,
                    lower_mass_tolerance_ppm,
                    upper_mass_tolerance_ppm,
                    max_variable_modifications,
                    decoys_per_peptide,
                    target_url.to_owned(),
                    decoy_url.clone(),
                    target_lookup_url.clone(),
                    decoy_cache_url.clone(),
                )
                .await?;
            }
        }
    }

    Ok(())
}

/// Calculates the goodness of fit and scores for the given PSM file,
/// using the python module `maccoys_scoring`.
///
/// # Arguments
/// * `psm_file_path` - Path to PSM file
///
pub async fn post_process(psm_file_path: &Path) -> Result<()> {
    // fdr calculation
    let mut psms = match read_comet_tsv(psm_file_path)? {
        Some(psms) => psms,
        None => return Ok(()),
    };
    psms = mark_target_and_decoys(psms)?;
    psms = false_discovery_rate(psms)?;
    overwrite_comet_tsv(psms, psm_file_path)?;

    // TODO: Find crates to substitute the Python module or compile it into the binary
    // // goodness of fit
    // let python_args: Vec<&str> = vec![
    //     "-m",
    //     "maccoys_scoring",
    //     "comet",
    //     "goodness",
    //     psm_file_path.to_str().unwrap(),
    //     COMET_EXP_BASE_SCORE,
    //     goodness_file_path.to_str().unwrap(),
    // ];
    // let output = Command::new("python")
    //     .args(python_args)
    //     .output()
    //     .await
    //     .context(
    //         "Error when calling Python module `maccoys_scoring` for calculating goodness of fit",
    //     )?;

    // info!("{}", String::from_utf8_lossy(&output.stdout));
    // if !output.status.success() {
    //     let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    //     error!("{:?}", &stderr);
    //     bail!(stderr)
    // }

    // // rescoring
    // let python_args: Vec<&str> = vec![
    //     "-m",
    //     "maccoys_scoring",
    //     "comet",
    //     "scoring",
    //     psm_file_path.to_str().unwrap(),
    //     COMET_EXP_BASE_SCORE,
    //     EXP_SCORE_NAME,
    //     COMET_DIST_BASE_SCORE,
    //     DIST_SCORE_NAME,
    // ];
    // let output = Command::new("python")
    //     .args(python_args)
    //     .output()
    //     .await
    //     .context("Error when calling Python module `maccoys_scoring` for scoring")?;

    // info!("{}", String::from_utf8_lossy(&output.stdout));
    // if !output.status.success() {
    //     let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    //     error!("{:?}", &stderr);
    //     bail!(stderr)
    // }
    Ok(())
}

/// Sanitizes the given string by replacing all non word characters with `_`.
///
/// # Arguments
/// * `some_str` - string
///
pub fn sanatize_string(some_str: &str) -> String {
    NON_WORD_CHAR_REGEX.replace_all(some_str, "_").to_string()
}

/// Add a column `is_target` to the given PSMs DataFrame.
///
/// # Arguments
/// * `psms` - PSMs DataFrame
///
pub fn mark_target_and_decoys(psms: DataFrame) -> Result<DataFrame> {
    let mut psms = psms.lazy();
    psms = psms.with_column(
        col("protein")
            .str()
            .starts_with(lit(FASTA_DECOY_ENTRY_PREFIX))
            .not()
            .alias("is_target"),
    );
    Ok(psms.collect()?)
}

/// Calculates the false discovery rate for the given PSMs DataFrame.
/// The DataFrame must have an `is_target`-column.
///
/// # Arguments
/// * `psms` - PSMs DataFrame
///
pub fn false_discovery_rate(psms: DataFrame) -> Result<DataFrame> {
    let num_psms = psms.height();
    let mut psms = psms.lazy();

    let sort_option = SortOptions {
        descending: true,
        nulls_last: true,
        multithreaded: false,
        maintain_order: false,
    };
    psms = psms.sort(COMET_EXP_BASE_SCORE, sort_option);
    psms = psms.with_column(col("is_target").not().cum_sum(false).alias("fdr"));
    psms = psms.with_column(cast(col("fdr"), DataType::Float64).alias("fdr"));
    let index: Series = (1..=num_psms).map(|idx| idx as f64).collect();
    psms = psms.with_column((col("fdr") / lit(index)).alias("fdr"));
    Ok(psms.collect()?)
}
