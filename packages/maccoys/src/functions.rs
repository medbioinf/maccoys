use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use fancy_regex::Regex;
use lazy_static::lazy_static;
use polars::prelude::*;
use tracing::error;

use crate::constants::{
    COMET_DIST_BASE_SCORE, COMET_EXP_BASE_SCORE, DIST_SCORE_NAME, EXP_SCORE_NAME,
    FASTA_DECOY_ENTRY_PREFIX,
};

lazy_static! {
    /// Regex for finding non word characters
    ///
    static ref NON_WORD_CHAR_REGEX: Regex = fancy_regex::Regex::new(r"\W").unwrap();
}

/// Creates the work directory if it does not exist.
///
/// # Arguments
/// * `work_dir` - Work directory
///
pub async fn create_work_dir(work_dir: &Path) -> Result<()> {
    if !work_dir.exists() {
        tokio::fs::create_dir_all(&work_dir)
            .await
            .context("Could not create work directory.")?;
    }
    Ok(())
}

/// Creates a spectrum work directory if it does not exist.s
///
/// # Arguments
/// * `work_dir` - Work directory
/// * `spectrum_id` - Spectrum ID
///
pub async fn create_spectrum_workdir(work_dir: &Path, spectrum_id: &str) -> Result<PathBuf> {
    let spectrum_workdir = work_dir.join(sanatize_string(spectrum_id));
    if !spectrum_workdir.exists() {
        tokio::fs::create_dir_all(&spectrum_workdir)
            .await
            .context("Could not create spectrum work directory.")?;
    }
    Ok(spectrum_workdir)
}

/// Calculates the goodness of fit and scores for the given PSM file,
/// using the python module `maccoys`.
///
/// # Arguments
/// * `psm_file_path` - Path to PSM file
///
pub async fn post_process(psm_file_path: &Path, goodness_file_path: &Path) -> Result<()> {
    // goodness of fit
    let python_args: Vec<&str> = vec![
        "-m",
        "maccoys",
        "comet",
        "goodness",
        psm_file_path.to_str().unwrap(),
        COMET_EXP_BASE_SCORE,
        goodness_file_path.to_str().unwrap(),
    ];
    let output = tokio::process::Command::new("python")
        .args(python_args)
        .output()
        .await
        .context("Error when calling Python module `maccoys` for calculating goodness of fit")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        error!("{:?}", &stderr);
        bail!(stderr)
    }

    // rescoring
    let python_args: Vec<&str> = vec![
        "-m",
        "maccoys",
        "comet",
        "scoring",
        psm_file_path.to_str().unwrap(),
        COMET_EXP_BASE_SCORE,
        EXP_SCORE_NAME,
        COMET_DIST_BASE_SCORE,
        DIST_SCORE_NAME,
    ];
    let output = tokio::process::Command::new("python")
        .args(python_args)
        .output()
        .await
        .context("Error when calling Python module `maccoys_scoring` for scoring")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        error!("{:?}", &stderr);
        bail!(stderr)
    }
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

    let sort_option = SortMultipleOptions::default()
        .with_order_descending(true)
        .with_nulls_last(true)
        .with_multithreaded(false)
        .with_maintain_order(false);

    psms = psms.sort([COMET_EXP_BASE_SCORE], sort_option);
    psms = psms.with_column(col("is_target").not().cum_sum(false).alias("fdr"));
    psms = psms.with_column(cast(col("fdr"), DataType::Float64).alias("fdr"));
    let index: Series = (1..=num_psms).map(|idx| idx as f64).collect();
    psms = psms.with_column((col("fdr") / lit(index)).alias("fdr"));
    Ok(psms.collect()?)
}
