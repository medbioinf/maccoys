use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{Context, Result};
use dihardts_omicstools::{
    mass_spectrometry::spectrum::{MsNSpectrum, Precursor, Spectrum as SpectrumTrait},
    proteomics::io::mzml::reader::{Reader as MzMlReader, Spectrum},
};
use metrics::counter;
use tracing::{debug, error, trace};

use crate::pipeline::{
    configuration::{SearchParameters, StandalonePreparationConfiguration},
    convert::AsInputOutputQueue,
    queue::PipelineQueue,
    storage::{PipelineStorage, RedisPipelineStorage},
};

use super::task::Task;

/// Default start tag for a spectrum in mzML
const SPECTRUM_START_TAG: &[u8; 10] = b"<spectrum ";

/// Default stop tag for a spectrum in mzML
const SPECTRUM_STOP_TAG: &[u8; 11] = b"</spectrum>";

/// Prefix for the preparation counter
///
const COUNTER_PREFIX: &str = "maccoys_preparations";

/// /// Task to prepare the spectra work directories for the search space generation and search.
///
pub struct PreparationTask;

impl PreparationTask {
    /// Start the preparation task
    ///
    ///
    /// # Arguments
    /// * `storage` - The storage to use
    /// * `preparation_queue` - The queue to get the spectra to prepare
    /// * `search_space_generation_queue` - The queue to push the prepared spectra to
    /// * `stop_flag` - The flag to stop the task
    ///
    pub async fn start<Q, S>(
        storage: Arc<S>,
        preparation_queue: Arc<Q>,
        search_space_generation_queue: Arc<Q>,
        stop_flag: Arc<AtomicBool>,
    ) where
        Q: PipelineQueue + 'static,
        S: PipelineStorage + 'static,
    {
        let mut current_search_params = SearchParameters::new();
        let mut last_search_uuid = String::new();
        let mut metrics_counter_name = COUNTER_PREFIX.to_string();

        loop {
            while let Some(mut manifest) = preparation_queue.pop().await {
                debug!("[{} / {}] Preparing", &manifest.uuid, &manifest.spectrum_id);

                if manifest.spectrum_id.is_empty() {
                    error!(
                        "[{} / {}] Spectrum ID is empty in spectra_dir_creation_thread",
                        &manifest.uuid, &manifest.spectrum_id
                    );
                    continue;
                }

                if !manifest.is_spectrum_mzml_set() {
                    error!(
                        "[{} / {}] Spectrum mzML is empty in spectra_dir_creation_thread",
                        &manifest.uuid, &manifest.spectrum_id
                    );
                    continue;
                }

                if last_search_uuid != manifest.uuid {
                    current_search_params =
                        match storage.get_search_parameters(&manifest.uuid).await {
                            Ok(Some(params)) => params,
                            Ok(None) => {
                                error!(
                                    "[{} / {}] Search params not found",
                                    manifest.uuid, manifest.spectrum_id
                                );
                                continue;
                            }
                            Err(e) => {
                                error!(
                                    "[{} / {}] Error getting search params from storage: {:?}",
                                    &manifest.uuid, &manifest.spectrum_id, e
                                );
                                continue;
                            }
                        };
                    last_search_uuid = manifest.uuid.clone();
                    metrics_counter_name = format!("{}_{}", COUNTER_PREFIX, last_search_uuid);
                }

                let spectrum_mzml = match manifest.get_spectrum_mzml() {
                    Ok(spectrum_mzml) => spectrum_mzml,
                    Err(e) => {
                        error!(
                            "[{} / {}] Error getting spectrum mzML: {:?}",
                            &manifest.uuid, &manifest.spectrum_id, e
                        );
                        continue;
                    }
                };

                let start = match spectrum_mzml
                    .windows(SPECTRUM_START_TAG.len())
                    .position(|window| window == SPECTRUM_START_TAG)
                {
                    Some(start) => start,
                    None => {
                        error!(
                            "[{} / {}] No spectrum start",
                            &manifest.uuid, &manifest.spectrum_id
                        );
                        continue;
                    }
                };

                let stop = match spectrum_mzml
                    .windows(SPECTRUM_STOP_TAG.len())
                    .position(|window| window == SPECTRUM_STOP_TAG)
                {
                    Some(stop) => stop,
                    None => {
                        error!(
                            "[{} / {}] No spectrum stop",
                            &manifest.uuid, &manifest.spectrum_id
                        );
                        continue;
                    }
                };

                // Reduce to spectrum
                let spectrum_mzml = spectrum_mzml[start..stop].to_vec();

                // As this mzML is already reduced to the spectrum of interest, we can parse it directly
                // using MzMlReader::parse_spectrum_xml
                let spectrum = match MzMlReader::parse_spectrum_xml(spectrum_mzml.as_slice()) {
                    Ok(spectrum) => spectrum,
                    Err(e) => {
                        error!(
                            "[{} / {}] Error parsing spectrum: {:?}",
                            &manifest.uuid, &manifest.spectrum_id, e
                        );
                        continue;
                    }
                };

                let spectrum = match spectrum {
                    Spectrum::MsNSpectrum(spectrum) => spectrum,
                    _ => {
                        // Ignore MS1
                        trace!(
                            "[{} / {}] Ignoring MS1 spectrum",
                            &manifest.uuid,
                            &manifest.spectrum_id
                        );
                        continue;
                    }
                };

                // Ignore MS3 and higher
                if spectrum.get_ms_level() != 2 {
                    trace!(
                        "[{} / {}] Ignoring MS{} spectrum",
                        &manifest.uuid,
                        &manifest.spectrum_id,
                        spectrum.get_ms_level()
                    );
                    continue;
                }

                // Get mass to charge ratio and charges the most complicated way possible...
                let precursors: Vec<(f64, u8)> = spectrum
                    .get_precursors()
                    .iter()
                    .flat_map(|precursor| {
                        precursor
                            .get_ions()
                            .iter()
                            .flat_map(|(mz, charges)| {
                                // If precursor has no charges, use the default charges
                                if charges.is_empty() {
                                    (2..=current_search_params.max_charge)
                                        .map(|charge| (*mz, charge))
                                        .collect::<Vec<(f64, u8)>>()
                                } else {
                                    charges
                                        .iter()
                                        .map(|charge| (*mz, *charge))
                                        .collect::<Vec<(f64, u8)>>()
                                }
                            })
                            .collect::<Vec<(f64, u8)>>()
                    })
                    .collect();

                drop(spectrum);

                counter!(metrics_counter_name.clone()).increment(1);

                manifest.precursors = precursors;

                loop {
                    manifest = match search_space_generation_queue.push(manifest).await {
                        Ok(_) => {
                            break;
                        }
                        Err(e) => {
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                            e
                        }
                    }
                }
            }
            if stop_flag.load(Ordering::Relaxed) {
                break;
            }
            // wait before checking the queue again
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    /// Run the preparation task by itself
    ///
    /// # Arguments
    /// * `work_dir` - Working directory
    /// * `config_file_path` - Path to the configuration file
    ///
    pub async fn run_standalone(config_file_path: PathBuf) -> Result<()> {
        let config: StandalonePreparationConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let (input_queue, output_queue) = config.as_input_output_queue().await?;
        let input_queue = Arc::new(input_queue);
        let output_queue = Arc::new(output_queue);
        let storage = Arc::new(RedisPipelineStorage::new(&config.storage).await?);

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.preparation.num_tasks)
            .map(|_| {
                tokio::spawn(PreparationTask::start(
                    storage.clone(),
                    input_queue.clone(),
                    output_queue.clone(),
                    stop_flag.clone(),
                ))
            })
            .collect();

        for handle in handles {
            handle.await?;
        }

        Ok(())
    }
}

impl Task for PreparationTask {
    fn get_counter_prefix() -> &'static str {
        COUNTER_PREFIX
    }
}
