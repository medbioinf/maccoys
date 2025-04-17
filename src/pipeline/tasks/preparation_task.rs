use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{anyhow, Context, Error, Result};
use dihardts_omicstools::proteomics::io::mzml::{elements::{has_cv_params::HasCvParams, is_list::IsList, precursor::Precursor, selected_ion::SelectedIon}, reader::Reader as MzMlReader};
use metrics::counter;
use tracing::{debug, error};

use crate::pipeline::{
    configuration::{SearchParameters, StandalonePreparationConfiguration},
    convert::AsInputOutputQueue,
    queue::PipelineQueue,
    storage::{PipelineStorage, RedisPipelineStorage},
};

use super::task::Task;

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


                let mzml_content = match manifest.get_spectrum_mzml() {
                    Ok(content) => content,
                    Err(e) => {
                        error!(
                            "[{} / {}] Error getting mzML content: {:?}",
                            &manifest.uuid, &manifest.spectrum_id, e
                        );
                        continue;
                    }
                };


                let mut mzml_bytes_reader = std::io::Cursor::new(mzml_content);

                let mut mzml_file = match MzMlReader::read_indexed(&mut mzml_bytes_reader, None, false, false) {
                    Ok(file) => file,
                    Err(e) => {
                        error!(
                            "[{} / {}] Error reading mzML content: {:?}",
                            &manifest.uuid, &manifest.spectrum_id, e
                        );
                        continue;
                    }
                };

                let spectrum = match mzml_file.get_spectrum(&manifest.spectrum_id) {
                    Ok(spectrum) => spectrum,
                    Err(e) => {
                        error!(
                            "[{} / {}] cannot find spectrum {:?}",
                            &manifest.uuid, &manifest.spectrum_id, e
                        );
                        continue;
                    }
                };

                match spectrum.get_ms_level() {
                    // Strange
                    Some(0) => {
                        error!(
                            "[{} / {}] MS level 0?!",
                            &manifest.uuid, &manifest.spectrum_id
                        );
                        continue;
                    }
                    // Just continue
                    Some(1) => continue, 
                    // MS level 2 or higher
                    Some(_) => (),
                    // MS level is not set
                    None => {
                        error!(
                            "[{} / {}] MS level is missing",
                            &manifest.uuid, &manifest.spectrum_id
                        );
                        continue;
                    }
                }

                if spectrum.precursor_list.is_none() {
                    error!(
                        "[{} / {}] Precursor list is missing",
                        &manifest.uuid, &manifest.spectrum_id
                    );
                    continue;
                }

                // Precursor list should not be none as it is a MS2 spectrum
                // Collect precursor references
                let precursor_refs: Vec<&Precursor> = spectrum.precursor_list.as_ref().unwrap().iter().collect();
                // Collect selected ion references
                let ion_refs: Vec<&SelectedIon> = precursor_refs
                    .iter()
                    .filter(|precursor| precursor.selected_ion_list.is_some())
                    .flat_map(|precursor| precursor.selected_ion_list.as_ref().unwrap().iter())
                    .collect();

                // Collect mz and charge states as in pair (mz, charge) from selected ion references
                let precursors_result: Result<Vec<Vec<(f64, u8)>>> = ion_refs
                    .iter()
                    .map(|ion| {
                        let mz: f64 = ion.get_cv_param("MS:1000744").first().ok_or_else(|| anyhow!("Spectrum does not have selected ion m/z"))?.value.parse().context("Cannot parse selected ion m/z to f64")?;
                        // // Select charge states statess
                        let mut charge_cv_params = ion.get_cv_param("MS:1000041");
                        // add possible charge states
                        charge_cv_params.extend(ion.get_cv_param("MS:1000633"));

                        let charges: Vec<u8> = if !charge_cv_params.is_empty() {
                            charge_cv_params.into_iter().map(|x| {
                                x.value.parse().map_err(|err| anyhow!("Error parsing charge: {}", err))
                            }).collect::<Result<Vec<u8>>>()?
                        } else {
                            (2..=current_search_params.max_charge).collect()
                        };

                        Ok::<_, Error>(charges.into_iter().map(|charge| (mz, charge)).collect::<Vec<_>>())
                    }).collect();

                // Check for error and flatten the result
                let precursors = match precursors_result {
                    Ok(tmp) => tmp.into_iter().flatten().collect(),
                    Err(e) => {
                        error!(
                            "[{} / {}] Error getting selected ion m/z {:?}",
                            &manifest.uuid, &manifest.spectrum_id, e
                        );
                        continue;
                    }
                };

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
