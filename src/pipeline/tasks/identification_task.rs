use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{Context, Result};
use polars::frame::DataFrame;
use tokio::fs::create_dir_all;
use tracing::{debug, error};

use crate::{
    functions::run_comet_search,
    io::comet::{
        configuration::Configuration as CometConfiguration,
        peptide_spectrum_match_tsv::PeptideSpectrumMatchTsv,
    },
    pipeline::{
        configuration::{
            CometSearchTaskConfiguration, SearchParameters, StandaloneCometSearchConfiguration,
        },
        convert::AsInputOutputQueueAndStorage,
        queue::PipelineQueue,
        storage::PipelineStorage,
    },
};

/// Task to identify the spectra
///
pub struct IdentificationTask<Q: PipelineQueue + 'static, S: PipelineStorage + 'static> {
    _phantom_queue: std::marker::PhantomData<Q>,
    _phantom_storage: std::marker::PhantomData<S>,
}

impl<Q, S> IdentificationTask<Q, S>
where
    Q: PipelineQueue + 'static,
    S: PipelineStorage + 'static,
{
    /// Starts the task
    ///
    /// # Arguments
    /// * `local_work_dir` -  Local work directory
    /// * `config` - Configuration for the Comet search task
    /// * `storage` - Storage to access params and PTMs
    /// * `comet_search_queue` - Queue for the Comet search task
    /// * `goodness_and_rescoreing_queue` - Goodness and rescoreing queue
    /// * `stop_flag` - Flag to indicate to stop once the Comet search queue is empty
    ///
    pub async fn start(
        local_work_dir: PathBuf,
        config: Arc<CometSearchTaskConfiguration>,
        storage: Arc<S>,
        comet_search_queue: Arc<Q>,
        goodness_and_rescoreing_queue: Arc<Q>,
        stop_flag: Arc<AtomicBool>,
    ) {
        match create_dir_all(&local_work_dir).await {
            Ok(_) => (),
            Err(e) => {
                error!("Error creating local work directory: {:?}", e);
                return;
            }
        }
        let mut last_search_uuid = String::new();
        let mut current_comet_config: Option<CometConfiguration> = None;
        let mut current_search_params = SearchParameters::new();

        let comet_params_file_path = local_work_dir.join("comet.params");
        let fasta_file_path = local_work_dir.join("search_space.fasta");
        let psms_file_path = local_work_dir.join("psms.txt");
        let mzml_file_path = local_work_dir.join("ms_run.mzML");

        loop {
            while let Some(mut manifest) = comet_search_queue.pop().await {
                debug!(
                    "[{} / {}] Running Comet search",
                    &manifest.uuid, &manifest.spectrum_id
                );

                if !manifest.is_fasta_set() {
                    error!(
                        "[{} / {}] Search space not generated in `comet_search_task`",
                        &manifest.uuid, &manifest.spectrum_id
                    );
                    continue;
                }

                if last_search_uuid != manifest.uuid {
                    current_comet_config = match storage.get_comet_config(&manifest.uuid).await {
                        Ok(Some(config)) => Some(config),
                        Ok(None) => {
                            error!(
                                "[{} / {}] Comet config not found",
                                &manifest.uuid, &manifest.spectrum_id
                            );
                            continue;
                        }
                        Err(e) => {
                            error!(
                                "[{} / {}] Error getting comet config from storage: {:?}",
                                &manifest.uuid, &manifest.spectrum_id, e
                            );
                            continue;
                        }
                    };

                    match current_comet_config
                        .as_mut()
                        .unwrap()
                        .set_option("threads", &format!("{}", config.threads))
                    {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error setting threads in Comet configuration: {:?}",
                                &manifest.uuid, &manifest.spectrum_id, e
                            );
                            continue;
                        }
                    }

                    current_search_params =
                        match storage.get_search_parameters(&manifest.uuid).await {
                            Ok(Some(params)) => params,
                            Ok(None) => {
                                error!(
                                    "[{} / {}] Search params not found`",
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
                }

                // Unwrap the current Comet configuration for easier access
                let comet_config = current_comet_config.as_mut().unwrap();

                match manifest.spectrum_mzml_to_file(&mzml_file_path).await {
                    Ok(_) => (),
                    Err(e) => {
                        error!(
                            "[{} / {}] Error writing spectrum mzML: {:?}",
                            &manifest.uuid, &manifest.spectrum_id, e
                        );
                        continue;
                    }
                }

                // Unset the spectrum mzML to free up memory
                manifest.unset_spectrum_mzml();

                // Clone precursor so manifest is not borrowed
                let precursor = manifest.precursors.clone();

                for (precusor_idx, (_, precursor_charge)) in precursor.into_iter().enumerate() {
                    match comet_config.set_charge(precursor_charge) {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error setting charge: {:?}",
                                &manifest.uuid, &manifest.spectrum_id, e
                            );
                            continue;
                        }
                    }

                    match comet_config.set_num_results(10000) {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error setting num results: {:?}",
                                &manifest.uuid, &manifest.spectrum_id, e
                            );
                            continue;
                        }
                    }

                    match comet_config.async_to_file(&comet_params_file_path).await {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error writing Comet params: {:?}",
                                &manifest.uuid, &manifest.spectrum_id, e
                            );
                            continue;
                        }
                    }

                    if !current_search_params.keep_fasta_files {
                        match manifest.pop_fasta_to_file(&fasta_file_path).await {
                            Ok(_) => (),
                            Err(e) => {
                                error!(
                                    "[{} / {}] Error writing fasta file: {:?}",
                                    &manifest.uuid, &manifest.spectrum_id, e
                                );
                                continue;
                            }
                        }
                    } else {
                        match manifest
                            .get_fasta_to_file(precusor_idx, &fasta_file_path)
                            .await
                        {
                            Ok(_) => (),
                            Err(e) => {
                                error!(
                                    "[{} / {}] Error writing fasta file: {:?}",
                                    &manifest.uuid, &manifest.spectrum_id, e
                                );
                                continue;
                            }
                        }
                    }

                    if current_search_params.keep_fasta_files {
                        let comet_config_content = comet_config.get_content().as_bytes();
                        match manifest.push_comet_config(comet_config_content) {
                            Ok(_) => (),
                            Err(e) => {
                                error!(
                                    "[{} / {}] Error pushing comet config to manifest: {:?}",
                                    &manifest.uuid, &manifest.spectrum_id, e
                                );
                                continue;
                            }
                        }
                    }

                    match run_comet_search(
                        &config.comet_exe_path,
                        &comet_params_file_path,
                        &fasta_file_path,
                        &psms_file_path.with_extension(""),
                        &mzml_file_path,
                    )
                    .await
                    {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Error running Comet search: {:?}", e);
                            continue;
                        }
                    }

                    // Add PSM file to the manifest
                    let psms = match PeptideSpectrumMatchTsv::read(&psms_file_path) {
                        Ok(Some(psms)) => psms,
                        Ok(None) => DataFrame::empty(),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error reading PSMs from `{}`: {:?}",
                                &manifest.uuid,
                                &manifest.spectrum_id,
                                psms_file_path.display(),
                                e
                            );
                            continue;
                        }
                    };

                    manifest.psms_dataframes.push(psms);

                    debug!(
                        "[{} / {}] Comet search done for {}",
                        &manifest.uuid,
                        &manifest.spectrum_id,
                        psms_file_path.display()
                    );
                }

                match storage.increment_comet_search_ctr(&manifest.uuid).await {
                    Ok(_) => (),
                    Err(e) => {
                        error!(
                            "[{} / {}] Error incrementing Comet search counter: {:?}",
                            &manifest.uuid, &manifest.spectrum_id, e
                        );
                        continue;
                    }
                }

                loop {
                    manifest = match goodness_and_rescoreing_queue.push(manifest).await {
                        Ok(_) => break,
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

    /// Run the identification task by itself
    ///
    /// # Arguments
    /// * `work_dir` - Working directory
    /// * `config_file_path` - Path to the configuration file
    ///
    pub async fn run_standalone(local_work_dir: PathBuf, config_file_path: PathBuf) -> Result<()> {
        let config: StandaloneCometSearchConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let (storage, input_queue, output_queue) =
            config.as_input_output_queue_and_storage().await?;
        let storage = Arc::new(storage);
        let input_queue = Arc::new(input_queue);
        let output_queue = Arc::new(output_queue);

        let comet_search_config = Arc::new(config.comet_search.clone());

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.comet_search.num_tasks)
            .map(|comet_proc_idx| {
                let comet_tmp_dir = local_work_dir.join(format!("comet_{}", comet_proc_idx));
                tokio::spawn(IdentificationTask::start(
                    comet_tmp_dir,
                    comet_search_config.clone(),
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
