use std::{
    fs,
    io::Cursor,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{Context, Result};
use dihardts_omicstools::{
    mass_spectrometry::unit_conversions::mass_to_charge_to_dalton,
    proteomics::post_translational_modifications::PostTranslationalModification,
};
use macpepdb::mass::convert::to_int as mass_to_int;
use metrics::counter;
use tokio::io::AsyncWriteExt;
use tracing::{debug, error};

use crate::{
    functions::create_search_space,
    pipeline::{
        configuration::{
            SearchParameters, SearchSpaceGenerationTaskConfiguration,
            StandaloneSearchSpaceGenerationConfiguration,
        },
        convert::AsInputOutputQueue,
        queue::PipelineQueue,
        storage::{PipelineStorage, RedisPipelineStorage},
    },
};

use super::task::Task;

/// Prefix for the search space generation counter
///
pub const COUNTER_PREFIX: &str = "maccoys_search_space_generation";

/// Task to generate the search space for the search engine
///
pub struct SearchSpaceGenerationTask;

impl SearchSpaceGenerationTask {
    /// Start the indexing task
    ///
    /// # Arguments
    /// * `config` - Configuration for the search space generation task
    /// * `search_space_generation_queue` - Queue for the search space generation task
    /// * `comet_search_queue` - Queue for the Comet search task
    /// * `storage` - Storage to access configuration and PTMs
    /// * `stop_flag` - Flag to indicate to stop once the search space generation queue is empty
    ///
    pub async fn start<Q, S>(
        config: Arc<SearchSpaceGenerationTaskConfiguration>,
        storage: Arc<S>,
        search_space_generation_queue: Arc<Q>,
        comet_search_queue: Arc<Q>,
        stop_flag: Arc<AtomicBool>,
    ) where
        Q: PipelineQueue + 'static,
        S: PipelineStorage + 'static,
    {
        let mut last_search_uuid = String::new();
        let mut current_search_params = SearchParameters::new();
        let mut current_ptms: Vec<PostTranslationalModification> = Vec::new();
        let mut metrics_counter_name = COUNTER_PREFIX.to_string();

        loop {
            while let Some(mut manifest) = search_space_generation_queue.pop().await {
                debug!(
                    "[{} / {}] Generating search space",
                    &manifest.uuid, &manifest.spectrum_id
                );
                if manifest.precursors.is_empty() {
                    error!(
                        "[{} / {}] Prepartion not done in search_space_generation_task",
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

                    current_ptms = match storage.get_ptms(&manifest.uuid).await {
                        Ok(Some(ptms)) => ptms,
                        Ok(None) => {
                            error!(
                                "[{} / {}] PTMs not found",
                                manifest.uuid, manifest.spectrum_id
                            );
                            continue;
                        }
                        Err(e) => {
                            error!(
                                "[{} / {}] Error getting PTMs from storage: {:?}",
                                &manifest.uuid, &manifest.spectrum_id, e
                            );
                            return;
                        }
                    };
                    last_search_uuid = manifest.uuid.clone();
                    metrics_counter_name = format!("{COUNTER_PREFIX}_{last_search_uuid}");
                }

                let precursors = &manifest.precursors.clone();

                for (precursor_mz, precursor_charge) in precursors.iter() {
                    let mass =
                        mass_to_int(mass_to_charge_to_dalton(*precursor_mz, *precursor_charge));

                    let mut fasta = Box::pin(Cursor::new(Vec::new()));

                    match create_search_space(
                        &mut fasta,
                        &current_ptms,
                        mass,
                        current_search_params.lower_mass_tolerance_ppm,
                        current_search_params.upper_mass_tolerance_ppm,
                        current_search_params.max_variable_modifications,
                        current_search_params.decoys_per_peptide,
                        config.target_url.to_owned(),
                        config.decoy_url.clone(),
                        config.target_lookup_url.clone(),
                        config.decoy_cache_url.clone(),
                    )
                    .await
                    {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error creating search space: {:?}",
                                &manifest.uuid, &manifest.spectrum_id, e
                            );
                            continue;
                        }
                    };

                    match fasta.flush().await {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error flushing search space: {:?}",
                                &manifest.uuid, &manifest.spectrum_id, e
                            );
                            continue;
                        }
                    };
                    fasta.set_position(0);

                    let buffered_fasta = std::io::BufReader::new(fasta.get_ref().as_slice());

                    manifest.push_fasta(buffered_fasta).unwrap();

                    counter!(metrics_counter_name.clone()).increment(1);
                }

                loop {
                    manifest = match comet_search_queue.push(manifest).await {
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

    /// Run the search space generation task by itself
    ///
    /// # Arguments
    /// * `work_dir` - Working directory
    /// * `config_file_path` - Path to the configuration file
    ///
    pub async fn run_standalone(config_file_path: PathBuf) -> Result<()> {
        let config: StandaloneSearchSpaceGenerationConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let (input_queue, output_queue) = config.as_input_output_queue().await?;
        let input_queue = Arc::new(input_queue);
        let output_queue = Arc::new(output_queue);
        let storage = Arc::new(RedisPipelineStorage::new(&config.storage).await?);

        let search_space_generation_config = Arc::new(config.search_space_generation.clone());

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> =
            (0..config.search_space_generation.num_tasks)
                .map(|_| {
                    tokio::spawn(SearchSpaceGenerationTask::start(
                        search_space_generation_config.clone(),
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

impl Task for SearchSpaceGenerationTask {
    fn get_counter_prefix() -> &'static str {
        COUNTER_PREFIX
    }
}
