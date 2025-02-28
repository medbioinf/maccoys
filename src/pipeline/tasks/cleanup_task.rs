use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{Context, Result};
use metrics::counter;
use tokio::fs::create_dir_all;
use tracing::{debug, error, trace};

use crate::pipeline::{
    configuration::{SearchParameters, StandaloneCleanupConfiguration},
    queue::{PipelineQueue, RedisPipelineQueue},
    storage::{PipelineStorage, RedisPipelineStorage},
};

use super::task::Task;

/// Prefix for the cleanup counter
///
pub const COUNTER_PREFIX: &str = "maccoys_cleanups";

/// Task to cleanup the search
///
pub struct CleanupTask;

impl CleanupTask {
    /// Start the cleanup task
    ///
    /// # Arguments
    /// * `result_dir` - Result directory where the results are stored
    /// * `storage` - Storage to access configuration
    /// * `cleanup_queue` - Queue for the cleanup task
    /// * `stop_flag` - Flag to indicate to stop once the cleanup queue is empty
    ///
    pub async fn start<Q, S>(
        result_dir: PathBuf,
        storage: Arc<S>,
        cleanup_queue: Arc<Q>,
        stop_flag: Arc<AtomicBool>,
    ) where
        Q: PipelineQueue + 'static,
        S: PipelineStorage + 'static,
    {
        loop {
            let mut last_search_uuid = String::new();
            let mut current_search_params = SearchParameters::new();
            let mut metrics_counter_name = COUNTER_PREFIX.to_string();

            while let Some(mut manifest) = cleanup_queue.pop().await {
                debug!(
                    "[{} / {}] Running cleanup",
                    &manifest.uuid, &manifest.spectrum_id
                );

                if last_search_uuid != manifest.uuid {
                    trace!(
                        "[cleanup_task] Loading data from storage (UUIDs: old => {}, new=> {})",
                        &last_search_uuid,
                        &manifest.uuid
                    );
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
                    metrics_counter_name = format!("{COUNTER_PREFIX}_{last_search_uuid}");
                }

                // Clone precursor so manifest is not borrowed
                let precursors = manifest.precursors.clone();

                match create_dir_all(manifest.get_spectrum_dir_path(&result_dir)).await {
                    Ok(_) => (),
                    Err(e) => {
                        error!(
                            "[{} / {}] Error creating spectrum directory: {:?}",
                            &manifest.uuid, &manifest.spectrum_id, e
                        );
                        continue;
                    }
                }

                for (precursor_mz, precursor_charge) in precursors.iter() {
                    let psms_file_path =
                        manifest.get_psms_file_path(&result_dir, *precursor_mz, *precursor_charge);
                    match manifest.pop_psms_to_file(&psms_file_path).await {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error writing PSMs to `{}`: {:?}",
                                &manifest.uuid,
                                &manifest.spectrum_id,
                                psms_file_path.display(),
                                e
                            );
                            continue;
                        }
                    }

                    let goodness_of_fit_file_path = manifest.get_goodness_file_path(
                        &result_dir,
                        *precursor_mz,
                        *precursor_charge,
                    );
                    match manifest.pop_goodness_of_fit_to_file(&goodness_of_fit_file_path) {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error writing goodness of fit to `{}`: {:?}",
                                &manifest.uuid,
                                &manifest.spectrum_id,
                                goodness_of_fit_file_path.display(),
                                e
                            );
                            continue;
                        }
                    }
                }

                if current_search_params.keep_fasta_files && manifest.is_fasta_set() {
                    for (precursor_mz, precursor_charge) in precursors.iter() {
                        let fasta_file_path = manifest.get_fasta_file_path(
                            &result_dir,
                            *precursor_mz,
                            *precursor_charge,
                        );
                        match manifest.pop_fasta_to_file(&fasta_file_path).await {
                            Ok(_) => (),
                            Err(e) => {
                                error!(
                                    "[{} / {}] Error writing fasta to `{}`: {:?}",
                                    &manifest.uuid,
                                    &manifest.spectrum_id,
                                    fasta_file_path.display(),
                                    e
                                );
                                continue;
                            }
                        }
                        let comet_config_file_path = fasta_file_path.with_extension("comet.params");
                        match manifest
                            .pop_comet_config_to_file(&comet_config_file_path)
                            .await
                        {
                            Ok(_) => (),
                            Err(e) => {
                                error!(
                                    "[{} / {}] Error writing Comet config to `{}`: {:?}",
                                    &manifest.uuid,
                                    &manifest.spectrum_id,
                                    comet_config_file_path.display(),
                                    e
                                );
                                continue;
                            }
                        }
                    }
                }

                counter!(metrics_counter_name.clone()).increment(1);

                debug!(
                    "[{} / {}] Cleanup done in `{}`",
                    &manifest.uuid,
                    &manifest.spectrum_id,
                    manifest.get_spectrum_dir_path(&result_dir).display()
                );
            }
            if stop_flag.load(Ordering::Relaxed) {
                break;
            }
            // wait before checking the queue again
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    /// Run the cleanup task by itself
    ///
    /// # Arguments
    /// * `work_dir` - Working directory
    /// * `config_file_path` - Path to the configuration file
    ///
    pub async fn run_standalone(work_dir: PathBuf, config_file_path: PathBuf) -> Result<()> {
        let config: StandaloneCleanupConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;
        let storage = Arc::new(RedisPipelineStorage::new(&config.storage).await?);
        let input_queue = Arc::new(RedisPipelineQueue::new(&config.cleanup).await?);

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.cleanup.num_tasks)
            .map(|_| {
                tokio::spawn(CleanupTask::start(
                    work_dir.clone(),
                    storage.clone(),
                    input_queue.clone(),
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

impl Task for CleanupTask {
    fn get_counter_prefix() -> &'static str {
        COUNTER_PREFIX
    }
}
