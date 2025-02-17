use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use tokio::fs::create_dir_all;
use tracing::{debug, error, trace};

use crate::pipeline::{
    configuration::SearchParameters, queue::PipelineQueue, storage::PipelineStorage,
};

/// Task to cleanup the search
///
pub struct CleanupTask<Q: PipelineQueue + 'static, S: PipelineStorage + 'static> {
    _phantom_queue: std::marker::PhantomData<Q>,
    _phantom_storage: std::marker::PhantomData<S>,
}

impl<Q, S> CleanupTask<Q, S>
where
    Q: PipelineQueue + 'static,
    S: PipelineStorage + 'static,
{
    /// Start the cleanup task
    ///
    /// # Arguments
    /// * `result_dir` - Result directory where the results are stored
    /// * `storage` - Storage to access configuration
    /// * `cleanup_queue` - Queue for the cleanup task
    /// * `stop_flag` - Flag to indicate to stop once the cleanup queue is empty
    ///
    pub async fn start(
        result_dir: PathBuf,
        storage: Arc<S>,
        cleanup_queue: Arc<Q>,
        stop_flag: Arc<AtomicBool>,
    ) {
        loop {
            let mut last_search_uuid = String::new();
            let mut current_search_params = SearchParameters::new();

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

                match storage.increment_cleanup_ctr(&manifest.uuid).await {
                    Ok(_) => (),
                    Err(e) => {
                        error!(
                            "[{} / {}] Error incrementing cleanup counter: {:?}",
                            &manifest.uuid, &manifest.spectrum_id, e
                        );
                        continue;
                    }
                }

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
}
