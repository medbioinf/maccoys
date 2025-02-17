use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::{bail, Context, Result};
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification;
use macpepdb::tools::{progress_monitor::ProgressMonitor, queue_monitor::QueueMonitor};
use tokio::fs::{create_dir_all, remove_dir_all};
use tracing::{error, info};
use uuid::Uuid;

use crate::{
    io::comet::configuration::Configuration as CometConfiguration,
    pipeline::{
        queue::PipelineQueueArc,
        search_manifest::SearchManifest,
        storage::{COUNTER_LABLES, NUMBER_OF_COUNTERS},
        tasks::{
            cleanup_task::CleanupTask, identification_task::IdentificationTask,
            indexing_task::IndexingTask, preparation_task::PreparationTask,
            scoring_task::ScoringTask, search_space_generation_task::SearchSpaceGenerationTask,
        },
    },
};

use super::{configuration::PipelineConfiguration, queue::PipelineQueue, storage::PipelineStorage};

/// Local pipeline for debugging and testing
/// Can run with local and remote storage and queues
///
pub struct LocalPipeline<Q, S>
where
    Q: PipelineQueue + 'static,
    S: PipelineStorage + 'static,
{
    _phantom_queue: std::marker::PhantomData<Q>,
    _phantom_storage: std::marker::PhantomData<S>,
}

impl<Q, S> LocalPipeline<Q, S>
where
    Q: PipelineQueue + 'static,
    S: PipelineStorage + 'static,
{
    /// Run the pipeline locally for each mzML file
    ///
    /// # Arguments
    /// * `result_dir` - Result directory where each search is stored
    /// * `tmp_dir` - Temporary directory for the pipeline
    /// * `config` - Configuration for the pipeline
    /// * `comet_config` - Configuration for Comet
    /// * `ptms` - Post translational modifications
    /// * `mzml_file_paths` - Paths to the mzML files to search
    ///
    pub async fn run(
        result_dir: PathBuf,
        tmp_dir: PathBuf,
        config: PipelineConfiguration,
        mut comet_config: CometConfiguration,
        ptms: Vec<PostTranslationalModification>,
        mzml_file_paths: Vec<PathBuf>,
    ) -> Result<()> {
        let uuid = Uuid::new_v4().to_string();
        info!("UUID: {}", uuid);

        create_dir_all(&result_dir)
            .await
            .context("Could not create result directory")?;
        create_dir_all(&tmp_dir)
            .await
            .context("Could not create temporery directory")?;

        comet_config.set_ptms(&ptms, config.search_parameters.max_variable_modifications)?;
        comet_config.set_num_results(10000)?;

        let mut storage = S::new(&config.storage).await?;
        storage
            .init_search(
                &uuid,
                config.search_parameters.clone(),
                &ptms,
                &comet_config,
            )
            .await?;

        let storage = Arc::new(storage);

        // Create the queues
        let index_queue = Arc::new(Q::new(&config.index).await?);
        let preparation_queue = Arc::new(Q::new(&config.preparation).await?);
        let search_space_generation_queue =
            Arc::new(Q::new(&config.search_space_generation).await?);
        let comet_search_queue = Arc::new(Q::new(&config.comet_search).await?);
        let goodness_and_rescoreing_queue = Arc::new(Q::new(&config.goodness_and_rescoring).await?);
        let cleanup_queue = Arc::new(Q::new(&config.cleanup).await?);

        // Create the stop flags
        let index_stop_flag = Arc::new(AtomicBool::new(false));
        let preparation_stop_flag = Arc::new(AtomicBool::new(false));
        let search_space_generation_stop_flag = Arc::new(AtomicBool::new(false));
        let comet_search_stop_flag = Arc::new(AtomicBool::new(false));
        let goodness_and_rescoreing_stop_flag = Arc::new(AtomicBool::new(false));
        let cleanup_stop_flag = Arc::new(AtomicBool::new(false));
        let metrics_stop_flag = Arc::new(AtomicBool::new(false));

        // Metrics
        let metrics = (0..NUMBER_OF_COUNTERS)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect::<Vec<Arc<AtomicUsize>>>();

        let mut queue_monitor = QueueMonitor::new::<PipelineQueueArc<Q>>(
            "",
            vec![
                cleanup_queue.clone().into(),
                goodness_and_rescoreing_queue.clone().into(),
                comet_search_queue.clone().into(),
                search_space_generation_queue.clone().into(),
                preparation_queue.clone().into(),
                index_queue.clone().into(),
            ],
            vec![
                config.cleanup.queue_capacity as u64,
                config.goodness_and_rescoring.queue_capacity as u64,
                config.comet_search.queue_capacity as u64,
                config.search_space_generation.queue_capacity as u64,
                config.preparation.queue_capacity as u64,
                config.index.queue_capacity as u64,
            ],
            vec![
                "Cleanup".to_owned(),
                "Goodness and Rescoring".to_owned(),
                "Comet Search".to_owned(),
                "Search Space Generation".to_owned(),
                "Preparation".to_owned(),
                "Index".to_owned(),
            ],
            None,
        )?;

        let metrics_poll_taks = tokio::spawn(Self::poll_store_metrics_task(
            storage.clone(),
            uuid.clone(),
            metrics.clone(),
            metrics_stop_flag.clone(),
        ));

        let mut metrics_monitor = ProgressMonitor::new(
            "",
            metrics.clone(),
            vec![None; NUMBER_OF_COUNTERS],
            COUNTER_LABLES
                .iter()
                .rev()
                .map(|label| label.to_string())
                .collect(),
            None,
        )?;

        let index_handler: tokio::task::JoinHandle<()> = {
            tokio::spawn(IndexingTask::start(
                result_dir.clone(),
                storage.clone(),
                index_queue.clone(),
                preparation_queue.clone(),
                index_stop_flag.clone(),
            ))
        };

        let preparation_handlers: Vec<tokio::task::JoinHandle<()>> =
            (0..config.preparation.num_tasks)
                .map(|_| {
                    tokio::spawn(PreparationTask::start(
                        storage.clone(),
                        preparation_queue.clone(),
                        search_space_generation_queue.clone(),
                        preparation_stop_flag.clone(),
                    ))
                })
                .collect();

        let search_space_generation_handlers: Vec<tokio::task::JoinHandle<()>> =
            (0..config.search_space_generation.num_tasks)
                .map(|_| {
                    tokio::spawn(SearchSpaceGenerationTask::start(
                        Arc::new(config.search_space_generation.clone()),
                        storage.clone(),
                        search_space_generation_queue.clone(),
                        comet_search_queue.clone(),
                        search_space_generation_stop_flag.clone(),
                    ))
                })
                .collect();

        let comet_search_handlers: Vec<tokio::task::JoinHandle<()>> =
            (0..config.comet_search.num_tasks)
                .map(|comet_proc_idx| {
                    let comet_tmp_dir = tmp_dir.join(format!("comet_{}", comet_proc_idx));
                    tokio::spawn(IdentificationTask::start(
                        comet_tmp_dir,
                        Arc::new(config.comet_search.clone()),
                        storage.clone(),
                        comet_search_queue.clone(),
                        goodness_and_rescoreing_queue.clone(),
                        comet_search_stop_flag.clone(),
                    ))
                })
                .collect();

        let goodness_and_resconfing_handlers: Vec<tokio::task::JoinHandle<()>> =
            (0..config.goodness_and_rescoring.num_tasks)
                .map(|_| {
                    tokio::spawn(ScoringTask::start(
                        storage.clone(),
                        goodness_and_rescoreing_queue.clone(),
                        cleanup_queue.clone(),
                        goodness_and_rescoreing_stop_flag.clone(),
                    ))
                })
                .collect();

        let cleanup_handlers: Vec<tokio::task::JoinHandle<()>> = (0..config.cleanup.num_tasks)
            .map(|_| {
                tokio::spawn(CleanupTask::start(
                    result_dir.clone(),
                    storage.clone(),
                    cleanup_queue.clone(),
                    cleanup_stop_flag.clone(),
                ))
            })
            .collect();

        for mzml_file_path in mzml_file_paths {
            let manifest = SearchManifest::new(
                uuid.clone(),
                mzml_file_path.file_name().unwrap().to_str().unwrap(),
            );

            tokio::fs::create_dir_all(manifest.get_ms_run_dir_path(&result_dir)).await?;
            tokio::fs::copy(&mzml_file_path, manifest.get_ms_run_mzml_path(&result_dir)).await?;

            match index_queue.push(manifest).await {
                Ok(_) => (),
                Err(e) => {
                    error!("[{}] Error pushing manifest to index queue.", &e.uuid);
                    continue;
                }
            }
        }

        index_stop_flag.store(true, Ordering::Relaxed);

        match index_handler.await {
            Ok(_) => (),
            Err(e) => {
                error!("Error joining index thread: {:?}", e);
            }
        }

        preparation_stop_flag.store(true, Ordering::Relaxed);

        for preparation_handler in preparation_handlers {
            match preparation_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining preparation thread: {:?}", e);
                }
            }
        }

        search_space_generation_stop_flag.store(true, Ordering::Relaxed);

        for search_space_generation_handler in search_space_generation_handlers {
            match search_space_generation_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining search space generation thread: {:?}", e);
                }
            }
        }

        comet_search_stop_flag.store(true, Ordering::Relaxed);

        for comet_search_handler in comet_search_handlers {
            match comet_search_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining comet search thread: {:?}", e);
                }
            }
        }

        goodness_and_rescoreing_stop_flag.store(true, Ordering::Relaxed);

        for goodness_and_resconfing_handler in goodness_and_resconfing_handlers {
            match goodness_and_resconfing_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining goodness and rescoring thread: {:?}", e);
                }
            }
        }

        cleanup_stop_flag.store(true, Ordering::Relaxed);

        for cleanup_handler in cleanup_handlers {
            match cleanup_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining cleanup thread: {:?}", e);
                }
            }
        }

        metrics_stop_flag.store(true, Ordering::Relaxed);
        metrics_poll_taks.await?;
        queue_monitor.stop().await?;
        metrics_monitor.stop().await?;

        let mut storage = match Arc::try_unwrap(storage) {
            Ok(storage) => storage,
            Err(_) => {
                bail!("Error unwrapping storage");
            }
        };
        storage.cleanup_search(&uuid).await?;

        remove_dir_all(&tmp_dir)
            .await
            .context("Could not delete temporary directory")?;

        Ok(())
    }

    /// Task to poll the metrcis from the storage to monitor them
    ///
    /// # Arguments
    /// * `storage` - Storage to access the metrics
    /// * `uuid` - UUID of the search
    /// * `metrics` - Metrics to store the values
    /// * `stop_flag` - Flag to indicate to stop polling
    ///
    async fn poll_store_metrics_task(
        storage: Arc<S>,
        uuid: String,
        metrics: Vec<Arc<AtomicUsize>>,
        stop_flag: Arc<AtomicBool>,
    ) {
        loop {
            metrics[0].store(
                storage.get_started_searches_ctr(&uuid).await.unwrap_or(0),
                Ordering::Relaxed,
            );
            metrics[1].store(
                storage.get_prepared_ctr(&uuid).await.unwrap_or(0),
                Ordering::Relaxed,
            );
            metrics[2].store(
                storage
                    .get_search_space_generation_ctr(&uuid)
                    .await
                    .unwrap_or(0),
                Ordering::Relaxed,
            );
            metrics[3].store(
                storage.get_comet_search_ctr(&uuid).await.unwrap_or(0),
                Ordering::Relaxed,
            );
            metrics[4].store(
                storage
                    .get_goodness_and_rescoring_ctr(&uuid)
                    .await
                    .unwrap_or(0),
                Ordering::Relaxed,
            );
            metrics[5].store(
                storage.get_cleanup_ctr(&uuid).await.unwrap_or(0),
                Ordering::Relaxed,
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(750)).await;
            if stop_flag.load(Ordering::Relaxed) {
                break;
            }
        }
    }
}
