use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{bail, Context, Result};
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification;
use macpepdb::tools::metrics_monitor::{MetricsMonitor, MonitorableMetric, MonitorableMetricType};
use tokio::fs::{create_dir_all, remove_dir_all};
use tracing::{error, info};
use uuid::Uuid;

use crate::{
    io::comet::configuration::Configuration as CometConfiguration,
    pipeline::{
        search_manifest::SearchManifest,
        tasks::{
            cleanup_task::CleanupTask, identification_task::IdentificationTask,
            indexing_task::IndexingTask, preparation_task::PreparationTask,
            scoring_task::ScoringTask, search_space_generation_task::SearchSpaceGenerationTask,
            task::Task,
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
        metrics_scrape_url: Option<String>,
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

        let mut metrics_monitor: Option<MetricsMonitor> = None;
        if let Some(metrics_scrape_url) = metrics_scrape_url {
            let monitorable_metrics = vec![
                MonitorableMetric::new(
                    IndexingTask::get_counter_name(&uuid).to_string(),
                    MonitorableMetricType::Rate,
                ),
                MonitorableMetric::new(
                    PreparationTask::get_counter_name(&uuid).to_string(),
                    MonitorableMetricType::Rate,
                ),
                MonitorableMetric::new(
                    SearchSpaceGenerationTask::get_counter_name(&uuid).to_string(),
                    MonitorableMetricType::Rate,
                ),
                MonitorableMetric::new(
                    IdentificationTask::get_counter_name(&uuid).to_string(),
                    MonitorableMetricType::Rate,
                ),
                MonitorableMetric::new(
                    ScoringTask::get_counter_name(&uuid).to_string(),
                    MonitorableMetricType::Rate,
                ),
                MonitorableMetric::new(
                    CleanupTask::get_counter_name(&uuid).to_string(),
                    MonitorableMetricType::Rate,
                ),
            ];

            metrics_monitor = Some(MetricsMonitor::new(
                "macpepdb.build.digest",
                monitorable_metrics,
                metrics_scrape_url,
            )?);
        }

        let index_handler: tokio::task::JoinHandle<()> = {
            tokio::spawn(IndexingTask::start(
                result_dir.clone(),
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

        if let Some(mut metrics_monitor) = metrics_monitor {
            metrics_monitor.stop().await?;
        }

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
}
