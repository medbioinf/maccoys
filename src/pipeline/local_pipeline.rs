use std::{
    marker::PhantomData,
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
        tasks::{
            error_task::ErrorTask, identification_task::IdentificationTask,
            indexing_task::IndexingTask, publication_task::PublicationTask,
            scoring_task::ScoringTask, search_space_generation_task::SearchSpaceGenerationTask,
            task::Task,
        },
        utils::create_file_path_on_ms_run_level,
    },
};

use super::{
    configuration::PipelineConfiguration,
    messages::{
        error_message::ErrorMessage, identification_message::IdentificationMessage,
        indexing_message::IndexingMessage, publication_message::PublicationMessage,
        scoring_message::ScoringMessage,
        search_space_generation_message::SearchSpaceGenerationMessage,
    },
    queue::PipelineQueue,
    storage::PipelineStorage,
};

/// Local pipeline for debugging and testing
/// Can run with local and remote storage and queues
///
/// # Generics
/// * `IQ` - Indexing Queue
/// * `SQ` - Search Space Generation Queue
/// * `CQ` - Identification Queue (formerly Comet Search Queue)
/// * `GQ` - Scoring Queue (formerly Goodness and Rescoring Queue)
/// * `PQ` - Publication Queue
/// * `EQ` - Error Queue
/// * `S` - Storage
///
///
pub struct LocalPipeline<IQ, SQ, CQ, GQ, PQ, EQ, S>
where
    IQ: PipelineQueue<IndexingMessage> + 'static,
    SQ: PipelineQueue<SearchSpaceGenerationMessage> + 'static,
    CQ: PipelineQueue<IdentificationMessage> + 'static,
    GQ: PipelineQueue<ScoringMessage> + 'static,
    PQ: PipelineQueue<PublicationMessage> + 'static,
    EQ: PipelineQueue<ErrorMessage> + 'static,
    S: PipelineStorage + 'static,
{
    _indexing_queue_phantom: PhantomData<IQ>,
    _search_space_generation_queue_phantom: PhantomData<SQ>,
    _identification_queue_phantom: PhantomData<CQ>,
    _scoring_queue_phantom: PhantomData<GQ>,
    _publication_queue_phantom: PhantomData<PQ>,
    _error_queue_phantom: PhantomData<EQ>,
    _storage_phantom: PhantomData<S>,
}

impl<IQ, SQ, CQ, GQ, PQ, EQ, S> LocalPipeline<IQ, SQ, CQ, GQ, PQ, EQ, S>
where
    IQ: PipelineQueue<IndexingMessage> + 'static,
    SQ: PipelineQueue<SearchSpaceGenerationMessage> + 'static,
    CQ: PipelineQueue<IdentificationMessage> + 'static,
    GQ: PipelineQueue<ScoringMessage> + 'static,
    PQ: PipelineQueue<PublicationMessage> + 'static,
    EQ: PipelineQueue<ErrorMessage> + 'static,
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
        let indexing_queue = Arc::new(IQ::new(&config.index).await?);
        let search_space_generation_queue =
            Arc::new(SQ::new(&config.search_space_generation).await?);
        let identification_queue = Arc::new(CQ::new(&config.identification).await?);
        let scoring_queue = Arc::new(GQ::new(&config.scoring).await?);
        let publication_queue = Arc::new(PQ::new(&config.publication).await?);
        let error_queue = Arc::new(EQ::new(&config.error).await?);

        // Create the stop flags
        let index_stop_flag = Arc::new(AtomicBool::new(false));
        let search_space_generation_stop_flag = Arc::new(AtomicBool::new(false));
        let identification_stop_flag = Arc::new(AtomicBool::new(false));
        let scoring_stop_flag = Arc::new(AtomicBool::new(false));
        let publication_stop_flag = Arc::new(AtomicBool::new(false));
        let error_stop_flag = Arc::new(AtomicBool::new(false));

        let mut metrics_monitor: Option<MetricsMonitor> = None;
        if let Some(metrics_scrape_url) = metrics_scrape_url {
            let monitorable_metrics = vec![
                MonitorableMetric::new(
                    IndexingTask::get_counter_name(&uuid).to_string(),
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
                    PublicationTask::get_counter_name(&uuid).to_string(),
                    MonitorableMetricType::Rate,
                ),
                MonitorableMetric::new(
                    ErrorTask::get_counter_name(&uuid).to_string(),
                    MonitorableMetricType::Rate,
                ),
            ];

            metrics_monitor = Some(MetricsMonitor::new(
                "maccoys.local_pipeline",
                monitorable_metrics,
                metrics_scrape_url,
            )?);
        }

        let indexing_handler: tokio::task::JoinHandle<()> = {
            tokio::spawn(IndexingTask::start(
                result_dir.clone(),
                indexing_queue.clone(),
                search_space_generation_queue.clone(),
                error_queue.clone(),
                index_stop_flag.clone(),
            ))
        };

        let search_space_generation_handlers: Vec<tokio::task::JoinHandle<()>> =
            (0..config.search_space_generation.num_tasks)
                .map(|_| {
                    tokio::spawn(SearchSpaceGenerationTask::start(
                        Arc::new(config.search_space_generation.clone()),
                        storage.clone(),
                        search_space_generation_queue.clone(),
                        identification_queue.clone(),
                        error_queue.clone(),
                        search_space_generation_stop_flag.clone(),
                    ))
                })
                .collect();

        let identification_handler: Vec<tokio::task::JoinHandle<()>> =
            (0..config.identification.num_tasks)
                .map(|comet_proc_idx| {
                    let comet_tmp_dir = tmp_dir.join(format!("comet_{}", comet_proc_idx));
                    tokio::spawn(IdentificationTask::start(
                        comet_tmp_dir,
                        Arc::new(config.identification.clone()),
                        storage.clone(),
                        identification_queue.clone(),
                        scoring_queue.clone(),
                        publication_queue.clone(),
                        error_queue.clone(),
                        identification_stop_flag.clone(),
                    ))
                })
                .collect();

        let scoring_handler: Vec<tokio::task::JoinHandle<()>> = (0..config.scoring.num_tasks)
            .map(|_| {
                tokio::spawn(ScoringTask::start(
                    scoring_queue.clone(),
                    publication_queue.clone(),
                    error_queue.clone(),
                    scoring_stop_flag.clone(),
                ))
            })
            .collect();

        let publication_handler: Vec<tokio::task::JoinHandle<()>> =
            (0..config.publication.num_tasks)
                .map(|_| {
                    tokio::spawn(PublicationTask::start(
                        result_dir.clone(),
                        publication_queue.clone(),
                        error_queue.clone(),
                        publication_stop_flag.clone(),
                    ))
                })
                .collect();

        let error_handler: Vec<tokio::task::JoinHandle<()>> = (0..config.error.num_tasks)
            .map(|_| {
                tokio::spawn(ErrorTask::start(
                    result_dir.clone(),
                    error_queue.clone(),
                    error_stop_flag.clone(),
                ))
            })
            .collect();

        for mzml_file_path in mzml_file_paths {
            let mzml_file_name = mzml_file_path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();

            let mzml_file_path = create_file_path_on_ms_run_level(&uuid, &mzml_file_name, "mzML");
            tokio::fs::create_dir_all(mzml_file_path.parent().unwrap()).await?;
            tokio::fs::copy(&mzml_file_path, &mzml_file_path).await?;

            let mut indexing_message: IndexingMessage =
                IndexingMessage::new(uuid.clone(), mzml_file_name);

            loop {
                indexing_message = match indexing_queue.push(indexing_message).await {
                    Ok(_) => break,
                    Err(original_message) => {
                        error!(
                            "[{}] Could not push message. Try again.",
                            &original_message.uuid()
                        );
                        original_message
                    }
                };
            }
        }

        index_stop_flag.store(true, Ordering::Relaxed);

        match indexing_handler.await {
            Ok(_) => (),
            Err(e) => {
                error!("Error joining index thread: {:?}", e);
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

        identification_stop_flag.store(true, Ordering::Relaxed);

        for identification_handler in identification_handler {
            match identification_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error identification thread: {:?}", e);
                }
            }
        }

        scoring_stop_flag.store(true, Ordering::Relaxed);

        for goodness_and_resconfing_handler in scoring_handler {
            match goodness_and_resconfing_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining scoring thread: {:?}", e);
                }
            }
        }

        publication_stop_flag.store(true, Ordering::Relaxed);

        for publication_handler in publication_handler {
            match publication_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining publication thread: {:?}", e);
                }
            }
        }

        error_stop_flag.store(true, Ordering::Relaxed);

        for error_handler in error_handler {
            match error_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining error thread: {:?}", e);
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
