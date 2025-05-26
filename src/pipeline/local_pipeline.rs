use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{Context, Result};
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification;
use futures::future::join_all;
use macpepdb::tools::metrics_monitor::{MetricsMonitor, MonitorableMetric, MonitorableMetricType};
use tokio::{fs::create_dir_all, time::sleep};
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::{
    io::comet::configuration::Configuration as CometConfiguration,
    pipeline::{
        errors::queue_error::QueueError,
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
    configuration::{PipelineConfiguration, SearchParameters},
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
    result_dir: PathBuf,
    tmp_dir: PathBuf,

    indexing_queue: Arc<IQ>,
    _search_space_generation_queue: Arc<SQ>,
    _identification_queue: Arc<CQ>,
    _scoring_queue: Arc<GQ>,
    _publication_queue: Arc<PQ>,
    _error_queue: Arc<EQ>,
    storage: Arc<S>,

    index_stop_flag: Arc<AtomicBool>,
    search_space_generation_stop_flag: Arc<AtomicBool>,
    identification_stop_flag: Arc<AtomicBool>,
    scoring_stop_flag: Arc<AtomicBool>,
    publication_stop_flag: Arc<AtomicBool>,
    error_stop_flag: Arc<AtomicBool>,

    indexing_tasks: Vec<tokio::task::JoinHandle<()>>,
    search_space_generation_tasks: Vec<tokio::task::JoinHandle<()>>,
    identification_tasks: Vec<tokio::task::JoinHandle<()>>,
    scoring_tasks: Vec<tokio::task::JoinHandle<()>>,
    publication_tasks: Vec<tokio::task::JoinHandle<()>>,
    error_tasks: Vec<tokio::task::JoinHandle<()>>,
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
    pub async fn new(
        result_dir: PathBuf,
        tmp_dir: PathBuf,
        config: PipelineConfiguration,
    ) -> Result<Self> {
        create_dir_all(&result_dir)
            .await
            .context("Could not create result directory")?;
        create_dir_all(&tmp_dir)
            .await
            .context("Could not create temporery directory")?;

        // Create the queues
        let indexing_queue = Arc::new(IQ::new(&config.index).await?);
        let search_space_generation_queue =
            Arc::new(SQ::new(&config.search_space_generation).await?);
        let identification_queue = Arc::new(CQ::new(&config.identification).await?);
        let scoring_queue = Arc::new(GQ::new(&config.scoring).await?);
        let publication_queue = Arc::new(PQ::new(&config.publication).await?);
        let error_queue = Arc::new(EQ::new(&config.error).await?);

        // Create the storage
        let storage = Arc::new(S::new(&config.storage).await?);

        let index_stop_flag = Arc::new(AtomicBool::new(false));
        let search_space_generation_stop_flag = Arc::new(AtomicBool::new(false));
        let identification_stop_flag = Arc::new(AtomicBool::new(false));
        let scoring_stop_flag = Arc::new(AtomicBool::new(false));
        let publication_stop_flag = Arc::new(AtomicBool::new(false));
        let error_stop_flag = Arc::new(AtomicBool::new(false));

        let indexing_tasks: Vec<tokio::task::JoinHandle<()>> = (0..config.index.num_tasks)
            .map(|_| {
                tokio::spawn(IndexingTask::start(
                    result_dir.clone(),
                    indexing_queue.clone(),
                    search_space_generation_queue.clone(),
                    error_queue.clone(),
                    storage.clone(),
                    index_stop_flag.clone(),
                ))
            })
            .collect();

        let search_space_generation_tasks: Vec<tokio::task::JoinHandle<()>> =
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

        let identification_tasks: Vec<tokio::task::JoinHandle<()>> =
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

        let scoring_tasks: Vec<tokio::task::JoinHandle<()>> = (0..config.scoring.num_tasks)
            .map(|_| {
                tokio::spawn(ScoringTask::start(
                    scoring_queue.clone(),
                    publication_queue.clone(),
                    error_queue.clone(),
                    scoring_stop_flag.clone(),
                ))
            })
            .collect();

        let publication_tasks: Vec<tokio::task::JoinHandle<()>> = (0..config.publication.num_tasks)
            .map(|_| {
                tokio::spawn(PublicationTask::start(
                    result_dir.clone(),
                    publication_queue.clone(),
                    error_queue.clone(),
                    storage.clone(),
                    publication_stop_flag.clone(),
                ))
            })
            .collect();

        let error_tasks: Vec<tokio::task::JoinHandle<()>> = (0..config.error.num_tasks)
            .map(|_| {
                tokio::spawn(ErrorTask::start(
                    result_dir.clone(),
                    error_queue.clone(),
                    error_stop_flag.clone(),
                ))
            })
            .collect();

        Ok(LocalPipeline {
            // Paths
            result_dir,
            tmp_dir,
            // Queues
            indexing_queue,
            _search_space_generation_queue: search_space_generation_queue,
            _identification_queue: identification_queue,
            _scoring_queue: scoring_queue,
            _publication_queue: publication_queue,
            _error_queue: error_queue,
            // Storage
            storage,
            // Flags
            index_stop_flag,
            search_space_generation_stop_flag,
            identification_stop_flag,
            scoring_stop_flag,
            publication_stop_flag,
            error_stop_flag,
            // Tasks
            indexing_tasks,
            search_space_generation_tasks,
            identification_tasks,
            scoring_tasks,
            publication_tasks,
            error_tasks,
        })
    }

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
        &self,
        search_params: SearchParameters,
        mut comet_config: CometConfiguration,
        ptms: Vec<PostTranslationalModification>,
        mzml_file_paths: Vec<PathBuf>,
        metrics_scrape_url: Option<String>,
    ) -> Result<()> {
        let uuid = Uuid::new_v4().to_string();
        info!("UUID: {}", uuid);

        comet_config.set_ptms(&ptms, search_params.max_variable_modifications)?;
        comet_config.set_num_results(10000)?;

        self.storage
            .init_search(&uuid, search_params.clone(), &ptms, &comet_config)
            .await?;

        // Create the stop flags

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

        // Sanitize file names and copy mzML files to the work directory
        let mzml_file_names = join_all(mzml_file_paths.into_iter().map(|mzml_file_path| {
            let uuid = uuid.clone();
            async move {
                let mzml_file_name = mzml_file_path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string();
                let relative_mzml_file_path =
                    create_file_path_on_ms_run_level(&uuid, &mzml_file_name, "mzML");
                let absolute_mzml_file_path = self.result_dir.join(&relative_mzml_file_path);
                tokio::fs::create_dir_all(absolute_mzml_file_path.parent().unwrap()).await?;
                tokio::fs::copy(&mzml_file_path, &absolute_mzml_file_path).await?;
                Ok::<_, anyhow::Error>(mzml_file_name)
            }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

        let mut indexing_message: IndexingMessage =
            IndexingMessage::new(uuid.clone(), mzml_file_names);

        loop {
            indexing_message = match self.indexing_queue.push(indexing_message).await {
                Ok(_) => {
                    debug!("Sucessfully pushed indexing message");
                    break;
                }
                Err((err, errored_message)) => match err {
                    QueueError::QueueFullError => *errored_message,
                    _ => {
                        error!("{}", err);
                        break;
                    }
                },
            };
        }

        info!("Indexing scheduled wait 5s for the indexing to start");

        // Wait a couple of seconds for the indexing to start
        sleep(Duration::from_millis(5000)).await;

        loop {
            if self.storage.is_search_finished(&uuid).await? {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        if let Some(mut metrics_monitor) = metrics_monitor {
            metrics_monitor.stop().await?;
        }

        Ok(())
    }
}

impl<IQ, SQ, CQ, GQ, PQ, EQ, S> Drop for LocalPipeline<IQ, SQ, CQ, GQ, PQ, EQ, S>
where
    IQ: PipelineQueue<IndexingMessage> + 'static,
    SQ: PipelineQueue<SearchSpaceGenerationMessage> + 'static,
    CQ: PipelineQueue<IdentificationMessage> + 'static,
    GQ: PipelineQueue<ScoringMessage> + 'static,
    PQ: PipelineQueue<PublicationMessage> + 'static,
    EQ: PipelineQueue<ErrorMessage> + 'static,
    S: PipelineStorage + 'static,
{
    fn drop(&mut self) {
        self.index_stop_flag.store(true, Ordering::Relaxed);
        self.search_space_generation_stop_flag
            .store(true, Ordering::Relaxed);
        self.identification_stop_flag.store(true, Ordering::Relaxed);
        self.scoring_stop_flag.store(true, Ordering::Relaxed);
        self.publication_stop_flag.store(true, Ordering::Relaxed);
        self.error_stop_flag.store(true, Ordering::Relaxed);

        for indexing_task in &self.indexing_tasks {
            indexing_task.abort();
        }

        for search_space_generation_task in &self.search_space_generation_tasks {
            search_space_generation_task.abort();
        }

        for identification_task in &self.identification_tasks {
            identification_task.abort();
        }

        for scoring_task in &self.scoring_tasks {
            scoring_task.abort();
        }

        for publication_task in &self.publication_tasks {
            publication_task.abort();
        }

        for error_task in &self.error_tasks {
            error_task.abort();
        }

        match std::fs::remove_dir_all(&self.tmp_dir) {
            Ok(_) => {
                info!("Removed temporary directory");
            }
            Err(err) => {
                error!("Could not remove temporary directory: {}", err);
            }
        }
    }
}
