use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{anyhow, Context, Result};
use clap::ValueEnum;
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification;
use futures::future::join_all;
use indicatif::ProgressStyle;
use macpepdb::tools::metrics_monitor::{MetricsMonitor, MonitorableMetric, MonitorableMetricType};
use tokio::{fs::create_dir_all, time::sleep};
use tracing::{debug, error, info, info_span};
use tracing_indicatif::span_ext::IndicatifSpanExt;
use uuid::Uuid;

use crate::pipeline::{
    errors::queue_error::QueueError,
    messages::is_message::IsMessage,
    queue::{HttpPipelineQueue, LocalPipelineQueue, RedisPipelineQueue},
    queue_server::{IsQueueRouteable, QueueServer, QueueServerState},
    storage::{LocalPipelineStorage, RedisPipelineStorage},
    tasks::{
        error_task::ErrorTask, identification_task::IdentificationTask,
        indexing_task::IndexingTask, publication_task::PublicationTask, scoring_task::ScoringTask,
        search_space_generation_task::SearchSpaceGenerationTask, task::Task,
    },
    utils::create_file_path_on_ms_run_level,
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

/// Target for local piplines storage
///
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum StorageTarget {
    Memory,
    Redis,
}

/// Target for local piplines queue
///
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum QueueTarget {
    Memory,
    Redis,
    Http,
}

pub trait QueueFactory {
    type Queue<M>: PipelineQueue<M>
    where
        M: IsMessage + IsQueueRouteable;
}

pub struct LocalQueue;
impl QueueFactory for LocalQueue {
    type Queue<M>
        = LocalPipelineQueue<M>
    where
        M: IsMessage + IsQueueRouteable;
}

pub struct RedisQueue;
impl QueueFactory for RedisQueue {
    type Queue<M>
        = RedisPipelineQueue<M>
    where
        M: IsMessage + IsQueueRouteable;
}

pub struct HttpQueue;
impl QueueFactory for HttpQueue {
    type Queue<M>
        = HttpPipelineQueue<M>
    where
        M: IsMessage + IsQueueRouteable;
}

/// Local pipeline for debugging and testing
/// Can run with local and remote storage and queues
///
/// # Generics
/// * `Q` - Queue factory
/// * `S` - Storage
///
///
pub struct LocalPipeline<Q, S>
where
    Q: QueueFactory + 'static,
    S: PipelineStorage + 'static,
{
    result_dir: PathBuf,
    tmp_dir: PathBuf,

    indexing_queue: Arc<<Q as QueueFactory>::Queue<IndexingMessage>>,
    _search_space_generation_queue: Arc<<Q as QueueFactory>::Queue<SearchSpaceGenerationMessage>>,
    _identification_queue: Arc<<Q as QueueFactory>::Queue<IdentificationMessage>>,
    _scoring_queue: Arc<<Q as QueueFactory>::Queue<ScoringMessage>>,
    _publication_queue: Arc<<Q as QueueFactory>::Queue<PublicationMessage>>,
    _error_queue: Arc<<Q as QueueFactory>::Queue<ErrorMessage>>,
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

impl<Q, S> LocalPipeline<Q, S>
where
    Q: QueueFactory + 'static,
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
        let indexing_queue =
            Arc::new(<Q as QueueFactory>::Queue::<IndexingMessage>::new(&config.index).await?);
        let search_space_generation_queue = Arc::new(
            <Q as QueueFactory>::Queue::<SearchSpaceGenerationMessage>::new(
                &config.search_space_generation,
            )
            .await?,
        );
        let identification_queue = Arc::new(
            <Q as QueueFactory>::Queue::<IdentificationMessage>::new(&config.identification)
                .await?,
        );
        let scoring_queue =
            Arc::new(<Q as QueueFactory>::Queue::<ScoringMessage>::new(&config.scoring).await?);
        let publication_queue = Arc::new(
            <Q as QueueFactory>::Queue::<PublicationMessage>::new(&config.publication).await?,
        );
        let error_queue =
            Arc::new(<Q as QueueFactory>::Queue::<ErrorMessage>::new(&config.error).await?);

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
                        publication_queue.clone(),
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
                    config.scoring.threads,
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
    /// * `xcorr_config` - Configuration for Comet
    /// * `ptms` - Post translational modifications
    /// * `mzml_file_paths` - Paths to the mzML files to search
    ///
    pub async fn run(
        &self,
        search_params: SearchParameters,
        ptms: Vec<PostTranslationalModification>,
        mzml_file_paths: Vec<PathBuf>,
        metrics_scrape_url: Option<String>,
    ) -> Result<()> {
        let uuid = Uuid::new_v4().to_string();
        info!("UUID: {}", uuid);

        self.storage
            .init_search(&uuid, search_params.clone(), &ptms)
            .await?;

        let mut metrics_monitor: Option<MetricsMonitor> = None;
        if let Some(metrics_scrape_url) = metrics_scrape_url {
            let corrected_uuid = uuid.clone().replace('-', "_"); // scrape endpoint replaces dashes with underscores
            let monitorable_metrics = vec![
                MonitorableMetric::new(
                    IndexingTask::get_counter_name(&corrected_uuid).to_string(),
                    MonitorableMetricType::Rate,
                ),
                MonitorableMetric::new(
                    SearchSpaceGenerationTask::get_counter_name(&corrected_uuid).to_string(),
                    MonitorableMetricType::Rate,
                ),
                MonitorableMetric::new(
                    IdentificationTask::get_counter_name(&corrected_uuid).to_string(),
                    MonitorableMetricType::Rate,
                ),
                MonitorableMetric::new(
                    ScoringTask::get_counter_name(&corrected_uuid).to_string(),
                    MonitorableMetricType::Rate,
                ),
                MonitorableMetric::new(
                    PublicationTask::get_counter_name(&corrected_uuid).to_string(),
                    MonitorableMetricType::Rate,
                ),
                MonitorableMetric::new(
                    ErrorTask::get_counter_name(&corrected_uuid).to_string(),
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
            match self.indexing_queue.push(indexing_message).await {
                Ok(_) => {
                    debug!("Sucessfully pushed indexing message");
                    break;
                }
                Err((err, errored_message)) => match err {
                    QueueError::QueueFullError => indexing_message = *errored_message,
                    _ => {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        return Err(anyhow!("Error pushing indexing message: {}", err));
                    }
                },
            };
        }

        info!("Indexing scheduled wait 5s for the indexing to start");

        // Wait a couple of seconds for the indexing to start
        sleep(Duration::from_millis(5000)).await;

        let progress_span = info_span!("progress");
        progress_span.pb_set_message("Finished spectra");
        progress_span.pb_set_style(
            &ProgressStyle::with_template("        {msg} {wide_bar} {pos}/{len} {per_sec} ")
                .unwrap(),
        );

        loop {
            if self.storage.is_search_finished(&uuid).await? {
                info!("Search finished");
                break;
            }
            sleep(Duration::from_millis(1000)).await;
            let _ = progress_span.enter();
            progress_span.pb_set_length(self.storage.get_total_spectrum_count(&uuid).await?);
            progress_span.pb_set_position(self.storage.get_finished_spectrum_count(&uuid).await?);
            if self.storage.is_search_fully_enqueued(&uuid).await? {
                progress_span.pb_set_message("Finished spectra");
            } else {
                progress_span.pb_set_message("Finished spectra (enqueuing still in progress)");
            }
        }
        drop(progress_span);

        if let Some(mut metrics_monitor) = metrics_monitor {
            metrics_monitor.stop().await?;
        }

        Ok(())
    }
}

impl<Q, S> Drop for LocalPipeline<Q, S>
where
    Q: QueueFactory + 'static,
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

/// Build and run a local pipeline with the specified queue and storage targets
///
/// # Arguments
/// * `queue` - Queue target
/// * `storage` - Storage target
/// * `result_dir` - Result directory where each search is stored
/// * `tmp_dir` - Temporary directory for the pipeline
/// * `config` - Configuration for the pipeline
/// * `xcorr_config` - Configuration for Comet
/// * `ptms` - Post translational modifications
/// * `mzml_file_paths` - Paths to the mzML files to search
///
#[allow(clippy::too_many_arguments)]
pub async fn build_and_run_local_pipeline(
    queue: QueueTarget,
    storage: StorageTarget,
    result_dir: PathBuf,
    tmp_dir: PathBuf,
    config: PipelineConfiguration,
    search_params: SearchParameters,
    ptms: Vec<PostTranslationalModification>,
    mzml_file_paths: Vec<PathBuf>,
    metrics_scrape_url: Option<String>,
) -> Result<()> {
    let http_queue_server = match queue {
        QueueTarget::Http => {
            let queue_server_config = config.to_remote_entrypoint_configuration();
            let queue_url = queue_server_config
                .index
                .queue_url
                .as_ref()
                .ok_or_else(|| {
                    anyhow!(
                        "HTTP queue selected but no queue URL provided in configuration"
                            .to_string(),
                    )
                })?;
            let url = url::Url::parse(queue_url).context("Parsing queue URL")?;

            let interface = url
                .host_str()
                .ok_or_else(|| anyhow!("No host found in queue URL"))?
                .to_string();

            let port: u16 = url
                .port_or_known_default()
                .ok_or_else(|| anyhow!("No port found in queue URL"))?;

            let state = Arc::new(QueueServerState::from_config(&queue_server_config));

            let (stop_signal_sender, stop_signal_receiver) = tokio::sync::oneshot::channel::<()>();

            let stop_signal = async move {
                stop_signal_receiver.await.ok();
            };

            let handle = tokio::spawn(async move {
                QueueServer::run(interface, port, state, Some(Box::pin(stop_signal))).await
            });

            Some((handle, stop_signal_sender))
        }
        _ => None,
    };

    match (queue, storage) {
        (QueueTarget::Memory, StorageTarget::Memory) => {
            LocalPipeline::<LocalQueue, LocalPipelineStorage>::new(result_dir, tmp_dir, config)
                .await?
                .run(search_params, ptms, mzml_file_paths, metrics_scrape_url)
                .await?
        }
        (QueueTarget::Memory, StorageTarget::Redis) => {
            LocalPipeline::<LocalQueue, RedisPipelineStorage>::new(result_dir, tmp_dir, config)
                .await?
                .run(search_params, ptms, mzml_file_paths, metrics_scrape_url)
                .await?
        }
        (QueueTarget::Redis, StorageTarget::Memory) => {
            LocalPipeline::<RedisQueue, LocalPipelineStorage>::new(result_dir, tmp_dir, config)
                .await?
                .run(search_params, ptms, mzml_file_paths, metrics_scrape_url)
                .await?
        }
        (QueueTarget::Redis, StorageTarget::Redis) => {
            LocalPipeline::<RedisQueue, RedisPipelineStorage>::new(result_dir, tmp_dir, config)
                .await?
                .run(search_params, ptms, mzml_file_paths, metrics_scrape_url)
                .await?
        }
        (QueueTarget::Http, StorageTarget::Memory) => {
            LocalPipeline::<HttpQueue, LocalPipelineStorage>::new(result_dir, tmp_dir, config)
                .await?
                .run(search_params, ptms, mzml_file_paths, metrics_scrape_url)
                .await?
        }
        (QueueTarget::Http, StorageTarget::Redis) => {
            LocalPipeline::<HttpQueue, RedisPipelineStorage>::new(result_dir, tmp_dir, config)
                .await?
                .run(search_params, ptms, mzml_file_paths, metrics_scrape_url)
                .await?
        }
    };

    if let Some((handle, stop_signal_sender)) = http_queue_server {
        stop_signal_sender
            .send(())
            .map_err(|_| anyhow!("Sending stop signal to HTTP queue server"))?;
        handle
            .await
            .context("Waiting for HTTP queue server to stop")??;
    }

    Ok(())
}
