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
use futures::{future::join_all, Future};
use macpepdb::post_translational_modification::PostTranslationalModification;
use macpepdb_tui::{MetricConfig, TuiHandle};
use tokio::{fs::create_dir_all, time::sleep};
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::pipeline::{
    errors::queue_error::QueueError,
    messages::is_message::IsMessage,
    queue::{HttpPipelineQueue, LocalPipelineQueue},
    queue_server::{IsQueueRouteable, QueueServer, QueueServerState},
    storage::{LocalPipelineStorage, RedisPipelineStorage},
    tasks::{
        error_task::{self, ErrorTask},
        identification_task::{self, IdentificationTask},
        indexing_task::{self, IndexingTask},
        publication_task::{self, PublicationTask},
        scoring_task::{self, ScoringTask},
        search_space_generation_task::{self, SearchSpaceGenerationTask},
        task::Task,
    },
    utils::create_file_path_on_ms_run_level,
};

use super::{
    configuration::{
        PipelineConfiguration, QueuingConfiguration, SearchParameters, TaskConfiguration,
    },
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
    Local,
    Http,
}

pub trait QueueFactory {
    type Queue<M>: PipelineQueue<M>
    where
        M: IsMessage + IsQueueRouteable;

    /// Constructs one of this factory's queues for a pipeline stage.
    ///
    /// # Arguments
    /// * `config` - The stage's own task configuration
    /// * `queueing` - The pipeline-wide queueing configuration (spool directory / WIP
    ///   timeout); only consumed by disk-backed implementations (`LocalQueue`), `HttpQueue`
    ///   ignores it since the broker owns its own spool directory/timeout.
    ///
    fn new_queue<M>(
        config: &TaskConfiguration,
        queueing: &QueuingConfiguration,
    ) -> impl Future<Output = Result<Self::Queue<M>, QueueError>> + Send
    where
        M: IsMessage + IsQueueRouteable;

    /// Reschedules any timed-out work-in-progress messages of `queue` back onto its pending
    /// queue. A no-op for queue implementations that don't track WIP state in-process (e.g.
    /// `HttpQueue`'s broker runs its own rescheduling loop out-of-process).
    ///
    fn reschedule<M>(queue: &Self::Queue<M>)
    where
        M: IsMessage + IsQueueRouteable;
}

pub struct LocalQueue;
impl QueueFactory for LocalQueue {
    type Queue<M>
        = LocalPipelineQueue<M>
    where
        M: IsMessage + IsQueueRouteable;

    async fn new_queue<M>(
        config: &TaskConfiguration,
        queueing: &QueuingConfiguration,
    ) -> Result<Self::Queue<M>, QueueError>
    where
        M: IsMessage + IsQueueRouteable,
    {
        Ok(LocalPipelineQueue::new_at(
            config.queue_capacity,
            queueing.message_timeout,
            queueing
                .data_directory
                .join("queue_data")
                .join(&config.queue_name),
        ))
    }

    fn reschedule<M>(queue: &Self::Queue<M>)
    where
        M: IsMessage + IsQueueRouteable,
    {
        if let Err(e) = queue.reschedule() {
            error!(
                "[LocalPipeline / Rescheduling] Could not reschedule: {:?}",
                e
            );
        }
    }
}

pub struct HttpQueue;
impl QueueFactory for HttpQueue {
    type Queue<M>
        = HttpPipelineQueue<M>
    where
        M: IsMessage + IsQueueRouteable;

    async fn new_queue<M>(
        config: &TaskConfiguration,
        _queueing: &QueuingConfiguration,
    ) -> Result<Self::Queue<M>, QueueError>
    where
        M: IsMessage + IsQueueRouteable,
    {
        HttpPipelineQueue::new(config).await
    }

    fn reschedule<M>(_queue: &Self::Queue<M>)
    where
        M: IsMessage + IsQueueRouteable,
    {
        // The HTTP broker (queue_server::QueueServer) runs its own rescheduling loop.
    }
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

    /// Periodically reschedules timed-out work-in-progress messages across all 6 queues.
    /// See [`QueueFactory::reschedule`] - a no-op for queue implementations which don't need it.
    reschedule_task: tokio::task::JoinHandle<()>,
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
        let indexing_queue = Arc::new(
            <Q as QueueFactory>::new_queue::<IndexingMessage>(&config.index, &config.queueing)
                .await?,
        );
        let search_space_generation_queue = Arc::new(
            <Q as QueueFactory>::new_queue::<SearchSpaceGenerationMessage>(
                &config.search_space_generation,
                &config.queueing,
            )
            .await?,
        );
        let identification_queue = Arc::new(
            <Q as QueueFactory>::new_queue::<IdentificationMessage>(
                &config.identification,
                &config.queueing,
            )
            .await?,
        );
        let scoring_queue = Arc::new(
            <Q as QueueFactory>::new_queue::<ScoringMessage>(&config.scoring, &config.queueing)
                .await?,
        );
        let publication_queue = Arc::new(
            <Q as QueueFactory>::new_queue::<PublicationMessage>(
                &config.publication,
                &config.queueing,
            )
            .await?,
        );
        let error_queue = Arc::new(
            <Q as QueueFactory>::new_queue::<ErrorMessage>(&config.error, &config.queueing).await?,
        );

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
                tokio::spawn(
                    IndexingTask::new(
                        result_dir.clone(),
                        indexing_queue.clone(),
                        search_space_generation_queue.clone(),
                        error_queue.clone(),
                        storage.clone(),
                        index_stop_flag.clone(),
                    )
                    .run(),
                )
            })
            .collect();

        let search_space_generation_tasks: Vec<tokio::task::JoinHandle<()>> =
            (0..config.search_space_generation.num_tasks)
                .map(|_| {
                    tokio::spawn(
                        SearchSpaceGenerationTask::new(
                            Arc::new(config.search_space_generation.clone()),
                            storage.clone(),
                            search_space_generation_queue.clone(),
                            identification_queue.clone(),
                            publication_queue.clone(),
                            error_queue.clone(),
                            search_space_generation_stop_flag.clone(),
                        )
                        .run(),
                    )
                })
                .collect();

        let identification_tasks: Vec<tokio::task::JoinHandle<()>> =
            (0..config.identification.num_tasks)
                .map(|comet_proc_idx| {
                    let comet_tmp_dir = tmp_dir.join(format!("comet_{}", comet_proc_idx));
                    tokio::spawn(
                        IdentificationTask::new(
                            comet_tmp_dir,
                            Arc::new(config.identification.clone()),
                            storage.clone(),
                            identification_queue.clone(),
                            scoring_queue.clone(),
                            publication_queue.clone(),
                            error_queue.clone(),
                            identification_stop_flag.clone(),
                        )
                        .run(),
                    )
                })
                .collect();

        let scoring_tasks: Vec<tokio::task::JoinHandle<()>> = (0..config.scoring.num_tasks)
            .map(|_| {
                tokio::spawn(
                    ScoringTask::new(
                        scoring_queue.clone(),
                        publication_queue.clone(),
                        error_queue.clone(),
                        scoring_stop_flag.clone(),
                        config.scoring.threads,
                    )
                    .run(),
                )
            })
            .collect();

        let publication_tasks: Vec<tokio::task::JoinHandle<()>> = (0..config.publication.num_tasks)
            .map(|_| {
                tokio::spawn(
                    PublicationTask::new(
                        result_dir.clone(),
                        publication_queue.clone(),
                        error_queue.clone(),
                        storage.clone(),
                        publication_stop_flag.clone(),
                    )
                    .run(),
                )
            })
            .collect();

        let error_tasks: Vec<tokio::task::JoinHandle<()>> = (0..config.error.num_tasks)
            .map(|_| {
                tokio::spawn(
                    ErrorTask::new(
                        result_dir.clone(),
                        error_queue.clone(),
                        error_stop_flag.clone(),
                    )
                    .run(),
                )
            })
            .collect();

        let reschedule_task = tokio::spawn(Self::reschedule_task(
            indexing_queue.clone(),
            search_space_generation_queue.clone(),
            identification_queue.clone(),
            scoring_queue.clone(),
            publication_queue.clone(),
            error_queue.clone(),
            config.queueing.reschedule_check_interval,
        ));

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
            reschedule_task,
        })
    }

    /// Periodically calls [`QueueFactory::reschedule`] on all 6 queues, so that messages
    /// whose processing errored out without acking (see `Task::process_message` impls, which
    /// route to the error queue and return early without acking on failure) don't get stuck
    /// in a disk-backed queue's work-in-progress map forever.
    ///
    async fn reschedule_task(
        indexing_queue: Arc<<Q as QueueFactory>::Queue<IndexingMessage>>,
        search_space_generation_queue: Arc<
            <Q as QueueFactory>::Queue<SearchSpaceGenerationMessage>,
        >,
        identification_queue: Arc<<Q as QueueFactory>::Queue<IdentificationMessage>>,
        scoring_queue: Arc<<Q as QueueFactory>::Queue<ScoringMessage>>,
        publication_queue: Arc<<Q as QueueFactory>::Queue<PublicationMessage>>,
        error_queue: Arc<<Q as QueueFactory>::Queue<ErrorMessage>>,
        check_interval: Duration,
    ) {
        loop {
            sleep(check_interval).await;
            Q::reschedule(indexing_queue.as_ref());
            Q::reschedule(search_space_generation_queue.as_ref());
            Q::reschedule(identification_queue.as_ref());
            Q::reschedule(scoring_queue.as_ref());
            Q::reschedule(publication_queue.as_ref());
            Q::reschedule(error_queue.as_ref());
        }
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
        ptms: Vec<Arc<PostTranslationalModification>>,
        mzml_file_paths: Vec<PathBuf>,
        tui: &Option<TuiHandle>,
    ) -> Result<()> {
        let uuid = Uuid::new_v4().to_string();
        info!("UUID: {}", uuid);

        self.storage
            .init_search(&uuid, search_params.clone(), ptms.as_slice())
            .await?;

        if let Some(tui) = tui {
            tui.add_metric(MetricConfig::rate(
                indexing_task::get_counter_name(&uuid),
                "Indexing rate",
            ));
            tui.add_metric(MetricConfig::rate(
                search_space_generation_task::get_counter_name(&uuid),
                "Search spaces generation rate",
            ));
            tui.add_metric(MetricConfig::rate(
                identification_task::get_counter_name(&uuid),
                "Identification rate",
            ));
            tui.add_metric(MetricConfig::rate(
                scoring_task::get_counter_name(&uuid),
                "Scoring rate",
            ));
            tui.add_metric(MetricConfig::rate(
                publication_task::get_counter_name(&uuid),
                "Publication rate",
            ));
            tui.add_metric(MetricConfig::rate(
                error_task::get_counter_name(&uuid),
                "Error rate",
            ));

            tui.add_metric(MetricConfig::counter(
                indexing_task::get_counter_name(&uuid),
                "Indexed runs",
            ));
            tui.add_metric(MetricConfig::counter(
                search_space_generation_task::get_counter_name(&uuid),
                "Search spaces generated",
            ));
            tui.add_metric(MetricConfig::counter(
                identification_task::get_counter_name(&uuid),
                "Identified spectra",
            ));
            tui.add_metric(MetricConfig::counter(
                scoring_task::get_counter_name(&uuid),
                "Scored PSM distributions",
            ));
            tui.add_metric(MetricConfig::counter(
                publication_task::get_counter_name(&uuid),
                "Publicated results",
            ));
            tui.add_metric(MetricConfig::counter(
                error_task::get_counter_name(&uuid),
                "Errors",
            ));
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

        let mut is_fully_enqueued_notified = false;
        loop {
            if self.storage.is_search_finished(&uuid).await? {
                info!("Search finished");
                break;
            }
            sleep(Duration::from_millis(1000)).await;
            if self.storage.is_search_fully_enqueued(&uuid).await? && !is_fully_enqueued_notified {
                is_fully_enqueued_notified = true;
                info!("Spectra fully enqueued");
            }
        }

        if let Some(tui) = tui {
            tui.remove_metric(indexing_task::get_counter_name(&uuid).as_str());
            tui.remove_metric(search_space_generation_task::get_counter_name(&uuid).as_str());
            tui.remove_metric(identification_task::get_counter_name(&uuid).as_str());
            tui.remove_metric(scoring_task::get_counter_name(&uuid).as_str());
            tui.remove_metric(publication_task::get_counter_name(&uuid).as_str());
            tui.remove_metric(error_task::get_counter_name(&uuid).as_str());
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

        self.reschedule_task.abort();

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
    ptms: Vec<Arc<PostTranslationalModification>>,
    mzml_file_paths: Vec<PathBuf>,
    tui: &Option<TuiHandle>,
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

            let state = Arc::new(QueueServerState::from_config(
                &queue_server_config,
                queue_server_config
                    .queueing
                    .data_directory
                    .join("queue_data"),
            ));

            let (stop_signal_sender, stop_signal_receiver) = tokio::sync::oneshot::channel::<()>();

            let stop_signal = async move {
                stop_signal_receiver.await.ok();
            };

            let handle = tokio::spawn(async move {
                QueueServer::run(
                    interface,
                    port,
                    state,
                    Duration::from_secs(5),
                    Some(Box::pin(stop_signal)),
                )
                .await
            });

            Some((handle, stop_signal_sender))
        }
        _ => None,
    };

    match (queue, storage) {
        (QueueTarget::Local, StorageTarget::Memory) => {
            LocalPipeline::<LocalQueue, LocalPipelineStorage>::new(result_dir, tmp_dir, config)
                .await?
                .run(search_params, ptms, mzml_file_paths, tui)
                .await?
        }
        (QueueTarget::Local, StorageTarget::Redis) => {
            LocalPipeline::<LocalQueue, RedisPipelineStorage>::new(result_dir, tmp_dir, config)
                .await?
                .run(search_params, ptms, mzml_file_paths, tui)
                .await?
        }
        (QueueTarget::Http, StorageTarget::Memory) => {
            LocalPipeline::<HttpQueue, LocalPipelineStorage>::new(result_dir, tmp_dir, config)
                .await?
                .run(search_params, ptms, mzml_file_paths, tui)
                .await?
        }
        (QueueTarget::Http, StorageTarget::Redis) => {
            LocalPipeline::<HttpQueue, RedisPipelineStorage>::new(result_dir, tmp_dir, config)
                .await?
                .run(search_params, ptms, mzml_file_paths, tui)
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
