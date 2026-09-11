use std::{
    fs,
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
};

use metrics::counter;
use signal_hook::{consts::SIGINT, iterator::Signals};
use tokio::fs::{create_dir_all, write};
use tracing::{error, info};

use crate::pipeline::{
    configuration::StandalonePublicationConfiguration,
    errors::{pipeline_error::PipelineError, publication_error::PublicationError},
    messages::{
        error_message::ErrorMessage, is_message::IsMessage, publication_message::PublicationMessage,
    },
    queue::{HttpPipelineQueue, PipelineQueue},
    storage::{PipelineStorage, RedisPipelineStorage},
};

use super::task::Task;

/// Prefix for the scoring counter
///
pub const COUNTER_PREFIX: &str = "maccoys_publication";

/// Returns the metrics counter name for a search UUID. Free function mirroring
/// [`Task::get_counter_name`] for use where `PublicationTask`'s generics aren't
/// in scope to infer.
///
pub fn get_counter_name(uuid: &str) -> String {
    format!("{}_{}", COUNTER_PREFIX, uuid)
}

/// Returns the Loki tracing label value for PublicationTask. Free function mirroring
/// [`Task::loki_tracing_label_value`] for use where PublicationTask's generics aren't
/// in scope to infer.
///
pub fn loki_tracing_label_value() -> &'static str {
    COUNTER_PREFIX
}

/// Task to write files into the search directories
///
/// # Generics
/// * `P` - Type of the publication queue (this task's own queue)
/// * `E` - Type of the error queue
/// * `S` - Type of the storage
///
pub struct PublicationTask<P, E, S>
where
    P: PipelineQueue<PublicationMessage> + Send + Sync + 'static,
    E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
    S: PipelineStorage + Send + Sync + 'static,
{
    work_dir: PathBuf,
    publication_queue: Arc<P>,
    error_queue: Arc<E>,
    storage: Arc<S>,
    stop_flag: Arc<AtomicBool>,
}

impl<P, E, S> PublicationTask<P, E, S>
where
    P: PipelineQueue<PublicationMessage> + Send + Sync + 'static,
    E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
    S: PipelineStorage + Send + Sync + 'static,
{
    /// Creates a new publication task
    ///
    /// # Arguments
    /// * `work_dir` - The working directory
    /// * `publication_queue` - The queue to get the messages to publish from
    /// * `error_queue` - The queue to push the errors to
    /// * `storage` - The storage to check the finished spectra
    /// * `stop_flag` - The flag to stop the task
    ///
    pub fn new(
        work_dir: PathBuf,
        publication_queue: Arc<P>,
        error_queue: Arc<E>,
        storage: Arc<S>,
        stop_flag: Arc<AtomicBool>,
    ) -> Self {
        Self {
            work_dir,
            publication_queue,
            error_queue,
            storage,
            stop_flag,
        }
    }

}

/// Standalone entrypoint used by the `maccoys pipeline publication` CLI command; always talks
/// to the HTTP queue broker and Redis storage, so it's implemented for the one concrete
/// instantiation rather than the generic one used by [`super::super::local_pipeline`].
///
impl
    PublicationTask<
        HttpPipelineQueue<PublicationMessage>,
        HttpPipelineQueue<ErrorMessage>,
        RedisPipelineStorage,
    >
{
    /// Run the publication task by itself
    ///
    /// # Arguments
    /// * `config_file_path` - Path to the configuration file
    ///
    pub async fn run_standalone(config_file_path: PathBuf) -> Result<(), PipelineError> {
        let config = &fs::read_to_string(&config_file_path).map_err(|err| {
            PipelineError::FileReadError(config_file_path.to_string_lossy().to_string(), err)
        })?;

        let config: StandalonePublicationConfiguration = toml::from_str(config).map_err(|err| {
            PipelineError::ConfigDeserializationError(
                config_file_path.to_string_lossy().to_string(),
                err,
            )
        })?;

        let publication_queue =
            Arc::new(HttpPipelineQueue::<PublicationMessage>::new(&config.publication).await?);

        let error_queue = Arc::new(HttpPipelineQueue::<ErrorMessage>::new(&config.error).await?);

        let storage = Arc::new(RedisPipelineStorage::new(&config.storage).await?);

        let mut signals = Signals::new([SIGINT]).map_err(PipelineError::SignalHandlerError)?;

        let stop_flag = Arc::new(AtomicBool::new(false));

        let signal_stop_flag = stop_flag.clone();
        std::thread::spawn(move || {
            for sig in signals.forever() {
                if sig == SIGINT {
                    info!("Gracefully stopping.");
                    signal_stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            }
        });

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.publication.num_tasks)
            .map(|_| {
                tokio::spawn(
                    PublicationTask::new(
                        config.work_directory.clone(),
                        publication_queue.clone(),
                        error_queue.clone(),
                        storage.clone(),
                        stop_flag.clone(),
                    )
                    .run(),
                )
            })
            .collect();

        for handle in handles {
            handle.await?;
        }

        Ok(())
    }
}

impl<P, E, S> Task for PublicationTask<P, E, S>
where
    P: PipelineQueue<PublicationMessage> + Send + Sync + 'static,
    E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
    S: PipelineStorage + Send + Sync + 'static,
{
    type Message = PublicationMessage;
    type Queue = P;

    fn get_counter_prefix() -> &'static str {
        COUNTER_PREFIX
    }

    fn queue(&self) -> &Arc<P> {
        &self.publication_queue
    }

    fn stop_flag(&self) -> &Arc<AtomicBool> {
        &self.stop_flag
    }

    async fn process_message(&mut self, message_id: String, message: PublicationMessage) {
        info!(
            "[publication] Got message {}/{}/{} with {} bytes to publish in {}",
            message.uuid(),
            message.ms_run_name(),
            message.spectrum_id(),
            message.content().len(),
            message.file_path().display(),
        );

        let metrics_counter_name = Self::get_counter_name(message.uuid());

        let absolute_file_path = self.work_dir.join(message.file_path());

        match create_dir_all(absolute_file_path.parent().unwrap()).await {
            Ok(_) => {}
            Err(e) => {
                let error_message = message.to_error_message(PipelineError::DirectoryCreationError(e));
                error!("{}", &error_message);
                Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                return;
            }
        }

        match write(absolute_file_path, message.content()).await {
            Ok(_) => {}
            Err(e) => {
                let error_message =
                    message.to_error_message(PublicationError::FilePublishingError(e).into());
                error!("{}", &error_message);
                Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                return;
            }
        }

        if message.update_spectrum_count() {
            match self.storage.increase_finished_spectrum_count(message.uuid()).await {
                Ok(_) => {}
                Err(e) => {
                    let error_message = message.to_error_message(e.into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                    return;
                }
            }
        }

        Self::ack_message(&message_id, self.publication_queue.as_ref()).await;

        let is_search_finished = match self.storage.is_search_finished(message.uuid()).await {
            Ok(is_finished) => is_finished,
            Err(e) => {
                let error_message = message.to_error_message(e.into());
                error!("{}", &error_message);
                Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                return;
            }
        };

        if is_search_finished {
            match self.storage.cleanup_search(message.uuid()).await {
                Ok(_) => {}
                Err(e) => {
                    let error_message = message.to_error_message(e.into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                    return;
                }
            }
        }

        counter!(metrics_counter_name.clone()).increment(1);
    }
}
