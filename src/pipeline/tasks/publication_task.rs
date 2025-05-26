use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
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
    queue::{PipelineQueue, RedisPipelineQueue},
    storage::{PipelineStorage, RedisPipelineStorage},
};

use super::task::Task;

/// Prefix for the scoring counter
///
pub const COUNTER_PREFIX: &str = "maccoys_publication";

/// Task to write files into the search directories
///
///
pub struct PublicationTask;

impl PublicationTask {
    /// Starts the task
    ///
    /// # Arguments
    /// * `work_dir` - The working directory
    /// * `publish_queue` - The queue to push the scored PSMs to
    /// * `error_queue` - The queue to push the errors to
    /// * `storage` - The storage to check the finished spectra
    /// * `stop_flag` - The flag to stop the task
    ///
    pub async fn start<P, E, S>(
        work_dir: PathBuf,
        publication_queue: Arc<P>,
        error_queue: Arc<E>,
        storage: Arc<S>,
        stop_flag: Arc<AtomicBool>,
    ) where
        P: PipelineQueue<PublicationMessage> + Send + Sync + 'static,
        E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
        S: PipelineStorage + Send + Sync + 'static,
    {
        'message_loop: loop {
            if stop_flag.load(Ordering::Relaxed) {
                break;
            }
            let (message_id, message) = match publication_queue.pop().await {
                Ok(Some(message)) => message,
                Ok(None) => {
                    // If the queue is empty, wait for a while before checking again
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    continue 'message_loop;
                }
                Err(e) => {
                    error!("{}", e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    continue 'message_loop;
                }
            };
            let metrics_counter_name = Self::get_counter_name(message.uuid());

            let absolute_file_path = work_dir.join(message.file_path());

            match create_dir_all(absolute_file_path.parent().unwrap()).await {
                Ok(_) => {}
                Err(e) => {
                    let error_message =
                        message.to_error_message(PipelineError::DirectoryCreationError(e));
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, error_queue.as_ref()).await;
                    continue 'message_loop;
                }
            }

            match write(absolute_file_path, message.content()).await {
                Ok(_) => {}
                Err(e) => {
                    let error_message =
                        message.to_error_message(PublicationError::FilePublishingError(e).into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, error_queue.as_ref()).await;
                    continue 'message_loop;
                }
            }

            if message.update_spectrum_count() {
                match storage
                    .increase_finished_spectrum_count(message.uuid())
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        let error_message = message.to_error_message(e.into());
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                }
            }

            Self::ack_message(&message_id, publication_queue.as_ref()).await;

            let is_search_finished = match storage.is_search_finished(message.uuid()).await {
                Ok(is_finished) => is_finished,
                Err(e) => {
                    let error_message = message.to_error_message(e.into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, error_queue.as_ref()).await;
                    continue 'message_loop;
                }
            };

            if is_search_finished {
                match storage.cleanup_search(message.uuid()).await {
                    Ok(_) => {}
                    Err(e) => {
                        let error_message = message.to_error_message(e.into());
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                }
            }

            counter!(metrics_counter_name.clone()).increment(1);
        }
        // wait before checking the queue again
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    /// Run the publication task by itself
    ///
    /// # Arguments
    /// * `work_dir` - Working directory
    /// * `config_file_path` - Path to the configuration file
    ///
    pub async fn run_standalone(
        work_dir: PathBuf,
        config_file_path: PathBuf,
    ) -> Result<(), PipelineError> {
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
            Arc::new(RedisPipelineQueue::<PublicationMessage>::new(&config.publication).await?);

        let error_queue = Arc::new(RedisPipelineQueue::<ErrorMessage>::new(&config.error).await?);

        let storage = Arc::new(RedisPipelineStorage::new(&config.storage).await?);

        let mut signals = Signals::new([SIGINT]).map_err(PipelineError::SignalHandlerError)?;

        let stop_flag = Arc::new(AtomicBool::new(false));

        let signal_stop_flag = stop_flag.clone();
        std::thread::spawn(move || {
            for sig in signals.forever() {
                if sig == SIGINT {
                    info!("Gracefully stopping.");
                    signal_stop_flag.store(true, Ordering::Relaxed);
                }
            }
        });

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.publication.num_tasks)
            .map(|_| {
                tokio::spawn(PublicationTask::start(
                    work_dir.clone(),
                    publication_queue.clone(),
                    error_queue.clone(),
                    storage.clone(),
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

impl Task for PublicationTask {
    fn get_counter_prefix() -> &'static str {
        COUNTER_PREFIX
    }
}
