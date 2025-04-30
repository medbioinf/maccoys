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
    /// * `publish_queue` - The queue to push the scored PSMs to
    /// * `stop_flag` - The flag to stop the task
    ///
    pub async fn start<P, E>(
        work_dir: PathBuf,
        publication_queue: Arc<P>,
        error_queue: Arc<E>,
        stop_flag: Arc<AtomicBool>,
    ) where
        P: PipelineQueue<PublicationMessage> + Send + Sync + 'static,
        E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
    {
        'message_loop: loop {
            while let Some(message) = publication_queue.pop().await {
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
                        let error_message = message
                            .to_error_message(PublicationError::FilePublishingError(e).into());
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                }

                counter!(metrics_counter_name.clone()).increment(1);
            }
            if stop_flag.load(Ordering::Relaxed) {
                break 'message_loop;
            }
            // wait before checking the queue again
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    /// Run the publication task by itself
    ///
    /// # Arguments
    /// * `work_dir` - Working directory
    /// * `config_file_path` - Path to the configuration file
    ///
    pub async fn run_standalone(work_dir: PathBuf, config_file_path: PathBuf) -> Result<()> {
        let config: StandalonePublicationConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let publication_queue =
            Arc::new(RedisPipelineQueue::<PublicationMessage>::new(&config.publication).await?);

        let error_queue = Arc::new(RedisPipelineQueue::<ErrorMessage>::new(&config.error).await?);

        let mut signals = Signals::new([SIGINT])?;

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
