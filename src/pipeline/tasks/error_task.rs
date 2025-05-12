use std::{
    fs::{self, File},
    io::Write,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{Context, Result};
use metrics::counter;
use signal_hook::{consts::SIGINT, iterator::Signals};
use tokio::fs::create_dir_all;
use tracing::{error, info};

use crate::pipeline::{
    configuration::StandaloneErrorConfiguration,
    errors::pipeline_error::PipelineError,
    messages::{error_message::ErrorMessage, is_message::IsMessage},
    queue::{PipelineQueue, RedisPipelineQueue},
    utils::{
        create_file_path_on_ms_run_level, create_file_path_on_precursor_level,
        create_file_path_on_spectrum_level,
    },
};

use super::task::Task;

/// Prefix for the scoring counter
///
pub const COUNTER_PREFIX: &str = "maccyos_errors";

/// Task to write errors to the corresponding search directories
/// This taks should run on the same server which is hosting the filesystems
/// to have minimal chance of network errors.
///
pub struct ErrorTask;

impl ErrorTask {
    /// Starts the task
    ///
    /// # Arguments
    /// * `publish_queue` - The queue to push the scored PSMs to
    /// * `stop_flag` - The flag to stop the task
    ///
    pub async fn start<E>(work_dir: PathBuf, error_queue: Arc<E>, stop_flag: Arc<AtomicBool>)
    where
        E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
    {
        'message_loop: loop {
            if stop_flag.load(Ordering::Relaxed) {
                break;
            }
            let (message_id, message) = match error_queue.pop().await {
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

            // Create a file path depending on the level of the message
            let relative_file_path =
                if message.spectrum_id().is_some() && message.precursor().is_some() {
                    create_file_path_on_precursor_level(
                        message.uuid(),
                        message.ms_run_name(),
                        message.spectrum_id().as_ref().unwrap(),
                        message.precursor().as_ref().unwrap(),
                        "err",
                    )
                } else if message.spectrum_id().is_some() {
                    create_file_path_on_spectrum_level(
                        message.uuid(),
                        message.ms_run_name(),
                        message.spectrum_id().as_ref().unwrap(),
                        "err",
                    )
                } else {
                    create_file_path_on_ms_run_level(message.uuid(), message.ms_run_name(), "err")
                };

            let absolute_file_path = work_dir.join(relative_file_path);

            match create_dir_all(absolute_file_path.parent().unwrap()).await {
                Ok(_) => {}
                Err(e) => {
                    let error_message =
                        message.to_error_message(PipelineError::DirectoryCreationError(e));
                    error!("{}", &error_message);
                    continue 'message_loop;
                }
            }

            let mut log_file = match File::options()
                .create(true)
                .append(true)
                .open(&absolute_file_path)
            {
                Ok(file) => file,
                Err(e) => {
                    let error_message = message.to_error_message(PipelineError::OpenLogError(e));
                    error!("{}", &error_message);
                    continue 'message_loop;
                }
            };

            match log_file.write(message.error().as_bytes()) {
                Ok(_) => {}
                Err(e) => {
                    let error_message = message.to_error_message(PipelineError::WriteLogError(e));
                    error!("{}", &error_message);
                    continue 'message_loop;
                }
            }

            Self::ack_message(&message_id, error_queue.as_ref()).await;

            counter!(metrics_counter_name.clone()).increment(1);
        }
        // wait before checking the queue again
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    /// Run the error handling task by itself
    ///
    /// # Arguments
    /// * `work_dir` - Working directory
    /// * `config_file_path` - Path to the configuration file
    ///
    pub async fn run_standalone(work_dir: PathBuf, config_file_path: PathBuf) -> Result<()> {
        let config: StandaloneErrorConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

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

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.error.num_tasks)
            .map(|_| {
                tokio::spawn(ErrorTask::start(
                    work_dir.clone(),
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

impl Task for ErrorTask {
    fn get_counter_prefix() -> &'static str {
        COUNTER_PREFIX
    }
}
