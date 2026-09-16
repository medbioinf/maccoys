use std::{
    fs::{self, File},
    io::Write,
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
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
    queue::{HttpPipelineQueue, PipelineQueue},
    storage::{PipelineStorage, RedisPipelineStorage},
    utils::{
        create_file_path_on_ms_run_level, create_file_path_on_precursor_level,
        create_file_path_on_search_level, create_file_path_on_spectrum_level,
    },
};

use super::task::Task;

/// Prefix for the scoring counter
///
pub const COUNTER_PREFIX: &str = "maccoys_errors";

/// Returns the metrics counter name for a search UUID. Free function mirroring
/// [`Task::get_counter_name`] for use where `ErrorTask`'s generics aren't in
/// scope to infer.
///
pub fn get_counter_name(uuid: &str) -> String {
    format!("{}_{}", COUNTER_PREFIX, uuid)
}

/// Returns the Loki tracing label value for ErrorTask. Free function mirroring
/// [`Task::loki_tracing_label_value`] for use where ErrorTask's generics aren't
/// in scope to infer.
///
pub fn loki_tracing_label_value() -> &'static str {
    COUNTER_PREFIX
}

/// Task to write errors to the corresponding search directories
/// This taks should run on the same server which is hosting the filesystems
/// to have minimal chance of network errors.
///
/// # Generics
/// * `St` - Type of the storage
/// * `E` - Type of the error queue (this task's own queue)
///
pub struct ErrorTask<St, E>
where
    St: PipelineStorage + Send + Sync + 'static,
    E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
{
    work_dir: PathBuf,
    storage: Arc<St>,
    error_queue: Arc<E>,
    stop_flag: Arc<AtomicBool>,
}

impl<St, E> ErrorTask<St, E>
where
    St: PipelineStorage + Send + Sync + 'static,
    E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
{
    /// Creates a new error task
    ///
    /// # Arguments
    /// * `work_dir` - The working directory
    /// * `storage` - Storage to track per-search stage counts
    /// * `error_queue` - The queue to get the errors to write from
    /// * `stop_flag` - The flag to stop the task
    ///
    pub fn new(
        work_dir: PathBuf,
        storage: Arc<St>,
        error_queue: Arc<E>,
        stop_flag: Arc<AtomicBool>,
    ) -> Self {
        Self {
            work_dir,
            storage,
            error_queue,
            stop_flag,
        }
    }

}

/// Standalone entrypoint used by the `maccoys pipeline error` CLI command; always talks to
/// the HTTP queue broker, so it's implemented for the one concrete instantiation rather than
/// the generic one used by [`super::super::local_pipeline`].
///
impl ErrorTask<RedisPipelineStorage, HttpPipelineQueue<ErrorMessage>> {
    /// Run the error handling task by itself
    ///
    /// # Arguments
    /// * `config_file_path` - Path to the configuration file
    ///
    pub async fn run_standalone(config_file_path: PathBuf) -> Result<()> {
        let config: StandaloneErrorConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let error_queue = Arc::new(HttpPipelineQueue::<ErrorMessage>::new(&config.error).await?);

        let storage = Arc::new(RedisPipelineStorage::new(&config.storage).await?);

        let mut signals = Signals::new([SIGINT])?;

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

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.error.num_tasks)
            .map(|_| {
                tokio::spawn(
                    ErrorTask::new(
                        config.work_directory.clone(),
                        storage.clone(),
                        error_queue.clone(),
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

impl<St, E> Task for ErrorTask<St, E>
where
    St: PipelineStorage + Send + Sync + 'static,
    E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
{
    type Message = ErrorMessage;
    type Queue = E;

    fn get_counter_prefix() -> &'static str {
        COUNTER_PREFIX
    }

    fn queue(&self) -> &Arc<E> {
        &self.error_queue
    }

    fn stop_flag(&self) -> &Arc<AtomicBool> {
        &self.stop_flag
    }

    async fn process_message(&mut self, message_id: String, message: ErrorMessage) {
        info!(
            "[error] Got message {}/{}/{}",
            message.uuid(),
            message.ms_run_name().as_ref().unwrap_or(&"n/a".to_string()),
            message.spectrum_id().as_ref().unwrap_or(&"n/a".to_string()),
        );

        let metrics_counter_name = Self::get_counter_name(message.uuid());

        // Create a file path depending on the level of the message
        let relative_file_path = if message.ms_run_name().is_some()
            && message.spectrum_id().is_some()
            && message.precursor().is_some()
        {
            create_file_path_on_precursor_level(
                message.uuid(),
                message.ms_run_name().as_ref().unwrap(),
                message.spectrum_id().as_ref().unwrap(),
                message.precursor().as_ref().unwrap(),
                "err",
            )
        } else if message.ms_run_name().is_some() && message.spectrum_id().is_some() {
            create_file_path_on_spectrum_level(
                message.uuid(),
                message.ms_run_name().as_ref().unwrap(),
                message.spectrum_id().as_ref().unwrap(),
                "err",
            )
        } else if message.ms_run_name().is_some() {
            create_file_path_on_ms_run_level(message.uuid(), message.ms_run_name().as_ref().unwrap(), "err")
        } else {
            create_file_path_on_search_level(message.uuid(), "err")
        };

        let absolute_file_path = self.work_dir.join(relative_file_path);

        match create_dir_all(absolute_file_path.parent().unwrap()).await {
            Ok(_) => {}
            Err(e) => {
                let error_message = message.to_error_message(PipelineError::DirectoryCreationError(e));
                error!("{}", &error_message);
                return;
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
                return;
            }
        };

        match log_file.write(message.error().as_bytes()) {
            Ok(_) => {}
            Err(e) => {
                let error_message = message.to_error_message(PipelineError::WriteLogError(e));
                error!("{}", &error_message);
                return;
            }
        }

        Self::ack_message(&message_id, self.error_queue.as_ref()).await;

        counter!(metrics_counter_name.clone()).increment(1);
        if let Err(e) = self
            .storage
            .increase_stage_count(COUNTER_PREFIX, message.uuid())
            .await
        {
            error!("Error increasing error stage count: {:?}", e);
        }
    }
}
