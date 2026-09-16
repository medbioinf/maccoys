use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use signal_hook::{consts::SIGINT, iterator::Signals};
use tracing::info;

use crate::pipeline::{
    configuration::StandaloneFinalizationConfiguration,
    errors::pipeline_error::PipelineError,
    messages::{error_message::ErrorMessage, publication_message::PublicationMessage},
    queue::{HttpPipelineQueue, PipelineQueue},
    storage::{PipelineStorage, RedisPipelineStorage},
    tasks::{
        error_task::ErrorTask,
        publication_task::PublicationTask,
        task::{backoff_sleep, poll_terminal_stage, Task, BACKOFF_START},
    },
};

/// Number of stages a worker slot round-robins across: publication, error.
const STAGE_COUNT: usize = 2;

/// Label used for Loki tracing, identifying this process as the combined finalization worker.
/// Free function mirroring the per-task `loki_tracing_label_value` functions, since this
/// module runs multiple task types rather than implementing [`Task`] itself.
///
pub fn loki_tracing_label_value() -> &'static str {
    "maccoys_finalization"
}

/// Runs publication and error handling in one process. Both are terminal stages (they write to
/// disk, not to a further pipeline queue), so each worker slot simply round-robins between the
/// two queues, picking whichever has a pending message, so idle capacity in one stage flows to
/// a busier one instead of sitting idle in a per-stage pool. Standalone entrypoint used by the
/// `maccoys pipeline finalization` CLI command; always talks to the HTTP queue broker and Redis
/// storage, mirroring each individual stage's own `run_standalone`. Should run on the same
/// machine which hosts the file system.
///
/// # Arguments
/// * `config_file_path` - Path to the configuration file
///
pub async fn run_standalone(config_file_path: PathBuf) -> Result<(), PipelineError> {
    let config = &fs::read_to_string(&config_file_path).map_err(|err| {
        PipelineError::FileReadError(config_file_path.to_string_lossy().to_string(), err)
    })?;

    let config: StandaloneFinalizationConfiguration = toml::from_str(config).map_err(|err| {
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
                signal_stop_flag.store(true, Ordering::Relaxed);
            }
        }
    });

    let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.num_tasks)
        .map(|_| {
            let publication_task = PublicationTask::new(
                config.work_directory.clone(),
                publication_queue.clone(),
                error_queue.clone(),
                storage.clone(),
                stop_flag.clone(),
            );
            let error_task = ErrorTask::new(
                config.work_directory.clone(),
                storage.clone(),
                error_queue.clone(),
                stop_flag.clone(),
            );

            tokio::spawn(run_slot(
                publication_task,
                error_task,
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

/// One worker slot's loop: round-robins between publication and error, picking whichever has a
/// pending message (via [`poll_terminal_stage`]) so a slot never sits idle on one stage while
/// the other has a backlog. Only backs off once a full round finds no work at all.
///
async fn run_slot(
    mut publication_task: PublicationTask<
        HttpPipelineQueue<PublicationMessage>,
        HttpPipelineQueue<ErrorMessage>,
        RedisPipelineStorage,
    >,
    mut error_task: ErrorTask<RedisPipelineStorage, HttpPipelineQueue<ErrorMessage>>,
    publication_queue: Arc<HttpPipelineQueue<PublicationMessage>>,
    error_queue: Arc<HttpPipelineQueue<ErrorMessage>>,
    stop_flag: Arc<AtomicBool>,
) {
    let mut backoff = BACKOFF_START;
    let mut start_stage = 0usize;

    'idle_loop: loop {
        if stop_flag.load(Ordering::Relaxed) {
            break;
        }

        for offset in 0..STAGE_COUNT {
            let stage = (start_stage + offset) % STAGE_COUNT;

            let did_work = match stage {
                0 => {
                    poll_terminal_stage(publication_queue.as_ref(), |id, msg| {
                        publication_task.process_message(id, msg)
                    })
                    .await
                }
                _ => {
                    poll_terminal_stage(error_queue.as_ref(), |id, msg| {
                        error_task.process_message(id, msg)
                    })
                    .await
                }
            };

            if did_work {
                backoff = BACKOFF_START;
                start_stage = (stage + 1) % STAGE_COUNT;
                continue 'idle_loop;
            }
        }

        // Neither stage had a pending message; back off before rescanning.
        backoff = backoff_sleep(backoff).await;
    }
}
