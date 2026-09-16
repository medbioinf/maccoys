use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use signal_hook::{consts::SIGINT, iterator::Signals};
use tracing::{error, info};

use crate::pipeline::{
    configuration::StandaloneProcessingConfiguration,
    errors::pipeline_error::PipelineError,
    messages::{
        error_message::ErrorMessage, identification_message::IdentificationMessage,
        publication_message::PublicationMessage, scoring_message::ScoringMessage,
        search_space_generation_message::SearchSpaceGenerationMessage,
    },
    queue::{HttpPipelineQueue, PipelineQueue},
    storage::{PipelineStorage, RedisPipelineStorage},
    tasks::{
        identification_task::IdentificationTask,
        scoring_task::ScoringTask,
        search_space_generation_task::SearchSpaceGenerationTask,
        task::{backoff_sleep, poll_stage_with_backpressure, Task, BACKOFF_START},
    },
};

/// Number of stages a worker slot round-robins across: search space generation,
/// identification, scoring.
const STAGE_COUNT: usize = 3;

/// Label used for Loki tracing, identifying this process as the combined processing worker.
/// Free function mirroring the per-task `loki_tracing_label_value` functions, since this
/// module runs multiple task types rather than implementing [`Task`] itself.
///
pub fn loki_tracing_label_value() -> &'static str {
    "maccoys_processing"
}

/// Runs search space generation, identification and scoring in one process. Each worker slot
/// round-robins across the three stages, skipping a stage when its outgoing queue is full, so
/// idle capacity in one stage's slot share flows to a busier stage instead of sitting idle in
/// a per-stage pool. Standalone entrypoint used by the `maccoys pipeline processing` CLI
/// command; always talks to the HTTP queue broker and Redis storage, mirroring each individual
/// stage's own `run_standalone`.
///
/// # Arguments
/// * `local_work_dir` - Local work directory for the identification stage's Comet processes
/// * `config_file_path` - Path to the configuration file
///
pub async fn run_standalone(
    local_work_dir: PathBuf,
    config_file_path: PathBuf,
) -> Result<(), PipelineError> {
    let config = &fs::read_to_string(&config_file_path).map_err(|err| {
        PipelineError::FileReadError(config_file_path.to_string_lossy().to_string(), err)
    })?;

    let config: StandaloneProcessingConfiguration = toml::from_str(config).map_err(|err| {
        PipelineError::ConfigDeserializationError(
            config_file_path.to_string_lossy().to_string(),
            err,
        )
    })?;

    let search_space_generation_queue = Arc::new(
        HttpPipelineQueue::<SearchSpaceGenerationMessage>::new(&config.search_space_generation)
            .await?,
    );
    let identification_queue =
        Arc::new(HttpPipelineQueue::<IdentificationMessage>::new(&config.identification).await?);
    let scoring_queue = Arc::new(HttpPipelineQueue::<ScoringMessage>::new(&config.scoring).await?);
    let publication_queue =
        Arc::new(HttpPipelineQueue::<PublicationMessage>::new(&config.publication).await?);
    let error_queue = Arc::new(HttpPipelineQueue::<ErrorMessage>::new(&config.error).await?);

    let storage = Arc::new(RedisPipelineStorage::new(&config.storage).await?);

    let search_space_generation_config = Arc::new(config.search_space_generation.clone());
    let identification_config = Arc::new(config.identification.clone());

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
        .map(|slot_idx| {
            let ssg_task = SearchSpaceGenerationTask::new(
                search_space_generation_config.clone(),
                storage.clone(),
                search_space_generation_queue.clone(),
                identification_queue.clone(),
                publication_queue.clone(),
                error_queue.clone(),
                stop_flag.clone(),
            );
            let identification_task = IdentificationTask::new(
                local_work_dir.join(format!("comet_{}", slot_idx)),
                identification_config.clone(),
                storage.clone(),
                identification_queue.clone(),
                scoring_queue.clone(),
                publication_queue.clone(),
                error_queue.clone(),
                stop_flag.clone(),
            );
            let scoring_task = ScoringTask::new(
                scoring_queue.clone(),
                publication_queue.clone(),
                error_queue.clone(),
                stop_flag.clone(),
                config.scoring.threads,
                config.scoring.neighbors,
                config.scoring.effective_memory_safety_margin(),
            );

            tokio::spawn(run_slot(
                ssg_task,
                identification_task,
                scoring_task,
                search_space_generation_queue.clone(),
                identification_queue.clone(),
                scoring_queue.clone(),
                publication_queue.clone(),
                stop_flag.clone(),
            ))
        })
        .collect();

    for handle in handles {
        handle.await?;
    }

    Ok(())
}

/// Polls the search space generation stage of a round-robin worker slot: skips the stage (like
/// [`poll_stage_with_backpressure`]) if `outgoing` is full, and also skips it (rather than
/// blocking the shared slot) if `ssg_task`'s concurrency limiter has no free slot right now -
/// the message is left untouched in `incoming` either way, so a sibling stage sharing this slot
/// can still make progress. Kept specific to this one stage/concrete instantiation rather than
/// folded into the generic `poll_stage_with_backpressure`, since no other stage needs a
/// concurrency limiter.
///
async fn poll_search_space_generation_stage(
    outgoing: &HttpPipelineQueue<IdentificationMessage>,
    incoming: &HttpPipelineQueue<SearchSpaceGenerationMessage>,
    ssg_task: &mut SearchSpaceGenerationTask<
        RedisPipelineStorage,
        HttpPipelineQueue<SearchSpaceGenerationMessage>,
        HttpPipelineQueue<IdentificationMessage>,
        HttpPipelineQueue<PublicationMessage>,
        HttpPipelineQueue<ErrorMessage>,
    >,
) -> bool {
    match outgoing.is_full().await {
        Ok(true) => return false,
        Ok(false) => {}
        Err(e) => {
            error!("{}", e);
            return false;
        }
    }

    let lease = match ssg_task.try_acquire_slot().await {
        Some(lease) => lease,
        None => return false,
    };

    match incoming.pop().await {
        Ok(Some((message_id, message))) => {
            ssg_task.process_message(message_id, message).await;
            ssg_task.release_slot(lease).await;
            true
        }
        Ok(None) => {
            ssg_task.release_slot(lease).await;
            false
        }
        Err(e) => {
            error!("{}", e);
            ssg_task.release_slot(lease).await;
            false
        }
    }
}

/// One worker slot's loop: round-robins across search space generation, identification and
/// scoring, skipping a stage when its outgoing queue is full (via
/// [`poll_stage_with_backpressure`]) so this slot never blocks on a saturated downstream while
/// a sibling stage still has room to make progress. Only backs off once a full round finds no
/// actionable work at all.
///
#[allow(clippy::too_many_arguments)]
async fn run_slot(
    mut ssg_task: SearchSpaceGenerationTask<
        RedisPipelineStorage,
        HttpPipelineQueue<SearchSpaceGenerationMessage>,
        HttpPipelineQueue<IdentificationMessage>,
        HttpPipelineQueue<PublicationMessage>,
        HttpPipelineQueue<ErrorMessage>,
    >,
    mut identification_task: IdentificationTask<
        RedisPipelineStorage,
        HttpPipelineQueue<IdentificationMessage>,
        HttpPipelineQueue<ScoringMessage>,
        HttpPipelineQueue<PublicationMessage>,
        HttpPipelineQueue<ErrorMessage>,
    >,
    mut scoring_task: ScoringTask<
        HttpPipelineQueue<ScoringMessage>,
        HttpPipelineQueue<PublicationMessage>,
        HttpPipelineQueue<ErrorMessage>,
    >,
    search_space_generation_queue: Arc<HttpPipelineQueue<SearchSpaceGenerationMessage>>,
    identification_queue: Arc<HttpPipelineQueue<IdentificationMessage>>,
    scoring_queue: Arc<HttpPipelineQueue<ScoringMessage>>,
    publication_queue: Arc<HttpPipelineQueue<PublicationMessage>>,
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
                    poll_search_space_generation_stage(
                        identification_queue.as_ref(),
                        search_space_generation_queue.as_ref(),
                        &mut ssg_task,
                    )
                    .await
                }
                1 => {
                    poll_stage_with_backpressure(
                        scoring_queue.as_ref(),
                        identification_queue.as_ref(),
                        |id, msg| identification_task.process_message(id, msg),
                    )
                    .await
                }
                _ => {
                    poll_stage_with_backpressure(
                        publication_queue.as_ref(),
                        scoring_queue.as_ref(),
                        |id, msg| scoring_task.process_message(id, msg),
                    )
                    .await
                }
            };

            if did_work {
                backoff = BACKOFF_START;
                start_stage = (stage + 1) % STAGE_COUNT;
                continue 'idle_loop;
            }
        }

        // No stage had both a pending message and room downstream; back off before rescanning.
        backoff = backoff_sleep(backoff).await;
    }
}
