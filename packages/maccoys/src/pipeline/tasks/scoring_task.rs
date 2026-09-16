use std::{
    fs,
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
};

use local_outlier_probabilities::local_outlier_probabilities;
use metrics::counter;
use signal_hook::{consts::SIGINT, iterator::Signals};
use sysinfo::System;
use tracing::{error, info, warn};

use crate::pipeline::{
    configuration::StandaloneScoringConfiguration,
    errors::{pipeline_error::PipelineError, scoring_error::ScoringError},
    messages::{
        error_message::ErrorMessage, is_message::IsMessage,
        publication_message::PublicationMessage, scoring_message::ScoringMessage,
    },
    queue::{HttpPipelineQueue, PipelineQueue},
    storage::{PipelineStorage, RedisPipelineStorage},
    utils::{create_file_path_on_precursor_level, create_metrics_file_path_on_precursor_level},
};

use super::task::Task;

/// Prefix for the scoring counter
///
pub const COUNTER_PREFIX: &str = "maccoys_scorings";

/// Returns the metrics counter name for a search UUID. Free function mirroring
/// [`Task::get_counter_name`] for use where `ScoringTask`'s generics aren't in
/// scope to infer.
///
pub fn get_counter_name(uuid: &str) -> String {
    format!("{}_{}", COUNTER_PREFIX, uuid)
}

/// Returns the Loki tracing label value for ScoringTask. Free function mirroring
/// [`Task::loki_tracing_label_value`] for use where ScoringTask's generics aren't
/// in scope to infer.
///
pub fn loki_tracing_label_value() -> &'static str {
    COUNTER_PREFIX
}

/// Name for the new ions matched ratio column
///
pub const IONS_MATCHED_RATIO_COL_NAME: &str = "ions_matched_ratio";

/// Name for the new mass diff column
///
pub const MASS_ERROR_COL_NAME: &str = "mass_error";

/// Features to use for the scoring
///
pub const FEATURES: [&str; 3] = ["xcorr", IONS_MATCHED_RATIO_COL_NAME, MASS_ERROR_COL_NAME];

/// Name for the LoOP score
///
pub const LOOP_COL_NAME: &str = "loop_score";

/// Fixed, per-PSM part of [`estimate_loop_memory_bytes`]'s estimate: our own
/// `prepare_loop_features` cache (3 `f64`s/row), the `local_outlier_probabilities` crate's
/// per-point `pdist`/`plof`/output vectors (~3 `f64`s/row), and the KD-tree's per-leaf
/// index/bounds storage (~32 bytes/row, empirically). Small next to the neighbor-index term
/// below once `neighbors` is more than a handful, but included so the estimate stays honest
/// at small `neighbors`/`num_psms` too.
///
const LOOP_FIXED_BYTES_PER_PSM: usize = 3 * std::mem::size_of::<f64>() * 2 + 32;

/// Rough upper bound on the additional heap memory needed to LoOP-score `num_psms` PSMs with
/// `neighbors` neighbors each. Dominated by the `local_outlier_probabilities` crate's internal
/// neighbor-index buffers: it keeps one `num_psms * neighbors`-length `Vec<usize>` of neighbor
/// indices, and while filling it a same-sized transient buffer briefly coexists, so that pair
/// peaks at roughly `2 * num_psms * neighbors * size_of::<usize>()` bytes. Everything else
/// scales with `num_psms` alone; see [`LOOP_FIXED_BYTES_PER_PSM`].
///
fn estimate_loop_memory_bytes(num_psms: usize, neighbors: usize) -> usize {
    2 * num_psms * neighbors * std::mem::size_of::<usize>() + num_psms * LOOP_FIXED_BYTES_PER_PSM
}

/// Inverse of [`estimate_loop_memory_bytes`]: the largest `num_psms` that fits within
/// `budget_bytes` for the given `neighbors`.
///
fn max_psms_for_memory_budget(budget_bytes: usize, neighbors: usize) -> usize {
    let bytes_per_psm = 2 * neighbors * std::mem::size_of::<usize>() + LOOP_FIXED_BYTES_PER_PSM;
    budget_bytes / bytes_per_psm
}

/// Task to score the PSMs.
/// This tasks runs a Python interpreter in the background to utilize functionality from [scipy](https://scipy.org/) which is currently not available in Rust
///
/// # Generics
/// * `St` - Type of the storage
/// * `S` - Type of the scoring queue (this task's own queue)
/// * `P` - Type of the publication queue
/// * `E` - Type of the error queue
///
pub struct ScoringTask<St, S, P, E>
where
    St: PipelineStorage + Send + Sync + 'static,
    S: PipelineQueue<ScoringMessage> + Send + Sync + 'static,
    P: PipelineQueue<PublicationMessage> + Send + Sync + 'static,
    E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
{
    storage: Arc<St>,
    scoring_queue: Arc<S>,
    publish_queue: Arc<P>,
    error_queue: Arc<E>,
    stop_flag: Arc<AtomicBool>,
    thread_pool: rayon::ThreadPool,
    neighbors: usize,
    /// Fraction of currently-available system memory this task may use for a single LoOP
    /// call, already divided by the number of concurrently running scoring tasks (see
    /// [`crate::pipeline::configuration::ScoringTaskConfiguration::effective_memory_safety_margin`]).
    memory_safety_margin: f64,
}

impl<St, S, P, E> ScoringTask<St, S, P, E>
where
    St: PipelineStorage + Send + Sync + 'static,
    S: PipelineQueue<ScoringMessage> + Send + Sync + 'static,
    P: PipelineQueue<PublicationMessage> + Send + Sync + 'static,
    E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
{
    /// Creates a new scoring task
    ///
    /// # Arguments
    /// * `storage` - Storage to track per-search stage counts
    /// * `scoring_queue` - The queue to get the PSMs to score
    /// * `publish_queue` - The queue to push the scored PSMs to
    /// * `error_queue` - The queue to push the errors to
    /// * `stop_flag` - The flag to stop the task
    /// * `threads` - Number of threads to use for the LoOP scoring thread pool
    /// * `neighbors` - Number of neighbors to use for the LoOP scoring
    /// * `memory_safety_margin` - Fraction of currently-available system memory this task may
    ///   use for a single LoOP call; collections whose estimated memory need exceeds that
    ///   budget are truncated to their highest-`xcorr` rows before scoring (see
    ///   [`crate::peptide_spectrum_match::PeptideSpectrumMatchCollection::truncate_to_top_by_xcorr`]
    ///   and [`crate::pipeline::configuration::ScoringTaskConfiguration::effective_memory_safety_margin`])
    ///
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage: Arc<St>,
        scoring_queue: Arc<S>,
        publish_queue: Arc<P>,
        error_queue: Arc<E>,
        stop_flag: Arc<AtomicBool>,
        threads: usize,
        neighbors: usize,
        memory_safety_margin: f64,
    ) -> Self {
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();

        Self {
            storage,
            scoring_queue,
            publish_queue,
            error_queue,
            stop_flag,
            thread_pool,
            neighbors,
            memory_safety_margin,
        }
    }

}

/// Standalone entrypoint used by the `maccoys pipeline scoring` CLI command; always talks to
/// the HTTP queue broker, so it's implemented for the one concrete instantiation rather than
/// the generic one used by [`super::super::local_pipeline`].
///
impl
    ScoringTask<
        RedisPipelineStorage,
        HttpPipelineQueue<ScoringMessage>,
        HttpPipelineQueue<PublicationMessage>,
        HttpPipelineQueue<ErrorMessage>,
    >
{
    /// Run the scoring task by itself
    ///
    /// # Arguments
    /// * `work_dir` - Working directory
    /// * `config_file_path` - Path to the configuration file
    ///
    pub async fn run_standalone(config_file_path: PathBuf) -> Result<(), PipelineError> {
        let config = &fs::read_to_string(&config_file_path).map_err(|err| {
            PipelineError::FileReadError(config_file_path.to_string_lossy().to_string(), err)
        })?;

        let config: StandaloneScoringConfiguration = toml::from_str(config).map_err(|err| {
            PipelineError::ConfigDeserializationError(
                config_file_path.to_string_lossy().to_string(),
                err,
            )
        })?;

        let scoring_queue =
            Arc::new(HttpPipelineQueue::<ScoringMessage>::new(&config.scoring).await?);

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

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.scoring.num_tasks)
            .map(|_| {
                tokio::spawn(
                    ScoringTask::new(
                        storage.clone(),
                        scoring_queue.clone(),
                        publication_queue.clone(),
                        error_queue.clone(),
                        stop_flag.clone(),
                        config.scoring.threads,
                        config.scoring.neighbors,
                        config.scoring.effective_memory_safety_margin(),
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

impl<St, S, P, E> Task for ScoringTask<St, S, P, E>
where
    St: PipelineStorage + Send + Sync + 'static,
    S: PipelineQueue<ScoringMessage> + Send + Sync + 'static,
    P: PipelineQueue<PublicationMessage> + Send + Sync + 'static,
    E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
{
    type Message = ScoringMessage;
    type Queue = S;

    fn get_counter_prefix() -> &'static str {
        COUNTER_PREFIX
    }

    fn queue(&self) -> &Arc<S> {
        &self.scoring_queue
    }

    fn stop_flag(&self) -> &Arc<AtomicBool> {
        &self.stop_flag
    }

    async fn process_message(&mut self, message_id: String, mut message: ScoringMessage) {
        info!(
            "[scoring] Got message {}/{}/{} with {} PSM(s)",
            message.uuid(),
            message.ms_run_name(),
            message.spectrum_id(),
            message.psms().len()
        );

        let metrics_counter_name = Self::get_counter_name(message.uuid());
        let search_uuid = message.uuid().clone();

        let relative_psms_path = create_file_path_on_precursor_level(
            message.uuid(),
            message.ms_run_name(),
            message.spectrum_id(),
            message.precursor(),
            "parquet",
        );

        if message.psms().is_empty() {
            warn!(
                "[{}] empty psms {} / {} / {} / {:?} ",
                &message_id,
                message.uuid(),
                message.ms_run_name(),
                message.spectrum_id(),
                message.precursor()
            );

            let publication_message = match message
                .into_final_publication_message(relative_psms_path)
                .await
            {
                Ok(publication_message) => publication_message,
                Err((message, err)) => {
                    let error_message = message.to_error_message(
                        ScoringError::IntoPublicationMessageError(Box::new(err)).into(),
                    );
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                    return;
                }
            };

            Self::enqueue_message(publication_message, self.publish_queue.as_ref()).await;
            return;
        }

        let mut system = System::new();
        system.refresh_memory();
        let available_bytes = system.available_memory() as usize;
        let budget_bytes = (available_bytes as f64 * self.memory_safety_margin) as usize;
        let estimated_bytes = estimate_loop_memory_bytes(message.psms().len(), self.neighbors);

        if estimated_bytes > budget_bytes {
            let original_len = message.psms().len();
            // Never truncate below what LoOP itself needs (`neighbors` neighbors + the point
            // itself), even if the memory budget is so tight the math above would ask for less.
            let max_len =
                max_psms_for_memory_budget(budget_bytes, self.neighbors).max(self.neighbors + 1);
            message.psms_mut().truncate_to_top_by_xcorr(max_len);
            warn!(
                "[{}] {} / {} / {} / {:?} had {} PSMs, estimated at {} bytes to LoOP-score with \
                 {} neighbors, exceeding the memory budget of {} bytes ({:.0}% of {} bytes \
                 available); truncated to {} highest-xcorr PSMs",
                &message_id,
                message.uuid(),
                message.ms_run_name(),
                message.spectrum_id(),
                message.precursor(),
                original_len,
                estimated_bytes,
                self.neighbors,
                budget_bytes,
                self.memory_safety_margin * 100.0,
                available_bytes,
                message.psms().len()
            );
        }

        message.psms_mut().prepare_loop_features();

        let now = std::time::Instant::now();

        let loop_score = match self
            .thread_pool
            .install(|| local_outlier_probabilities(message.psms(), self.neighbors, 3, None))
        {
            Ok(loop_score) => loop_score,
            Err(err) => {
                let error_message = message.to_error_message(ScoringError::LoOPError(err).into());
                error!("{}", &error_message);
                Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                return;
            }
        };

        message.psms_mut().set_loop_score_col(loop_score);

        let elapsed = now.elapsed();

        info!(
            "[scoring] Took {:.4}s to process message {}/{}/{} with {} PSM(s)",
            elapsed.as_secs_f32(),
            message.uuid(),
            message.ms_run_name(),
            message.spectrum_id(),
            message.psms().len()
        );

        let metrics_path = create_metrics_file_path_on_precursor_level(
            message.uuid(),
            message.ms_run_name(),
            message.spectrum_id(),
            message.precursor(),
            "scoring.time",
        );

        let metrics_message = message
            .into_publication_message(metrics_path, elapsed.as_millis().to_string().into_bytes());

        let publication_message = match message
            .into_final_publication_message(relative_psms_path)
            .await
        {
            Ok(publication_message) => publication_message,
            Err((message, err)) => {
                let error_message = message.to_error_message(
                    ScoringError::IntoPublicationMessageError(Box::new(err)).into(),
                );
                error!("{}", &error_message);
                Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                return;
            }
        };

        Self::enqueue_message(publication_message, self.publish_queue.as_ref()).await;
        Self::ack_message(&message_id, self.scoring_queue.as_ref()).await;
        counter!(metrics_counter_name.clone()).increment(1);
        if let Err(e) = self
            .storage
            .increase_stage_count(COUNTER_PREFIX, &search_uuid)
            .await
        {
            error!("Error increasing scoring stage count: {:?}", e);
        }

        // send some metrics

        Self::enqueue_message(metrics_message, self.publish_queue.as_ref()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_loop_memory_bytes_scales_with_num_psms_and_neighbors() {
        let small = estimate_loop_memory_bytes(1_000, 20);
        let more_psms = estimate_loop_memory_bytes(2_000, 20);
        let more_neighbors = estimate_loop_memory_bytes(1_000, 40);

        assert!(more_psms > small);
        assert!(more_neighbors > small);
        // Doubling num_psms should roughly double the estimate (dominant term is linear in
        // num_psms), give or take the fixed per-psm overhead.
        assert!(more_psms > small * 19 / 10);
    }

    #[test]
    fn test_max_psms_for_memory_budget_is_inverse_of_estimate() {
        let neighbors = 20;
        let num_psms = 500_000;
        let budget = estimate_loop_memory_bytes(num_psms, neighbors);

        // The budget that exactly covers `num_psms` PSMs should allow at least that many.
        assert!(max_psms_for_memory_budget(budget, neighbors) >= num_psms);
        // Half that budget shouldn't allow the full amount back.
        assert!(max_psms_for_memory_budget(budget / 2, neighbors) < num_psms);
    }

    #[test]
    fn test_max_psms_for_memory_budget_zero_budget_is_zero() {
        assert_eq!(max_psms_for_memory_budget(0, 20), 0);
    }
}
