use std::{
    fs,
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc, Mutex},
};

use metrics::counter;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tokio::fs::create_dir_all;
use tracing::{error, info};
use xcorrrs::{
    configuration::FinalizedConfiguration as FinalizedXcorrConfiguration, fast_xcorr::FastXcorr,
};

use crate::{
    peptide_spectrum_match::PeptideSpectrumMatchCollection,
    pipeline::{
        configuration::{
            IdentificationTaskConfiguration, SearchParameters,
            StandaloneIdentificationConfiguration,
        },
        errors::{identification_error::IdentificationError, pipeline_error::PipelineError},
        messages::{
            error_message::ErrorMessage, identification_message::IdentificationMessage,
            is_message::IsMessage, publication_message::PublicationMessage,
            scoring_message::ScoringMessage,
        },
        queue::{HttpPipelineQueue, PipelineQueue},
        storage::{PipelineStorage, RedisPipelineStorage},
        utils::create_metrics_file_path_on_precursor_level,
    },
    precursor::Precursor,
    search_space::search_space_generator::InMemorySearchSpace,
};

use super::task::Task;

/// Prefix for the identification counter
///
pub const COUNTER_PREFIX: &str = "maccoys_identifications";

/// Returns the metrics counter name for a search UUID. Free function mirroring
/// [`Task::get_counter_name`] for use where `IdentificationTask`'s generics aren't
/// in scope to infer.
///
pub fn get_counter_name(uuid: &str) -> String {
    format!("{}_{}", COUNTER_PREFIX, uuid)
}

/// Returns the Loki tracing label value for IdentificationTask. Free function mirroring
/// [`Task::loki_tracing_label_value`] for use where IdentificationTask's generics aren't
/// in scope to infer.
///
pub fn loki_tracing_label_value() -> &'static str {
    COUNTER_PREFIX
}

/// Task to identify the spectra
///
/// # Generics
/// * `S` - Type of the storage
/// * `I` - Type of the identification queue (this task's own queue)
/// * `C` - Type of the scoring queue
/// * `P` - Type of the publication queue
/// * `E` - Type of the error queue
///
pub struct IdentificationTask<S, I, C, P, E>
where
    S: PipelineStorage + Send + Sync + 'static,
    I: PipelineQueue<IdentificationMessage> + Send + Sync + 'static,
    C: PipelineQueue<ScoringMessage> + Send + Sync + 'static,
    P: PipelineQueue<PublicationMessage> + Send + Sync + 'static,
    E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
{
    local_work_dir: PathBuf,
    config: Arc<IdentificationTaskConfiguration>,
    storage: Arc<S>,
    identification_queue: Arc<I>,
    scoring_queue: Arc<C>,
    publication_queue: Arc<P>,
    error_queue: Arc<E>,
    stop_flag: Arc<AtomicBool>,
    /// UUID of the search the cached parameters below belong to, to avoid refetching
    /// them for every message of the same search.
    last_search_uuid: String,
    current_search_params: SearchParameters,
    metrics_counter_name: String,
}

impl<S, I, C, P, E> IdentificationTask<S, I, C, P, E>
where
    S: PipelineStorage + Send + Sync + 'static,
    I: PipelineQueue<IdentificationMessage> + Send + Sync + 'static,
    C: PipelineQueue<ScoringMessage> + Send + Sync + 'static,
    P: PipelineQueue<PublicationMessage> + Send + Sync + 'static,
    E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
{
    /// Creates a new identification task
    ///
    /// # Arguments
    /// * `local_work_dir` -  Local work directory
    /// * `config` - Configuration for the Comet search task
    /// * `storage` - Storage to access params and PTMs
    /// * `identification_queue` - Incomming queue for the identification task
    /// * `scoring_queue` - Scoring queue
    /// * `publication_queue` - Publication queue for persisting intermediate results if needed
    /// * `error_queue` - Queue for the error task
    /// * `stop_flag` - Flag to indicate to stop once the Comet search queue is empty
    ///
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        local_work_dir: PathBuf,
        config: Arc<IdentificationTaskConfiguration>,
        storage: Arc<S>,
        identification_queue: Arc<I>,
        scoring_queue: Arc<C>,
        publication_queue: Arc<P>,
        error_queue: Arc<E>,
        stop_flag: Arc<AtomicBool>,
    ) -> Self {
        Self {
            local_work_dir,
            config,
            storage,
            identification_queue,
            scoring_queue,
            publication_queue,
            error_queue,
            stop_flag,
            last_search_uuid: String::new(),
            current_search_params: SearchParameters::new(),
            metrics_counter_name: COUNTER_PREFIX.to_string(),
        }
    }

}

/// Standalone entrypoint used by the `maccoys pipeline identification` CLI command; always
/// talks to the HTTP queue broker and Redis storage, so it's implemented for the one concrete
/// instantiation rather than the generic one used by [`super::super::local_pipeline`].
///
impl
    IdentificationTask<
        RedisPipelineStorage,
        HttpPipelineQueue<IdentificationMessage>,
        HttpPipelineQueue<ScoringMessage>,
        HttpPipelineQueue<PublicationMessage>,
        HttpPipelineQueue<ErrorMessage>,
    >
{
    /// Run the identification task by itself
    ///
    /// # Arguments
    /// * `work_dir` - Working directory
    /// * `config_file_path` - Path to the configuration file
    ///
    pub async fn run_standalone(
        local_work_dir: PathBuf,
        config_file_path: PathBuf,
    ) -> Result<(), PipelineError> {
        let config = &fs::read_to_string(&config_file_path).map_err(|err| {
            PipelineError::FileReadError(config_file_path.to_string_lossy().to_string(), err)
        })?;

        let config: StandaloneIdentificationConfiguration =
            toml::from_str(config).map_err(|err| {
                PipelineError::ConfigDeserializationError(
                    config_file_path.to_string_lossy().to_string(),
                    err,
                )
            })?;

        let identification_queue = Arc::new(
            HttpPipelineQueue::<IdentificationMessage>::new(&config.identification).await?,
        );

        let scoring_queue =
            Arc::new(HttpPipelineQueue::<ScoringMessage>::new(&config.scoring).await?);

        let publication_queue =
            Arc::new(HttpPipelineQueue::<PublicationMessage>::new(&config.publication).await?);

        let error_queue = Arc::new(HttpPipelineQueue::<ErrorMessage>::new(&config.error).await?);

        let storage = Arc::new(RedisPipelineStorage::new(&config.storage).await?);

        let identification_config = Arc::new(config.identification.clone());

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.identification.num_tasks)
            .map(|comet_proc_idx| {
                let comet_tmp_dir = local_work_dir.join(format!("comet_{}", comet_proc_idx));
                tokio::spawn(
                    IdentificationTask::new(
                        comet_tmp_dir,
                        identification_config.clone(),
                        storage.clone(),
                        identification_queue.clone(),
                        scoring_queue.clone(),
                        publication_queue.clone(),
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

impl<S, I, C, P, E> Task for IdentificationTask<S, I, C, P, E>
where
    S: PipelineStorage + Send + Sync + 'static,
    I: PipelineQueue<IdentificationMessage> + Send + Sync + 'static,
    C: PipelineQueue<ScoringMessage> + Send + Sync + 'static,
    P: PipelineQueue<PublicationMessage> + Send + Sync + 'static,
    E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
{
    type Message = IdentificationMessage;
    type Queue = I;

    fn get_counter_prefix() -> &'static str {
        COUNTER_PREFIX
    }

    fn queue(&self) -> &Arc<I> {
        &self.identification_queue
    }

    fn stop_flag(&self) -> &Arc<AtomicBool> {
        &self.stop_flag
    }

    async fn process_message(&mut self, message_id: String, mut message: IdentificationMessage) {
        if let Err(e) = create_dir_all(&self.local_work_dir).await {
            error!("Error creating local work directory: {:?}", e);
            return;
        }

        info!(
            "[identification] Got message {}/{}/{} with {} candidate peptide(s)",
            message.uuid(),
            message.ms_run_name(),
            message.spectrum_id(),
            message.search_space().len()
        );

        if self.last_search_uuid != *message.uuid() {
            self.current_search_params = match self.storage.get_search_parameters(message.uuid()).await {
                Ok(Some(params)) => params,
                Ok(None) => {
                    let error_message = message
                        .to_error_message(IdentificationError::SearchParametersNotFoundError().into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                    return;
                }
                Err(e) => {
                    let error_message =
                        message.to_error_message(IdentificationError::GetSearchParametersError(e).into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                    return;
                }
            };

            self.last_search_uuid = message.uuid().clone();
            self.metrics_counter_name = Self::get_counter_name(message.uuid());
        }

        let now = std::time::Instant::now();

        let search_space = message.take_search_space();
        let peptides_len = search_space.len();

        let psms = match run_xcorr_search(
            &FinalizedXcorrConfiguration::from(self.current_search_params.xcorr.clone()),
            message.spectrum_id(),
            (message.mz_list(), message.intensity_list()),
            message.precursor(),
            search_space,
            self.config.threads,
            Some(self.current_search_params.score_threshold),
        ) {
            Ok(psms) => psms,
            Err(e) => {
                let error_message = message.to_error_message(e.into());
                error!("{}", &error_message);
                Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                return;
            }
        };

        let elapsed = now.elapsed();
        let psms_len = psms.len();

        let scoring_message = match message.into_scoring_message(psms) {
            Ok(msg) => msg,
            Err(e) => {
                let error_message = message.to_error_message(e.into());
                error!("{}", &error_message);
                Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                return;
            }
        };

        info!(
            "[identification] Took {:.4}s to process message {}/{}/{} with {} candidate peptide(s)",
            elapsed.as_secs_f32(),
            message.uuid(),
            message.ms_run_name(),
            message.spectrum_id(),
            peptides_len
        );

        Self::enqueue_message(scoring_message, self.scoring_queue.as_ref()).await;
        Self::ack_message(&message_id, self.identification_queue.as_ref()).await;
        counter!(self.metrics_counter_name.clone()).increment(1);
        if let Err(e) = self.storage.increase_stage_count(COUNTER_PREFIX, message.uuid()).await {
            error!("Error increasing identification stage count: {:?}", e);
        }

        // sending metrics

        let metrics_path = create_metrics_file_path_on_precursor_level(
            message.uuid(),
            message.ms_run_name(),
            message.spectrum_id(),
            message.precursor(),
            "identification.time",
        );

        Self::enqueue_message(
            message.into_publication_message(
                metrics_path,
                elapsed.as_millis().to_string().into_bytes(),
            ),
            self.publication_queue.as_ref(),
        )
        .await;

        let metrics_path = create_metrics_file_path_on_precursor_level(
            message.uuid(),
            message.ms_run_name(),
            message.spectrum_id(),
            message.precursor(),
            "identification.len",
        );

        Self::enqueue_message(
            message.into_publication_message(metrics_path, psms_len.to_string().into_bytes()),
            self.publication_queue.as_ref(),
        )
        .await;
    }
}

/// Runs Xcorr search with the given parameters.
///
/// # Arguments
/// * `config` - Finalized Xcorr configuration
/// * `scan_id` - Scan ID
/// * `experimental_spectrum` - Tuple of m/z and intensity arrays
/// * `charge` - Charge state
/// * `peptides` - Peptides to search
/// * `threads` - Number of threads to use
/// * `score_threshold` - xcorr needs to be equals or above this threshold to be reported.
///
pub fn run_xcorr_search(
    config: &FinalizedXcorrConfiguration,
    scan_id: &str,
    experimental_spectrum: (&ndarray::Array1<f64>, &ndarray::Array1<f64>),
    precursor: &Precursor,
    search_space: InMemorySearchSpace,
    threads: usize,
    score_threshold: Option<f64>,
) -> Result<PeptideSpectrumMatchCollection, IdentificationError> {
    let score_threshold = score_threshold.unwrap_or(f64::NEG_INFINITY);
    let peptides = search_space.into_single_peptide_arena();

    let mut psm_collection = PeptideSpectrumMatchCollection::with_capacity(
        scan_id.to_string(),
        precursor.clone(),
        peptides,
    );

    let xcorrer = FastXcorr::new(config, experimental_spectrum, precursor.charge() as usize)
        .map_err(IdentificationError::from)?;

    let errors: Mutex<Vec<IdentificationError>> = Mutex::new(Vec::new());

    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .map_err(IdentificationError::CreateIdentificationThreadPoolError)?;

    let (
        peptides_iter,
        xcorr_col,
        ions_total_col,
        ions_matched_col,
        theoretical_mass_col,
        second_theoretical_mass_col,
    ) = psm_collection.peptides_and_columns_mut();

    // Materialized once
    let peptide_refs: Vec<&str> = peptides_iter.collect();

    // Every iteration writes straight into its own column slot
    thread_pool.install(|| {
        (
            peptide_refs,
            xcorr_col,
            ions_total_col,
            ions_matched_col,
            theoretical_mass_col,
            second_theoretical_mass_col,
        )
            .into_par_iter()
            .for_each(
                |(
                    peptide,
                    xcorr,
                    ions_total,
                    ions_matched,
                    theoretical_mass,
                    second_theoretical_mass,
                )| {
                    match xcorrer.xcorr_peptide(peptide, None) {
                        Ok(scoring_result) if scoring_result.score >= score_threshold => {
                            *xcorr = scoring_result.score;
                            *ions_total = scoring_result.ions_total as u64;
                            *ions_matched = scoring_result.ions_matched as u64;
                            *theoretical_mass = scoring_result.min_theoretical_mass;
                            *second_theoretical_mass = if scoring_result.min_theoretical_mass
                                == scoring_result.max_theoretical_mass
                            {
                                f64::NAN
                            } else {
                                scoring_result.max_theoretical_mass
                            };
                        }
                        Ok(_) => {}
                        Err(e) => {
                            errors.lock().unwrap().push(IdentificationError::from(e));
                        }
                    }
                },
            );
    });

    let errors = errors
        .into_inner()
        .map_err(|_| IdentificationError::MutexLockError("Errors".to_string()))?;

    if errors.is_empty() {
        psm_collection.retain_scored();
        Ok(psm_collection)
    } else {
        error!("Errors occurred during XCorr search:");
        for e in errors.iter() {
            error!("{}", e);
        }
        Err(errors.into_iter().next().unwrap())
    }
}
