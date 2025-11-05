use std::{
    borrow::Cow,
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use metrics::counter;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tokio::fs::create_dir_all;
use tracing::{error, info};
use xcorrrs::{
    configuration::FinalizedConfiguration as FinalizedXcorrConfiguration, fast_xcorr::FastXcorr,
};

use crate::{
    peptide_spectrum_match::{PeptideSpectrumMatch, PeptideSpectrumMatchCollection},
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
        utils::{create_file_path_on_precursor_level, create_metrics_file_path_on_precursor_level},
    },
    precursor::Precursor,
};

use super::task::Task;

/// Prefix for the identification counter
///
pub const COUNTER_PREFIX: &str = "maccoys_identifications";

/// Task to identify the spectra
///
pub struct IdentificationTask;

impl IdentificationTask {
    /// Starts the task
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
    pub async fn start<S, I, C, P, E>(
        local_work_dir: PathBuf,
        config: Arc<IdentificationTaskConfiguration>,
        storage: Arc<S>,
        identification_queue: Arc<I>,
        scoring_queue: Arc<C>,
        publication_queue: Arc<P>,
        error_queue: Arc<E>,
        stop_flag: Arc<AtomicBool>,
    ) where
        S: PipelineStorage + Send + Sync + 'static,
        I: PipelineQueue<IdentificationMessage> + Send + Sync + 'static,
        C: PipelineQueue<ScoringMessage> + Send + Sync + 'static,
        P: PipelineQueue<PublicationMessage> + Send + Sync + 'static,
        E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
    {
        match create_dir_all(&local_work_dir).await {
            Ok(_) => (),
            Err(e) => {
                error!("Error creating local work directory: {:?}", e);
                return;
            }
        }
        let mut last_search_uuid = String::new();
        let mut current_search_params = SearchParameters::new();
        let mut metrics_counter_name = COUNTER_PREFIX.to_string();

        'message_loop: loop {
            if stop_flag.load(Ordering::Relaxed) {
                break;
            }
            let (message_id, mut message) = match identification_queue.pop().await {
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

            info!(
                "[identification] Got message {}/{}/{} with {} candidate peptide(s)",
                message.uuid(),
                message.ms_run_name(),
                message.spectrum_id(),
                message.peptides().len()
            );

            if last_search_uuid != *message.uuid() {
                current_search_params = match storage.get_search_parameters(message.uuid()).await {
                    Ok(Some(params)) => params,
                    Ok(None) => {
                        let error_message = message.to_error_message(
                            IdentificationError::SearchParametersNotFoundError().into(),
                        );
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                    Err(e) => {
                        let error_message = message.to_error_message(
                            IdentificationError::GetSearchParametersError(e).into(),
                        );
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                };

                last_search_uuid = message.uuid().clone();
                metrics_counter_name = Self::get_counter_name(message.uuid());
            }

            if current_search_params.keep_fasta_files {
                let relative_fasta_path = create_file_path_on_precursor_level(
                    message.uuid(),
                    message.ms_run_name(),
                    message.spectrum_id(),
                    message.precursor(),
                    "fasta",
                );
                let fasta_publication_message = message.into_publication_message(
                    relative_fasta_path,
                    message.peptides().join("\n").into_bytes(),
                );
                Self::enqueue_message(fasta_publication_message, publication_queue.as_ref()).await;
            }

            let now = std::time::Instant::now();

            let peptides = message.take_peptides();
            let peptides_len = peptides.len();

            let psms = match run_xcorr_search(
                &FinalizedXcorrConfiguration::from(current_search_params.xcorr.clone()),
                message.spectrum_id(),
                (message.mz_list(), message.intensity_list()),
                message.precursor(),
                peptides,
                config.threads,
                None,
            ) {
                Ok(psms) => psms,
                Err(e) => {
                    let error_message = message.to_error_message(e.into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, error_queue.as_ref()).await;
                    continue 'message_loop;
                }
            };

            let elapsed = now.elapsed();
            let psms_len = psms.len();

            let scoring_message = match message.into_scoring_message(psms) {
                Ok(msg) => msg,
                Err(e) => {
                    let error_message = message.to_error_message(e.into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, error_queue.as_ref()).await;
                    continue 'message_loop;
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

            Self::enqueue_message(scoring_message, scoring_queue.as_ref()).await;
            Self::ack_message(&message_id, identification_queue.as_ref()).await;
            counter!(metrics_counter_name.clone()).increment(1);

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
                publication_queue.as_ref(),
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
                publication_queue.as_ref(),
            )
            .await;
        }
    }

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
                tokio::spawn(IdentificationTask::start(
                    comet_tmp_dir,
                    identification_config.clone(),
                    storage.clone(),
                    identification_queue.clone(),
                    scoring_queue.clone(),
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

impl Task for IdentificationTask {
    fn get_counter_prefix() -> &'static str {
        COUNTER_PREFIX
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
    peptides: Vec<String>,
    threads: usize,
    score_threshold: Option<f64>,
) -> Result<PeptideSpectrumMatchCollection, IdentificationError> {
    let score_threshold = score_threshold.unwrap_or(f64::NEG_INFINITY);

    let psm_collection: Arc<Mutex<PeptideSpectrumMatchCollection>> = Arc::new(Mutex::new(
        PeptideSpectrumMatchCollection::with_capacity(peptides.len()),
    ));

    let xcorrer = FastXcorr::new(config, experimental_spectrum, precursor.charge() as usize)
        .map_err(IdentificationError::from)?;

    let errors: Arc<Mutex<Vec<IdentificationError>>> = Arc::new(Mutex::new(Vec::new()));

    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .map_err(IdentificationError::CreateIdentificationThreadPoolError)?;

    thread_pool.install(|| {
        peptides
            .into_par_iter()
            .for_each(|peptide| match xcorrer.xcorr_peptide(&peptide) {
                Ok(scoring_result) => {
                    if scoring_result.score >= score_threshold {
                        let psm = PeptideSpectrumMatch::new(
                            Cow::Owned(scan_id.to_string()),
                            Cow::Owned(peptide),
                            Cow::Owned(precursor.clone()),
                            scoring_result,
                        );
                        let mut guard = psm_collection.lock().unwrap();
                        guard.push(psm);
                    }
                }
                Err(e) => {
                    let mut error_guard = errors.lock().unwrap();
                    error_guard.push(IdentificationError::from(e));
                }
            })
    });

    let psm_collect = Arc::try_unwrap(psm_collection)
        .map_err(|_| {
            IdentificationError::ArcIntoError("PeptideSpectrumMatchCollection".to_string())
        })?
        .into_inner()
        .map_err(|_| {
            IdentificationError::MutexLockError("PeptideSpectrumMatchCollection".to_string())
        })?;

    let errors = Arc::try_unwrap(errors)
        .map_err(|_| IdentificationError::ArcIntoError("Errors".to_string()))?
        .into_inner()
        .map_err(|_| IdentificationError::MutexLockError("Errors".to_string()))?;

    if errors.is_empty() {
        Ok(psm_collect)
    } else {
        error!("Errors occurred during XCorr search:");
        for e in errors.iter() {
            error!("{}", e);
        }
        Err(errors.into_iter().next().unwrap())
    }
}
