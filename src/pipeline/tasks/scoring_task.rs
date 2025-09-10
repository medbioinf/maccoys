use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use local_outlier_probabilities::local_outlier_probabilities;
use metrics::counter;
use polars::prelude::*;
use signal_hook::{consts::SIGINT, iterator::Signals};
use tracing::{debug, error, info, warn};

use crate::pipeline::{
    configuration::StandaloneScoringConfiguration,
    errors::{pipeline_error::PipelineError, scoring_error::ScoringError},
    messages::{
        error_message::ErrorMessage,
        is_message::IsMessage,
        publication_message::PublicationMessage,
        scoring_message::{IntoPublicationMessageError, ScoringMessage},
    },
    queue::{PipelineQueue, RedisPipelineQueue},
    utils::{create_file_path_on_precursor_level, create_metrics_file_path_on_precursor_level},
};

use super::task::Task;

/// Prefix for the scoring counter
///
pub const COUNTER_PREFIX: &str = "maccoys_scorings";

/// Number of neighbors to use for the scoring
///
pub const N_NEIGHBORS: usize = 1000;

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

/// Task to score the PSMs.
/// This tasks runs a Python interpreter in the background to utilize functionality from [scipy](https://scipy.org/) which is currently not available in Rust
///
pub struct ScoringTask;

impl ScoringTask {
    /// Starts the task
    ///
    /// # Arguments
    /// * `scoring_queue` - The queue to get the PSMs to score
    /// * `publish_queue` - The queue to push the scored PSMs to
    /// * `stop_flag` - The flag to stop the task
    ///
    pub async fn start<S, P, E>(
        scoring_queue: Arc<S>,
        publish_queue: Arc<P>,
        error_queue: Arc<E>,
        stop_flag: Arc<AtomicBool>,
    ) where
        S: PipelineQueue<ScoringMessage> + Send + Sync + 'static,
        P: PipelineQueue<PublicationMessage> + Send + Sync + 'static,
        E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
    {
        'message_loop: loop {
            if stop_flag.load(Ordering::Relaxed) {
                break;
            }
            let (message_id, mut message) = match scoring_queue.pop().await {
                Ok(Some(message)) => {
                    debug!("[{}] recv", &message.0);
                    message
                }
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
                "[scoring] Got message {}/{}/{} with {} PSM(s)",
                message.uuid(),
                message.ms_run_name(),
                message.spectrum_id(),
                message.psms().height()
            );

            let metrics_counter_name = Self::get_counter_name(message.uuid());

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
                let publication_message = message
                    .into_final_publication_message(relative_psms_path)
                    .unwrap();
                Self::enqueue_message(publication_message, publish_queue.as_ref()).await;
                continue 'message_loop;
            }

            let now = std::time::Instant::now();

            match calc_features(message.psms_mut()) {
                Ok(_) => {}
                Err(e) => {
                    let error_message = message.to_error_message(e.into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, error_queue.as_ref()).await;
                    continue 'message_loop;
                }
            }

            let feature_df = match message.psms().select(FEATURES) {
                Ok(feature_df) => feature_df,
                Err(e) => {
                    let error_message =
                        message.to_error_message(ScoringError::PolarsError(e).into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, error_queue.as_ref()).await;
                    continue 'message_loop;
                }
            };

            let feature_array = match feature_df.to_ndarray::<Float64Type>(IndexOrder::C) {
                Ok(feature_array) => feature_array,
                Err(e) => {
                    let error_message =
                        message.to_error_message(ScoringError::PolarsError(e).into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, error_queue.as_ref()).await;
                    continue 'message_loop;
                }
            };

            let loop_score = match local_outlier_probabilities(feature_array, N_NEIGHBORS, 3, None)
            {
                Ok(loop_score) => loop_score,
                Err(err) => {
                    let error_message =
                        message.to_error_message(ScoringError::LoOPError(err).into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, error_queue.as_ref()).await;
                    continue 'message_loop;
                }
            };

            let loop_score = loop_score
                .into_iter() // convert outlier probability to inlier probability
                .collect::<Vec<f64>>();

            match message
                .psms_mut()
                .with_column(Series::new(LOOP_COL_NAME.into(), loop_score))
            {
                Ok(_) => {}
                Err(e) => {
                    let error_message =
                        message.to_error_message(ScoringError::PolarsError(e).into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, error_queue.as_ref()).await;
                    continue 'message_loop;
                }
            }

            let elapsed = now.elapsed();

            info!(
                "[scoring] Took {:.4}s to process message {}/{}/{} with {} PSM(s)",
                elapsed.as_secs_f32(),
                message.uuid(),
                message.ms_run_name(),
                message.spectrum_id(),
                message.psms().height()
            );

            let metrics_path = create_metrics_file_path_on_precursor_level(
                message.uuid(),
                message.ms_run_name(),
                message.spectrum_id(),
                message.precursor(),
                "scoring.time",
            );

            let metrics_message = message.into_publication_message(
                metrics_path,
                elapsed.as_millis().to_string().into_bytes(),
            );

            let publication_message = match message
                .into_final_publication_message(relative_psms_path)
            {
                Ok(publication_message) => publication_message,
                Err(e) => {
                    let error_message = match *e {
                        IntoPublicationMessageError::CsvWriteError(e, message) => message
                            .to_error_message(ScoringError::IntoPublicationMessageError(e).into()),
                    };
                    error!("{},", &error_message);
                    Self::enqueue_message(error_message, error_queue.as_ref()).await;
                    continue 'message_loop;
                }
            };

            Self::enqueue_message(publication_message, publish_queue.as_ref()).await;
            Self::ack_message(&message_id, scoring_queue.as_ref()).await;
            counter!(metrics_counter_name.clone()).increment(1);

            // send some metrics

            Self::enqueue_message(metrics_message, publish_queue.as_ref()).await;
        }
    }

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
            Arc::new(RedisPipelineQueue::<ScoringMessage>::new(&config.scoring).await?);

        let publication_queue =
            Arc::new(RedisPipelineQueue::<PublicationMessage>::new(&config.publication).await?);

        let error_queue = Arc::new(RedisPipelineQueue::<ErrorMessage>::new(&config.error).await?);

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

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.scoring.num_tasks)
            .map(|_| {
                tokio::spawn(ScoringTask::start(
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

impl Task for ScoringTask {
    fn get_counter_prefix() -> &'static str {
        COUNTER_PREFIX
    }
}

/// Calculates features for the scoring
///
/// # Arguments
/// * `psms` - The PSMs to calculate the features for
///
fn calc_features(psms: &mut DataFrame) -> Result<(), ScoringError> {
    let ions_matches = psms.column("ions_matched")?.cast(&DataType::Float64)?;
    let ions_total = psms.column("ions_total")?.cast(&DataType::Float64)?;
    let ions_matched_ratio =
        (ions_matches / ions_total)?.with_name(IONS_MATCHED_RATIO_COL_NAME.into());

    let experimental_mass = psms.column("experimental_mass")?;
    let theoretical_mass = psms.column("theoretical_mass")?;
    let mass_error = abs((experimental_mass - theoretical_mass)?.as_materialized_series())?
        .with_name(MASS_ERROR_COL_NAME.into());

    psms.with_column(ions_matched_ratio)?;
    psms.with_column(mass_error)?;

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::constants::COMET_SEPARATOR;

    #[test]
    fn test_calculate_features() {
        let psm_file_path = PathBuf::from("test_files/well_separated_psms.tsv");

        let mut psms = CsvReadOptions::default()
            .with_has_header(true)
            .with_parse_options(
                CsvParseOptions::default().with_separator(COMET_SEPARATOR.as_bytes()[0]),
            )
            .try_into_reader_with_file_path(Some(psm_file_path))
            .unwrap()
            .finish()
            .unwrap();

        calc_features(&mut psms).unwrap();

        assert!(psms
            .get_column_names()
            .contains(&&PlSmallStr::from(FEATURES[1])));
        assert!(psms
            .get_column_names()
            .contains(&&PlSmallStr::from(FEATURES[2])));
    }
}
