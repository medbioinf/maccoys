use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use metrics::counter;
use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use signal_hook::{consts::SIGINT, iterator::Signals};
use tokio::sync::mpsc::error::TryRecvError;
use tracing::{debug, error, info, trace, warn};

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
    utils::create_file_path_on_precursor_level,
};

use super::task::Task;

/// Prefix for the scoring counter
///
pub const COUNTER_PREFIX: &str = "maccoys_scorings";

/// Number of neighbors to use for the scoring
///
pub const N_NEIGHBORS: i64 = 100;

/// Name for the new ions matched ratio column
///
pub const IONS_MATCHED_RATIO_COL_NAME: &str = "ions_matched_ratio";

/// Name for the new mass diff column
///
pub const MASS_DIFF_COL_NAME: &str = "mass_diff";

/// Features to use for the scoring
///
pub const FEATURES: [&str; 3] = ["xcorr", IONS_MATCHED_RATIO_COL_NAME, MASS_DIFF_COL_NAME];

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
        let (to_python, mut from_rust) = tokio::sync::mpsc::channel::<DataFrame>(1);
        let (to_rust, mut from_python) =
            tokio::sync::mpsc::channel::<Result<DataFrame, ScoringError>>(1);
        let python_stop_flag = Arc::new(AtomicBool::new(false));
        let python_thread_stop_flag = python_stop_flag.clone(); // Getting moved in to the python thread

        let python_handle: std::thread::JoinHandle<std::result::Result<(), ScoringError>> =
            std::thread::spawn(move || {
                let pyreturn = Python::with_gil(|py| {
                    loop {
                        if python_thread_stop_flag.load(Ordering::Relaxed) {
                            break;
                        }

                        let psms = match from_rust.try_recv() {
                            Ok(psms) => {
                                debug!("[PYTHON] recv from rust");
                                psms
                            }
                            Err(TryRecvError::Empty) => {
                                trace!("[PYTHON] recv empty, retrying");
                                std::thread::sleep(tokio::time::Duration::from_millis(100));
                                continue;
                            }
                            Err(TryRecvError::Disconnected) => {
                                trace!("[PYTHON] connection to rust closed");
                                break;
                            }
                        };

                        trace!("[PYTHON] calc features and LoOP...");
                        let now = std::time::Instant::now();
                        let psms = calc_features(psms)
                            .and_then(|psms| calc_local_outlier_probabilities(&py, psms));
                        let elapsed = now.elapsed();
                        debug!(
                            "[PYTHON] calc features and LoOP took: {} s",
                            elapsed.as_secs()
                        );

                        trace!("[PYTHON] attempt to send results back to rust");
                        match to_rust.blocking_send(psms) {
                            Ok(_) => {
                                trace!("[PYTHON] send results back to rust");
                            }
                            Err(e) => {
                                error!("[PYTHON] Error sending results back to rust: {:?}", e);
                            }
                        }
                    }

                    Ok::<_, ScoringError>(())
                });

                match pyreturn {
                    Ok(_) => (),
                    Err(e) => {
                        error!("[PYTHON] Error in Python thread: {:?}", e);
                    }
                }
                debug!("[PYTHON] Python thread stopped");
                Ok(())
            });

        'message_loop: loop {
            if stop_flag.load(Ordering::Relaxed) {
                python_stop_flag.store(true, Ordering::Relaxed);
                break;
            }
            let (message_id, mut message) = match scoring_queue.pop().await {
                Ok(Some(message)) => {
                    debug!("[{}] recv", &message.0);
                    message
                }
                Ok(None) => {
                    debug!("recv None, retrying");
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

            // check if python thread is still running
            if python_handle.is_finished() {
                error!(
                        "[{} / {}] Python thread stopped unexpectedly. This scoring task is shutting down",
                        message.uuid(),
                        message.spectrum_id()
                    );
                let uuid = message.uuid().to_string();
                let spectrum_id = message.spectrum_id().to_string();
                match scoring_queue.push(message).await {
                    Ok(_) => (),
                    Err(_) => {
                        error!(
                                "[{} / {}] Error pushing manifest back to queue after Python thread stopped unexpectedly",
                                uuid,
                                spectrum_id,
                            );
                    }
                }
                break;
            }

            let metrics_counter_name = Self::get_counter_name(message.uuid());

            let relative_psms_path = create_file_path_on_precursor_level(
                message.uuid(),
                message.ms_run_name(),
                message.spectrum_id(),
                message.precursor(),
                "tsv",
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
                let psms = message.take_psms();
                let publication_message = message
                    .into_publication_message(relative_psms_path, psms)
                    .unwrap();
                Self::enqueue_message(publication_message, publish_queue.as_ref()).await;
                continue 'message_loop;
            }

            match to_python.send(message.take_psms()).await {
                Ok(_) => {
                    trace!("[{}] send to python", &message_id,);
                }
                Err(e) => {
                    let error_message =
                        message.to_error_message(ScoringError::RustToPythonSendError(e).into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, error_queue.as_ref()).await;
                    continue 'message_loop;
                }
            }

            let psms = match from_python.recv().await {
                Some(Ok(psms)) => psms,
                Some(Err(e)) => {
                    let error_message = message.to_error_message(e.into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, error_queue.as_ref()).await;
                    continue 'message_loop;
                }
                None => {
                    let error_message = message.to_error_message(
                        ScoringError::PythonThreadUnexpectedlyClosedError().into(),
                    );
                    error!("{},", &error_message);
                    Self::enqueue_message(error_message, error_queue.as_ref()).await;
                    continue 'message_loop;
                }
            };
            trace!("[{}] recv from python", &message_id,);

            let publication_message = match message
                .into_publication_message(relative_psms_path, psms)
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

            trace!("[{}] enqueued for publication", &message_id,);

            Self::ack_message(&message_id, scoring_queue.as_ref()).await;

            debug!("[{}] ack", &message_id,);

            counter!(metrics_counter_name.clone()).increment(1);
        }
        drop(to_python);
        match python_handle.join() {
            Ok(_) => {
                debug!("Joinded Python thread");
            }
            Err(e) => {
                error!("Error joining Python thread: {:?}", e);
            }
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
fn calc_features(mut psms: DataFrame) -> Result<DataFrame, ScoringError> {
    let ions_matches = psms.column("ions_matched")?.cast(&DataType::Float64)?;
    let ions_total = psms.column("ions_total")?.cast(&DataType::Float64)?;
    let ions_matched_ratio =
        (ions_matches / ions_total)?.with_name(IONS_MATCHED_RATIO_COL_NAME.into());

    let exp_neutral_mass = psms.column("exp_neutral_mass")?;
    let calc_neutral_mass = psms.column("calc_neutral_mass")?;
    let mass_diff = abs((exp_neutral_mass - calc_neutral_mass)?.as_materialized_series())?
        .with_name(MASS_DIFF_COL_NAME.into());

    psms.with_column(ions_matched_ratio)?;
    psms.with_column(mass_diff)?;

    Ok(psms)
}

/// Calculates the local outlier probabilities for the PSMs
///
/// # Arguments
/// * `py` - The Python interpreter
/// * `psms` - The PSMs to calculate the local outlier probabilities for
///
fn calc_local_outlier_probabilities(
    py: &Python,
    psms: DataFrame,
) -> Result<DataFrame, ScoringError> {
    // Import the Python modules and functionss
    let loop_mod = PyModule::import(*py, "maccoys.local_outlier_probability_wrapper")?;
    let calc_loop_fn = loop_mod.getattr("calculate_local_outlier_probability")?;

    // Wrap it into a PyDataFrame
    let mut psms = PyDataFrame(psms);

    psms = calc_loop_fn
        .call1((
            psms,
            FEATURES.into_pyobject(*py)?,
            N_NEIGHBORS,
            LOOP_COL_NAME,
        ))?
        .extract()?;

    Ok(psms.into())
}

#[cfg(test)]
mod tests {
    use std::fs::File;

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

        psms = calc_features(psms).unwrap();

        assert!(psms
            .get_column_names()
            .contains(&&PlSmallStr::from(FEATURES[1])));
        assert!(psms
            .get_column_names()
            .contains(&&PlSmallStr::from(FEATURES[2])));
    }

    #[tokio::test]
    /// Checks if the imports and usage of python is working
    ///
    async fn test_loop_calculation_using_python() {
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

        psms = calc_features(psms).unwrap();

        Python::with_gil(|py| {
            psms = calc_local_outlier_probabilities(&py, psms).unwrap();

            println!("LOOP: {:?}", psms);
            CsvWriter::new(File::create("./scored_psms.tsv").unwrap())
                .include_header(true)
                .with_separator(COMET_SEPARATOR.as_bytes()[0])
                .finish(&mut psms)
                .unwrap();
        });
    }
}
