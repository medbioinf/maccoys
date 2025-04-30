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
use numpy::IntoPyArray;
use polars::{prelude::NamedFrom, series::Series};
use pyo3::prelude::*;
use signal_hook::{consts::SIGINT, iterator::Signals};
use tokio::sync::mpsc::error::TryRecvError;
use tracing::{debug, error, info};

use crate::{
    constants::{COMET_EXP_BASE_SCORE, LOOP_SCORE_NAME},
    pipeline::{
        configuration::StandaloneScoringConfiguration,
        errors::scoring_error::ScoringError,
        messages::{
            error_message::ErrorMessage,
            is_message::IsMessage,
            publication_message::PublicationMessage,
            scoring_message::{IntoPublicationMessageError, ScoringMessage},
        },
        queue::{PipelineQueue, RedisPipelineQueue},
        utils::create_file_path_on_precursor_level,
    },
};

use super::task::Task;

/// Prefix for the scoring counter
///
pub const COUNTER_PREFIX: &str = "maccoys_scorings";

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
        let (to_python, mut from_rust) = tokio::sync::mpsc::channel::<Vec<f64>>(1);
        let (to_rust, mut from_python) = tokio::sync::mpsc::channel::<Result<Vec<f64>, PyErr>>(1);
        let python_stop_flag = Arc::new(AtomicBool::new(false));
        let python_thread_stop_flag = python_stop_flag.clone(); // Getting moved in to the python thread

        let python_handle: std::thread::JoinHandle<std::result::Result<(), ScoringError>> =
            std::thread::spawn(move || {
                match Python::with_gil(|py| {
                    // imports
                    let pynomaly_loop = PyModule::import(py, "PyNomaly.loop")?;

                    // classes
                    #[allow(non_snake_case)]
                    let LocalOutlierProbability =
                        pynomaly_loop.getattr("LocalOutlierProbability")?;

                    loop {
                        if python_thread_stop_flag.load(Ordering::Relaxed) {
                            break;
                        }

                        let psm_scores = match from_rust.try_recv() {
                            Ok(scores) => scores,
                            Err(TryRecvError::Empty) => {
                                std::thread::sleep(tokio::time::Duration::from_millis(100));
                                continue;
                            }
                            Err(TryRecvError::Disconnected) => {
                                break;
                            }
                        };

                        let psm_scores = psm_scores.into_pyarray(py);

                        let local_outlier_probabilities: PyResult<Vec<f64>> =
                            LocalOutlierProbability
                                .call1((psm_scores,))
                                // .fit()
                                .and_then(|local_outlier_probabilities| {
                                    local_outlier_probabilities.call_method0("fit")
                                })
                                // LocalOutlierProbability.fit().local_outlier_probability
                                .and_then(|local_outlier_probabilities| {
                                    local_outlier_probabilities
                                        .getattr("local_outlier_probabilities")
                                })
                                // Cast to Vec
                                .and_then(|local_outlier_probabilities| {
                                    local_outlier_probabilities.extract::<Vec<f64>>()
                                });

                        match local_outlier_probabilities {
                            Ok(score) => to_rust.blocking_send(Ok(score))?,
                            Err(e) => to_rust.blocking_send(Err(e))?,
                        }
                    }

                    Ok::<_, ScoringError>(())
                }) {
                    Ok(_) => (),
                    Err(e) => {
                        error!("[PYTHON] Error running Python thread: {:?}", e);
                    }
                }
                debug!("[PYTHON] Python thread stopped");
                Ok(())
            });

        'message_loop: loop {
            while let Some(mut message) = scoring_queue.pop().await {
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
                    let publication_message = message
                        .into_publication_message(relative_psms_path)
                        .unwrap();
                    Self::enqueue_message(publication_message, publish_queue.as_ref()).await;
                    continue 'message_loop;
                }

                let psms_score_series = match message.psms().column(COMET_EXP_BASE_SCORE) {
                    Ok(scores) => scores,
                    Err(e) => {
                        let error_message = message.to_error_message(
                            ScoringError::MissingScoreError(COMET_EXP_BASE_SCORE.to_string(), e)
                                .into(),
                        );
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                };

                let psms_score: Vec<f64> = match psms_score_series.f64() {
                    Ok(scores) => scores
                        .to_vec()
                        .into_iter()
                        .map(|score| score.unwrap_or(-1.0))
                        .collect(),
                    Err(e) => {
                        let error_message =
                            message.to_error_message(ScoringError::ScoreToF64VecError(e).into());
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                };

                match to_python.send(psms_score).await {
                    Ok(_) => (),
                    Err(e) => {
                        let error_message =
                            message.to_error_message(ScoringError::RustToPythonSendError(e).into());
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                }

                let local_outlier_probabilities = match from_python.recv().await {
                    Some(Ok(loop_score)) => loop_score,
                    Some(Err(e)) => {
                        let error_message =
                            message.to_error_message(ScoringError::PythonError(e).into());
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

                match message
                    .psms_mut()
                    .with_column(Series::new(LOOP_SCORE_NAME, local_outlier_probabilities))
                {
                    Ok(_) => (),
                    Err(e) => {
                        let error_message =
                            message.to_error_message(ScoringError::AddingScoreToPSMError(e).into());
                        error!("{},", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                }

                let publication_message = match message.into_publication_message(relative_psms_path)
                {
                    Ok(publication_message) => publication_message,
                    Err(e) => {
                        let error_message = match *e {
                            IntoPublicationMessageError::CsvWriteError(e, message) => message
                                .to_error_message(
                                    ScoringError::IntoPublicationMessageError(e).into(),
                                ),
                        };
                        error!("{},", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                };

                Self::enqueue_message(publication_message, publish_queue.as_ref()).await;

                counter!(metrics_counter_name.clone()).increment(1);
            }
            if stop_flag.load(Ordering::Relaxed) {
                python_stop_flag.store(true, Ordering::Relaxed);
                break;
            }
            // wait before checking the queue again
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        drop(to_python);
        match python_handle.join() {
            Ok(_) => (),
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
    pub async fn run_standalone(config_file_path: PathBuf) -> Result<()> {
        let config: StandaloneScoringConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let scoring_queue =
            Arc::new(RedisPipelineQueue::<ScoringMessage>::new(&config.scoring).await?);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    /// Checks if the imports and usage of python is working
    ///
    async fn test_loop_calculation_using_python() {
        let xcorr = vec![
            469.0, 415.0, 361.0, 335.0, 271.0, 256.0, 231.0, 212.0, 193.0, 155.0, 71.0, 55.0, 36.0,
            3.6854,
        ];

        match Python::with_gil(|py| {
            // imports
            let pynomaly_loop = PyModule::import(py, "PyNomaly.loop")?;

            // submodules

            // classes
            #[allow(non_snake_case)]
            let LocalOutlierProbability = pynomaly_loop.getattr("LocalOutlierProbability")?;

            let xcorrpy = xcorr.into_pyarray(py);

            let local_outlier_probability_fit = LocalOutlierProbability
                .call1((xcorrpy,))?
                .call_method0("fit")?;

            let _local_outlier_probability: Vec<f64> = local_outlier_probability_fit
                .getattr("local_outlier_probabilities")?
                .extract()?;

            Ok::<(), anyhow::Error>(())
        }) {
            Ok(_) => (),
            Err(e) => {
                println!("[PYTHON] Error running Python thread: {:?}", e);
            }
        }
    }
}
