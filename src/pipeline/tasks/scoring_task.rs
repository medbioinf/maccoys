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
use polars::{prelude::NamedFrom, series::Series};
use pyo3::{prelude::*, types::PyList};
use signal_hook::{consts::SIGINT, iterator::Signals};
use tokio::sync::mpsc::error::TryRecvError;
use tracing::{debug, error, info};

use crate::{
    constants::{COMET_EXP_BASE_SCORE, DIST_SCORE_NAME, EXP_SCORE_NAME},
    goodness_of_fit_record::GoodnessOfFitRecord,
    pipeline::{
        configuration::StandaloneScoringConfiguration, convert::AsInputOutputQueue,
        queue::PipelineQueue,
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
    /// * `storage` - The storage to use
    /// * `scoring_queue` - The queue to get the PSMs to score
    /// * `cleanup_queue` - The queue to push the scored PSMs to
    /// * `stop_flag` - The flag to stop the task
    ///
    pub async fn start<Q>(scoring_queue: Arc<Q>, cleanup_queue: Arc<Q>, stop_flag: Arc<AtomicBool>)
    where
        Q: PipelineQueue + 'static,
    {
        let (to_python, mut from_rust) = tokio::sync::mpsc::channel::<Vec<f64>>(1);
        let (to_rust, mut from_python) =
            tokio::sync::mpsc::channel::<(Vec<GoodnessOfFitRecord>, Vec<f64>, Vec<f64>)>(1);
        let python_stop_flag = Arc::new(AtomicBool::new(false));
        let python_thread_stop_flag = python_stop_flag.clone(); // Getting moved in to the python thread

        let python_handle: std::thread::JoinHandle<Result<()>> = std::thread::spawn(move || {
            match Python::with_gil(|py| {
                // imports
                let goodness_of_fit_mod = PyModule::import(py, "maccoys.goodness_of_fit")?;
                let scoring_mod = PyModule::import(py, "maccoys.scoring")?;

                // Load all necessary functions
                let calc_goodnesses_fn = goodness_of_fit_mod.getattr("calc_goodnesses")?;
                let calculate_exp_score_fn = scoring_mod.getattr("calculate_exp_score")?;
                let calculate_distance_score_fn =
                    scoring_mod.getattr("calculate_distance_score")?;

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

                    let psm_scores = PyList::new(py, psm_scores)?;

                    let goodness_of_fits: Vec<GoodnessOfFitRecord> = calc_goodnesses_fn
                        .call1((&psm_scores,))?
                        .extract::<Vec<(String, String, f64, f64)>>()?
                        .into_iter()
                        .map(GoodnessOfFitRecord::from)
                        .collect();

                    let exponential_score: Vec<f64> =
                        calculate_exp_score_fn.call1((&psm_scores,))?.extract()?;

                    let distance_score: Vec<f64> = calculate_distance_score_fn
                        .call1((&psm_scores,))?
                        .extract()?;

                    to_rust.blocking_send((goodness_of_fits, exponential_score, distance_score))?;
                }

                Ok::<_, anyhow::Error>(())
            }) {
                Ok(_) => (),
                Err(e) => {
                    error!("[PYTHON] Error running Python thread: {:?}", e);
                }
            }
            debug!("[PYTHON] Python thread stopped");
            Ok(())
        });

        loop {
            while let Some(mut manifest) = scoring_queue.pop().await {
                debug!(
                    "[{} / {}] Goodness and rescoring",
                    &manifest.uuid, &manifest.spectrum_id
                );

                let metrics_counter_name = format!("{}_{}", COUNTER_PREFIX, manifest.uuid);

                if manifest.precursors.len() != manifest.psms_dataframes.len() {
                    info!(
                        "[{} / {}] Number of PSM dataframe and precursors do not match",
                        &manifest.uuid, &manifest.spectrum_id
                    );
                    continue;
                }

                for psms in manifest.psms_dataframes.iter_mut() {
                    if psms.is_empty() {
                        manifest.goodness.push(Vec::with_capacity(0));
                        continue;
                    }

                    let psms_score_series = match psms.column(COMET_EXP_BASE_SCORE) {
                        Ok(scores) => scores,
                        Err(e) => {
                            error!(
                                "[{} / {}] Error selecting scores `{}` from PSMs: {:?}",
                                &manifest.uuid, &manifest.spectrum_id, COMET_EXP_BASE_SCORE, e
                            );
                            continue;
                        }
                    };

                    let psms_score: Vec<f64> = match psms_score_series.f64() {
                        Ok(scores) => scores
                            .to_vec()
                            .into_iter()
                            .map(|score| score.unwrap_or(-1.0))
                            .collect(),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error converting scores to f64: {:?}",
                                &manifest.uuid, &manifest.spectrum_id, e
                            );
                            continue;
                        }
                    };

                    match to_python.send(psms_score).await {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error sending scores to Python: {:?}",
                                &manifest.uuid, &manifest.spectrum_id, e
                            );
                            continue;
                        }
                    }

                    let (goodness_of_fits, exponential_scores, dist_scores) =
                        match from_python.recv().await {
                            Some(goodness_of_fit) => goodness_of_fit,
                            None => {
                                error!(
                                    "[{} / {}] No goodness of fit received from Python",
                                    &manifest.uuid, &manifest.spectrum_id
                                );
                                continue;
                            }
                        };

                    match psms.with_column(Series::new(EXP_SCORE_NAME, exponential_scores)) {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error adding exponential scores to PSMs: {:?}",
                                &manifest.uuid, &manifest.spectrum_id, e
                            );
                            continue;
                        }
                    }

                    match psms.with_column(Series::new(DIST_SCORE_NAME, dist_scores)) {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error adding distance scores to PSMs: {:?}",
                                &manifest.uuid, &manifest.spectrum_id, e
                            );
                            continue;
                        }
                    }

                    manifest.goodness.push(goodness_of_fits);

                    debug!(
                        "[{} / {}] Goodness and rescoring done",
                        &manifest.uuid, &manifest.spectrum_id
                    );
                }

                counter!(metrics_counter_name.clone()).increment(1);

                loop {
                    manifest = match cleanup_queue.push(manifest).await {
                        Ok(_) => break,
                        Err(e) => {
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                            e
                        }
                    }
                }
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

        let (input_queue, output_queue) = config.as_input_output_queue().await?;
        let input_queue = Arc::new(input_queue);
        let output_queue = Arc::new(output_queue);
        let stop_flag = Arc::new(AtomicBool::new(false));

        let mut signals = Signals::new([SIGINT])?;

        let signal_stop_flag = stop_flag.clone();
        std::thread::spawn(move || {
            for sig in signals.forever() {
                if sig == SIGINT {
                    info!("Gracefully stopping.");
                    signal_stop_flag.store(true, Ordering::Relaxed);
                }
            }
        });

        let handles: Vec<tokio::task::JoinHandle<()>> =
            (0..config.goodness_and_rescoring.num_tasks)
                .map(|_| {
                    tokio::spawn(ScoringTask::start(
                        input_queue.clone(),
                        output_queue.clone(),
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
