use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use macpepdb_tui::{MetricConfig, MetricRowId, TuiHandle};
use metrics::gauge;
use thiserror::Error;
use tokio::{spawn, task::JoinHandle, time::sleep_until};
use tracing::{error, info};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Unable to access mzML from {0}, {1}")]
    AccessMzMl(PathBuf, Box<tokio::io::Error>),
    #[error("Unable to access PTMs from {0}, {1}")]
    AccessPtms(PathBuf, Box<tokio::io::Error>),
    #[error("Unable to access search paramters from {0}, {1}")]
    AccessSearchParameter(PathBuf, Box<tokio::io::Error>),
    #[error("Unable to handle CTRL-C signal {0}")]
    CtrlCSignal(Box<tokio::io::Error>),
    #[error("Unable to decode UUID {0}")]
    DecodeUuidResponse(Box<reqwest::Error>),
    #[error("Unable to send enqueuiung request {0}")]
    EnqueueRequest(Box<reqwest::Error>),
    #[error("MzMl file `{0}` has no name part.")]
    NoMzMlName(PathBuf),
    #[error("Unable to stop monitor due to {0}")]
    MonitorStop(Box<tokio::task::JoinError>),
    #[error("Unsuccessful response from remote pipleine: {0}, status code: {1}")]
    UnsuccessfuleEnqueueResponse(String, http::StatusCode),
}

use crate::pipeline::remote_pipeline_web_api::ProgressRepsonse;

/// Interval to refresh the monitored metrics
///
const REFRESH_INTERVAL: u64 = 1000;

/// TUI metric keys for [`RemotePipelineProgressMonitor`]. Fixed (not per-search-UUID) since one
/// `search-monitor` process only ever monitors a single search at a time.
const SEARCH_SPACE_GENERATION_METRIC_KEY: &str = "remote_search_monitor_search_space_generation";
const IDENTIFICATION_METRIC_KEY: &str = "remote_search_monitor_identification";
const SCORING_METRIC_KEY: &str = "remote_search_monitor_scoring";
const ERROR_METRIC_KEY: &str = "remote_search_monitor_errors";
const FINISHED_METRIC_KEY: &str = "remote_search_monitor_finished";

pub struct RemotePipeline {}

impl RemotePipeline {
    /// Enqueues a search to a remote pipeline
    ///
    /// # Arguments
    /// * `base_url` - Base URL of the remote pipeline
    /// * `search_parameters_path` - Path to the search parameters file
    /// * `comet_params_path` - Path to the Comet parameters file
    /// * `mzml_file_paths` - Paths to the mzML files
    /// * `ptms_path` - Optional path to the PTMs file
    /// * `tui` - Optional TUI handle to show progress as gauges instead of log lines
    ///
    pub async fn run(
        base_url: String,
        search_parameters_path: PathBuf,
        mzml_file_paths: Vec<PathBuf>,
        ptms_path: Option<PathBuf>,
        tui: Option<TuiHandle>,
    ) -> Result<String, Error> {
        let enqueue_url = format!("{}/api/pipeline/enqueue", base_url);

        let search_parameters_reader =
            reqwest::Body::wrap_stream(tokio_util::codec::FramedRead::new(
                tokio::fs::File::open(&search_parameters_path)
                    .await
                    .map_err(|err| {
                        Error::AccessSearchParameter(search_parameters_path, Box::new(err))
                    })?,
                tokio_util::codec::BytesCodec::new(),
            ));

        let mut form = reqwest::multipart::Form::new().part(
            "search_params",
            reqwest::multipart::Part::stream(search_parameters_reader),
        );

        if let Some(ptms_path) = ptms_path {
            let ptms_reader = reqwest::Body::wrap_stream(tokio_util::codec::FramedRead::new(
                tokio::fs::File::open(&ptms_path)
                    .await
                    .map_err(|err| Error::AccessPtms(ptms_path, Box::new(err)))?,
                tokio_util::codec::BytesCodec::new(),
            ));

            form = form.part("ptms", reqwest::multipart::Part::stream(ptms_reader));
        }

        for (index, mzml_file_path) in mzml_file_paths.iter().enumerate() {
            let mzml_reader = reqwest::Body::wrap_stream(tokio_util::codec::FramedRead::new(
                tokio::fs::File::open(mzml_file_path)
                    .await
                    .map_err(|err| Error::AccessMzMl(mzml_file_path.clone(), Box::new(err)))?,
                tokio_util::codec::BytesCodec::new(),
            ));

            let file_name = mzml_file_path
                .file_name()
                .ok_or(Error::NoMzMlName(mzml_file_path.clone()))?
                .to_string_lossy()
                .to_string();

            form = form.part(
                format!("mzml_{}", index),
                reqwest::multipart::Part::stream(mzml_reader).file_name(file_name),
            );
        }

        let client = reqwest::Client::new();
        tracing::info!("Submitting search to remote pipeline at {}", enqueue_url);
        let response = client
            .post(enqueue_url)
            .multipart(form)
            .header("Connection", "close")
            .send()
            .await
            .map_err(|err| Error::EnqueueRequest(Box::new(err)))?;

        if !response.status().is_success() {
            let status_code = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or("Unable to decode body as text.".to_string());
            return Err(Error::UnsuccessfuleEnqueueResponse(error_text, status_code));
        }

        let uuid = response
            .text()
            .await
            .map_err(|err| Error::DecodeUuidResponse(Box::new(err)))?;
        info!(
            "Search submitted with UUID: {}. Start search monitor (exit with CTRL-C).",
            uuid
        );

        Self::start_remote_search_monitor(base_url, &uuid, tui).await?;

        Ok(uuid)
    }

    /// Starts a progress monitor for a remote search
    ///
    /// # Arguments
    /// * `base_url` - Base URL of the remote server
    /// * `uuid` - UUID of the search
    /// * `tui` - Optional TUI handle to show progress as gauges instead of log lines
    ///
    pub async fn start_remote_search_monitor(
        base_url: String,
        uuid: &str,
        tui: Option<TuiHandle>,
    ) -> Result<(), Error> {
        let has_tui = tui.is_some();
        let progress_monitor = RemotePipelineProgressMonitor::new(uuid, base_url, tui);

        if has_tui {
            // The TUI's raw terminal mode consumes Ctrl+C itself (alongside `q`) as its own quit
            // key, so `tokio::signal::ctrl_c()` below would never fire while it's active. Instead
            // wait for the monitor's background task, which exits once the TUI quits on its own.
            progress_monitor.join().await?;
        } else {
            tokio::signal::ctrl_c()
                .await
                .map_err(|err| Error::CtrlCSignal(Box::new(err)))?;

            progress_monitor.stop().await?;
        }

        Ok(())
    }
}

/// Tracks the live TUI display rows for a monitored search: one row per stage gauge, plus the
/// one row (`finished`) rendered as a progress bar, since `finished_spectrum_count` is the only
/// count guaranteed `<= total_spectrum_count` by construction. The other stage counts
/// (search-space-generation/identification/scoring) increment once per precursor rather than
/// once per spectrum, so a multi-precursor spectrum can push them past `total_spectrum_count` -
/// showing them as plain gauges avoids a misleading "over 100%" progress bar.
struct TuiRows {
    finished_row: MetricRowId,
    /// `total_spectrum_count` the `finished_row`'s progress bar was last created with; the total
    /// can keep growing while spectra are still being enqueued, so this is compared against each
    /// poll's freshly-fetched total to decide whether the row needs to be recreated with a new max.
    finished_max: u64,
}

struct RemotePipelineProgressMonitor {
    stop_flag: Arc<AtomicBool>,
    handle: JoinHandle<Result<(), Error>>,
}

impl RemotePipelineProgressMonitor {
    pub fn new(search_uuid: &str, remote_pipline_api: String, tui: Option<TuiHandle>) -> Self {
        let url = format!("{remote_pipline_api}/api/pipeline/progress/{search_uuid}");
        let stop_flag = Arc::new(AtomicBool::new(false));
        let handle = spawn(Self::view(stop_flag.clone(), url, tui));

        Self { stop_flag, handle }
    }

    /// Registers the gauge/progress rows on `tui` the first time this is called (`rows` is
    /// `None`), and thereafter recreates the `finished` progress bar's row whenever
    /// `total_spectrum_count` has grown since it was last created, so the bar's denominator
    /// tracks the total while spectra are still being enqueued.
    fn update_tui(tui: &TuiHandle, rows: &mut Option<TuiRows>, progress: &ProgressRepsonse) {
        let total = progress.total_spectrum_count();

        match rows {
            None => {
                tui.add_metric(MetricConfig::gauge(
                    SEARCH_SPACE_GENERATION_METRIC_KEY,
                    "Search space generated",
                ));
                tui.add_metric(MetricConfig::gauge(
                    IDENTIFICATION_METRIC_KEY,
                    "Identified",
                ));
                tui.add_metric(MetricConfig::gauge(SCORING_METRIC_KEY, "Scored"));
                tui.add_metric(MetricConfig::gauge(ERROR_METRIC_KEY, "Errors"));
                let finished_row = tui.add_metric(MetricConfig::progress(
                    FINISHED_METRIC_KEY,
                    "Finished spectra",
                    total as f64,
                ));
                *rows = Some(TuiRows {
                    finished_row,
                    finished_max: total,
                });
            }
            Some(rows) if rows.finished_max != total => {
                tui.remove_metric_row(rows.finished_row);
                rows.finished_row = tui.add_metric(MetricConfig::progress(
                    FINISHED_METRIC_KEY,
                    "Finished spectra",
                    total as f64,
                ));
                rows.finished_max = total;
            }
            Some(_) => {}
        }

        gauge!(SEARCH_SPACE_GENERATION_METRIC_KEY)
            .set(progress.search_space_generated_count() as f64);
        gauge!(IDENTIFICATION_METRIC_KEY).set(progress.identified_count() as f64);
        gauge!(SCORING_METRIC_KEY).set(progress.scored_count() as f64);
        gauge!(ERROR_METRIC_KEY).set(progress.error_count() as f64);
        gauge!(FINISHED_METRIC_KEY).set(progress.finished_spectrum_count() as f64);
    }

    async fn view(
        stop_flag: Arc<AtomicBool>,
        monitor_endpoint_url: String,
        mut tui: Option<TuiHandle>,
    ) -> Result<(), Error> {
        let mut is_fully_enqueued_notified = false;
        let mut tui_rows: Option<TuiRows> = None;

        while !stop_flag.load(Ordering::Relaxed) {
            let next_refresh =
                tokio::time::Instant::now() + tokio::time::Duration::from_millis(REFRESH_INTERVAL);

            let response = match reqwest::get(&monitor_endpoint_url).await {
                Ok(response) => response,
                Err(e) => {
                    error!("Error getting progress: {:?}", e);
                    sleep_until(next_refresh).await;
                    continue;
                }
            };
            if !response.status().is_success() {
                error!(
                    "Error getting progress - Status Code: {} {:?}",
                    response.status(),
                    response
                        .text()
                        .await
                        .unwrap_or("Unable to decode body as text.".to_string())
                );
                sleep_until(next_refresh).await;
                continue;
            }
            let progress: ProgressRepsonse = match response.json().await {
                Ok(progress) => progress,
                Err(e) => {
                    error!("Error converting progress: {:?}", e);
                    sleep_until(next_refresh).await;
                    continue;
                }
            };

            if progress.is_search_fully_enqueued() && !is_fully_enqueued_notified {
                is_fully_enqueued_notified = true;
                info!("Spectra fully enqueued");
            }

            match tui.as_ref() {
                Some(tui) => Self::update_tui(tui, &mut tui_rows, &progress),
                None => {
                    info!(
                        "indexed {} | search-space {} | identified {} | scored {} | errors {} | finished {}/{}",
                        progress.total_spectrum_count(),
                        progress.search_space_generated_count(),
                        progress.identified_count(),
                        progress.scored_count(),
                        progress.error_count(),
                        progress.finished_spectrum_count(),
                        progress.total_spectrum_count(),
                    );
                }
            }

            match tui.as_mut() {
                // Race the next poll tick against the TUI quitting on its own (user pressed `q`
                // or Ctrl+C inside it); if the TUI wins, stop polling - it has already restored
                // the terminal by the time `wait()` resolves.
                Some(tui) => tokio::select! {
                    _ = sleep_until(next_refresh) => {}
                    _ = tui.wait() => break,
                },
                None => sleep_until(next_refresh).await,
            }
        }

        Ok(())
    }

    /// Waits for the background polling task to finish on its own, without ever requesting it to
    /// stop. Used when a TUI is active: its render task ends only once the user quits it, and
    /// `view`'s loop then exits by itself (see the `tui.wait()` race above).
    pub async fn join(self) -> Result<(), Error> {
        self.handle
            .await
            .map_err(|err| Error::MonitorStop(Box::new(err)))?
    }

    pub async fn stop(self) -> Result<(), Error> {
        self.stop_flag.store(true, Ordering::Relaxed);
        self.handle
            .await
            .map_err(|err| Error::MonitorStop(Box::new(err)))?
    }
}
