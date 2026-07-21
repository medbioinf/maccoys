use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{anyhow, bail, Result};
use indicatif::ProgressStyle;
use tokio::{spawn, task::JoinHandle, time::sleep_until};
use tracing::{error, info, info_span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

use crate::pipeline::remote_pipeline_web_api::ProgressRepsonse;

/// Interval to refresh the monitored metrics
///
const REFRESH_INTERVAL: u64 = 1000;

/// Progress bar style
const PROGRESS_STYLE: &str = "\t{msg} {wide_bar} {pos}/{len} {per_sec} ";

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
    ///
    pub async fn run(
        base_url: String,
        search_parameters_path: PathBuf,
        mzml_file_paths: Vec<PathBuf>,
        ptms_path: Option<PathBuf>,
    ) -> Result<String> {
        let enqueue_url = format!("{}/api/pipeline/enqueue", base_url);

        let search_parameters_reader =
            reqwest::Body::wrap_stream(tokio_util::codec::FramedRead::new(
                tokio::fs::File::open(search_parameters_path).await?,
                tokio_util::codec::BytesCodec::new(),
            ));

        let mut form = reqwest::multipart::Form::new().part(
            "search_params",
            reqwest::multipart::Part::stream(search_parameters_reader),
        );

        if let Some(ptms_path) = ptms_path {
            let ptms_reader = reqwest::Body::wrap_stream(tokio_util::codec::FramedRead::new(
                tokio::fs::File::open(ptms_path).await?,
                tokio_util::codec::BytesCodec::new(),
            ));

            form = form.part("ptms", reqwest::multipart::Part::stream(ptms_reader));
        }

        for (index, mzml_file_path) in mzml_file_paths.iter().enumerate() {
            let mzml_reader = reqwest::Body::wrap_stream(tokio_util::codec::FramedRead::new(
                tokio::fs::File::open(mzml_file_path).await?,
                tokio_util::codec::BytesCodec::new(),
            ));

            let file_name = match mzml_file_path.file_name() {
                Some(file_name) => file_name.to_string_lossy().to_string(),
                None => {
                    bail!(
                        "Error getting file name from path: {}",
                        mzml_file_path.display()
                    );
                }
            };

            form = form.part(
                format!("mzml_{}", index),
                reqwest::multipart::Part::stream(mzml_reader).file_name(file_name),
            );
        }

        let client = reqwest::Client::new();
        let response = client
            .post(enqueue_url)
            .multipart(form)
            .header("Connection", "close")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!(
                "Error submitting search: {:?}",
                response.text().await?
            ));
        }

        let uuid = response.text().await?;
        info!(
            "Search submitted with UUID: {}. Start search monitor (exit with CTRL-C).",
            uuid
        );

        Self::start_remote_search_monitor(base_url, &uuid).await?;

        Ok(uuid)
    }

    /// Starts a progress monitor for a remote search
    ///
    /// # Arguments
    /// * `base_url` - Base URL of the remote server
    /// * `uuid` - UUID of the search
    ///
    pub async fn start_remote_search_monitor(base_url: String, uuid: &str) -> Result<()> {
        let progress_monitor = RemotePipelineProgressMonitor::new(uuid, base_url);

        tokio::signal::ctrl_c().await?;

        progress_monitor.stop().await?;

        Ok(())
    }
}

struct RemotePipelineProgressMonitor {
    stop_flag: Arc<AtomicBool>,
    handle: JoinHandle<Result<()>>,
}

impl RemotePipelineProgressMonitor {
    pub fn new(search_uuid: &str, remote_pipline_api: String) -> Self {
        let url = format!("{remote_pipline_api}/api/pipeline/progress/{search_uuid}");
        let stop_flag = Arc::new(AtomicBool::new(false));
        let handle = spawn(Self::view(stop_flag.clone(), url));

        Self { stop_flag, handle }
    }

    async fn view(stop_flag: Arc<AtomicBool>, monitor_endpoint_url: String) -> Result<()> {
        let progress_span = info_span!("progress");
        progress_span.pb_set_message("Finished spectra");
        progress_span.pb_set_style(&ProgressStyle::with_template(PROGRESS_STYLE).unwrap());

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
                    response.text().await?
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

            let _ = progress_span.enter();
            progress_span.pb_set_length(progress.total_spectrum_count());
            progress_span.pb_set_position(progress.finished_spectrum_count());
            if progress.is_search_fully_enqueued() {
                progress_span.pb_set_message("Finished spectra");
            } else {
                progress_span.pb_set_message("Finished spectra (enqueuing still in progress)");
            }

            sleep_until(next_refresh).await;
        }

        Ok(())
    }

    pub async fn stop(self) -> Result<()> {
        self.stop_flag.store(true, Ordering::Relaxed);
        self.handle.await?
    }
}
