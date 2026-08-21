use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

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

        Self::start_remote_search_monitor(base_url, &uuid).await?;

        Ok(uuid)
    }

    /// Starts a progress monitor for a remote search
    ///
    /// # Arguments
    /// * `base_url` - Base URL of the remote server
    /// * `uuid` - UUID of the search
    ///
    pub async fn start_remote_search_monitor(base_url: String, uuid: &str) -> Result<(), Error> {
        let progress_monitor = RemotePipelineProgressMonitor::new(uuid, base_url);

        tokio::signal::ctrl_c()
            .await
            .map_err(|err| Error::CtrlCSignal(Box::new(err)))?;

        progress_monitor.stop().await?;

        Ok(())
    }
}

struct RemotePipelineProgressMonitor {
    stop_flag: Arc<AtomicBool>,
    handle: JoinHandle<Result<(), Error>>,
}

impl RemotePipelineProgressMonitor {
    pub fn new(search_uuid: &str, remote_pipline_api: String) -> Self {
        let url = format!("{remote_pipline_api}/api/pipeline/progress/{search_uuid}");
        let stop_flag = Arc::new(AtomicBool::new(false));
        let handle = spawn(Self::view(stop_flag.clone(), url));

        Self { stop_flag, handle }
    }

    async fn view(stop_flag: Arc<AtomicBool>, monitor_endpoint_url: String) -> Result<(), Error> {
        let mut is_fully_enqueued_notified = false;

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

            sleep_until(next_refresh).await;
        }

        Ok(())
    }

    pub async fn stop(self) -> Result<(), Error> {
        self.stop_flag.store(true, Ordering::Relaxed);
        self.handle
            .await
            .map_err(|err| Error::MonitorStop(Box::new(err)))?
    }
}
