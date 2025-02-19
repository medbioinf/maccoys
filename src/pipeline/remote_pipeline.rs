use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{anyhow, bail, Result};
use indicatif::ProgressStyle;
use lazy_static::lazy_static;
use tokio::{spawn, task::JoinHandle, time::sleep_until};
use tracing::{error, info, info_span, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

use super::tasks::{
    cleanup_task::CleanupTask, identification_task::IdentificationTask,
    indexing_task::IndexingTask, preparation_task::PreparationTask, scoring_task::ScoringTask,
    search_space_generation_task::SearchSpaceGenerationTask, task::Task,
};

/// Interval to refresh the monitored metrics
///
const REFRESH_INTERVAL: u64 = 1000;

/// Span style for simple metrics
///
const SIMPLE_STYLE: &str = "{msg} {pos}/s";

lazy_static! {
    static ref METRICS_RENDER_ORDER: [&'static str; 6] = [
        IndexingTask::get_counter_prefix(),
        PreparationTask::get_counter_prefix(),
        SearchSpaceGenerationTask::get_counter_prefix(),
        IdentificationTask::get_counter_prefix(),
        ScoringTask::get_counter_prefix(),
        CleanupTask::get_counter_prefix(),
    ];
}

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
        comet_params_path: PathBuf,
        mzml_file_paths: Vec<PathBuf>,
        ptms_path: Option<PathBuf>,
    ) -> Result<String> {
        let enqueue_url = format!("{}/api/pipeline/enqueue", base_url);

        let search_parameters_reader =
            reqwest::Body::wrap_stream(tokio_util::codec::FramedRead::new(
                tokio::fs::File::open(search_parameters_path).await?,
                tokio_util::codec::BytesCodec::new(),
            ));

        let comet_params_reader = reqwest::Body::wrap_stream(tokio_util::codec::FramedRead::new(
            tokio::fs::File::open(comet_params_path).await?,
            tokio_util::codec::BytesCodec::new(),
        ));

        let mut form = reqwest::multipart::Form::new()
            .part(
                "search_params",
                reqwest::multipart::Part::stream(search_parameters_reader),
            )
            .part(
                "comet_params",
                reqwest::multipart::Part::stream(comet_params_reader),
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
        let metrics_monitor = RemotePiplineMetricsMonitor::new(uuid, base_url);

        tokio::signal::ctrl_c().await?;

        metrics_monitor.stop().await?;

        Ok(())
    }
}

struct RemotePiplineMetricsMonitor {
    stop_flag: Arc<AtomicBool>,
    handle: JoinHandle<Result<()>>,
}

impl RemotePiplineMetricsMonitor {
    pub fn new(search_uuid: &str, remote_pipline_api: String) -> Self {
        let url = format!("{remote_pipline_api}/api/pipeline/monitor/{search_uuid}");
        let stop_flag = Arc::new(AtomicBool::new(false));
        let handle = spawn(Self::view(stop_flag.clone(), url));

        Self { stop_flag, handle }
    }

    async fn view(stop_flag: Arc<AtomicBool>, monitor_endpoint_url: String) -> Result<()> {
        let spans: HashMap<String, Span> = METRICS_RENDER_ORDER
            .iter()
            .map(|metric| {
                let span = info_span!("");
                let _ = span.enter();
                span.pb_set_position(0);
                span.pb_set_message(metric);
                span.pb_set_style(&ProgressStyle::with_template(SIMPLE_STYLE).unwrap());
                (metric.to_string(), span)
            })
            .collect();

        while !stop_flag.load(Ordering::Relaxed) {
            let next_refresh =
                tokio::time::Instant::now() + tokio::time::Duration::from_millis(REFRESH_INTERVAL);
            let response = reqwest::get(&monitor_endpoint_url).await?;
            if !response.status().is_success() {
                continue;
            }
            let counter: HashMap<String, usize> = match response.json().await {
                Ok(counter) => counter,
                Err(e) => {
                    error!("Error reading metrics line: {:?}", e);
                    continue;
                }
            };

            for metrics in METRICS_RENDER_ORDER.iter() {
                let value = counter.get(*metrics).unwrap_or(&0);
                let span = spans.get(*metrics).unwrap();
                let _ = span.enter();
                span.pb_set_position(*value as u64);
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
