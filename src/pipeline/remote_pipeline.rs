use std::{
    num::ParseIntError,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::{anyhow, bail, Result};
use macpepdb::tools::progress_monitor::ProgressMonitor;
use tracing::{error, info};

use super::storage::{COUNTER_LABLES, NUMBER_OF_COUNTERS};

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
        let monitor_url = format!("{}/api/pipeline/monitor/{}", base_url, uuid);
        let metrics_stop_flag = Arc::new(AtomicBool::new(false));
        let metrics = (0..NUMBER_OF_COUNTERS)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect::<Vec<Arc<AtomicUsize>>>();

        let thread_metrics_stop_flag = metrics_stop_flag.clone();
        let thread_metrics = metrics.clone();

        // Polls the metrics from the remote server
        let metrics_poll_taks: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
            let mut next_poll = tokio::time::Instant::now();
            while !thread_metrics_stop_flag.load(Ordering::Relaxed) {
                if next_poll >= tokio::time::Instant::now() {
                    tokio::time::sleep(next_poll - tokio::time::Instant::now()).await;
                }
                next_poll = tokio::time::Instant::now() + tokio::time::Duration::from_millis(300); // Should be smaller then the monitoring interval

                let response = match reqwest::get(&monitor_url).await {
                    Ok(response) => response,
                    Err(e) => {
                        error!("Error getting metrics: {:?}", e);
                        continue;
                    }
                };

                let tsv = match response.text().await {
                    Ok(csv) => csv,
                    Err(e) => {
                        error!("Error reading metrics line: {:?}", e);
                        continue;
                    }
                };

                let mut tsv_line_iter = tsv.lines().skip(1);

                let polled_metrics: Result<Vec<usize>, ParseIntError> = match tsv_line_iter.next() {
                    Some(metrics) => metrics
                        .trim()
                        .split("\t")
                        .map(|metric| metric.parse::<usize>())
                        .collect(),
                    None => {
                        error!("No metrics found in response");
                        continue;
                    }
                };

                match polled_metrics {
                    Ok(polled_metrics) => polled_metrics
                        .into_iter()
                        .zip(thread_metrics.iter())
                        .for_each(|(polled_metric, metric)| {
                            metric.store(polled_metric, Ordering::Relaxed);
                        }),
                    Err(e) => {
                        error!("Error parsing metrics: {:?}", e);
                        continue;
                    }
                };
            }
            Ok(())
        });

        let mut metrics_monitor = ProgressMonitor::new(
            "",
            metrics.clone(),
            vec![None; NUMBER_OF_COUNTERS],
            COUNTER_LABLES
                .iter()
                .rev()
                .map(|label| label.to_string())
                .collect(),
            None,
        )?;

        tokio::signal::ctrl_c().await?;

        metrics_stop_flag.store(true, Ordering::Relaxed);
        metrics_poll_taks.await??;

        metrics_monitor.stop().await?;

        Ok(())
    }
}
