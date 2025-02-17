// std imports
use std::{
    fs,
    io::Cursor,
    num::ParseIntError,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

// 3rd party imports
use anyhow::{anyhow, bail, Context, Result};
use axum::{
    extract::{DefaultBodyLimit, Multipart, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification;
use macpepdb::tools::{progress_monitor::ProgressMonitor, queue_monitor::QueueMonitor};
use signal_hook::{consts::SIGINT, iterator::Signals};
use tokio::{
    fs::{create_dir_all, remove_dir_all},
    sync::RwLock,
};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// local imports
use super::{
    configuration::{
        PipelineConfiguration, RemoteEntypointConfiguration, SearchParameters,
        StandaloneCometSearchConfiguration, StandaloneGoodnessAndRescoringConfiguration,
        StandaloneIndexingConfiguration, StandalonePreparationConfiguration,
        StandaloneSearchSpaceGenerationConfiguration,
    },
    convert::AsInputOutputQueueAndStorage,
    queue::{PipelineQueue, RedisPipelineQueue},
    storage::{PipelineStorage, RedisPipelineStorage},
};
use crate::{
    errors::axum::web_error::AnyhowWebError,
    io::{
        axum::multipart::write_streamed_file,
        comet::configuration::Configuration as CometConfiguration,
    },
    pipeline::{
        queue::PipelineQueueArc,
        search_manifest::SearchManifest,
        storage::{COUNTER_LABLES, NUMBER_OF_COUNTERS},
        tasks::{
            cleanup_task::CleanupTask, identification_task::IdentificationTask,
            indexing_task::IndexingTask, preparation_task::PreparationTask,
            scoring_task::ScoringTask, search_space_generation_task::SearchSpaceGenerationTask,
        },
    },
};

/// Shared state for the remote entrypoint service
///
struct EntrypointServiceState {
    index_queue: RedisPipelineQueue,
    preparation_queue: RedisPipelineQueue,
    search_space_generation_queue: RedisPipelineQueue,
    comet_search_queue: RedisPipelineQueue,
    goodness_and_rescoring_queue: RedisPipelineQueue,
    cleanup_queue: RedisPipelineQueue,
    storage: RwLock<RedisPipelineStorage>,
    work_dir: PathBuf,
}

/// Stucture to share pipline workload
///
#[derive(serde::Deserialize, serde::Serialize)]
struct PipelineWorkload {
    pub index_queue: usize,
    pub preparation_queue: usize,
    pub search_space_generation_queue: usize,
    pub comet_search_queue: usize,
    pub goodness_and_rescoring_queue: usize,
    pub cleanup_queue: usize,
}

impl PipelineWorkload {
    pub async fn new(
        index_queue: &impl PipelineQueue,
        preparation_queue: &impl PipelineQueue,
        search_space_generation_queue: &impl PipelineQueue,
        comet_search_queue: &impl PipelineQueue,
        goodness_and_rescoring_queue: &impl PipelineQueue,
        cleanup_queue: &impl PipelineQueue,
    ) -> Self {
        Self {
            index_queue: index_queue.len().await,
            preparation_queue: preparation_queue.len().await,
            search_space_generation_queue: search_space_generation_queue.len().await,
            comet_search_queue: comet_search_queue.len().await,
            goodness_and_rescoring_queue: goodness_and_rescoring_queue.len().await,
            cleanup_queue: cleanup_queue.len().await,
        }
    }
}

/// Pipeline to run the MaCcoyS identification pipeline
///
pub struct Pipeline;

impl Pipeline {
    /// Run the pipeline locally for each mzML file
    ///
    /// # Arguments
    /// * `result_dir` - Result directory where each search is stored
    /// * `tmp_dir` - Temporary directory for the pipeline
    /// * `config` - Configuration for the pipeline
    /// * `comet_config` - Configuration for Comet
    /// * `ptms` - Post translational modifications
    /// * `mzml_file_paths` - Paths to the mzML files to search
    ///
    pub async fn run_locally<Q: PipelineQueue + 'static, S: PipelineStorage + 'static>(
        result_dir: PathBuf,
        tmp_dir: PathBuf,
        config: PipelineConfiguration,
        mut comet_config: CometConfiguration,
        ptms: Vec<PostTranslationalModification>,
        mzml_file_paths: Vec<PathBuf>,
    ) -> Result<()> {
        let uuid = Uuid::new_v4().to_string();
        info!("UUID: {}", uuid);

        create_dir_all(&result_dir)
            .await
            .context("Could not create result directory")?;
        create_dir_all(&tmp_dir)
            .await
            .context("Could not create temporery directory")?;

        comet_config.set_ptms(&ptms, config.search_parameters.max_variable_modifications)?;
        comet_config.set_num_results(10000)?;

        let mut storage = S::new(&config.storage).await?;
        storage
            .init_search(
                &uuid,
                config.search_parameters.clone(),
                &ptms,
                &comet_config,
            )
            .await?;

        let storage = Arc::new(storage);

        // Create the queues
        let index_queue = Arc::new(Q::new(&config.index).await?);
        let preparation_queue = Arc::new(Q::new(&config.preparation).await?);
        let search_space_generation_queue =
            Arc::new(Q::new(&config.search_space_generation).await?);
        let comet_search_queue = Arc::new(Q::new(&config.comet_search).await?);
        let goodness_and_rescoreing_queue = Arc::new(Q::new(&config.goodness_and_rescoring).await?);
        let cleanup_queue = Arc::new(Q::new(&config.cleanup).await?);

        // Create the stop flags
        let index_stop_flag = Arc::new(AtomicBool::new(false));
        let preparation_stop_flag = Arc::new(AtomicBool::new(false));
        let search_space_generation_stop_flag = Arc::new(AtomicBool::new(false));
        let comet_search_stop_flag = Arc::new(AtomicBool::new(false));
        let goodness_and_rescoreing_stop_flag = Arc::new(AtomicBool::new(false));
        let cleanup_stop_flag = Arc::new(AtomicBool::new(false));
        let metrics_stop_flag = Arc::new(AtomicBool::new(false));

        // Metrics
        let metrics = (0..NUMBER_OF_COUNTERS)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect::<Vec<Arc<AtomicUsize>>>();

        let mut queue_monitor = QueueMonitor::new::<PipelineQueueArc<Q>>(
            "",
            vec![
                cleanup_queue.clone().into(),
                goodness_and_rescoreing_queue.clone().into(),
                comet_search_queue.clone().into(),
                search_space_generation_queue.clone().into(),
                preparation_queue.clone().into(),
                index_queue.clone().into(),
            ],
            vec![
                config.cleanup.queue_capacity as u64,
                config.goodness_and_rescoring.queue_capacity as u64,
                config.comet_search.queue_capacity as u64,
                config.search_space_generation.queue_capacity as u64,
                config.preparation.queue_capacity as u64,
                config.index.queue_capacity as u64,
            ],
            vec![
                "Cleanup".to_owned(),
                "Goodness and Rescoring".to_owned(),
                "Comet Search".to_owned(),
                "Search Space Generation".to_owned(),
                "Preparation".to_owned(),
                "Index".to_owned(),
            ],
            None,
        )?;

        let metrics_poll_taks = tokio::spawn(Self::poll_store_metrics_task(
            storage.clone(),
            uuid.clone(),
            metrics.clone(),
            metrics_stop_flag.clone(),
        ));

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

        let index_handler: tokio::task::JoinHandle<()> = {
            tokio::spawn(IndexingTask::start(
                result_dir.clone(),
                storage.clone(),
                index_queue.clone(),
                preparation_queue.clone(),
                index_stop_flag.clone(),
            ))
        };

        let preparation_handlers: Vec<tokio::task::JoinHandle<()>> =
            (0..config.preparation.num_tasks)
                .map(|_| {
                    tokio::spawn(PreparationTask::start(
                        storage.clone(),
                        preparation_queue.clone(),
                        search_space_generation_queue.clone(),
                        preparation_stop_flag.clone(),
                    ))
                })
                .collect();

        let search_space_generation_handlers: Vec<tokio::task::JoinHandle<()>> =
            (0..config.search_space_generation.num_tasks)
                .map(|_| {
                    tokio::spawn(SearchSpaceGenerationTask::start(
                        Arc::new(config.search_space_generation.clone()),
                        storage.clone(),
                        search_space_generation_queue.clone(),
                        comet_search_queue.clone(),
                        search_space_generation_stop_flag.clone(),
                    ))
                })
                .collect();

        let comet_search_handlers: Vec<tokio::task::JoinHandle<()>> =
            (0..config.comet_search.num_tasks)
                .map(|comet_proc_idx| {
                    let comet_tmp_dir = tmp_dir.join(format!("comet_{}", comet_proc_idx));
                    tokio::spawn(IdentificationTask::start(
                        comet_tmp_dir,
                        Arc::new(config.comet_search.clone()),
                        storage.clone(),
                        comet_search_queue.clone(),
                        goodness_and_rescoreing_queue.clone(),
                        comet_search_stop_flag.clone(),
                    ))
                })
                .collect();

        let goodness_and_resconfing_handlers: Vec<tokio::task::JoinHandle<()>> =
            (0..config.goodness_and_rescoring.num_tasks)
                .map(|_| {
                    tokio::spawn(ScoringTask::start(
                        storage.clone(),
                        goodness_and_rescoreing_queue.clone(),
                        cleanup_queue.clone(),
                        goodness_and_rescoreing_stop_flag.clone(),
                    ))
                })
                .collect();

        let cleanup_handlers: Vec<tokio::task::JoinHandle<()>> = (0..config.cleanup.num_tasks)
            .map(|_| {
                tokio::spawn(CleanupTask::start(
                    result_dir.clone(),
                    storage.clone(),
                    cleanup_queue.clone(),
                    cleanup_stop_flag.clone(),
                ))
            })
            .collect();

        for mzml_file_path in mzml_file_paths {
            let manifest = SearchManifest::new(
                uuid.clone(),
                mzml_file_path.file_name().unwrap().to_str().unwrap(),
            );

            tokio::fs::create_dir_all(manifest.get_ms_run_dir_path(&result_dir)).await?;
            tokio::fs::copy(&mzml_file_path, manifest.get_ms_run_mzml_path(&result_dir)).await?;

            match index_queue.push(manifest).await {
                Ok(_) => (),
                Err(e) => {
                    error!("[{}] Error pushing manifest to index queue.", &e.uuid);
                    continue;
                }
            }
        }

        index_stop_flag.store(true, Ordering::Relaxed);

        match index_handler.await {
            Ok(_) => (),
            Err(e) => {
                error!("Error joining index thread: {:?}", e);
            }
        }

        preparation_stop_flag.store(true, Ordering::Relaxed);

        for preparation_handler in preparation_handlers {
            match preparation_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining preparation thread: {:?}", e);
                }
            }
        }

        search_space_generation_stop_flag.store(true, Ordering::Relaxed);

        for search_space_generation_handler in search_space_generation_handlers {
            match search_space_generation_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining search space generation thread: {:?}", e);
                }
            }
        }

        comet_search_stop_flag.store(true, Ordering::Relaxed);

        for comet_search_handler in comet_search_handlers {
            match comet_search_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining comet search thread: {:?}", e);
                }
            }
        }

        goodness_and_rescoreing_stop_flag.store(true, Ordering::Relaxed);

        for goodness_and_resconfing_handler in goodness_and_resconfing_handlers {
            match goodness_and_resconfing_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining goodness and rescoring thread: {:?}", e);
                }
            }
        }

        cleanup_stop_flag.store(true, Ordering::Relaxed);

        for cleanup_handler in cleanup_handlers {
            match cleanup_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining cleanup thread: {:?}", e);
                }
            }
        }

        metrics_stop_flag.store(true, Ordering::Relaxed);
        metrics_poll_taks.await?;
        queue_monitor.stop().await?;
        metrics_monitor.stop().await?;

        let mut storage = match Arc::try_unwrap(storage) {
            Ok(storage) => storage,
            Err(_) => {
                bail!("Error unwrapping storage");
            }
        };
        storage.cleanup_search(&uuid).await?;

        remove_dir_all(&tmp_dir)
            .await
            .context("Could not delete temporary directory")?;

        Ok(())
    }

    /// Task to poll the metrcis from the storage to monitor them
    ///
    /// # Arguments
    /// * `storage` - Storage to access the metrics
    /// * `uuid` - UUID of the search
    /// * `metrics` - Metrics to store the values
    /// * `stop_flag` - Flag to indicate to stop polling
    ///
    async fn poll_store_metrics_task<S: PipelineStorage>(
        storage: Arc<S>,
        uuid: String,
        metrics: Vec<Arc<AtomicUsize>>,
        stop_flag: Arc<AtomicBool>,
    ) {
        loop {
            metrics[0].store(
                storage.get_started_searches_ctr(&uuid).await.unwrap_or(0),
                Ordering::Relaxed,
            );
            metrics[1].store(
                storage.get_prepared_ctr(&uuid).await.unwrap_or(0),
                Ordering::Relaxed,
            );
            metrics[2].store(
                storage
                    .get_search_space_generation_ctr(&uuid)
                    .await
                    .unwrap_or(0),
                Ordering::Relaxed,
            );
            metrics[3].store(
                storage.get_comet_search_ctr(&uuid).await.unwrap_or(0),
                Ordering::Relaxed,
            );
            metrics[4].store(
                storage
                    .get_goodness_and_rescoring_ctr(&uuid)
                    .await
                    .unwrap_or(0),
                Ordering::Relaxed,
            );
            metrics[5].store(
                storage.get_cleanup_ctr(&uuid).await.unwrap_or(0),
                Ordering::Relaxed,
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(750)).await;
            if stop_flag.load(Ordering::Relaxed) {
                break;
            }
        }
    }

    /// Enqueues a search to a remote server
    ///
    /// # Arguments
    /// * `base_url` - Base URL of the remote server
    /// * `search_parameters_path` - Path to the search parameters file
    /// * `comet_params_path` - Path to the Comet parameters file
    /// * `mzml_file_paths` - Paths to the mzML files
    /// * `ptms_path` - Optional path to the PTMs file
    ///
    pub async fn run_remotely(
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

    /// Starts a http service to submit and monitor searches
    /// and monitor the progress of the searches.
    ///
    /// # Arguments
    /// * `interface` - Interface to bind the service to
    /// * `port` - Port to bind the service to
    /// * `work_dir` - Work directory where the results are stored
    /// * `config` - Configuration for the remote entrypoint
    ///
    pub async fn start_remote_entrypoint(
        interface: String,
        port: u16,
        work_dir: PathBuf,
        config: RemoteEntypointConfiguration,
    ) -> Result<()> {
        let index_queue = RedisPipelineQueue::new(&config.index).await?;
        let preparation_queue = RedisPipelineQueue::new(&config.preparation).await?;
        let search_space_generation_queue =
            RedisPipelineQueue::new(&config.search_space_generation).await?;
        let comet_search_queue = RedisPipelineQueue::new(&config.comet_search).await?;
        let goodness_and_rescoring_queue =
            RedisPipelineQueue::new(&config.goodness_and_rescoring).await?;
        let cleanup_queue = RedisPipelineQueue::new(&config.cleanup).await?;
        let storage = RwLock::new(RedisPipelineStorage::new(&config.storage).await?);

        let state = Arc::new(EntrypointServiceState {
            index_queue,
            preparation_queue,
            search_space_generation_queue,
            comet_search_queue,
            goodness_and_rescoring_queue,
            cleanup_queue,
            storage,
            work_dir,
        });

        // Build our application with route
        let app = Router::new()
            .route(
                "/api/pipeline/enqueue",
                post(Self::remote_entrypoint_enqueue_endpoint),
            )
            .route(
                "/api/pipeline/monitor/:uuid",
                get(Self::remote_entrypoint_monitor_endpoint),
            )
            .route(
                "/api/pipeline/queues",
                get(Self::remote_entrypoint_queue_monitor),
            )
            .layer(DefaultBodyLimit::disable())
            .with_state(state);

        let listener = tokio::net::TcpListener::bind(format!("{}:{}", interface, port)).await?;
        tracing::info!("ready for connections, listening on {}", interface);
        axum::serve(listener, app).await.unwrap();

        Ok(())
    }

    /// Entrypoint for submitting new searches. Returns the UUID of the search.
    ///
    /// # Arguments
    /// * `state` - Application state containing the storage, work directory and index queue
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/pipeline/enqueue`
    /// * Method: `POST`
    /// * Content-Type: `multipart/form-data`
    ///
    /// ### Body
    /// * `mzml_*` - Every fields starting with `mzml_` is considered as a mzML file
    /// * `search_parameters` - Search parameters in TOML format (section `search_parameters` from the configuration file without the section name)
    /// * `comet_params` - Comet parameter file
    /// * `ptms` - CSV file containing PTMs
    ///
    /// ## Response
    /// ```
    /// ae2439d1-7940-4b43-b96e-444a1e99e78d
    /// ```
    ///
    async fn remote_entrypoint_enqueue_endpoint(
        State(state): State<Arc<EntrypointServiceState>>,
        mut payload: Multipart,
    ) -> Result<Response, AnyhowWebError> {
        // Manifest files
        let uuid = Uuid::new_v4().to_string();
        let mut search_params: Option<SearchParameters> = None;
        let mut comet_params: Option<CometConfiguration> = None;
        let mut ptms: Vec<PostTranslationalModification> = Vec::new();
        let mut manifests: Vec<SearchManifest> = Vec::new();

        while let Ok(Some(field)) = payload.next_field().await {
            let field_name = match field.name() {
                Some(name) => name,
                None => return Err(anyhow!("Field has no name").into()),
            };

            if field_name.starts_with("mzml_") {
                let file_name = match field.file_name() {
                    Some(file_name) => file_name.to_string(),
                    None => continue,
                };
                let manifest = SearchManifest::new(uuid.clone(), &file_name);
                tokio::fs::create_dir_all(&manifest.get_ms_run_dir_path(&state.work_dir)).await?;

                write_streamed_file(&manifest.get_ms_run_mzml_path(&state.work_dir), field).await?;
                manifests.push(manifest);
                continue;
            }

            if field_name == "search_params" {
                search_params = match &field.text().await {
                    Ok(text) => {
                        Some(toml::from_str(text).context("Parsing search parameters TOML")?)
                    }
                    Err(err) => {
                        return Err(anyhow!("Error reading search params field: {:?}", err).into())
                    }
                };
                continue;
            }

            if field_name == "comet_params" {
                comet_params = match &field.text().await {
                    Ok(text) => Some(CometConfiguration::new(text.to_string())?),
                    Err(err) => {
                        return Err(anyhow!("Error reading Comet params field: {:?}", err).into())
                    }
                };
                continue;
            }

            // Optional PTMs
            if field_name == "ptms" {
                let ptm_csv = match field.bytes().await {
                    Ok(csv) => Cursor::new(csv),
                    Err(err) => return Err(anyhow!("Error reading PTMs field: {:?}", err).into()),
                };

                let reader = csv::ReaderBuilder::new()
                    .has_headers(true)
                    .delimiter(b',')
                    .from_reader(ptm_csv);

                ptms = reader
                    .into_deserialize::<PostTranslationalModification>()
                    .map(|ptm_result| match ptm_result {
                        Ok(ptm) => Ok(ptm),
                        Err(e) => Err(anyhow::Error::new(e)),
                    })
                    .collect::<Result<Vec<PostTranslationalModification>>>()?;
            }
        }

        if ptms.is_empty() {
            warn!("Not PTMs submitted, which is unusual")
        }

        for ptm in ptms.iter() {
            debug!(
                "PTM: {}, {}, {:?}, {:?}",
                ptm.get_amino_acid().get_code(),
                ptm.get_mass_delta(),
                ptm.get_position(),
                ptm.get_mod_type()
            );
        }

        if manifests.is_empty() {
            return Err(anyhow!("No mzML files uploaded").into());
        }

        let search_params = match search_params {
            Some(search_params) => search_params,
            None => {
                tokio::fs::remove_dir_all(manifests[0].get_search_dir(&state.work_dir)).await?;
                return Err(anyhow!("Search parameters not uploaded").into());
            }
        };

        let mut comet_params = match comet_params {
            Some(comet_params) => comet_params,
            None => {
                tokio::fs::remove_dir_all(manifests[0].get_search_dir(&state.work_dir)).await?;
                return Err(anyhow!("Comet parameters not uploaded").into());
            }
        };

        comet_params.set_num_results(10000)?;
        comet_params.set_ptms(&ptms, search_params.max_variable_modifications)?;

        // Init new search
        match state
            .storage
            .write()
            .await
            .init_search(&uuid, search_params, &ptms, &comet_params)
            .await
        {
            Ok(_) => (),
            Err(e) => {
                tokio::fs::remove_dir_all(manifests[0].get_search_dir(&state.work_dir)).await?;
                return Err(anyhow!("Error initializing search: {:?}", e).into());
            }
        }

        // Start enqueuing manifest
        for mut manifest in manifests.into_iter() {
            loop {
                manifest = match state.index_queue.push(manifest).await {
                    Ok(_) => break,
                    Err(errored_manifest) => {
                        error!("Error pushing manifest to index queue");
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        errored_manifest
                    }
                }
            }
        }

        Ok((StatusCode::OK, uuid).into_response())
    }

    /// Entrypoint for monitoring the progress of a search
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/pipeline/monitor/:uuid`
    /// * Method: `GET`
    ///
    /// ## Response
    /// RSV with metrics
    /// ```tsv
    /// started_searches        prepared        search_space_generation comet_search    goodness_and_rescoring  cleanup
    /// 13852   10357   286     64      47      47
    /// ```
    ///
    async fn remote_entrypoint_monitor_endpoint(
        State(state): State<Arc<EntrypointServiceState>>,
        axum::extract::Path(uuid): axum::extract::Path<String>,
    ) -> Result<Response<String>, AnyhowWebError> {
        let counters: Vec<usize> = {
            let storage = state.storage.read().await;
            vec![
                storage.get_started_searches_ctr(&uuid).await?,
                storage.get_prepared_ctr(&uuid).await?,
                storage.get_search_space_generation_ctr(&uuid).await?,
                storage.get_comet_search_ctr(&uuid).await?,
                storage.get_goodness_and_rescoring_ctr(&uuid).await?,
                storage.get_cleanup_ctr(&uuid).await?,
            ]
        };

        // Start with column names
        let mut csv = COUNTER_LABLES
            .iter()
            .map(|label| label.to_string())
            .collect::<Vec<String>>()
            .join("\t");
        // add a new line
        csv.push('\n');
        // Add the counters
        csv.push_str(
            &counters
                .into_iter()
                .map(|ctr| format!("{}", ctr))
                .collect::<Vec<String>>()
                .join("\t"),
        );

        let response = axum::response::Response::builder()
            .header("Content-Type", "text/tab-separated-values")
            .status(StatusCode::OK)
            .body(csv)?;

        Ok(response)
    }

    /// Entrypoint for monitoring the queue occupation
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/pipelines/queues`
    /// * Method: `GET`
    ///
    /// ## Response
    /// CSV with metrics
    /// ```tsv
    /// ```
    ///
    async fn remote_entrypoint_queue_monitor(
        State(state): State<Arc<EntrypointServiceState>>,
    ) -> Result<Response<String>, AnyhowWebError> {
        let workload = PipelineWorkload::new(
            &state.index_queue,
            &state.preparation_queue,
            &state.search_space_generation_queue,
            &state.comet_search_queue,
            &state.goodness_and_rescoring_queue,
            &state.cleanup_queue,
        )
        .await;

        let response = axum::response::Response::builder()
            .header("Content-Type", "application/json")
            .status(StatusCode::OK)
            .body(serde_json::to_string(&workload)?)?;

        Ok(response)
    }

    pub async fn standalone_indexing(work_dir: PathBuf, config_file_path: PathBuf) -> Result<()> {
        let config: StandaloneIndexingConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let (storage, input_queue, output_queue) =
            config.as_input_output_queue_and_storage().await?;
        let storage = Arc::new(storage);
        let input_queue = Arc::new(input_queue);
        let output_queue = Arc::new(output_queue);

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.index.num_tasks)
            .map(|_| {
                tokio::spawn(IndexingTask::start(
                    work_dir.clone(),
                    storage.clone(),
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

    pub async fn standalone_preparation(config_file_path: PathBuf) -> Result<()> {
        let config: StandalonePreparationConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let (storage, input_queue, output_queue) =
            config.as_input_output_queue_and_storage().await?;
        let storage = Arc::new(storage);
        let input_queue = Arc::new(input_queue);
        let output_queue = Arc::new(output_queue);

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.preparation.num_tasks)
            .map(|_| {
                tokio::spawn(PreparationTask::start(
                    storage.clone(),
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

    pub async fn standalone_search_space_generation(config_file_path: PathBuf) -> Result<()> {
        let config: StandaloneSearchSpaceGenerationConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let (storage, input_queue, output_queue) =
            config.as_input_output_queue_and_storage().await?;
        let storage = Arc::new(storage);
        let input_queue = Arc::new(input_queue);
        let output_queue = Arc::new(output_queue);

        let search_space_generation_config = Arc::new(config.search_space_generation.clone());

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> =
            (0..config.search_space_generation.num_tasks)
                .map(|_| {
                    tokio::spawn(SearchSpaceGenerationTask::start(
                        search_space_generation_config.clone(),
                        storage.clone(),
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

    pub async fn standalone_comet_search(
        local_work_dir: PathBuf,
        config_file_path: PathBuf,
    ) -> Result<()> {
        let config: StandaloneCometSearchConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let (storage, input_queue, output_queue) =
            config.as_input_output_queue_and_storage().await?;
        let storage = Arc::new(storage);
        let input_queue = Arc::new(input_queue);
        let output_queue = Arc::new(output_queue);

        let comet_search_config = Arc::new(config.comet_search.clone());

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.comet_search.num_tasks)
            .map(|comet_proc_idx| {
                let comet_tmp_dir = local_work_dir.join(format!("comet_{}", comet_proc_idx));
                tokio::spawn(IdentificationTask::start(
                    comet_tmp_dir,
                    comet_search_config.clone(),
                    storage.clone(),
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

    pub async fn standalone_goodness_and_rescoring(config_file_path: PathBuf) -> Result<()> {
        let config: StandaloneGoodnessAndRescoringConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let (storage, input_queue, output_queue) =
            config.as_input_output_queue_and_storage().await?;
        let storage = Arc::new(storage);
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
                        storage.clone(),
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

    pub async fn standalone_cleanup(work_dir: PathBuf, config_file_path: PathBuf) -> Result<()> {
        let config: StandaloneGoodnessAndRescoringConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;
        let storage = Arc::new(RedisPipelineStorage::new(&config.storage).await?);
        let input_queue = Arc::new(RedisPipelineQueue::new(&config.cleanup).await?);

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.cleanup.num_tasks)
            .map(|_| {
                tokio::spawn(CleanupTask::start(
                    work_dir.clone(),
                    storage.clone(),
                    input_queue.clone(),
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
