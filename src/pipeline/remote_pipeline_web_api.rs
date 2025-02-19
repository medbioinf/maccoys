use std::{io::Cursor, path::PathBuf, sync::Arc};

use anyhow::{anyhow, Context, Result};
use axum::{
    extract::{DefaultBodyLimit, Multipart, State},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification;
use futures::future::join_all;
use http::StatusCode;
use tokio::sync::RwLock;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::{
    errors::axum::web_error::AnyhowWebError,
    io::{
        axum::multipart::write_streamed_file,
        comet::configuration::Configuration as CometConfiguration,
    },
    pipeline::{
        queue::RedisPipelineQueue,
        storage::{PipelineStorage, RedisPipelineStorage},
        tasks::{cleanup_task::CleanupTask, task::Task},
    },
    web::web_error::WebError,
};

use super::{
    configuration::{RemoteEntypointConfiguration, SearchParameters},
    queue::PipelineQueue,
    search_manifest::SearchManifest,
    tasks::{
        identification_task::IdentificationTask, indexing_task::IndexingTask,
        preparation_task::PreparationTask, scoring_task::ScoringTask,
        search_space_generation_task::SearchSpaceGenerationTask,
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
    prometheus_base_url: String,
}

/// Struct to share pipeline workload
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

/// Web api to enqueue search in remote pipeline and receive results
///
pub struct RemotePipelineWebApi {}

impl RemotePipelineWebApi {
    /// Starts a http service to submit and monitor searches
    /// and monitor the progress of the searches.
    ///
    /// # Arguments
    /// * `interface` - Interface to bind the service to
    /// * `port` - Port to bind the service to
    /// * `work_dir` - Work directory where the results are stored
    /// * `config` - Configuration for the remote entrypoint
    ///
    pub async fn start(
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

        let state: Arc<EntrypointServiceState> = Arc::new(EntrypointServiceState {
            index_queue,
            preparation_queue,
            search_space_generation_queue,
            comet_search_queue,
            goodness_and_rescoring_queue,
            cleanup_queue,
            storage,
            work_dir,
            prometheus_base_url: config.prometheus_base_url.clone(),
        });

        // Build our application with route
        let app = Router::new()
            .route("/api/pipeline/enqueue", post(Self::enqueue))
            .route("/api/pipeline/monitor/:uuid", get(Self::monitor))
            .route("/api/pipeline/queues", get(Self::queue_monitor))
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
    async fn enqueue(
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
    async fn monitor(
        State(state): State<Arc<EntrypointServiceState>>,
        axum::extract::Path(uuid): axum::extract::Path<String>,
    ) -> Result<(StatusCode, String), WebError> {
        let counters: Vec<f64> = join_all(vec![
            Self::get_prometheus_counter_rate(
                &state.prometheus_base_url,
                &CleanupTask::get_counter_name(&uuid),
            ),
            Self::get_prometheus_counter_rate(
                &state.prometheus_base_url,
                &IdentificationTask::get_counter_name(&uuid),
            ),
            Self::get_prometheus_counter_rate(
                &state.prometheus_base_url,
                &IndexingTask::get_counter_name(&uuid),
            ),
            Self::get_prometheus_counter_rate(
                &state.prometheus_base_url,
                &PreparationTask::get_counter_name(&uuid),
            ),
            Self::get_prometheus_counter_rate(
                &state.prometheus_base_url,
                &ScoringTask::get_counter_name(&uuid),
            ),
            Self::get_prometheus_counter_rate(
                &state.prometheus_base_url,
                &SearchSpaceGenerationTask::get_counter_name(&uuid),
            ),
        ])
        .await;

        match serde_json::to_string(&counters) {
            Ok(counters) => Ok((StatusCode::OK, counters)),
            Err(e) => Err(anyhow!("Error serializing counters: {:?}", e).into()),
        }
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
    async fn queue_monitor(
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

    async fn get_prometheus_counter_rate(prometheus_url: &str, metric_name: &str) -> f64 {
        let url = format!("{}/api/v1/query", prometheus_url);
        let from =
            reqwest::multipart::Form::new().text("query", format!("rate({}[5m])", metric_name));
        let response = reqwest::Client::new()
            .post(&url)
            .multipart(from)
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(e) => {
                error!("Error getting Prometheus counter rate: {:?}", e);
                return -1.0;
            }
        };
        if !response.status().is_success() {
            error!(
                "Error getting Prometheus counter rate: {:?}",
                response.text().await
            );
            return -1.0;
        }
        let data = match response.json::<serde_json::Value>().await {
            Ok(data) => data,
            Err(e) => {
                error!("Error parsing Prometheus counter rate: {:?}", e);
                return -1.0;
            }
        };

        let mut data = data.get("data");
        if data.is_none() {
            error!("Error parsing Prometheus counter rate: {:?}", data);
            return -1.0;
        }

        data = data.unwrap().get("result");
        if data.is_none() {
            error!("Error parsing Prometheus counter rate: {:?}", data);
            return -1.0;
        }

        data = data.unwrap().get(0);
        if data.is_none() {
            error!("Error parsing Prometheus counter rate: {:?}", data);
            return -1.0;
        }

        data = data.unwrap().get("value");
        if data.is_none() {
            error!("Error parsing Prometheus counter rate: {:?}", data);
            return -1.0;
        }

        let data: (f64, f64) = match serde_json::from_value(data.unwrap().clone()) {
            Ok(data) => data,
            Err(e) => {
                error!("Error parsing Prometheus counter rate: {:?}", e);
                return -1.0;
            }
        };

        // Prometheus is queried for the last 5 minutes, so we need to divide by 300 to get the rate per second
        data.1 / 300.0
    }
}
