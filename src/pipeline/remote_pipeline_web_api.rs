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
        tasks::task::Task,
    },
    web::web_error::WebError,
};

use super::{
    configuration::{RemoteEntypointConfiguration, SearchParameters},
    errors::queue_error::QueueError,
    messages::{
        error_message::ErrorMessage, identification_message::IdentificationMessage,
        indexing_message::IndexingMessage, publication_message::PublicationMessage,
        scoring_message::ScoringMessage,
        search_space_generation_message::SearchSpaceGenerationMessage,
    },
    queue::PipelineQueue,
    tasks::{
        error_task::ErrorTask, identification_task::IdentificationTask,
        indexing_task::IndexingTask, publication_task::PublicationTask, scoring_task::ScoringTask,
        search_space_generation_task::SearchSpaceGenerationTask,
    },
    utils::create_file_path_on_ms_run_level,
};

/// Shared state for the remote entrypoint service
///
struct EntrypointServiceState {
    index_queue: RedisPipelineQueue<IndexingMessage>,
    search_space_generation_queue: RedisPipelineQueue<SearchSpaceGenerationMessage>,
    identification_queue: RedisPipelineQueue<IdentificationMessage>,
    scoring_queue: RedisPipelineQueue<ScoringMessage>,
    publication_queue: RedisPipelineQueue<PublicationMessage>,
    error_queue: RedisPipelineQueue<ErrorMessage>,
    storage: RwLock<RedisPipelineStorage>,
    work_dir: PathBuf,
    prometheus_base_url: String,
}

/// Struct to share pipeline workload
///
#[derive(serde::Deserialize, serde::Serialize)]
struct PipelineWorkload {
    pub index_queue: usize,
    pub search_space_generation_queue: usize,
    pub identification_queue: usize,
    pub scoring_queue: usize,
    pub publication_queue: usize,
    pub error_queue: usize,
}

impl PipelineWorkload {
    pub async fn new(
        index_queue: &impl PipelineQueue<IndexingMessage>,
        search_space_generation_queue: &impl PipelineQueue<SearchSpaceGenerationMessage>,
        identification_queue: &impl PipelineQueue<IdentificationMessage>,
        scoring_queue: &impl PipelineQueue<ScoringMessage>,
        publication_queue: &impl PipelineQueue<PublicationMessage>,
        error_queue: &impl PipelineQueue<ErrorMessage>,
    ) -> Self {
        Self {
            index_queue: index_queue.len().await.unwrap_or(0),
            search_space_generation_queue: search_space_generation_queue.len().await.unwrap_or(0),
            identification_queue: identification_queue.len().await.unwrap_or(0),
            scoring_queue: scoring_queue.len().await.unwrap_or(0),
            publication_queue: publication_queue.len().await.unwrap_or(0),
            error_queue: error_queue.len().await.unwrap_or(0),
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
        let search_space_generation_queue =
            RedisPipelineQueue::new(&config.search_space_generation).await?;
        let identification_queue = RedisPipelineQueue::new(&config.identification).await?;
        let scoring_queue = RedisPipelineQueue::new(&config.scoring).await?;
        let publication_queue = RedisPipelineQueue::new(&config.publication).await?;
        let error_queue = RedisPipelineQueue::new(&config.error).await?;
        let storage = RwLock::new(RedisPipelineStorage::new(&config.storage).await?);

        let state: Arc<EntrypointServiceState> = Arc::new(EntrypointServiceState {
            index_queue,
            search_space_generation_queue,
            identification_queue,
            scoring_queue,
            publication_queue,
            error_queue,
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
        let mut mzml_file_names: Vec<String> = Vec::new();

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

                let mzml_path = state
                    .work_dir
                    .join(create_file_path_on_ms_run_level(&uuid, &file_name, "mzML"));

                tokio::fs::create_dir_all(&mzml_path.parent().unwrap()).await?;

                write_streamed_file(&mzml_path, field).await?;
                mzml_file_names.push(file_name);
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

        if mzml_file_names.is_empty() {
            return Err(anyhow!("No mzML files uploaded").into());
        }

        let search_params = match search_params {
            Some(search_params) => search_params,
            None => {
                tokio::fs::remove_dir_all(&state.work_dir.join(&uuid)).await?;
                return Err(anyhow!("Search parameters not uploaded").into());
            }
        };

        let mut comet_params = match comet_params {
            Some(comet_params) => comet_params,
            None => {
                tokio::fs::remove_dir_all(&state.work_dir.join(&uuid)).await?;
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
                tokio::fs::remove_dir_all(&state.work_dir.join(&uuid)).await?;
                return Err(anyhow!("Error initializing search: {:?}", e).into());
            }
        }

        let mut indexing_message = IndexingMessage::new(uuid.clone(), mzml_file_names);
        loop {
            indexing_message = match state.index_queue.push(indexing_message).await {
                Ok(_) => break,
                Err((err, errored_message)) => match err {
                    QueueError::QueueFullError => *errored_message,
                    _ => {
                        error!("{}", err);
                        break;
                    }
                },
            };
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
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
                &ErrorTask::get_counter_name(&uuid),
            ),
            Self::get_prometheus_counter_rate(
                &state.prometheus_base_url,
                &PublicationTask::get_counter_name(&uuid),
            ),
            Self::get_prometheus_counter_rate(
                &state.prometheus_base_url,
                &ScoringTask::get_counter_name(&uuid),
            ),
            Self::get_prometheus_counter_rate(
                &state.prometheus_base_url,
                &IdentificationTask::get_counter_name(&uuid),
            ),
            Self::get_prometheus_counter_rate(
                &state.prometheus_base_url,
                &SearchSpaceGenerationTask::get_counter_name(&uuid),
            ),
            Self::get_prometheus_counter_rate(
                &state.prometheus_base_url,
                &IndexingTask::get_counter_name(&uuid),
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
            &state.search_space_generation_queue,
            &state.identification_queue,
            &state.scoring_queue,
            &state.publication_queue,
            &state.error_queue,
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
