use std::{io::Cursor, path::PathBuf, sync::Arc};

use anyhow::{anyhow, Context, Result};
use axum::{
    extract::{DefaultBodyLimit, Multipart, State},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification;
use http::StatusCode;
use macpepdb::functions::post_translational_modification::PTMCollection;
use serde_json::Value as JsonValue;
use tokio::sync::RwLock;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::{
    errors::axum::web_error::AnyhowWebError,
    io::axum::multipart::write_streamed_file,
    pipeline::{
        queue::RedisPipelineQueue,
        storage::{PipelineStorage, RedisPipelineStorage},
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
}

/// Struct to share pipeline workload
///
#[derive(serde::Deserialize, serde::Serialize)]
pub struct QueueResponse {
    index_queue: usize,
    search_space_generation_queue: usize,
    identification_queue: usize,
    scoring_queue: usize,
    publication_queue: usize,
    error_queue: usize,
}

impl QueueResponse {
    pub async fn new(
        index_queue: &impl PipelineQueue<IndexingMessage>,
        search_space_generation_queue: &impl PipelineQueue<SearchSpaceGenerationMessage>,
        identification_queue: &impl PipelineQueue<IdentificationMessage>,
        scoring_queue: &impl PipelineQueue<ScoringMessage>,
        publication_queue: &impl PipelineQueue<PublicationMessage>,
        error_queue: &impl PipelineQueue<ErrorMessage>,
    ) -> Result<Self, QueueError> {
        Ok(Self {
            index_queue: index_queue.len().await?,
            search_space_generation_queue: search_space_generation_queue.len().await?,
            identification_queue: identification_queue.len().await?,
            scoring_queue: scoring_queue.len().await?,
            publication_queue: publication_queue.len().await?,
            error_queue: error_queue.len().await?,
        })
    }

    pub fn index_queue(&self) -> usize {
        self.index_queue
    }

    pub fn search_space_generation_queue(&self) -> usize {
        self.search_space_generation_queue
    }

    pub fn identification_queue(&self) -> usize {
        self.identification_queue
    }

    pub fn scoring_queue(&self) -> usize {
        self.scoring_queue
    }

    pub fn publication_queue(&self) -> usize {
        self.publication_queue
    }

    pub fn error_queue(&self) -> usize {
        self.error_queue
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct ProgressRepsonse {
    total_spectrum_count: u64,
    finished_spectrum_count: u64,
    is_search_fully_enqueued: bool,
}

impl ProgressRepsonse {
    pub fn new(
        total_spectrum_count: u64,
        finished_spectrum_count: u64,
        is_search_fully_enqueued: bool,
    ) -> Self {
        Self {
            total_spectrum_count,
            finished_spectrum_count,
            is_search_fully_enqueued,
        }
    }

    pub fn total_spectrum_count(&self) -> u64 {
        self.total_spectrum_count
    }

    pub fn finished_spectrum_count(&self) -> u64 {
        self.finished_spectrum_count
    }

    pub fn is_search_fully_enqueued(&self) -> bool {
        self.is_search_fully_enqueued
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
    /// * `config` - Configuration for the remote entrypoint
    ///
    pub async fn start(
        interface: String,
        port: u16,
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
            work_dir: config.work_directory.clone(),
        });

        // Build our application with route
        let app = Router::new()
            .route("/api/pipeline/enqueue", post(Self::enqueue))
            .route("/api/pipeline/queues", get(Self::queue_monitor))
            .route("/api/pipeline/progress/:uuid", get(Self::progress_monitor))
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
    /// * `xcorr_config` - Comet parameter file
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

        if !ptms.is_empty() {
            // validate PTMs
            let _ =
                PTMCollection::new(&ptms).map_err(|e| anyhow!("Error validating PTMs: {:?}", e))?;
        } else {
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

        // Init new search
        match state
            .storage
            .write()
            .await
            .init_search(&uuid, search_params, &ptms)
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

    /// Entrypoint for monitoring the queue occupation
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/pipelines/queues`
    /// * Method: `GET`
    ///
    /// ## Response
    /// ```json
    /// {
    ///  "index_queue": 0,
    ///  "search_space_generation_queue": 0,
    ///  ...
    /// }
    /// ```
    ///
    async fn queue_monitor(
        State(state): State<Arc<EntrypointServiceState>>,
    ) -> Result<Json<JsonValue>, WebError> {
        let response = QueueResponse::new(
            &state.index_queue,
            &state.search_space_generation_queue,
            &state.identification_queue,
            &state.scoring_queue,
            &state.publication_queue,
            &state.error_queue,
        )
        .await
        .map_err(|e| {
            WebError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Error getting queue lengths: {:?}", e),
            )
        })?;

        Ok(Json(serde_json::to_value(response)?))
    }

    /// Entrypoint for monitoring the progress of a search
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/pipeline/progress/:uuid`
    /// * Method: `GET`
    ///
    /// ## Response
    ///
    /// ```tsv
    /// {
    ///  "total_spectrum_count": 100,
    ///  "finished_spectrum_count": 50,
    ///  "is_search_fully_enqueued": false
    /// }
    /// ```
    ///
    async fn progress_monitor(
        State(state): State<Arc<EntrypointServiceState>>,
        axum::extract::Path(uuid): axum::extract::Path<String>,
    ) -> Result<Json<JsonValue>, WebError> {
        let storage = state.storage.read().await;
        let response = ProgressRepsonse::new(
            storage.get_total_spectrum_count(&uuid).await.map_err(|e| {
                WebError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Error getting total spectrum count: {:?}", e),
                )
            })?,
            storage
                .get_finished_spectrum_count(&uuid)
                .await
                .map_err(|e| {
                    WebError::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Error getting finished spectrum count: {:?}", e),
                    )
                })?,
            storage.is_search_fully_enqueued(&uuid).await.map_err(|e| {
                WebError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Error getting total spectrum count: {:?}", e),
                )
            })?,
        );

        Ok(Json(serde_json::to_value(response)?))
    }
}
