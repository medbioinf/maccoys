use std::{io::Cursor, ops::Deref, path::PathBuf, sync::Arc};

use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Multipart, State},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use http::StatusCode;
use macpepdb::{
    post_translational_modification::{PTMCollection, PostTranslationalModification},
    web::DEFAULT_ERROR_HEADER_MAP,
};
use serde_json::Value as JsonValue;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::{
    io::axum::multipart::write_streamed_file,
    pipeline::{
        queue::HttpPipelineQueue,
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

/// Loki tracing label value for the remote pipeline web api
///
static LOKI_TRACING_LABEL_VALUE: &str = "remote_pipeline_web_api";

#[derive(Debug, Error)]
pub enum Error {
    #[error("Error creating directory `{0}`: {1}")]
    CreateDir(PathBuf, Box<tokio::io::Error>),
    #[error("Unbable to decode received PTMs: {0}")]
    DecodeModificationsField(Box<axum::extract::multipart::MultipartError>),
    #[error("Unbable to decode received search parameters: {0}")]
    DecodeSearchParameterField(Box<axum::extract::multipart::MultipartError>),
    #[error("Unable to deserialize modifications: {0}")]
    DeserializeModifications(Box<csv::Error>),
    #[error("Unable to deserialize search parameters: {0}")]
    DeserializeSearchParameter(Box<toml::de::Error>),
    #[error("Error writing streamed file: {0}")]
    FileUploadStream(Box<anyhow::Error>),
    #[error("Missing request field name")]
    MissingRequestFieldName,
    #[error("No mzML file names")]
    NoMzMLFileNames,
    #[error("No search parameters uploaded")]
    NoSearchParameters,
    #[error("PTMCollection error in remote pipeline API: {0}")]
    ModificationCollection(Box<macpepdb::post_translational_modification::Error>),
    #[error("Queue error in remote pipeline web API: {0}")]
    QueueError(Box<crate::pipeline::errors::queue_error::QueueError>),
    #[error("Unable to remove directory {0}`: {1}")]
    RemoveDir(PathBuf, Box<tokio::io::Error>),
    #[error("Stroage error in remote pipeline web API: {0}")]
    StorageError(Box<crate::pipeline::errors::storage_error::StorageError>),
    #[error("Error binding TCP listener to {0}:{1}: {2}")]
    TcpListenerBind(String, u16, Box<tokio::io::Error>),
}

into_thiserror_boxed!(csv::Error, Error, DeserializeModifications);
into_thiserror_boxed!(toml::de::Error, Error, DeserializeSearchParameter);
into_thiserror_boxed!(
    macpepdb::post_translational_modification::Error,
    Error,
    ModificationCollection
);
into_thiserror_boxed!(
    crate::pipeline::errors::queue_error::QueueError,
    Error,
    QueueError
);
into_thiserror_boxed!(
    crate::pipeline::errors::storage_error::StorageError,
    Error,
    StorageError
);

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Error::MissingRequestFieldName => (
                StatusCode::UNPROCESSABLE_ENTITY,
                DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                Body::from(self.to_string()),
            )
                .into_response(),
            _ => {
                let uuid = uuid::Uuid::now_v7();
                tracing::error!("[{uuid}] {self}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    DEFAULT_ERROR_HEADER_MAP.deref().clone(),
                    Body::from(format!("Internal server error. Contact the admin and provide this UUID `{uuid}` to help identifying the error.")),
                )
                .into_response()
            }
        }
    }
}

/// Shared state for the remote entrypoint service
///
struct EntrypointServiceState {
    index_queue: HttpPipelineQueue<IndexingMessage>,
    search_space_generation_queue: HttpPipelineQueue<SearchSpaceGenerationMessage>,
    identification_queue: HttpPipelineQueue<IdentificationMessage>,
    scoring_queue: HttpPipelineQueue<ScoringMessage>,
    publication_queue: HttpPipelineQueue<PublicationMessage>,
    error_queue: HttpPipelineQueue<ErrorMessage>,
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
    ) -> Result<(), Error> {
        let index_queue = HttpPipelineQueue::new(&config.index).await?;
        let search_space_generation_queue =
            HttpPipelineQueue::new(&config.search_space_generation).await?;
        let identification_queue = HttpPipelineQueue::new(&config.identification).await?;
        let scoring_queue = HttpPipelineQueue::new(&config.scoring).await?;
        let publication_queue = HttpPipelineQueue::new(&config.publication).await?;
        let error_queue = HttpPipelineQueue::new(&config.error).await?;
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

        let listener = tokio::net::TcpListener::bind(format!("{}:{}", interface, port))
            .await
            .map_err(|err| Error::TcpListenerBind(interface.clone(), port, Box::new(err)))?;
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
    ) -> Result<Response, Error> {
        // Manifest files
        let uuid = Uuid::new_v4().to_string();
        let mut search_params: Option<SearchParameters> = None;
        let mut ptms: Vec<Arc<PostTranslationalModification>> = Vec::new();
        let mut mzml_file_names: Vec<String> = Vec::new();

        while let Ok(Some(field)) = payload.next_field().await {
            let field_name = field.name().ok_or(Error::MissingRequestFieldName)?;

            if field_name.starts_with("mzml_") {
                let file_name = match field.file_name() {
                    Some(file_name) => file_name.to_string(),
                    None => continue,
                };

                let mzml_path = state
                    .work_dir
                    .join(create_file_path_on_ms_run_level(&uuid, &file_name, "mzML"));

                tokio::fs::create_dir_all(&mzml_path.parent().unwrap())
                    .await
                    .map_err(|err| {
                        Error::CreateDir(mzml_path.parent().unwrap().into(), Box::new(err))
                    })?;

                write_streamed_file(&mzml_path, field)
                    .await
                    .map_err(|err| Error::FileUploadStream(Box::new(err)))?;
                mzml_file_names.push(file_name);
                continue;
            }

            if field_name == "search_params" {
                search_params = field
                    .text()
                    .await
                    .map(|text| {
                        toml::from_str::<SearchParameters>(&text)
                            .map(Some)
                            .map_err(Error::from)
                    })
                    .map_err(|err| Error::DecodeSearchParameterField(Box::new(err)))??;
                continue;
            }

            // Optional PTMs
            if field_name == "ptms" {
                let ptm_tsv = field
                    .bytes()
                    .await
                    .map(Cursor::new)
                    .map_err(|err| Error::DecodeModificationsField(Box::new(err)))?;

                let reader = csv::ReaderBuilder::new()
                    .has_headers(true)
                    .delimiter(b'\t')
                    .from_reader(ptm_tsv);

                ptms = reader
                    .into_deserialize::<PostTranslationalModification>()
                    .map(|ptm_result| ptm_result.map(Arc::new).map_err(Error::from))
                    .collect::<Result<Vec<Arc<PostTranslationalModification>>, Error>>()?;
            }
        }

        if !ptms.is_empty() {
            // validate PTMs
            let _ = PTMCollection::new(ptms.clone()).map_err(Error::from)?;
        } else {
            warn!("No PTMs submitted, which is unusual")
        }

        for ptm in ptms.iter() {
            debug!(
                "PTM: {}, {}, {:?}, {:?}",
                ptm.amino_acid().code(),
                ptm.mass_delta(),
                ptm.position(),
                ptm.mod_type()
            );
        }

        if mzml_file_names.is_empty() {
            return Err(Error::NoMzMLFileNames);
        }

        let workdir = state.work_dir.join(&uuid);

        let search_params = match search_params {
            Some(search_params) => search_params,
            None => {
                tokio::fs::remove_dir_all(&workdir)
                    .await
                    .map_err(|err| Error::RemoveDir(workdir, Box::new(err)))?;
                return Err(Error::NoSearchParameters);
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
            Err(err) => {
                tokio::fs::remove_dir_all(&workdir)
                    .await
                    .map_err(|err| Error::RemoveDir(workdir, Box::new(err)))?;
                return Err(Error::StorageError(Box::new(err)));
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

    pub fn loki_tracing_label_value() -> &'static str {
        LOKI_TRACING_LABEL_VALUE
    }
}
