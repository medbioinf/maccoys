use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{DefaultBodyLimit, OriginalUri};
use axum::{
    body::Bytes,
    extract::{Json, State},
    routing::{get, post},
    Router,
};
use dashmap::DashMap;
use deadqueue::limited::Queue;
use http::status::StatusCode;
use paste::paste;
use serde::{Deserialize, Serialize};

use crate::pipeline::configuration::RemoteEntypointConfiguration;
use crate::pipeline::messages::error_message::ErrorMessage;
use crate::pipeline::messages::identification_message::IdentificationMessage;
use crate::pipeline::messages::publication_message::PublicationMessage;
use crate::pipeline::messages::scoring_message::ScoringMessage;
use crate::pipeline::messages::search_space_generation_message::SearchSpaceGenerationMessage;
use crate::pipeline::{
    errors::http_queue_server_error::HttpQueueServerError,
    messages::{
        compressed_binary_message::CompressedBinaryMessage, indexing_message::IndexingMessage,
        is_message::IsMessage,
    },
};

static LOKI_TRACING_LABEL_VALUE: &str = "http_queue_server";

/// A redundant queue implementation that holds messages to be processed
/// and messages being processed with timestamp of when processing started
/// Operations are lockfree where possible
///
pub struct RedundantQueue<M>
where
    M: IsMessage,
{
    /// Queue holding messages (Postcard formatted CompressedBinaryMessage) to be processed
    work: Queue<CompressedBinaryMessage<M>>,
    /// Queue holding messages being processed with timestamp of when processing started
    work_in_progress: DashMap<String, (usize, CompressedBinaryMessage<M>)>,
}

impl<M> RedundantQueue<M>
where
    M: IsMessage + Clone,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            work: Queue::new(capacity),
            work_in_progress: DashMap::new(),
        }
    }

    /// Tries to push a message into the queue
    ///
    /// # Arguments
    /// * `message` - CompressedBinaryMessage to be pushed into the queue
    ///
    pub fn try_push(
        &self,
        message: CompressedBinaryMessage<M>,
    ) -> Result<(), HttpQueueServerError> {
        self.work
            .try_push(message)
            .map_err(|_| HttpQueueServerError::QueueFullError)
    }

    /// Tries to pop a message from the queue
    /// If a message is popped, it is added to the work_in_progress map with the current timestamp
    ///
    pub fn try_pop(&self) -> Result<CompressedBinaryMessage<M>, HttpQueueServerError> {
        let message = self
            .work
            .try_pop()
            .ok_or(HttpQueueServerError::QueueEmptyError)?;

        self.work_in_progress.insert(
            message.id().to_string(),
            (
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or(Duration::from_secs(0))
                    .as_secs() as usize,
                message.clone(),
            ),
        );

        Ok(message)
    }

    /// Acknowledges a message as processed and removes it from the work_in_progress map
    ///
    /// # Arguments
    /// * `message_id` - ID of the message to acknowledge
    ///
    pub fn acknowledge(&self, message_id: &str) -> Result<(), HttpQueueServerError> {
        self.work_in_progress
            .remove(message_id)
            .ok_or(HttpQueueServerError::KeyNotFoundError(
                message_id.to_string(),
            ))?;

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.work.len()
    }

    pub fn is_empty(&self) -> bool {
        self.work.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.work.is_full()
    }

    pub fn capacity(&self) -> usize {
        self.work.capacity()
    }
}

impl<M> From<SerializableRedundantQueue<M>> for RedundantQueue<M>
where
    M: IsMessage,
{
    fn from(mut queue: SerializableRedundantQueue<M>) -> Self {
        let work = Queue::new(queue.capacity);
        for message in queue.work.drain(..) {
            let _ = work.try_push(message);
        }

        Self {
            work,
            work_in_progress: queue.work_in_progress,
        }
    }
}

/// Serializable version of RedundantQueue for saving/loading state.
///
#[derive(Serialize, Deserialize)]
#[serde(bound = "M: IsMessage")]
struct SerializableRedundantQueue<M>
where
    M: IsMessage,
{
    capacity: usize,
    work: Vec<CompressedBinaryMessage<M>>,
    work_in_progress: DashMap<String, (usize, CompressedBinaryMessage<M>)>,
}

impl<M> From<RedundantQueue<M>> for SerializableRedundantQueue<M>
where
    M: IsMessage,
{
    fn from(queue: RedundantQueue<M>) -> Self {
        let mut work = Vec::with_capacity(queue.work.len());
        while let Some(message) = queue.work.try_pop() {
            work.push(message);
        }

        Self {
            capacity: queue.work.capacity(),
            work,
            work_in_progress: queue.work_in_progress,
        }
    }
}

/// Trait defining methods to get the routes for queue operations
///
pub trait IsQueueRouteable: IsMessage {
    /// Returns the snake_case segment for the message type
    fn push_route() -> &'static str;
    fn pop_route() -> &'static str;
    fn ack_route() -> &'static str;
    fn is_full_route() -> &'static str;
    fn capacity_route() -> &'static str;
    fn len_route() -> &'static str;
}

/// Macro to generate a queue controller for multiple message types
/// and implement IsQueueRouteable for each message type
///
macro_rules! queue_namespace {
    (
        $( $data_type:ty ),+
    ) => {
        paste! {
            $(
                impl IsQueueRouteable for $data_type {
                    fn push_route() -> &'static str {
                        concat!("/", stringify!([<$data_type:snake>]), "/push")
                    }

                    fn pop_route() -> &'static str {
                        concat!("/", stringify!([<$data_type:snake>]), "/pop")
                    }

                    fn ack_route() -> &'static str {
                        concat!("/", stringify!([<$data_type:snake>]), "/ack")
                    }

                    fn is_full_route() -> &'static str {
                        concat!("/", stringify!([<$data_type:snake>]), "/is_full")
                    }

                    fn capacity_route() -> &'static str {
                        concat!("/", stringify!([<$data_type:snake>]), "/capacity")
                    }

                    fn len_route() -> &'static str {
                        concat!("/", stringify!([<$data_type:snake>]), "/len")
                    }
                }
            )+


            /// Collection of all queues for different message types
            ///
            pub struct QueueCollection {
                $([< $data_type:snake _queue >]: RedundantQueue<$data_type> ),+
            }

            /// Implementation of QueueCollection
            ///
            impl QueueCollection {
                pub fn new(
                     $([< $data_type:snake _queue_capacity >]: usize ),+
                ) -> Self {
                    Self {
                        $([< $data_type:snake _queue >]: RedundantQueue::<$data_type>::new([< $data_type:snake _queue_capacity >]) ),+
                    }
                }

                /// Creates a QueueCollection from RemoteEntypointConfiguration
                ///
                pub fn from_config(
                    config: &RemoteEntypointConfiguration,
                ) -> Self {
                    Self::new(
                        config.index.queue_capacity,
                        config.search_space_generation.queue_capacity,
                        config.identification.queue_capacity,
                        config.scoring.queue_capacity,
                        config.publication.queue_capacity,
                        config.error.queue_capacity,
                    )
                }
            }

            impl From<SerializableQueueCollection> for QueueCollection {
                fn from(serializable_queue_collection: SerializableQueueCollection) -> Self {
                    Self {
                        $([< $data_type:snake _queue >]: RedundantQueue::<$data_type>::from(serializable_queue_collection.[< $data_type:snake _queue >]) ),+
                    }
                }
            }

            /// Serializable version of QueueCollection for saving/loading state
            ///
            #[derive(Serialize, Deserialize)]
            pub struct SerializableQueueCollection {
                $([< $data_type:snake _queue >]: SerializableRedundantQueue<$data_type> ),+
            }

            impl SerializableQueueCollection {
                pub fn from_file(path: &PathBuf) -> Result<Self, HttpQueueServerError> {
                    let serialized = std::fs::read(path).map_err(|_| HttpQueueServerError::CacheFileReadError)?;
                    let queue_collection: Self = postcard::from_bytes(&serialized).map_err(|_| HttpQueueServerError::StateDeserializationError)?;
                    Ok(queue_collection)
                }

                pub fn to_file(&self, path: &PathBuf) -> Result<(), HttpQueueServerError> {
                    let serialized = postcard::to_allocvec(self).map_err(|_| HttpQueueServerError::StateSerializationError)?;
                    std::fs::write(path, serialized).map_err(|_| HttpQueueServerError::CacheFileWriteError)?;
                    Ok(())
                }
            }

            impl From<QueueCollection> for SerializableQueueCollection {
                fn from(queue_collection: QueueCollection) -> Self {
                    Self {
                        $([< $data_type:snake _queue >]: SerializableRedundantQueue::<$data_type>::from(queue_collection.[< $data_type:snake _queue >]) ),+
                    }
                }
            }

            /// Controller for queue operations
            ///
            pub struct QueueController;

            impl QueueController {
                $(

                    /// Pushes a message into the specified queue
                    ///
                    /// # Arguments
                    /// * `state` - State of the queue server
                    /// * `serialized_compressed_binary_message` - Postcard serialized CompressedBinaryMessage<T>
                    ///
                    pub async fn [< push_ $data_type:snake >](State(state): State<Arc<QueueServerState>>, serialized_compressed_binary_message: Bytes) -> Result<(StatusCode, &'static str), HttpQueueServerError> {
                        let compressed_binary_message: CompressedBinaryMessage<$data_type> = CompressedBinaryMessage::from_postcard(&serialized_compressed_binary_message).map_err(HttpQueueServerError::MessageError)?;
                        state.queue_collection().[< $data_type:snake _queue >].try_push(compressed_binary_message).map_err(|_| HttpQueueServerError::QueueFullError)?;
                        Ok((StatusCode::OK, "Message pushed successfully"))
                    }

                    /// Pops a message from the specified queue
                    ///
                    /// # Arguments
                    /// * `state` - State of the queue server
                    ///
                    pub async fn [< pop_ $data_type:snake >](State(state): State<Arc<QueueServerState>>) -> Result<Bytes, HttpQueueServerError> {
                        let message = state.queue_collection().[< $data_type:snake _queue >].try_pop().map_err(|_| HttpQueueServerError::QueueEmptyError)?;
                        let serialized_message = message.to_postcard().map_err(HttpQueueServerError::MessageError)?;
                        Ok(Bytes::from(serialized_message))
                    }

                    /// Acknowledges a message as processed in the specified queue
                    ///
                    /// # Arguments
                    /// * `state` - State of the queue server
                    /// * `message_id` - ID of the message to acknowledge as JSON
                    ///
                    pub async fn [< acknowledge_ $data_type:snake >](State(state): State<Arc<QueueServerState>>, message_id: Json<String>) -> Result<(StatusCode, &'static str), HttpQueueServerError> {
                        state.queue_collection().[< $data_type:snake _queue >].acknowledge(&message_id).map_err(|_| HttpQueueServerError::KeyNotFoundError(message_id.to_string()))?;
                        Ok((StatusCode::OK, "Message acknowledged successfully"))
                    }

                    /// Checks if the specified queue is full
                    ///
                    /// # Arguments
                    /// * `state` - State of the queue server
                    ///
                    pub async fn [< is_ $data_type:snake _full>](State(state): State<Arc<QueueServerState>>) -> Result<(StatusCode, Json<bool>), HttpQueueServerError> {
                        let is_full = state.queue_collection().[< $data_type:snake _queue >].is_full();
                        Ok((StatusCode::OK, is_full.into()))
                    }

                    /// Gets the capacity of the specified queue
                    ///
                    /// # Arguments
                    /// * `state` - State of the queue server
                    ///
                    pub async fn [< $data_type:snake _capacity >](State(state): State<Arc<QueueServerState>>) -> Result<(StatusCode, Json<usize>), HttpQueueServerError> {
                        let capacity = state.queue_collection().[< $data_type:snake _queue >].capacity();
                        Ok((StatusCode::OK, capacity.into()))
                    }

                    /// Gets the current length of the specified queue
                    ///
                    /// # Arguments
                    /// * `state` - State of the queue server
                    ///
                    pub async fn [< len_ $data_type:snake >](State(state): State<Arc<QueueServerState>>) -> Result<(StatusCode, Json<usize>), HttpQueueServerError> {
                        let len = state.queue_collection().[< $data_type:snake _queue >].len();
                        Ok((StatusCode::OK, len.into()))
                    }
                )+

                /// Returns the router for queue operations
                ///
                pub fn router() -> Router<Arc<QueueServerState>> {
                    Router::new()
                    $(
                        .route($data_type::push_route(), post(Self::[< push_ $data_type:snake >]))
                        .route($data_type::pop_route(), get(Self::[< pop_ $data_type:snake >]))
                        .route($data_type::ack_route(), post(Self::[< acknowledge_ $data_type:snake >]))
                        .route($data_type::is_full_route(), get(Self::[< is_ $data_type:snake _full>]))
                        .route($data_type::capacity_route(), get(Self::[< $data_type:snake _capacity >]))
                        .route($data_type::len_route(), get(Self::[< len_ $data_type:snake >]))
                    )+
                }

                /// Returns the base path for queue operations
                ///
                pub fn path() -> &'static str {
                    "/queues"
                }


            }
        }
    };
}

queue_namespace! {
    IndexingMessage,
    SearchSpaceGenerationMessage,
    IdentificationMessage,
    ScoringMessage,
    PublicationMessage,
    ErrorMessage
}

/// Shared state for the queues server
///
pub struct QueueServerState {
    queue_collection: QueueCollection,
}

impl QueueServerState {
    /// Creates a new QueueServerState and initializes the queue collection from configuration
    ///
    pub fn from_config(config: &RemoteEntypointConfiguration) -> Self {
        Self {
            queue_collection: QueueCollection::from_config(config),
        }
    }

    /// Returns the queue collection
    ///
    pub fn queue_collection(&self) -> &QueueCollection {
        &self.queue_collection
    }
}

/// Queue server for queueing pipeline messages over HTTP
/// in a distributed setup
///
pub struct QueueServer;

impl QueueServer {
    /// Returns the label for loki tracing
    ///
    pub fn loki_tracing_label_value() -> &'static str {
        LOKI_TRACING_LABEL_VALUE
    }

    /// Fallback handler for unknown routes
    ///
    async fn fallback(uri: OriginalUri) -> HttpQueueServerError {
        HttpQueueServerError::RouteNotFoundError(uri.0.path().to_string())
    }

    /// Default shutdown signal handler for ctrl-c and terminate signals
    ///
    async fn default_shutdown_signal() {
        let ctrl_c = async {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install Ctrl+C handler");
        };

        let terminate = async {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install signal handler")
                .recv()
                .await;
        };

        tokio::select! {
            _ = ctrl_c => {},
            _ = terminate => {},
        }
    }

    /// Starts the HTTP Queue Server
    ///
    /// # Arguments
    /// * `interface` - Interface to bind the service to
    /// * `port` - Port to bind the service to
    /// * `state` - State of the queue server
    /// * `shutdown_signal` - Optional shutdown signal future
    ///
    pub async fn run(
        interface: String,
        port: u16,
        state: Arc<QueueServerState>,
        shutdown_signal: Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    ) -> Result<(), HttpQueueServerError> {
        let shutdown_signal = match shutdown_signal {
            Some(signal) => signal,
            None => Box::pin(Self::default_shutdown_signal()),
        };

        // Build our application with route
        let app = Router::new()
            .nest(QueueController::path(), QueueController::router())
            .fallback(Self::fallback)
            .layer(DefaultBodyLimit::disable())
            .with_state(state);

        let listener = tokio::net::TcpListener::bind(format!("{}:{}", interface, port))
            .await
            .map_err(HttpQueueServerError::TcpListenerError)?;

        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal)
            .await
            .map_err(HttpQueueServerError::ServerStartError)?;

        Ok(())
    }

    /// Runs the HTTP Queue Server as standalone application
    /// in a distributed setup
    ///
    /// # Arguments
    /// * `interface` - Interface to bind the service to
    /// * `port` - Port to bind the service to
    /// * `config` - RemoteEntypointConfiguration for initializing the queues
    /// * `state_path` - Optional path to load/save the server state
    ///
    pub async fn run_standalone(
        interface: String,
        port: u16,
        config: RemoteEntypointConfiguration,
        state_path: Option<PathBuf>,
    ) -> Result<(), HttpQueueServerError> {
        let state = match state_path.as_ref() {
            Some(path) => {
                if path.is_file() {
                    let collection = SerializableQueueCollection::from_file(path)?;
                    QueueServerState {
                        queue_collection: collection.into(),
                    }
                } else {
                    QueueServerState {
                        queue_collection: QueueCollection::from_config(&config),
                    }
                }
            }
            None => QueueServerState {
                queue_collection: QueueCollection::from_config(&config),
            },
        };

        let state = Arc::new(state);

        Self::run(interface, port, state.clone(), None).await?;

        if let Some(state_path) = state_path {
            let collection: SerializableQueueCollection = Arc::try_unwrap(state)
                .map_err(|_| HttpQueueServerError::UnreadableHubStateError)?
                .queue_collection
                .into();
            collection.to_file(&state_path)?;
        }

        Ok(())
    }
}
