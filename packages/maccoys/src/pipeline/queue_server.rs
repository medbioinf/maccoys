use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{DefaultBodyLimit, OriginalUri};
use axum::{
    body::Bytes,
    extract::{Json, State},
    routing::{get, post},
    Router,
};
use http::status::StatusCode;
use parking_lot::RwLock;
use pastey::paste;
use serde::{Deserialize, Serialize};
use tokio::select;
use tokio::sync::watch::Receiver as WatchReceiver;
use tokio::time::{sleep_until, Instant};
use tracing::error;

use crate::pipeline::configuration::RemoteEntypointConfiguration;
use crate::pipeline::messages::error_message::ErrorMessage;
use crate::pipeline::messages::identification_message::IdentificationMessage;
use crate::pipeline::messages::publication_message::PublicationMessage;
use crate::pipeline::messages::scoring_message::ScoringMessage;
use crate::pipeline::messages::search_space_generation_message::SearchSpaceGenerationMessage;
use crate::pipeline::utils::sanatize_string_for_path;
use crate::pipeline::{
    errors::http_queue_server_error::HttpQueueServerError,
    messages::{
        compressed_binary_message::CompressedBinaryMessage, indexing_message::IndexingMessage,
        is_message::IsMessage,
    },
};

static LOKI_TRACING_LABEL_VALUE: &str = "http_queue_server";

/// Type alias for the RwLock holding the pending storage keys (in push order) and the
/// work-in-progress map of message ID to (storage key, absolute timeout).
/// Message payloads are not held in memory; they are spooled to disk under
/// `RedundantQueue::directory`, keyed by storage key rather than message ID, since a
/// message's own ID is not guaranteed unique across pushes (e.g. reprocessing/retries).
///
type RWLockedMessages = RwLock<(VecDeque<String>, HashMap<String, (String, Duration)>)>;

/// A redundant queue implementation that holds messages to be processed
/// and messages being processed with timestamp of when processing started
/// Operations are lockfree where possible
///
/// Message payloads are written to disk as soon as they are pushed and only read back
/// into memory when popped, since they can be quite large. Only storage keys, message
/// IDs and work-in-progress timeouts are kept in memory.
///
pub struct RedundantQueue<M>
where
    M: IsMessage,
{
    capacity: usize,
    /// After this duration a message in work_in_progress is considered timed out and eligible for rescheduling
    timeout: Duration,
    /// Directory where message payloads for this queue are spooled to disk
    directory: PathBuf,
    messages: RWLockedMessages,
    /// Monotonic counter used to derive a storage key unique to each push, even if two
    /// pushed messages share the same `IsMessage::get_id()`
    sequence: AtomicU64,
    _message_type: std::marker::PhantomData<M>,
}

impl<M> RedundantQueue<M>
where
    M: IsMessage,
{
    pub fn new(capacity: usize, timeout: Duration, directory: PathBuf) -> Self {
        Self::with_state(
            capacity,
            timeout,
            directory,
            VecDeque::with_capacity(capacity),
            HashMap::new(),
            0,
        )
    }

    /// Constructs a queue from an existing state, e.g. loaded from a state file.
    /// Ensures the spool directory exists.
    ///
    fn with_state(
        capacity: usize,
        timeout: Duration,
        directory: PathBuf,
        work: VecDeque<String>,
        work_in_progress: HashMap<String, (String, Duration)>,
        next_sequence: u64,
    ) -> Self {
        std::fs::create_dir_all(&directory).unwrap_or_else(|e| {
            panic!(
                "[HttpQueueServer] Could not create queue data directory {:?}: {}",
                directory, e
            )
        });
        Self {
            capacity,
            timeout,
            directory,
            messages: RwLock::new((work, work_in_progress)),
            sequence: AtomicU64::new(next_sequence),
            _message_type: std::marker::PhantomData,
        }
    }

    /// Builds a storage key unique to this push, so that two messages sharing the same
    /// `IsMessage::get_id()` never overwrite each other's spooled payload on disk.
    ///
    fn new_storage_key(&self, message_id: &str) -> String {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        format!("{}__{}", sanatize_string_for_path(message_id), sequence)
    }

    /// Tries to push a message into the queue, spooling its payload to disk
    ///
    /// # Arguments
    /// * `message` - CompressedBinaryMessage to be pushed into the queue
    ///
    pub async fn try_push(
        &self,
        message: CompressedBinaryMessage<M>,
    ) -> Result<(), (Box<CompressedBinaryMessage<M>>, HttpQueueServerError)> {
        if self.len() >= self.capacity {
            return Err((Box::new(message), HttpQueueServerError::QueueFullError));
        }

        let bytes = match message.to_postcard() {
            Ok(bytes) => bytes,
            Err(e) => return Err((Box::new(message), HttpQueueServerError::MessageError(e))),
        };

        let storage_key = self.new_storage_key(message.id());
        let file_path = self.directory.join(&storage_key);
        if tokio::fs::write(&file_path, bytes).await.is_err() {
            return Err((
                Box::new(message),
                HttpQueueServerError::UnwritableQueueError,
            ));
        }

        self.messages.write().0.push_back(storage_key);

        Ok(())
    }

    /// Tries to pop a message from the queue, reading its payload back from disk
    /// If a message is popped, it is added to the work_in_progress map with the current timestamp
    ///
    pub async fn try_pop(&self) -> Result<CompressedBinaryMessage<M>, HttpQueueServerError> {
        let storage_key = self
            .messages
            .write()
            .0
            .pop_front()
            .ok_or(HttpQueueServerError::QueueEmptyError)?;

        let file_path = self.directory.join(&storage_key);
        let message = match tokio::fs::read(&file_path).await {
            Ok(bytes) => CompressedBinaryMessage::<M>::from_postcard(&bytes)
                .map_err(HttpQueueServerError::MessageError),
            Err(_) => Err(HttpQueueServerError::UnreadableQueueError),
        };

        let message = match message {
            Ok(message) => message,
            Err(e) => {
                // Put the storage key back so the message is not silently lost
                self.messages.write().0.push_front(storage_key);
                return Err(e);
            }
        };

        let unix_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0));

        let timeout = unix_now + self.timeout;

        self.messages
            .write()
            .1
            .insert(message.id().to_string(), (storage_key, timeout));

        Ok(message)
    }

    /// Acknowledges a message as processed, removing it from the work_in_progress map
    /// and deleting its spooled payload from disk
    ///
    /// # Arguments
    /// * `message_id` - ID of the message to acknowledge
    ///
    pub async fn acknowledge(&self, message_id: &str) -> Result<(), HttpQueueServerError> {
        let (storage_key, _timeout) = self
            .messages
            .write()
            .1
            .remove(message_id)
            .ok_or_else(|| HttpQueueServerError::KeyNotFoundError(message_id.to_string()))?;

        let file_path = self.directory.join(&storage_key);
        tokio::fs::remove_file(&file_path)
            .await
            .map_err(|_| HttpQueueServerError::UnwritableQueueError)?;

        Ok(())
    }

    pub fn len(&self) -> usize {
        let (work, work_in_progress) = &*self.messages.read();
        work.len() + work_in_progress.len()
    }

    pub fn is_empty(&self) -> bool {
        let (work, work_in_progress) = &*self.messages.read();
        work.is_empty() && work_in_progress.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.len() >= self.capacity
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Moves timed out work-in-progress messages back onto the pending queue.
    /// No disk I/O is necessary here, the spooled payload is left untouched.
    ///
    pub fn reschedule(&self) -> Result<(), HttpQueueServerError> {
        let (work, work_in_progress) = &mut *self.messages.write();

        let now: usize = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs() as usize;

        let ids_to_reschedule = work_in_progress
            .iter()
            .filter_map(|(message_id, (_, timeout))| {
                if now >= timeout.as_secs() as usize {
                    Some(message_id.to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for message_id in ids_to_reschedule.into_iter() {
            if let Some((storage_key, _)) = work_in_progress.remove(&message_id) {
                work.push_back(storage_key);
            }
        }

        Ok(())
    }
}

/// Serializable version of RedundantQueue's bookkeeping (storage keys, message IDs &
/// timeouts) for saving/loading state. Message payloads themselves are not part of this
/// snapshot, they already live on disk under the queue's data directory.
///
#[derive(Serialize, Deserialize)]
struct SerializableRedundantQueue {
    capacity: usize,
    #[serde(with = "crate::utils::serde::duration_as_secs")]
    timeout: Duration,
    work: VecDeque<String>,
    work_in_progress: HashMap<String, (String, Duration)>,
    next_sequence: u64,
}

impl SerializableRedundantQueue {
    /// Reconstructs a `RedundantQueue<M>` from this snapshot, pointing it at `directory`
    /// for its message payloads.
    ///
    fn into_redundant_queue<M>(self, directory: PathBuf) -> RedundantQueue<M>
    where
        M: IsMessage,
    {
        RedundantQueue::with_state(
            self.capacity,
            self.timeout,
            directory,
            self.work,
            self.work_in_progress,
            self.next_sequence,
        )
    }
}

impl<M> From<RedundantQueue<M>> for SerializableRedundantQueue
where
    M: IsMessage,
{
    fn from(queue: RedundantQueue<M>) -> Self {
        let next_sequence = queue.sequence.load(Ordering::Relaxed);
        let (work, work_in_progress) = queue.messages.into_inner();

        Self {
            work,
            work_in_progress,
            capacity: queue.capacity,
            timeout: queue.timeout,
            next_sequence,
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
                #[allow(clippy::too_many_arguments)]
                pub fn new(
                     $([< $data_type:snake _queue_capacity >]: usize ),+,
                     timeout: Duration,
                     data_directory: PathBuf,
                ) -> Self {
                    Self {
                        $([< $data_type:snake _queue >]: RedundantQueue::<$data_type>::new(
                            [< $data_type:snake _queue_capacity >],
                            timeout,
                            data_directory.join(stringify!([<$data_type:snake>])),
                        ) ),+
                    }
                }

                /// Creates a QueueCollection from RemoteEntypointConfiguration
                ///
                pub fn from_config(
                    config: &RemoteEntypointConfiguration,
                    data_directory: PathBuf,
                ) -> Self {
                    Self::new(
                        config.index.queue_capacity,
                        config.search_space_generation.queue_capacity,
                        config.identification.queue_capacity,
                        config.scoring.queue_capacity,
                        config.publication.queue_capacity,
                        config.error.queue_capacity,
                        config.queueing.message_timeout,
                        data_directory,
                    )
                }

                pub fn reschedule_all(&self) -> Result<(), HttpQueueServerError> {
                    [
                        $(
                            self.[< $data_type:snake _queue >].reschedule(),
                        )+
                    ].into_iter().collect::<Result<Vec<()>, HttpQueueServerError>>()?;
                    Ok(())
                }
            }

            /// Serializable version of QueueCollection for saving/loading state
            ///
            #[derive(Serialize, Deserialize)]
            pub struct SerializableQueueCollection {
                $([< $data_type:snake _queue >]: SerializableRedundantQueue ),+
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

                /// Reconstructs a QueueCollection from this snapshot, rooting each queue's
                /// spool directory under `data_directory`.
                ///
                pub fn into_queue_collection(self, data_directory: PathBuf) -> QueueCollection {
                    QueueCollection {
                        $([< $data_type:snake _queue >]: self.[< $data_type:snake _queue >].into_redundant_queue(
                            data_directory.join(stringify!([<$data_type:snake>]))
                        ) ),+
                    }
                }
            }

            impl From<QueueCollection> for SerializableQueueCollection {
                fn from(queue_collection: QueueCollection) -> Self {
                    Self {
                        $([< $data_type:snake _queue >]: SerializableRedundantQueue::from(queue_collection.[< $data_type:snake _queue >]) ),+
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
                        state.queue_collection().[< $data_type:snake _queue >].try_push(compressed_binary_message).await.map_err(|(_, e)| e)?;
                        Ok((StatusCode::OK, "Message pushed successfully"))
                    }

                    /// Pops a message from the specified queue
                    ///
                    /// # Arguments
                    /// * `state` - State of the queue server
                    ///
                    pub async fn [< pop_ $data_type:snake >](State(state): State<Arc<QueueServerState>>) -> Result<Bytes, HttpQueueServerError> {
                        let message = state.queue_collection().[< $data_type:snake _queue >].try_pop().await?;
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
                        state.queue_collection().[< $data_type:snake _queue >].acknowledge(&message_id).await?;
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

                async fn healthcheck(State(_state): State<Arc<QueueServerState>>) -> (StatusCode, &'static str) {
                    (StatusCode::OK, "Queue Server is healthy")
                }

                /// Returns the router for queue operations
                ///
                pub fn router() -> Router<Arc<QueueServerState>> {
                    Router::new()
                    .route("/healthcheck", get(Self::healthcheck))
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
    pub fn from_config(config: &RemoteEntypointConfiguration, data_directory: PathBuf) -> Self {
        Self {
            queue_collection: QueueCollection::from_config(config, data_directory),
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

    async fn rescheduling_task(
        state: Arc<QueueServerState>,
        check_interval: Duration,
        mut stop_signal: WatchReceiver<bool>,
    ) {
        let mut next_reschedule = Instant::now() + check_interval;
        loop {
            select! {
                _ = stop_signal.changed() => {
                    if *stop_signal.borrow() {
                        break;
                    }
                },
                _ = sleep_until(next_reschedule) => {}
            }
            next_reschedule = Instant::now() + check_interval;
            match state.queue_collection().reschedule_all() {
                Ok(_) => {}
                Err(e) => {
                    error!("[QueueServer / Rescheduling] Could not reschedule: {:?}", e);
                }
            }
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
        reschedule_check_interval: Duration,
        shutdown_signal: Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    ) -> Result<(), HttpQueueServerError> {
        let shutdown_signal = match shutdown_signal {
            Some(signal) => signal,
            None => Box::pin(Self::default_shutdown_signal()),
        };

        let (rescheduler_stop_signal, rescheduler_stop_receiver) =
            tokio::sync::watch::channel(false);
        let rescheduler = tokio::spawn(Self::rescheduling_task(
            state.clone(),
            reschedule_check_interval,
            rescheduler_stop_receiver,
        ));

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

        match rescheduler_stop_signal.send(true) {
            Ok(_) => {}
            Err(_) => {
                error!("Could not send stop signal to rescheduler task");
            }
        }

        rescheduler
            .await
            .map_err(|_| HttpQueueServerError::ReschedulerJoinError)?;

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
    /// * `queue_data_directory` - Directory where queued message payloads are spooled to disk
    ///
    pub async fn run_standalone(
        interface: String,
        port: u16,
        config: RemoteEntypointConfiguration,
        data_directory: PathBuf,
    ) -> Result<(), HttpQueueServerError> {
        let state_path = data_directory.join("queue_server_state.bin");
        let queue_data_directory = data_directory.join("queue_data");

        let state = if state_path.is_file() {
            let collection = SerializableQueueCollection::from_file(&state_path)?;
            QueueServerState {
                queue_collection: collection.into_queue_collection(queue_data_directory),
            }
        } else {
            QueueServerState {
                queue_collection: QueueCollection::from_config(&config, queue_data_directory),
            }
        };

        let state = Arc::new(state);

        Self::run(
            interface,
            port,
            state.clone(),
            config.queueing.reschedule_check_interval,
            None,
        )
        .await?;

        let collection: SerializableQueueCollection = Arc::try_unwrap(state)
            .map_err(|_| HttpQueueServerError::UnreadableHubStateError)?
            .queue_collection
            .into();
        collection.to_file(&state_path)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::{env::temp_dir, net::TcpListener};

    use tracing::warn;
    use tracing_test::traced_test;

    use crate::pipeline::{
        configuration::PipelineConfiguration,
        queue::{HttpPipelineQueue, PipelineQueue},
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    #[traced_test]
    async fn test_queue_push_pop_ack_reschedule() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        drop(listener);

        let config_file_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("test_files")
            .join("maccoys.http-queue.toml");

        let config = std::fs::read_to_string(config_file_path).unwrap();
        let config = toml::from_str::<PipelineConfiguration>(&config)
            .unwrap()
            .to_remote_entrypoint_configuration();

        let queue_data_directory =
            temp_dir().join("maccoys.tests.queue_server.test_queue_push_pop_ack_reschedule");

        let state: Arc<QueueServerState> = Arc::new(QueueServerState::from_config(
            &config,
            queue_data_directory.clone(),
        ));
        let (stop_signal_sender, stop_signal_receiver) = tokio::sync::oneshot::channel::<()>();
        let stop_signal = async move {
            stop_signal_receiver.await.ok();
        };

        let handle = tokio::spawn(async move {
            match QueueServer::run(
                "127.0.0.1".to_string(),
                port,
                state,
                config.queueing.reschedule_check_interval,
                Some(Box::pin(stop_signal)),
            )
            .await
            {
                Ok(_) => {}
                Err(e) => error!("{}", e),
            }
        });

        let mut healthcheck_status_code = None;
        for _ in 0..11 {
            tokio::time::sleep(Duration::from_secs(2)).await;
            match reqwest::get(format!(
                "http://127.0.0.1:{port}{}/healthcheck",
                QueueController::path()
            ))
            .await
            {
                Ok(response) => {
                    healthcheck_status_code = Some(response.status());
                    break;
                }
                Err(_) => {
                    warn!("waiting for queue server to get online");
                    continue;
                }
            }
        }

        if let Some(status_code) = healthcheck_status_code {
            assert_eq!(status_code, StatusCode::OK);
        } else {
            panic!("Queue server did not start in time");
        }

        let mut indexing_task_config = config.index.clone();
        indexing_task_config.queue_url = Some(format!("http://127.0.0.1:{}", port));
        let queue = HttpPipelineQueue::<IndexingMessage>::new(&indexing_task_config)
            .await
            .unwrap();

        let message1 = IndexingMessage::new("test1".to_string(), vec!["run1".to_string()]);
        let message2 = IndexingMessage::new("test1".to_string(), vec!["run1".to_string()]);

        queue.push(message1.clone()).await.unwrap();
        queue.push(message2.clone()).await.unwrap();

        // Check if length is 2
        assert_eq!(queue.len().await.unwrap(), 2);

        // Popping message 1 and acknowledging it
        if let Some((_, popped_message1)) = queue.pop().await.unwrap() {
            assert_eq!(popped_message1.get_id(), message1.get_id());
            queue.ack(&popped_message1.get_id()).await.unwrap();
        }

        // Check if length is 1 now
        assert_eq!(queue.len().await.unwrap(), 1);

        // Popping message 2 without acknowledging
        assert!(queue.pop().await.unwrap().is_some());

        // Wait for rescheduling
        tokio::time::sleep(
            (config.queueing.reschedule_check_interval + config.queueing.message_timeout) * 2,
        )
        .await;

        // Popping message 2 and acknowledging it
        if let Some((_, popped_message2)) = queue.pop().await.unwrap() {
            assert_eq!(popped_message2.get_id(), message2.get_id());
            queue.ack(&popped_message2.get_id()).await.unwrap();
        }

        // Queue should be empty now
        assert_eq!(queue.len().await.unwrap(), 0);

        stop_signal_sender.send(()).unwrap();
        handle.await.unwrap();

        std::fs::remove_dir_all(&queue_data_directory).ok();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_queue_collection_serialization_deserialization() {
        let state_path = temp_dir().join("maccoys.tests.queue_server.queue.bin");
        let queue_data_directory =
            temp_dir().join("maccoys.tests.queue_server.test_queue_collection_serde");

        let config_file_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("test_files")
            .join("maccoys.http-queue.toml");

        let config = std::fs::read_to_string(config_file_path).unwrap();
        let config = toml::from_str::<PipelineConfiguration>(&config)
            .unwrap()
            .to_remote_entrypoint_configuration();

        let queue_collection = QueueCollection::from_config(&config, queue_data_directory.clone());

        let message1 = IndexingMessage::new("test1".to_string(), vec!["run1".to_string()]);
        let message2 = IndexingMessage::new("test1".to_string(), vec!["run1".to_string()]);

        queue_collection
            .indexing_message_queue
            .try_push(CompressedBinaryMessage::try_from_message(&message1).unwrap())
            .await
            .unwrap();
        queue_collection
            .indexing_message_queue
            .try_push(CompressedBinaryMessage::try_from_message(&message2).unwrap())
            .await
            .unwrap();

        let serializable_queue_collection: SerializableQueueCollection = queue_collection.into();

        serializable_queue_collection.to_file(&state_path).unwrap();

        drop(serializable_queue_collection);

        let loaded_serializable_queue_collection =
            SerializableQueueCollection::from_file(&state_path).unwrap();
        let loaded_queue_collection: QueueCollection = loaded_serializable_queue_collection
            .into_queue_collection(queue_data_directory.clone());

        assert_eq!(loaded_queue_collection.indexing_message_queue.len(), 2);

        let popped_message1 = loaded_queue_collection
            .indexing_message_queue
            .try_pop()
            .await
            .unwrap();
        assert_eq!(popped_message1.id(), message1.get_id());
        loaded_queue_collection
            .indexing_message_queue
            .acknowledge(popped_message1.id())
            .await
            .unwrap();

        let popped_message2 = loaded_queue_collection
            .indexing_message_queue
            .try_pop()
            .await
            .unwrap();
        assert_eq!(popped_message2.id(), message2.get_id());
        loaded_queue_collection
            .indexing_message_queue
            .acknowledge(popped_message2.id())
            .await
            .unwrap();

        std::fs::remove_file(&state_path).unwrap();
        std::fs::remove_dir_all(&queue_data_directory).ok();
    }
}
