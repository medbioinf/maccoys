use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::Future;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::warn;

use super::{
    configuration::TaskConfiguration, errors::queue_error::QueueError,
    messages::is_message::IsMessage,
};

use crate::pipeline::{
    errors::{
        http_queue_server_error::HttpQueueServerError,
        queue_error::{HttpQueueError, LocalQueueError},
    },
    messages::compressed_binary_message::CompressedBinaryMessage,
    queue_server::{IsQueueRouteable, QueueController},
    utils::sanatize_string_for_path,
};

/// Trait defining the methods for a pipeline queue
///
pub trait PipelineQueue<M>: Sized + Send + Sync
where
    M: IsMessage,
{
    /// Create a new pipeline queue
    fn new(config: &TaskConfiguration) -> impl Future<Output = Result<Self, QueueError>> + Send;

    fn get_capacity(&self) -> impl Future<Output = Result<usize, QueueError>>;

    /// Pop a message_id and the message from the queue
    /// Returns None if the queue is empty
    ///
    fn pop(&self) -> impl Future<Output = Result<Option<(String, M)>, QueueError>> + Send;

    /// Push a message to the queue
    /// Returns the message if the queue is full
    ///
    /// # Arguments
    /// * `message` - Manifest to be pushed`
    ///
    fn push(&self, message: M) -> impl Future<Output = Result<(), (QueueError, Box<M>)>> + Send;

    /// Acknowledge a message
    ///
    fn ack(&self, message_id: &str) -> impl Future<Output = Result<(), QueueError>> + Send;

    /// Get the length of the queue
    ///
    fn len(&self) -> impl Future<Output = Result<usize, QueueError>> + Send;

    /// Checks if the queue is empty
    ///
    fn is_empty(&self) -> impl Future<Output = Result<bool, QueueError>> + Send {
        async { Ok(self.len().await? == 0) }
    }

    /// Checks if the queue is empty
    ///
    fn is_full(&self) -> impl Future<Output = Result<bool, QueueError>> + Send;
}

/// Type alias for the RwLock holding the pending storage keys (in push order) and the
/// work-in-progress map of message ID to (storage key, absolute timeout).
/// Message payloads are not held in memory; they are spooled to disk under
/// `LocalPipelineQueue::directory`, keyed by storage key rather than message ID, since a
/// message's own ID is not guaranteed unique across pushes (e.g. reprocessing/retries).
///
type RWLockedMessages = RwLock<(VecDeque<String>, HashMap<String, (String, Duration)>)>;

/// Disk-backed pipeline queue. Message payloads are written to disk as soon as they are
/// pushed and only read back into memory when popped, since they can be quite large. Only
/// storage keys, message IDs and work-in-progress timeouts are kept in memory.
///
/// Exposes two API layers on the same struct:
/// * a compressed-level API (`try_push_compressed`/`try_pop_compressed`/`acknowledge`/
///   `reschedule`/`len`/`is_empty`/`is_full`/`capacity`), operating directly on
///   [`CompressedBinaryMessage<M>`] and returning [`HttpQueueServerError`]. This is what
///   [`crate::pipeline::queue_server::QueueController`]'s HTTP handlers call, so a push/pop
///   over HTTP never gets transcoded twice.
/// * the [`PipelineQueue<M>`] trait-level API, used in-process (no HTTP) by
///   [`crate::pipeline::local_pipeline::LocalPipeline`], which compresses on push and
///   decompresses on pop, delegating to the compressed-level methods.
///
pub struct LocalPipelineQueue<M>
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

impl<M> LocalPipelineQueue<M>
where
    M: IsMessage,
{
    pub fn new_at(capacity: usize, timeout: Duration, directory: PathBuf) -> Self {
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
                "[LocalPipelineQueue] Could not create queue data directory {:?}: {}",
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
    pub async fn try_push_compressed(
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
        if let Err(e) = tokio::fs::write(&file_path, bytes).await {
            return Err((
                Box::new(message),
                HttpQueueServerError::UnwritableQueueError(e),
            ));
        }

        self.messages.write().0.push_back(storage_key);

        Ok(())
    }

    /// Tries to pop a message from the queue, reading its payload back from disk
    /// If a message is popped, it is added to the work_in_progress map with the current timestamp
    ///
    pub async fn try_pop_compressed(
        &self,
    ) -> Result<CompressedBinaryMessage<M>, HttpQueueServerError> {
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
            Err(e) => Err(HttpQueueServerError::UnreadableQueueError(e)),
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
            .map_err(HttpQueueServerError::UnwritableQueueError)?;

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

impl<M> PipelineQueue<M> for LocalPipelineQueue<M>
where
    M: IsMessage,
{
    /// Not constructible from `TaskConfiguration` alone — needs a spool directory and WIP
    /// timeout, which live on the sibling `QueuingConfiguration`, not `TaskConfiguration`.
    /// Real construction is via [`LocalPipelineQueue::new_at`], used by
    /// [`crate::pipeline::local_pipeline::QueueFactory::new_queue`] and
    /// [`crate::pipeline::queue_server::QueueCollection`]. This impl exists only to satisfy
    /// the trait bound on `QueueFactory::Queue<M>`; calling it is a bug.
    ///
    async fn new(config: &TaskConfiguration) -> Result<Self, QueueError> {
        Err(
            LocalQueueError::NotConstructibleFromTaskConfiguration(config.queue_name.clone())
                .into(),
        )
    }

    async fn get_capacity(&self) -> Result<usize, QueueError> {
        Ok(self.capacity())
    }

    async fn pop(&self) -> Result<Option<(String, M)>, QueueError> {
        match self.try_pop_compressed().await {
            Ok(compressed) => {
                let message = compressed
                    .try_into_message()
                    .map_err(LocalQueueError::MessageError)?;
                Ok(Some((message.get_id(), message)))
            }
            Err(HttpQueueServerError::QueueEmptyError) => Ok(None),
            Err(e) => Err(LocalQueueError::DiskSpoolError(e).into()),
        }
    }

    async fn push(&self, message: M) -> Result<(), (QueueError, Box<M>)> {
        let compressed = match CompressedBinaryMessage::try_from_message(&message) {
            Ok(compressed) => compressed,
            Err(e) => return Err((LocalQueueError::MessageError(e).into(), Box::new(message))),
        };

        match self.try_push_compressed(compressed).await {
            Ok(()) => Ok(()),
            Err((_, HttpQueueServerError::QueueFullError)) => {
                Err((QueueError::QueueFullError, Box::new(message)))
            }
            Err((_, e)) => Err((LocalQueueError::DiskSpoolError(e).into(), Box::new(message))),
        }
    }

    async fn ack(&self, message_id: &str) -> Result<(), QueueError> {
        match self.acknowledge(message_id).await {
            Ok(()) => Ok(()),
            // Mirrors HttpPipelineQueue's client-side handling of a 410 on ack: already-acked
            // or unknown IDs are not an error.
            Err(HttpQueueServerError::KeyNotFoundError(_)) => Ok(()),
            Err(e) => Err(LocalQueueError::DiskSpoolError(e).into()),
        }
    }

    async fn len(&self) -> Result<usize, QueueError> {
        Ok(self.len())
    }

    async fn is_full(&self) -> Result<bool, QueueError> {
        Ok(self.is_full())
    }
}

/// Serializable version of LocalPipelineQueue's bookkeeping (storage keys, message IDs &
/// timeouts) for saving/loading state. Message payloads themselves are not part of this
/// snapshot, they already live on disk under the queue's data directory.
///
#[derive(Serialize, Deserialize)]
pub struct SerializableLocalPipelineQueue {
    capacity: usize,
    #[serde(with = "crate::utils::serde::duration_as_secs")]
    timeout: Duration,
    work: VecDeque<String>,
    work_in_progress: HashMap<String, (String, Duration)>,
    next_sequence: u64,
}

impl SerializableLocalPipelineQueue {
    /// Reconstructs a `LocalPipelineQueue<M>` from this snapshot, pointing it at `directory`
    /// for its message payloads.
    ///
    pub fn into_local_pipeline_queue<M>(self, directory: PathBuf) -> LocalPipelineQueue<M>
    where
        M: IsMessage,
    {
        LocalPipelineQueue::with_state(
            self.capacity,
            self.timeout,
            directory,
            self.work,
            self.work_in_progress,
            self.next_sequence,
        )
    }
}

impl<M> From<LocalPipelineQueue<M>> for SerializableLocalPipelineQueue
where
    M: IsMessage,
{
    fn from(queue: LocalPipelineQueue<M>) -> Self {
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

/// Queue implemented used [crate::pipeline::queue_server::QueueServer] as backend
/// Messages are convert to compressed binary messages and transfered using HTTP.
///
/// Timeout used for every request made by [`HttpPipelineQueue`]'s client
const HTTP_QUEUE_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

/// Builds the context string for a [`HttpQueueError::RequestError`], noting when the
/// underlying `reqwest::Error` was a timeout so it can be told apart from e.g. a
/// connection-refused/DNS failure without inspecting the nested error.
///
fn request_error_context(context: &'static str, err: &reqwest::Error) -> String {
    if err.is_timeout() {
        format!(
            "{context} (timed out after {}s)",
            HTTP_QUEUE_REQUEST_TIMEOUT.as_secs()
        )
    } else {
        context.to_string()
    }
}

pub struct HttpPipelineQueue<M>
where
    M: IsMessage + IsQueueRouteable,
{
    /// Base URL of the http queueing
    base_url: String,

    /// Redis client
    client: reqwest::Client,

    /// Name of the queue
    queue_name: String,

    _phantom_message: std::marker::PhantomData<M>,
}

impl<M> PipelineQueue<M> for HttpPipelineQueue<M>
where
    M: IsMessage + IsQueueRouteable,
{
    async fn new(config: &TaskConfiguration) -> Result<Self, QueueError> {
        if config.queue_url.is_none() {
            return Err(HttpQueueError::BaseUrlMissing(config.queue_name.clone()).into());
        }

        let queue_url = config.queue_url.as_ref().unwrap();

        let base_url = format!("{}{}", queue_url, QueueController::path());

        let mut client_builder = reqwest::Client::builder()
            .timeout(HTTP_QUEUE_REQUEST_TIMEOUT)
            .user_agent(config.queue_name.clone());

        if queue_url.starts_with("http://") {
            // No TLS means no ALPN, so force HTTP/2 prior knowledge (h2c) explicitly —
            // the queue server (axum::serve, "http2" feature) auto-detects it.
            client_builder = client_builder.http2_prior_knowledge();
        }

        let client = client_builder.build().map_err(|err| {
            QueueError::from(HttpQueueError::BuildError(config.queue_name.clone(), err))
        })?;

        Ok(Self {
            base_url,
            client,
            queue_name: config.queue_name.clone(),
            _phantom_message: std::marker::PhantomData,
        })
    }

    async fn get_capacity(&self) -> Result<usize, QueueError> {
        let url = format!("{}{}", self.base_url, M::capacity_route());

        let response = self.client.get(&url).send().await.map_err(|err| {
            let context = request_error_context("getting capacity", &err);
            QueueError::from(HttpQueueError::RequestError(
                self.queue_name.clone(),
                url.clone(),
                context,
                err,
            ))
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let reason = response
                .text()
                .await
                .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));
            return Err(HttpQueueError::UnsuccessStatusCode(
                self.queue_name.clone(),
                url,
                status,
                reason,
                "getting capacity",
            )
            .into());
        }

        let capacity: usize = response.json().await.map_err(|err| {
            QueueError::from(HttpQueueError::DeserializationError(
                self.queue_name.clone(),
                "deserializing capacity response",
                err,
            ))
        })?;

        Ok(capacity)
    }

    async fn pop(&self) -> Result<Option<(String, M)>, QueueError> {
        let url = format!("{}{}", self.base_url, M::pop_route());

        let response = self.client.get(&url).send().await.map_err(|err| {
            let context = request_error_context("popping message", &err);
            QueueError::from(HttpQueueError::RequestError(
                self.queue_name.clone(),
                url.clone(),
                context,
                err,
            ))
        })?;

        // not found is acceptable as the queue might not exists yet
        match response.status() {
            reqwest::StatusCode::OK => {}
            reqwest::StatusCode::NO_CONTENT | reqwest::StatusCode::NOT_FOUND => {
                return Ok(None);
            }
            _ => {
                let status = response.status();
                let reason = response
                    .text()
                    .await
                    .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));
                return Err(HttpQueueError::UnsuccessStatusCode(
                    self.queue_name.clone(),
                    url,
                    status,
                    reason,
                    "popping message",
                )
                .into());
            }
        }

        let message_bytes = response.bytes().await.map_err(|err| {
            let context = request_error_context("accessing body for message", &err);
            HttpQueueError::RequestError(self.queue_name.clone(), url.clone(), context, err)
        })?;

        let message: M = CompressedBinaryMessage::from_postcard(&message_bytes)
            .map_err(|err| {
                QueueError::from(HttpQueueError::MessageError(
                    self.queue_name.clone(),
                    "deserializing compressed message",
                    err,
                ))
            })?
            .try_into_message()
            .map_err(|err| {
                QueueError::from(HttpQueueError::MessageError(
                    self.queue_name.clone(),
                    "uncompressing message",
                    err,
                ))
            })?;

        Ok(Some((message.get_id(), message)))
    }

    async fn push(&self, message: M) -> Result<(), (QueueError, Box<M>)> {
        let url = format!("{}{}", self.base_url, M::push_route());

        let compressed_message = match CompressedBinaryMessage::try_from_message(&message) {
            Ok(compressed_message) => compressed_message,
            Err(err) => {
                return Err((
                    HttpQueueError::MessageError(
                        self.queue_name.clone(),
                        "compressing message",
                        err,
                    )
                    .into(),
                    Box::new(message),
                ))
            }
        };

        let serialized_compressed_message = match compressed_message.to_postcard() {
            Ok(serialized_message) => serialized_message,
            Err(err) => {
                return Err((
                    HttpQueueError::MessageError(
                        self.queue_name.clone(),
                        "serializing compressed message",
                        err,
                    )
                    .into(),
                    Box::new(message),
                ))
            }
        };

        let response = match self
            .client
            .post(&url)
            .body(serialized_compressed_message)
            .send()
            .await
        {
            Ok(response) => response,
            Err(err) => {
                let context = request_error_context("pushing message", &err);
                return Err((
                    HttpQueueError::RequestError(self.queue_name.clone(), url.clone(), context, err)
                        .into(),
                    Box::new(message),
                ))
            }
        };

        if !response.status().is_success() {
            let status = response.status();
            match status {
                reqwest::StatusCode::INSUFFICIENT_STORAGE => {
                    return Err((QueueError::QueueFullError, Box::new(message)))
                }
                _ => {
                    let reason: String = response
                        .text()
                        .await
                        .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));
                    return Err((
                        HttpQueueError::UnsuccessStatusCode(
                            self.queue_name.clone(),
                            url,
                            status,
                            reason,
                            "pushing message",
                        )
                        .into(),
                        Box::new(message),
                    ));
                }
            }
        }

        Ok(())
    }

    async fn ack(&self, message_id: &str) -> Result<(), QueueError> {
        let url = format!("{}{}", self.base_url, M::ack_route());

        let response = self
            .client
            .post(&url)
            .json(message_id)
            .send()
            .await
            .map_err(|err| {
                let context = request_error_context("acknowledging message", &err);
                QueueError::from(HttpQueueError::RequestError(
                    self.queue_name.clone(),
                    url.clone(),
                    context,
                    err,
                ))
            })?;

        if !response.status().is_success() {
            let status = response.status();

            // 410
            if status == reqwest::StatusCode::GONE {
                warn!(
                    "Tried to acknowledge message '{}' which does not exist in queue '{}'",
                    message_id, self.queue_name
                );
                return Ok(());
            }

            let reason = response
                .text()
                .await
                .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));
            return Err(HttpQueueError::UnsuccessStatusCode(
                self.queue_name.clone(),
                url,
                status,
                reason,
                "acknowledging message",
            )
            .into());
        }

        Ok(())
    }

    async fn len(&self) -> Result<usize, QueueError> {
        let url = format!("{}{}", self.base_url, M::len_route());

        let response = self.client.get(&url).send().await.map_err(|err| {
            let context = request_error_context("getting length", &err);
            HttpQueueError::RequestError(self.queue_name.clone(), url.clone(), context, err)
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let reason = response
                .text()
                .await
                .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));
            return Err(HttpQueueError::UnsuccessStatusCode(
                self.queue_name.clone(),
                url,
                status,
                reason,
                "getting length",
            )
            .into());
        }

        let len: usize = response.json().await.map_err(|err| {
            QueueError::from(HttpQueueError::DeserializationError(
                self.queue_name.clone(),
                "deserializing length response",
                err,
            ))
        })?;

        Ok(len)
    }

    async fn is_full(&self) -> Result<bool, QueueError> {
        let url = format!("{}{}", self.base_url, M::is_full_route());

        let response = self.client.get(&url).send().await.map_err(|err| {
            let context = request_error_context("checking if full", &err);
            HttpQueueError::RequestError(self.queue_name.clone(), url.clone(), context, err)
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let reason = response
                .text()
                .await
                .unwrap_or_else(|e| format!("<failed to read response body: {e}>"));
            return Err(HttpQueueError::UnsuccessStatusCode(
                self.queue_name.clone(),
                url,
                status,
                reason,
                "checking if full",
            )
            .into());
        }

        let is_full: bool = response.json().await.map_err(|err| {
            QueueError::from(HttpQueueError::DeserializationError(
                self.queue_name.clone(),
                "deserializing is_full response",
                err,
            ))
        })?;

        Ok(is_full)
    }
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;

    use crate::pipeline::messages::indexing_message::IndexingMessage;

    use super::*;

    /// Exercises the trait-level `PipelineQueue<M>` API (used in-process by `LocalPipeline`,
    /// unlike `queue_server`'s tests which only exercise the compressed-level API), asserting
    /// that pushed payloads actually land on disk and are removed again on ack.
    ///
    #[tokio::test]
    async fn test_local_pipeline_queue_push_pop_ack_len_is_full() {
        let directory = temp_dir()
            .join("maccoys.tests.queue.test_local_pipeline_queue_push_pop_ack_len_is_full");
        let _ = std::fs::remove_dir_all(&directory);

        let queue = LocalPipelineQueue::<IndexingMessage>::new_at(
            2,
            Duration::from_secs(900),
            directory.clone(),
        );

        let message1 = IndexingMessage::new("test1".to_string(), vec!["run1".to_string()]);
        let message2 = IndexingMessage::new("test2".to_string(), vec!["run1".to_string()]);

        queue.push(message1.clone()).await.unwrap();
        queue.push(message2.clone()).await.unwrap();

        assert_eq!(queue.len(), 2);
        assert!(queue.is_full());
        // Both payloads should have been spooled to disk
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 2);

        let (id1, popped1) = queue.pop().await.unwrap().unwrap();
        assert_eq!(popped1.get_id(), message1.get_id());

        queue.ack(&id1).await.unwrap();
        assert_eq!(queue.len(), 1);
        // The acked message's spooled payload should have been removed
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 1);

        let (id2, popped2) = queue.pop().await.unwrap().unwrap();
        assert_eq!(popped2.get_id(), message2.get_id());
        queue.ack(&id2).await.unwrap();

        assert_eq!(queue.len(), 0);
        assert!(queue.pop().await.unwrap().is_none());

        std::fs::remove_dir_all(&directory).ok();
    }

    /// Pops without acking, waits past the WIP timeout, and confirms `reschedule()` makes the
    /// message poppable again - the mechanism `LocalPipeline`'s reschedule loop relies on.
    ///
    #[tokio::test]
    async fn test_local_pipeline_queue_reschedule() {
        let directory = temp_dir().join("maccoys.tests.queue.test_local_pipeline_queue_reschedule");
        let _ = std::fs::remove_dir_all(&directory);

        let queue = LocalPipelineQueue::<IndexingMessage>::new_at(
            1,
            Duration::from_millis(1),
            directory.clone(),
        );

        let message = IndexingMessage::new("test1".to_string(), vec!["run1".to_string()]);
        queue.push(message.clone()).await.unwrap();

        let (_id, popped) = queue.pop().await.unwrap().unwrap();
        assert_eq!(popped.get_id(), message.get_id());
        // Not acked - still counted, and no longer poppable until rescheduled
        assert_eq!(queue.len(), 1);
        assert!(queue.pop().await.unwrap().is_none());

        tokio::time::sleep(Duration::from_millis(50)).await;
        queue.reschedule().unwrap();

        let (rescheduled_id, rescheduled) = queue.pop().await.unwrap().unwrap();
        assert_eq!(rescheduled.get_id(), message.get_id());
        queue.ack(&rescheduled_id).await.unwrap();
        assert_eq!(queue.len(), 0);

        std::fs::remove_dir_all(&directory).ok();
    }
}
