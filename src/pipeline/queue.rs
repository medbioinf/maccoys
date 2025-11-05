use std::{future::IntoFuture, str::FromStr, sync::Arc};

use deadqueue::limited::Queue;
use futures::Future;
use macpepdb::tools::queue_monitor::MonitorableQueue;
use rustis::{
    client::BatchPreparedCommand,
    commands::{GenericCommands, LMoveWhere, ListCommands, StringCommands},
};
use tracing::debug;

use super::{
    configuration::TaskConfiguration, errors::queue_error::QueueError,
    messages::is_message::IsMessage,
};

use crate::pipeline::{
    errors::queue_error::{HttpQueueError, RedisQueueError},
    messages::compressed_binary_message::CompressedBinaryMessage,
    queue_server::{IsQueueRouteable, QueueController},
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

/// Implementation of a local pipeline queue. useful to debug, testing, reviewing or
/// very beefy servers
///
pub struct LocalPipelineQueue<M>
where
    M: IsMessage,
{
    /// Queue for search messages to be processed
    queue: Queue<M>,

    /// Capacity of the queue
    capacity: usize,
}

impl<M> PipelineQueue<M> for LocalPipelineQueue<M>
where
    M: IsMessage,
{
    async fn new(config: &TaskConfiguration) -> Result<Self, QueueError> {
        Ok(Self {
            queue: Queue::new(config.queue_capacity),
            capacity: config.queue_capacity,
        })
    }

    async fn get_capacity(&self) -> Result<usize, QueueError> {
        Ok(self.capacity)
    }

    async fn pop(&self) -> Result<Option<(String, M)>, QueueError> {
        let message = self.queue.try_pop();
        if let Some(message) = message {
            return Ok(Some((message.get_id(), message)));
        }
        Ok(None)
    }

    async fn push(&self, message: M) -> Result<(), (QueueError, Box<M>)> {
        match self.queue.try_push(message) {
            Ok(_) => Ok(()),
            Err(message) => Err((QueueError::QueueFullError, Box::new(message))),
        }
    }

    async fn ack(&self, _message_id: &str) -> Result<(), QueueError> {
        // No need to do anything for local queue
        Ok(())
    }

    async fn len(&self) -> Result<usize, QueueError> {
        Ok(self.queue.len())
    }

    async fn is_full(&self) -> Result<bool, QueueError> {
        Ok(self.queue.is_full())
    }
}

/// Queue implemented to use Redis as backend
/// The messages are stored as string with their ID as key. The queue itself is a list
/// containing the message IDs. When popping a message, the ID is moved to a backup list (WIP)
/// It is important to note, that redis lists seem to have no maximum capacity. So the capacity
/// is enforced by this implementation. and can slightly differ from the configured capacity.
///
/// This implementation proofed unstable with very large messages (20MB +) as Redis seems to
/// to close connections to client when the transfer takes to long.
///
pub struct RedisPipelineQueue<M>
where
    M: IsMessage,
{
    /// Redis client
    client: rustis::client::Client,

    /// Capacity of the queue
    capacity: usize,

    /// Name of the queue
    queue_name: String,

    /// WIP queue name
    wip_queue_name: String,

    _phantom_message: std::marker::PhantomData<M>,
}

impl<M> RedisPipelineQueue<M>
where
    M: IsMessage,
{
    /// Creates a new WIP queue name
    ///
    /// # Arguments
    /// * `queue_name` - Name of the queue
    ///
    fn create_wip_queue_name(queue_name: &str) -> String {
        format!("{}_wip", queue_name)
    }
}

impl<M> PipelineQueue<M> for RedisPipelineQueue<M>
where
    M: IsMessage,
{
    async fn new(config: &TaskConfiguration) -> Result<Self, QueueError> {
        // Check redis URL as it is optional in the config
        if config.queue_url.is_none() {
            return Err(RedisQueueError::RedisUrlMissing(config.queue_name.clone()).into());
        }

        debug!("Storages: {:?} / {}", &config.queue_url, &config.queue_name);

        let mut redis_client_config =
            match rustis::client::Config::from_str(config.queue_url.as_ref().unwrap()) {
                Ok(config) => config,
                Err(e) => {
                    return Err(RedisQueueError::ConfigError(config.queue_name.clone(), e).into())
                }
            };

        redis_client_config.retry_on_error = true;
        redis_client_config.reconnection = rustis::client::ReconnectionConfig::new_constant(0, 5);

        let client = match rustis::client::Client::connect(redis_client_config).await {
            Ok(client) => client,
            Err(e) => {
                return Err(RedisQueueError::ConnectionError(config.queue_name.clone(), e).into())
            }
        };

        Ok(Self {
            client,
            queue_name: config.queue_name.clone(),
            wip_queue_name: Self::create_wip_queue_name(&config.queue_name),
            capacity: config.queue_capacity,
            _phantom_message: std::marker::PhantomData,
        })
    }

    async fn get_capacity(&self) -> Result<usize, QueueError> {
        Ok(self
            .client
            .llen(self.queue_name.clone())
            .await
            .map_err(|e| {
                RedisQueueError::RedisError(self.queue_name.clone(), "getting capacity", e)
            })?)
    }

    async fn pop(&self) -> Result<Option<(String, M)>, QueueError> {
        let message_id: String = self
            .client
            .lmove(
                &self.queue_name,
                &self.wip_queue_name,
                LMoveWhere::Right,
                LMoveWhere::Left,
            )
            .into_future()
            .await
            .map_err(|e| {
                RedisQueueError::RedisError(
                    self.queue_name.clone(),
                    "getting message ID during message popping",
                    e,
                )
            })?;

        if message_id.is_empty() {
            return Ok(None);
        }

        let serialized_message: String = self.client.get(&message_id).await.map_err(|e| {
            RedisQueueError::RedisError(
                self.queue_name.clone(),
                "getting message during message popping",
                e,
            )
        })?;

        match serde_json::from_str::<M>(&serialized_message) {
            Ok(message) => Ok(Some((message_id, message))),
            Err(e) => Err(RedisQueueError::DeserializationError(
                self.queue_name.clone(),
                "popping message",
                e,
            )
            .into()),
        }
    }

    async fn push(&self, message: M) -> Result<(), (QueueError, Box<M>)> {
        let current_len = match self.len().await {
            Ok(len) => len,
            Err(e) => {
                return Err((e, Box::new(message)));
            }
        };

        if current_len >= self.capacity {
            return Err((QueueError::QueueFullError, Box::new(message)));
        }

        let serialized_message = match serde_json::to_string(&message) {
            Ok(serialized_message) => serialized_message,
            Err(e) => {
                return Err((
                    RedisQueueError::SerializationError(
                        self.queue_name.clone(),
                        "pushing message",
                        e,
                    )
                    .into(),
                    Box::new(message),
                ))
            }
        };
        let message_id = message.get_id();

        let mut transaction = self.client.create_transaction();

        transaction.set(&message_id, serialized_message).forget();
        transaction.rpush(&self.queue_name, message_id).forget();
        let _: Vec<rustis::resp::Value> = transaction.execute().await.map_err(|e| {
            (
                RedisQueueError::RedisError(self.queue_name.clone(), "pushing message", e).into(),
                Box::new(message),
            )
        })?;

        Ok(())
    }

    async fn ack(&self, message_id: &str) -> Result<(), QueueError> {
        let mut transaction = self.client.create_transaction();

        transaction.del(message_id).forget();
        transaction
            .lrem(&self.wip_queue_name, 0, message_id)
            .forget();
        let _: Vec<rustis::resp::Value> = transaction.execute().await.map_err(|e| {
            RedisQueueError::RedisError(self.queue_name.clone(), "acknowledging message", e)
        })?;

        Ok(())
    }

    async fn len(&self) -> Result<usize, QueueError> {
        Ok(self.client.llen(&self.queue_name).await.map_err(|e| {
            RedisQueueError::RedisError(self.queue_name.clone(), "getting length", e)
        })?)
    }

    async fn is_full(&self) -> Result<bool, QueueError> {
        let len = self.len().await?;
        Ok(len >= self.capacity)
    }
}

/// Queue implemented used [crate::pipeline::queue_server::QueueServer] as backend
/// Messages are convert to compressed binary messages and transfered using HTTP.
///
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

        let base_url = format!(
            "{}{}",
            config.queue_url.as_ref().unwrap(),
            QueueController::path()
        );

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .user_agent(config.queue_name.clone())
            // .default_headers(default_header)
            .http2_prior_knowledge()
            .build()
            .map_err(|err| {
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
            QueueError::from(HttpQueueError::RequestError(
                self.queue_name.clone(),
                "getting capacity",
                err,
            ))
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let reason = response.text().await.unwrap_or_default();
            return Err(HttpQueueError::UnsuccessStatusCode(
                self.queue_name.clone(),
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
            QueueError::from(HttpQueueError::RequestError(
                self.queue_name.clone(),
                "getting message",
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
                let reason = response.text().await.unwrap_or_default();
                return Err(HttpQueueError::UnsuccessStatusCode(
                    self.queue_name.clone(),
                    status,
                    reason,
                    "popping message",
                )
                .into());
            }
        }

        let message_bytes = response.bytes().await.map_err(|err| {
            HttpQueueError::RequestError(self.queue_name.clone(), "accessing body for message", err)
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
                return Err((
                    HttpQueueError::RequestError(self.queue_name.clone(), "getting message", err)
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
                    let reason: String = response.text().await.unwrap_or_default();
                    return Err((
                        HttpQueueError::UnsuccessStatusCode(
                            self.queue_name.clone(),
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
                QueueError::from(HttpQueueError::RequestError(
                    self.queue_name.clone(),
                    "acknowledging message",
                    err,
                ))
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let reason = response.text().await.unwrap_or_default();
            return Err(HttpQueueError::UnsuccessStatusCode(
                self.queue_name.clone(),
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
            HttpQueueError::RequestError(self.queue_name.clone(), "acknowledging message", err)
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let reason = response.text().await.unwrap_or_default();
            return Err(HttpQueueError::UnsuccessStatusCode(
                self.queue_name.clone(),
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
            HttpQueueError::RequestError(self.queue_name.clone(), "checking if full", err)
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let reason = response.text().await.unwrap_or_default();
            return Err(HttpQueueError::UnsuccessStatusCode(
                self.queue_name.clone(),
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

/// New Arc type to implement the MonitorableQueue trait from `macpepdb`
///
pub struct PipelineQueueArc<M, Q>
where
    M: IsMessage + 'static,
    Q: PipelineQueue<M> + 'static,
{
    /// Queue
    queue: Arc<Q>,
    _phantom_message: std::marker::PhantomData<M>,
}

impl<M, Q> MonitorableQueue for PipelineQueueArc<M, Q>
where
    M: IsMessage + 'static,
    Q: PipelineQueue<M> + 'static,
{
    async fn len(&self) -> usize {
        self.queue.len().await.unwrap_or(0)
    }
}

impl<M, Q> From<Arc<Q>> for PipelineQueueArc<M, Q>
where
    M: IsMessage + 'static,
    Q: PipelineQueue<M> + 'static,
{
    fn from(queue: Arc<Q>) -> Self {
        Self {
            queue,
            _phantom_message: std::marker::PhantomData,
        }
    }
}
