// std import
use std::{future::IntoFuture, str::FromStr, sync::Arc};

// 3rd party imports
use deadqueue::limited::Queue;
use futures::Future;
use macpepdb::tools::queue_monitor::MonitorableQueue;
use rustis::{
    client::BatchPreparedCommand,
    commands::{GenericCommands, LMoveWhere, ListCommands, StringCommands},
};
use tracing::debug;

use crate::pipeline::errors::queue_error::RedisQueueError;

//  local imports
use super::{
    configuration::TaskConfiguration, errors::queue_error::QueueError,
    messages::is_message::IsMessage,
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
}

/// Redis implementation of the pipeline queue for distributed systems
/// Messages are stored as key value pairs in Redis
/// while the queue is stored as a list. This way we do not need the hole message again for
/// acknowledging the message. As some messages get deconstructed during the task, to safe some memory
/// we need to store the message in redis.
/// Be aware that the max queue size is limited by this implementation not Redis.
/// So it is important that every client is using the same queue size.
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
        if config.redis_url.is_none() {
            return Err(RedisQueueError::RedisUrlMissing(config.queue_name.clone()).into());
        }

        debug!("Storages: {:?} / {}", &config.redis_url, &config.queue_name);

        let mut redis_client_config =
            match rustis::client::Config::from_str(config.redis_url.as_ref().unwrap()) {
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
