// std import
use std::{str::FromStr, sync::Arc};

// 3rd party imports
use anyhow::{bail, Context, Result};
use deadqueue::limited::Queue;
use futures::Future;
use macpepdb::tools::queue_monitor::MonitorableQueue;
use rustis::commands::ListCommands;
use tracing::{error, trace};

//  local imports
use super::{configuration::TaskConfiguration, messages::is_message::IsMessage};

/// Trait defining the methods for a pipeline queue
///
pub trait PipelineQueue<M>: Sized + Send + Sync
where
    M: IsMessage,
{
    /// Create a new pipeline queue
    fn new(config: &TaskConfiguration) -> impl Future<Output = Result<Self>> + Send;

    fn get_capacity(&self) -> usize;

    /// Pop a message from the queue
    /// Returns None if the queue is empty
    ///
    fn pop(&self) -> impl Future<Output = Option<M>> + Send;

    /// Push a message to the queue
    /// Returns the message if the queue is full
    ///
    /// # Arguments
    /// * `message` - Manifest to be pushed`
    ///
    fn push(&self, message: M) -> impl Future<Output = Result<(), M>> + Send;

    /// Get the length of the queue
    ///
    fn len(&self) -> impl Future<Output = usize> + Send;

    /// Checks if the queue is empty
    ///
    fn is_empty(&self) -> impl Future<Output = bool> + Send {
        async { self.len().await == 0 }
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
    async fn new(config: &TaskConfiguration) -> Result<Self> {
        Ok(Self {
            queue: Queue::new(config.queue_capacity),
            capacity: config.queue_capacity,
        })
    }

    fn get_capacity(&self) -> usize {
        self.capacity
    }

    async fn pop(&self) -> Option<M> {
        self.queue.try_pop()
    }

    async fn push(&self, message: M) -> Result<(), M> {
        match self.queue.try_push(message) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn len(&self) -> usize {
        self.queue.len()
    }
}

/// Redis implementation of the pipeline queue for distributed systems
///
pub struct RedisPipelineQueue<M>
where
    M: IsMessage,
{
    /// Redis client
    client: rustis::client::Client,

    /// Name of the queue
    queue_name: String,

    /// Capacity of the queue
    capacity: usize,

    _phantom_message: std::marker::PhantomData<M>,
}

impl<M> PipelineQueue<M> for RedisPipelineQueue<M>
where
    M: IsMessage,
{
    async fn new(config: &TaskConfiguration) -> Result<Self> {
        if config.redis_url.is_none() {
            bail!("Redis URL is None")
        }

        trace!("Storages: {:?} / {}", &config.redis_url, &config.queue_name);

        let mut redis_client_config =
            rustis::client::Config::from_str(config.redis_url.as_ref().unwrap())?;
        redis_client_config.retry_on_error = true;
        redis_client_config.reconnection = rustis::client::ReconnectionConfig::new_constant(0, 5);

        let client = rustis::client::Client::connect(redis_client_config)
            .await
            .context("Error opening connection to Redis")?;
        Ok(Self {
            client,
            queue_name: config.queue_name.clone(),
            capacity: config.queue_capacity,
            _phantom_message: std::marker::PhantomData,
        })
    }

    fn get_capacity(&self) -> usize {
        self.capacity
    }

    async fn pop(&self) -> Option<M> {
        let serialized_message: String = match self
            .client
            .lpop::<_, _, Vec<String>>(&self.queue_name, 1)
            .await
        {
            Ok(response) => {
                if !response.is_empty() {
                    response[0].clone()
                } else {
                    String::new()
                }
            }
            Err(e) => {
                error!("Error popping message from queue: {:?}", e);
                return None;
            }
        };

        if serialized_message.is_empty() {
            return None;
        }

        match serde_json::from_str(&serialized_message) {
            Ok(message) => message,
            Err(e) => {
                error!("[{}] Error deserializing message: {:?}", self.queue_name, e);
                None
            }
        }
    }

    async fn push(&self, message: M) -> Result<(), M> {
        // Simple mechanism to prevent overcommitment of queue
        loop {
            if self.len().await < self.get_capacity() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
        let serialized_message = match serde_json::to_string(&message) {
            Ok(serialized_message) => serialized_message,
            Err(e) => {
                error!("[{}] Error serializing message: {:?}", self.queue_name, e);
                return Err(message);
            }
        };

        match self
            .client
            .rpush(&self.queue_name, serialized_message)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                error!(
                    "[{}] Error pushing message to queue: {:?}",
                    self.queue_name, e
                );
                Err(message)
            }
        }
    }

    async fn len(&self) -> usize {
        match self.client.llen(&self.queue_name).await {
            Ok(size) => size,
            Err(e) => {
                error!("[{}] Error getting queue size: {:?}", self.queue_name, e);
                self.capacity + 111
            }
        }
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
        self.queue.len().await
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
