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
use super::{configuration::TaskConfiguration, search_manifest::SearchManifest};

/// Trait defining the methods for a pipeline queue
///
pub trait PipelineQueue: Send + Sync + Sized {
    /// Create a new pipeline queue
    fn new(config: &TaskConfiguration) -> impl Future<Output = Result<Self>> + Send;

    fn get_capacity(&self) -> usize;

    /// Pop a manifest from the queue
    /// Returns None if the queue is empty
    ///
    fn pop(&self) -> impl Future<Output = Option<SearchManifest>> + Send;

    /// Push a manifest to the queue
    /// Returns the manifest if the queue is full
    ///
    /// # Arguments
    /// * `manifest` - The manifest to push to the queue
    ///
    fn push(
        &self,
        manifest: SearchManifest,
    ) -> impl Future<Output = Result<(), SearchManifest>> + Send;

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
pub struct LocalPipelineQueue {
    /// Queue for search manifests to be processed
    queue: Queue<SearchManifest>,

    /// Capacity of the queue
    capacity: usize,
}

impl PipelineQueue for LocalPipelineQueue {
    async fn new(config: &TaskConfiguration) -> Result<Self> {
        Ok(Self {
            queue: Queue::new(config.queue_capacity),
            capacity: config.queue_capacity,
        })
    }

    fn get_capacity(&self) -> usize {
        self.capacity
    }

    async fn pop(&self) -> Option<SearchManifest> {
        self.queue.try_pop()
    }

    async fn push(&self, manifest: SearchManifest) -> Result<(), SearchManifest> {
        match self.queue.try_push(manifest) {
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
pub struct RedisPipelineQueue {
    /// Redis client
    client: rustis::client::Client,

    /// Name of the queue
    queue_name: String,

    /// Capacity of the queue
    capacity: usize,
}

impl PipelineQueue for RedisPipelineQueue {
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
        })
    }

    fn get_capacity(&self) -> usize {
        self.capacity
    }

    async fn pop(&self) -> Option<SearchManifest> {
        let serialized_manifest: String = match self
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
                error!("Error popping manifest from queue: {:?}", e);
                return None;
            }
        };

        if serialized_manifest.is_empty() {
            return None;
        }

        match serde_json::from_str(&serialized_manifest) {
            Ok(manifest) => manifest,
            Err(e) => {
                error!(
                    "[{}] Error deserializing manifest: {:?}",
                    self.queue_name, e
                );
                None
            }
        }
    }

    async fn push(&self, manifest: SearchManifest) -> Result<(), SearchManifest> {
        // Simple mechanism to prevent overcommitment of queue
        loop {
            if self.len().await < self.get_capacity() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
        let serialized_manifest = match serde_json::to_string(&manifest) {
            Ok(serialized_manifest) => serialized_manifest,
            Err(e) => {
                error!("[{}] Error serializing manifest: {:?}", self.queue_name, e);
                return Err(manifest);
            }
        };

        match self
            .client
            .rpush(&self.queue_name, serialized_manifest)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                error!(
                    "[{}] Error pushing manifest to queue: {:?}",
                    self.queue_name, e
                );
                Err(manifest)
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
pub struct PipelineQueueArc<T>(Arc<T>)
where
    T: PipelineQueue;

impl<T> MonitorableQueue for PipelineQueueArc<T>
where
    T: PipelineQueue + 'static,
{
    async fn len(&self) -> usize {
        self.0.len().await
    }
}

impl<Q> From<Arc<Q>> for PipelineQueueArc<Q>
where
    Q: PipelineQueue,
{
    fn from(queue: Arc<Q>) -> Self {
        Self(queue)
    }
}
