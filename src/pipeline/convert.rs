// 3rd party imports
use anyhow::Result;
use futures::Future;

// local imports
use super::{
    configuration::{
        StandaloneCometSearchConfiguration, StandaloneIndexingConfiguration,
        StandalonePreparationConfiguration, StandaloneScoringConfiguration,
        StandaloneSearchSpaceGenerationConfiguration,
    },
    queue::{PipelineQueue, RedisPipelineQueue},
    storage::{PipelineStorage, RedisPipelineStorage},
};

/// Trait to convert a configuration into input and output queues
/// and storages
///
pub trait AsInputOutputQueueAndStorage {
    /// Convert the configuration into input and output queues
    ///
    fn as_input_output_queue_and_storage(
        &self,
    ) -> impl Future<Output = Result<(RedisPipelineStorage, RedisPipelineQueue, RedisPipelineQueue)>>;
}

impl AsInputOutputQueueAndStorage for StandaloneIndexingConfiguration {
    async fn as_input_output_queue_and_storage(
        &self,
    ) -> Result<(RedisPipelineStorage, RedisPipelineQueue, RedisPipelineQueue)> {
        let storage = RedisPipelineStorage::new(&self.storage).await?;
        let input_queue = RedisPipelineQueue::new(&self.index).await?;
        let output_queue = RedisPipelineQueue::new(&self.preparation).await?;

        Ok((storage, input_queue, output_queue))
    }
}

impl AsInputOutputQueueAndStorage for StandalonePreparationConfiguration {
    async fn as_input_output_queue_and_storage(
        &self,
    ) -> Result<(RedisPipelineStorage, RedisPipelineQueue, RedisPipelineQueue)> {
        let storage = RedisPipelineStorage::new(&self.storage).await?;
        let input_queue = RedisPipelineQueue::new(&self.preparation).await?;
        let output_queue = RedisPipelineQueue::new(&self.search_space_generation).await?;

        Ok((storage, input_queue, output_queue))
    }
}

impl AsInputOutputQueueAndStorage for StandaloneSearchSpaceGenerationConfiguration {
    async fn as_input_output_queue_and_storage(
        &self,
    ) -> Result<(RedisPipelineStorage, RedisPipelineQueue, RedisPipelineQueue)> {
        let storage = RedisPipelineStorage::new(&self.storage).await?;
        let input_queue = RedisPipelineQueue::new(&self.search_space_generation).await?;
        let output_queue = RedisPipelineQueue::new(&self.comet_search).await?;

        Ok((storage, input_queue, output_queue))
    }
}

impl AsInputOutputQueueAndStorage for StandaloneCometSearchConfiguration {
    async fn as_input_output_queue_and_storage(
        &self,
    ) -> Result<(RedisPipelineStorage, RedisPipelineQueue, RedisPipelineQueue)> {
        let storage = RedisPipelineStorage::new(&self.storage).await?;
        let input_queue = RedisPipelineQueue::new(&self.comet_search).await?;
        let output_queue = RedisPipelineQueue::new(&self.goodness_and_rescoring).await?;

        Ok((storage, input_queue, output_queue))
    }
}

impl AsInputOutputQueueAndStorage for StandaloneScoringConfiguration {
    async fn as_input_output_queue_and_storage(
        &self,
    ) -> Result<(RedisPipelineStorage, RedisPipelineQueue, RedisPipelineQueue)> {
        let storage = RedisPipelineStorage::new(&self.storage).await?;
        let input_queue = RedisPipelineQueue::new(&self.goodness_and_rescoring).await?;
        let output_queue = RedisPipelineQueue::new(&self.cleanup).await?;

        Ok((storage, input_queue, output_queue))
    }
}
