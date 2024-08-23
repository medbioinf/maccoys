// 3rd party imports
use anyhow::Result;
use futures::Future;

// local imports
use super::{
    configuration::{
        StandaloneCometSearchConfiguration, StandaloneGoodnessAndRescoringConfiguration,
        StandaloneIndexingConfiguration, StandalonePreparationConfiguration,
        StandaloneSearchSpaceGenerationConfiguration,
    },
    queue::{PipelineQueue, RedisPipelineQueue},
    storage::{PipelineStorage, RedisPipelineStorage},
};

/// Trait to convert a configuration into input and output queues
/// and storages
///
pub trait IntoInputOutputQueueAndStorage {
    /// Convert the configuration into input and output queues
    ///
    fn into_input_output_queue_and_storage(
        &self,
    ) -> impl Future<Output = Result<(RedisPipelineStorage, RedisPipelineQueue, RedisPipelineQueue)>>;
}

impl IntoInputOutputQueueAndStorage for StandaloneIndexingConfiguration {
    async fn into_input_output_queue_and_storage(
        &self,
    ) -> Result<(RedisPipelineStorage, RedisPipelineQueue, RedisPipelineQueue)> {
        let storage = RedisPipelineStorage::new(&self.storage).await?;
        let input_queue = RedisPipelineQueue::new(&self.index).await?;
        let output_queue = RedisPipelineQueue::new(&self.preparation).await?;

        Ok((storage, input_queue, output_queue))
    }
}

impl IntoInputOutputQueueAndStorage for StandalonePreparationConfiguration {
    async fn into_input_output_queue_and_storage(
        &self,
    ) -> Result<(RedisPipelineStorage, RedisPipelineQueue, RedisPipelineQueue)> {
        let storage = RedisPipelineStorage::new(&self.storage).await?;
        let input_queue = RedisPipelineQueue::new(&self.preparation).await?;
        let output_queue = RedisPipelineQueue::new(&self.search_space_generation).await?;

        Ok((storage, input_queue, output_queue))
    }
}

impl IntoInputOutputQueueAndStorage for StandaloneSearchSpaceGenerationConfiguration {
    async fn into_input_output_queue_and_storage(
        &self,
    ) -> Result<(RedisPipelineStorage, RedisPipelineQueue, RedisPipelineQueue)> {
        let storage = RedisPipelineStorage::new(&self.storage).await?;
        let input_queue = RedisPipelineQueue::new(&self.search_space_generation).await?;
        let output_queue = RedisPipelineQueue::new(&self.comet_search).await?;

        Ok((storage, input_queue, output_queue))
    }
}

impl IntoInputOutputQueueAndStorage for StandaloneCometSearchConfiguration {
    async fn into_input_output_queue_and_storage(
        &self,
    ) -> Result<(RedisPipelineStorage, RedisPipelineQueue, RedisPipelineQueue)> {
        let storage = RedisPipelineStorage::new(&self.storage).await?;
        let input_queue = RedisPipelineQueue::new(&self.comet_search).await?;
        let output_queue = RedisPipelineQueue::new(&self.goodness_and_rescoring).await?;

        Ok((storage, input_queue, output_queue))
    }
}

impl IntoInputOutputQueueAndStorage for StandaloneGoodnessAndRescoringConfiguration {
    async fn into_input_output_queue_and_storage(
        &self,
    ) -> Result<(RedisPipelineStorage, RedisPipelineQueue, RedisPipelineQueue)> {
        let storage = RedisPipelineStorage::new(&self.storage).await?;
        let input_queue = RedisPipelineQueue::new(&self.goodness_and_rescoring).await?;
        let output_queue = RedisPipelineQueue::new(&self.cleanup).await?;

        Ok((storage, input_queue, output_queue))
    }
}
