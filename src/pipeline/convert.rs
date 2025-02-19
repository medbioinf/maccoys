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
};

/// Trait to convert a configuration into input and output queues
/// and storages
///
pub trait AsInputOutputQueue {
    /// Convert the configuration into input and output queues
    ///
    fn as_input_output_queue(
        &self,
    ) -> impl Future<Output = Result<(RedisPipelineQueue, RedisPipelineQueue)>>;
}

impl AsInputOutputQueue for StandaloneIndexingConfiguration {
    async fn as_input_output_queue(&self) -> Result<(RedisPipelineQueue, RedisPipelineQueue)> {
        let input_queue = RedisPipelineQueue::new(&self.index).await?;
        let output_queue = RedisPipelineQueue::new(&self.preparation).await?;

        Ok((input_queue, output_queue))
    }
}

impl AsInputOutputQueue for StandalonePreparationConfiguration {
    async fn as_input_output_queue(&self) -> Result<(RedisPipelineQueue, RedisPipelineQueue)> {
        let input_queue = RedisPipelineQueue::new(&self.preparation).await?;
        let output_queue = RedisPipelineQueue::new(&self.search_space_generation).await?;

        Ok((input_queue, output_queue))
    }
}

impl AsInputOutputQueue for StandaloneSearchSpaceGenerationConfiguration {
    async fn as_input_output_queue(&self) -> Result<(RedisPipelineQueue, RedisPipelineQueue)> {
        let input_queue = RedisPipelineQueue::new(&self.search_space_generation).await?;
        let output_queue = RedisPipelineQueue::new(&self.comet_search).await?;

        Ok((input_queue, output_queue))
    }
}

impl AsInputOutputQueue for StandaloneCometSearchConfiguration {
    async fn as_input_output_queue(&self) -> Result<(RedisPipelineQueue, RedisPipelineQueue)> {
        let input_queue = RedisPipelineQueue::new(&self.comet_search).await?;
        let output_queue = RedisPipelineQueue::new(&self.goodness_and_rescoring).await?;

        Ok((input_queue, output_queue))
    }
}

impl AsInputOutputQueue for StandaloneScoringConfiguration {
    async fn as_input_output_queue(&self) -> Result<(RedisPipelineQueue, RedisPipelineQueue)> {
        let input_queue = RedisPipelineQueue::new(&self.goodness_and_rescoring).await?;
        let output_queue = RedisPipelineQueue::new(&self.cleanup).await?;

        Ok((input_queue, output_queue))
    }
}
