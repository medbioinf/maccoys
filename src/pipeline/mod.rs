/// Functions to run the pipeline
#[allow(clippy::module_inception)]
pub mod pipeline;

/// Pipleine configuration
pub mod configuration;

/// Pipeline queue
pub mod queue;

/// Pipeline storage
pub mod storage;

/// Search manifest which tracks the informationa and progress through out the pipeline
pub mod search_manifest;

/// Type conversion
pub mod convert;
/// Local pipeline for debugging and testing
pub mod local_pipeline;
/// Pipeline running remotely
pub mod remote_pipeline;
/// Web api to enqueue search in remote pipeline and receive results
pub mod remote_pipeline_web_api;
/// Separate tasks of the pipeline
pub mod tasks;
