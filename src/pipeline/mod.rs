/// Pipleine configuration
pub mod configuration;

/// Pipeline queue
pub mod queue;

/// Pipeline storage
pub mod storage;

/// Local pipeline for debugging and testing
pub mod local_pipeline;
/// Messages between the tasks
pub mod messages;
/// Pipeline running remotely
pub mod remote_pipeline;
/// Web api to enqueue search in remote pipeline and receive results
pub mod remote_pipeline_web_api;
/// Separate tasks of the pipeline
pub mod tasks;

/// Errors in the pipeline
pub mod errors;

/// Utility functions for the pipeline
pub mod utils;
