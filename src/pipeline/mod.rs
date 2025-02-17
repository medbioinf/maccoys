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
