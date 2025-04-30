/// Task to write errors to the search directories
pub mod error_task;
/// Task for spectrum identification
pub mod identification_task;
/// Start of the pipeline. indexing the
pub mod indexing_task;
/// Task to publish files into the search directory
pub mod publication_task;
/// Task to calculate the scoring
pub mod scoring_task;
/// Task to generate the search space
pub mod search_space_generation_task;
/// Trait for defining some shared behavior for the tasks
pub mod task;
