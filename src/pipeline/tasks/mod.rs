/// Task to cleanup the pipeline
pub mod cleanup_task;
/// Task for spectrum identification
pub mod identification_task;
/// Start of the pipeline. indexing the
pub mod indexing_task;
/// Task to prepare the spectrum folders
pub mod preparation_task;
/// Task to calculate the scoring
pub mod scoring_task;
/// Task to generate the search space
pub mod search_space_generation_task;
/// Trait for defining some shared behavior for the tasks
pub mod task;
