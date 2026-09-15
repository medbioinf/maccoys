/// Task to write errors to the search directories
pub mod error_task;
/// Combined worker running publication and error handling in one process
pub mod finalization_worker;
/// Task for spectrum identification
pub mod identification_task;
/// Start of the pipeline. indexing the
pub mod indexing_task;
/// Combined worker running search space generation, identification and scoring in one process
pub mod processing_worker;
/// Task to publish files into the search directory
pub mod publication_task;
/// Task to calculate the scoring
pub mod scoring_task;
/// Task to generate the search space
pub mod search_space_generation_task;
/// Trait for defining some shared behavior for the tasks
pub mod task;
