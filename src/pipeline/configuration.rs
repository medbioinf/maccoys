// std imports
use std::{ops::Deref, path::PathBuf};

pub const CLEANUP_QUEUE_KEY: &str = "cleanup";
pub const GOODNESS_AND_RESCORING_QUEUE_KEY: &str = "goodness_and_rescoring";
pub const COMET_SEARCH_QUEUE_KEY: &str = "comet_search";
pub const SEARCH_SPACE_GENERATION_QUEUE_KEY: &str = "search_space_generation";
pub const PREPARATION_QUEUE_KEY: &str = "preparation";
pub const INDEX_QUEUE_KEY: &str = "index";

/// All queue keys
///
pub const QUEUE_KEYS: [&str; 6] = [
    CLEANUP_QUEUE_KEY,
    GOODNESS_AND_RESCORING_QUEUE_KEY,
    COMET_SEARCH_QUEUE_KEY,
    SEARCH_SPACE_GENERATION_QUEUE_KEY,
    PREPARATION_QUEUE_KEY,
    INDEX_QUEUE_KEY,
];

/// Search paramerter
///
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct SearchParameters {
    /// Maximum charge state to search if charge is not provided by the precursor
    pub max_charge: u8,

    /// Lower mass tolerance in ppm
    pub lower_mass_tolerance_ppm: i64,

    /// Upper mass tolerance in ppm
    pub upper_mass_tolerance_ppm: i64,

    /// Maximum number of variable modifications
    pub max_variable_modifications: i8,

    /// Number of decoys per peptide
    pub decoys_per_peptide: usize,

    /// Score threshold, everything above will be discarded
    pub score_threshold: f64,

    /// If true the FASTA files will be kept
    pub keep_fasta_files: bool,
}

impl SearchParameters {
    /// Create a new default search parameter
    ///
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for SearchParameters {
    fn default() -> Self {
        Self {
            max_charge: 6,
            lower_mass_tolerance_ppm: 10,
            upper_mass_tolerance_ppm: 10,
            max_variable_modifications: 3,
            decoys_per_peptide: 0,
            score_threshold: 0.1,
            keep_fasta_files: false,
        }
    }
}

/// General task configuration
///
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct TaskConfiguration {
    /// Number of concurrent tasks
    pub num_tasks: usize,
    /// Queue name
    pub queue_name: String,
    /// Queue capacity
    pub queue_capacity: usize,
    /// Optional redis URL if the queue is not local
    pub redis_url: Option<String>,
}

/// Configuration for the search
///
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct SearchSpaceGenerationTaskConfiguration {
    #[serde(flatten)]
    pub general: TaskConfiguration,

    /// URL to the target database
    pub target_url: String,

    /// Optional URL to the decoy database
    pub decoy_url: Option<String>,

    /// Optional URL to the target lookup database
    pub target_lookup_url: Option<String>,

    /// Optional URL to the decoy cache database
    pub decoy_cache_url: Option<String>,
}

impl Deref for SearchSpaceGenerationTaskConfiguration {
    type Target = TaskConfiguration;

    fn deref(&self) -> &TaskConfiguration {
        &self.general
    }
}

/// Comet configuration
///
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct CometSearchTaskConfiguration {
    /// General task configuration
    #[serde(flatten)]
    pub general: TaskConfiguration,
    /// Path to comet executable
    pub comet_exe_path: PathBuf,
    /// Number of threads to use
    pub threads: usize,
}

impl Deref for CometSearchTaskConfiguration {
    type Target = TaskConfiguration;

    fn deref(&self) -> &TaskConfiguration {
        &self.general
    }
}

/// Configuration for the pipeline
///
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct PipelineConfiguration {
    /// Search parameters
    pub search_parameters: SearchParameters,
    /// Index task configuration
    pub index: TaskConfiguration,
    /// Preparation task configuration
    pub preparation: TaskConfiguration,
    /// Search space generation task configuration
    pub search_space_generation: SearchSpaceGenerationTaskConfiguration,
    /// Comet search task configuration
    pub comet_search: CometSearchTaskConfiguration,
    /// Goodness and rescoring task configuration
    pub goodness_and_rescoring: TaskConfiguration,
    /// Cleanup task configuration
    pub cleanup: TaskConfiguration,
    /// Storage configuration
    pub storage: PipelineStorageConfiguration,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PipelineStorageConfiguration {
    pub redis_url: Option<String>,
}

impl PipelineConfiguration {
    /// Create a new default configuration
    ///
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for PipelineConfiguration {
    fn default() -> Self {
        Self {
            search_parameters: SearchParameters::new(),
            index: TaskConfiguration {
                num_tasks: 1,
                queue_name: INDEX_QUEUE_KEY.to_string(),
                queue_capacity: 100,
                redis_url: None,
            },
            preparation: TaskConfiguration {
                num_tasks: 4,
                queue_name: PREPARATION_QUEUE_KEY.to_string(),
                queue_capacity: 1000,
                redis_url: None,
            },
            search_space_generation: SearchSpaceGenerationTaskConfiguration {
                general: TaskConfiguration {
                    num_tasks: 64,
                    queue_name: SEARCH_SPACE_GENERATION_QUEUE_KEY.to_string(),
                    queue_capacity: 100,
                    redis_url: None,
                },
                target_url: "".to_string(),
                decoy_url: None,
                target_lookup_url: None,
                decoy_cache_url: None,
            },
            comet_search: CometSearchTaskConfiguration {
                general: TaskConfiguration {
                    num_tasks: 4,
                    queue_name: COMET_SEARCH_QUEUE_KEY.to_string(),
                    queue_capacity: 100,
                    redis_url: None,
                },
                comet_exe_path: PathBuf::from("/usr/local/bin/comet"),
                threads: 2,
            },
            goodness_and_rescoring: TaskConfiguration {
                num_tasks: 2,
                queue_name: GOODNESS_AND_RESCORING_QUEUE_KEY.to_string(),
                queue_capacity: 100,
                redis_url: None,
            },
            cleanup: TaskConfiguration {
                num_tasks: 1,
                queue_name: CLEANUP_QUEUE_KEY.to_string(),
                queue_capacity: 100,
                redis_url: None,
            },
            storage: PipelineStorageConfiguration { redis_url: None },
        }
    }
}

/// Configuration for the remote entrypoint
///
#[derive(serde::Deserialize, Debug, Clone)]
pub struct RemoteEntypointConfiguration {
    /// Index task configuration
    pub index: TaskConfiguration,
    /// Preparation task configuration
    pub preparation: TaskConfiguration,
    /// Search space generation task configuration
    pub search_space_generation: SearchSpaceGenerationTaskConfiguration,
    /// Comet search task configuration
    pub comet_search: CometSearchTaskConfiguration,
    /// Goodness and rescoring task configuration
    pub goodness_and_rescoring: TaskConfiguration,
    /// Cleanup task configuration
    pub cleanup: TaskConfiguration,
    /// Storage configuration
    pub storage: PipelineStorageConfiguration,
}

/// Configuration for standalone indexing
///
#[derive(serde::Deserialize, Debug, Clone)]
pub struct StandaloneIndexingConfiguration {
    /// Index task configuration
    pub index: TaskConfiguration,
    /// Preparation task configuration
    pub preparation: TaskConfiguration,
    /// Storage configuration
    pub storage: PipelineStorageConfiguration,
}

/// Configuration for standalone preparation
///
#[derive(serde::Deserialize, Debug, Clone)]
pub struct StandalonePreparationConfiguration {
    /// Preparation task configuration
    pub preparation: TaskConfiguration,
    /// Search space generation task configuration
    pub search_space_generation: SearchSpaceGenerationTaskConfiguration,
    /// Storage configuration
    pub storage: PipelineStorageConfiguration,
}

/// Configuration for standalone search space generation
///
#[derive(serde::Deserialize, Debug, Clone)]
pub struct StandaloneSearchSpaceGenerationConfiguration {
    /// Search space generation task configuration
    pub search_space_generation: SearchSpaceGenerationTaskConfiguration,
    /// Comet search task configuration
    pub comet_search: CometSearchTaskConfiguration,
    /// Storage configuration
    pub storage: PipelineStorageConfiguration,
}

/// Configuration for standalone comet search
///
#[derive(serde::Deserialize, Debug, Clone)]
pub struct StandaloneCometSearchConfiguration {
    /// Comet search task configuration
    pub comet_search: CometSearchTaskConfiguration,
    /// Goodness and rescoring task configuration
    pub goodness_and_rescoring: TaskConfiguration,
    /// Cleanup task configuration
    pub cleanup: TaskConfiguration,
    /// Storage configuration
    pub storage: PipelineStorageConfiguration,
}

/// Configuration for standalone indexing
///
#[derive(serde::Deserialize, Debug, Clone)]
pub struct StandaloneGoodnessAndRescoringConfiguration {
    /// Goodness and rescoring task configuration
    pub goodness_and_rescoring: TaskConfiguration,
    /// Cleanup task configuration
    pub cleanup: TaskConfiguration,
    /// Storage configuration
    pub storage: PipelineStorageConfiguration,
}

/// Configuration for standalone cleanup
///
#[derive(serde::Deserialize, Debug, Clone)]
pub struct StandaloneCleanupConfiguration {
    /// Cleanup task configuration
    pub cleanup: TaskConfiguration,
    /// Storage configuration
    pub storage: PipelineStorageConfiguration,
}
