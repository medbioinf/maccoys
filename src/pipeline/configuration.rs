use std::{ops::Deref, path::PathBuf};

use xcorrrs::configuration::Configuration as XcorrConfiguration;

pub const INDEXING_QUEUE_KEY: &str = "index";
pub const SEARCH_SPACE_GENERATION_QUEUE_KEY: &str = "search_space_generation";
pub const IDENTIFICATION_QUEUE_KEY: &str = "identification";
pub const SCORING_QUEUE_KEY: &str = "scoring";
pub const PUBLICATION_QUEUE_KEY: &str = "publication";
pub const ERROR_QUEUE_KEY: &str = "publication";

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

    pub xcorr: XcorrConfiguration,
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
            xcorr: XcorrConfiguration::default(),
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
pub struct IdentificationTaskConfiguration {
    /// General task configuration
    #[serde(flatten)]
    pub general: TaskConfiguration,
    /// Number of threads to use
    pub threads: usize,
}

impl Deref for IdentificationTaskConfiguration {
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
    /// Search space generation task configuration
    pub search_space_generation: SearchSpaceGenerationTaskConfiguration,
    /// Identification task configuration
    pub identification: IdentificationTaskConfiguration,
    /// scoring task configuration
    pub scoring: TaskConfiguration,
    /// Publication task configuration
    pub publication: TaskConfiguration,
    /// Error task configuration
    pub error: TaskConfiguration,
    /// Storage configuration
    pub storage: PipelineStorageConfiguration,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PipelineStorageConfiguration {
    /// Seconds after the last access to a flag after which is automatically removed.
    pub time_to_idle: u64,
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
                queue_name: INDEXING_QUEUE_KEY.to_string(),
                queue_capacity: 100,
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
            identification: IdentificationTaskConfiguration {
                general: TaskConfiguration {
                    num_tasks: 4,
                    queue_name: IDENTIFICATION_QUEUE_KEY.to_string(),
                    queue_capacity: 100,
                    redis_url: None,
                },
                threads: 2,
            },
            scoring: TaskConfiguration {
                num_tasks: 2,
                queue_name: SCORING_QUEUE_KEY.to_string(),
                queue_capacity: 100,
                redis_url: None,
            },
            publication: TaskConfiguration {
                num_tasks: 1,
                queue_name: PUBLICATION_QUEUE_KEY.to_string(),
                queue_capacity: 100,
                redis_url: None,
            },
            error: TaskConfiguration {
                num_tasks: 1,
                queue_name: ERROR_QUEUE_KEY.to_string(),
                queue_capacity: 100,
                redis_url: None,
            },
            storage: PipelineStorageConfiguration {
                time_to_idle: 86_400, // 24 hours
                redis_url: None,
            },
        }
    }
}

/// Configuration for the remote entrypoint
///
#[derive(serde::Deserialize, Debug, Clone)]
pub struct RemoteEntypointConfiguration {
    /// Search parameters
    pub search_parameters: SearchParameters,
    /// Index task configuration
    pub index: TaskConfiguration,
    /// Search space generation task configuration
    pub search_space_generation: SearchSpaceGenerationTaskConfiguration,
    /// Identification task configuration
    pub identification: IdentificationTaskConfiguration,
    /// scoring task configuration
    pub scoring: TaskConfiguration,
    /// Publication task configuration
    pub publication: TaskConfiguration,
    /// Error task configuration
    pub error: TaskConfiguration,
    /// Storage configuration
    pub storage: PipelineStorageConfiguration,
    /// Promtheus base URL
    pub prometheus_base_url: String,
    /// Work directory
    pub work_directory: PathBuf,
}

/// Configuration for standalone indexing
///
#[derive(serde::Deserialize, Debug, Clone)]
pub struct StandaloneIndexingConfiguration {
    /// Index task configuration
    pub index: TaskConfiguration,
    /// Search space generation task configuration
    pub search_space_generation: SearchSpaceGenerationTaskConfiguration,
    /// Error task configuration
    pub error: TaskConfiguration,
    /// Storage configuration
    pub storage: PipelineStorageConfiguration,
    /// Work directory
    pub work_directory: PathBuf,
}

/// Configuration for standalone search space generation
///
#[derive(serde::Deserialize, Debug, Clone)]
pub struct StandaloneSearchSpaceGenerationConfiguration {
    /// Search space generation task configuration
    pub search_space_generation: SearchSpaceGenerationTaskConfiguration,
    /// Comet search task configuration
    pub identification: IdentificationTaskConfiguration,
    /// Publication task configuration
    pub publication: TaskConfiguration,
    /// Error task configuration
    pub error: TaskConfiguration,
    /// Storage configuration
    pub storage: PipelineStorageConfiguration,
}

/// Configuration for standalone comet search
///
#[derive(serde::Deserialize, Debug, Clone)]
pub struct StandaloneIdentificationConfiguration {
    /// Comet search task configuration
    pub identification: IdentificationTaskConfiguration,
    /// Goodness and rescoring task configuration
    pub scoring: TaskConfiguration,
    /// Publication task configuration
    pub publication: TaskConfiguration,
    /// Error task configuration
    pub error: TaskConfiguration,
    /// Storage configuration
    pub storage: PipelineStorageConfiguration,
}

/// Configuration for standalone indexing
///
#[derive(serde::Deserialize, Debug, Clone)]
pub struct StandaloneScoringConfiguration {
    /// Scoring task configuration
    pub scoring: TaskConfiguration,
    /// Publication task configuration
    pub publication: TaskConfiguration,
    /// Error task configuration
    pub error: TaskConfiguration,
}

/// Configuration for standalone file publication
///
#[derive(serde::Deserialize, Debug, Clone)]
pub struct StandalonePublicationConfiguration {
    /// Cleanup task configuration
    pub publication: TaskConfiguration,
    /// Error task configuration
    pub error: TaskConfiguration,
    /// Storage configuration
    pub storage: PipelineStorageConfiguration,
    /// Work directory
    pub work_directory: PathBuf,
}

/// Configuration for standalone error handling
///
#[derive(serde::Deserialize, Debug, Clone)]
pub struct StandaloneErrorConfiguration {
    /// Error task configuration
    pub error: TaskConfiguration,
    /// Work directory
    pub work_directory: PathBuf,
}
