// std imports
use std::{collections::HashMap, path::PathBuf};

pub const CLEANUP_QUEUE_KEY: &str = "cleanup";
pub const GOODNESS_AND_RESCORING_QUEUE_KEY: &str = "goodness_and_rescoring";
pub const COMET_SEARCH_QUEUE_KEY: &str = "comet_search";
pub const SEARCH_SPACE_GENERATION_QUEUE_KEY: &str = "search_space_generation";
pub const PREPARATION_QUEUE_KEY: &str = "preparation";
pub const INDEX_QUEUE_KEY: &str = "index";

pub const QUEUE_KEYS: [&str; 6] = [
    CLEANUP_QUEUE_KEY,
    GOODNESS_AND_RESCORING_QUEUE_KEY,
    COMET_SEARCH_QUEUE_KEY,
    SEARCH_SPACE_GENERATION_QUEUE_KEY,
    PREPARATION_QUEUE_KEY,
    INDEX_QUEUE_KEY,
];

/// Comet configuration
///
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct PipelineCometConfiguration {
    pub comet_exe_path: PathBuf,
    pub threads: usize,
}

/// General pipeline configuration
///
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct PipelineGeneralConfiguration {
    /// Work directory where on folder per MS run is created
    pub work_dir: PathBuf,

    /// Number of concurrent preparation tasks
    pub num_preparation_tasks: usize,

    /// Number of concurrent search space generation tasks
    pub num_search_space_generation_tasks: usize,

    /// Number of concurrent Comet search tasks
    pub num_comet_search_tasks: usize,

    /// Number of concurrent goodness and rescoring tasks
    pub num_goodness_and_rescoring_tasks: usize,

    /// Number of concurrent cleanup tasks
    pub num_cleanup_tasks: usize,

    /// Keep fasta files after search
    pub keep_fasta_files: bool,
}

/// Configuration for the search
///
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct PipelineSearchConfiguration {
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

    /// Optional Path to the PTM file
    pub ptm_file_path: Option<PathBuf>,

    /// URL to the target database
    pub target_url: String,

    /// Optional URL to the decoy database
    pub decoy_url: Option<String>,

    /// Optional URL to the target lookup database
    pub target_lookup_url: Option<String>,

    /// Optional URL to the decoy cache database
    pub decoy_cache_url: Option<String>,
}

/// Configuration for the queues
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct PipelineQueueConfiguration {
    /// Default capacity for the queues
    pub default_capacity: usize,

    /// Capacities for the different queues
    pub capacities: HashMap<String, usize>,

    /// Optional URL to the Redis server
    pub redis_url: Option<String>,

    /// Optional names for the Redis queues to be used.
    /// If not set, the queues are named afte the [QUEUE_KEYS]
    pub redis_queue_names: HashMap<String, String>,
}

/// Configuration for the pipeline
///
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct PipelineConfiguration {
    /// General pipeline configuration
    pub general: PipelineGeneralConfiguration,

    /// Search configuration
    pub search: PipelineSearchConfiguration,

    /// Comet configuration
    pub comet: PipelineCometConfiguration,

    /// Queue configuration
    pub pipelines: PipelineQueueConfiguration,
}

impl PipelineConfiguration {
    /// Create a new default configuration
    ///
    pub fn new() -> Self {
        Self {
            general: PipelineGeneralConfiguration {
                work_dir: PathBuf::from("./"),
                num_preparation_tasks: 1,
                num_search_space_generation_tasks: 1,
                num_comet_search_tasks: 1,
                num_goodness_and_rescoring_tasks: 1,
                num_cleanup_tasks: 1,
                keep_fasta_files: true,
            },
            search: PipelineSearchConfiguration {
                max_charge: 6,
                lower_mass_tolerance_ppm: 10,
                upper_mass_tolerance_ppm: 10,
                max_variable_modifications: 3,
                decoys_per_peptide: 1,
                ptm_file_path: Some(PathBuf::from("./ptms.csv")),
                target_url: "http://127.0.0.1:3000".to_owned(),
                decoy_url: None,
                target_lookup_url: None,
                decoy_cache_url: None,
            },
            comet: PipelineCometConfiguration {
                threads: 8,
                comet_exe_path: PathBuf::from("/usr/local/bin/comet"),
            },
            pipelines: PipelineQueueConfiguration {
                default_capacity: 100,
                capacities: QUEUE_KEYS
                    .iter()
                    .map(|key| (key.to_string(), 100))
                    .collect(),
                redis_url: None,
                redis_queue_names: QUEUE_KEYS
                    .iter()
                    .map(|key| (key.to_string(), key.to_string()))
                    .collect(),
            },
        }
    }
}
