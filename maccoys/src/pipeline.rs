use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::{bail, Context, Result};
use deadqueue::limited::Queue;
use dihardts_omicstools::{
    mass_spectrometry::spectrum::{MsNSpectrum, Precursor, Spectrum as SpectrumTrait},
    proteomics::io::mzml::{
        indexed_reader::IndexedReader,
        indexer::Indexer,
        reader::{Reader as MzMlReader, Spectrum},
    },
};
use dihardts_omicstools::{
    mass_spectrometry::unit_conversions::mass_to_charge_to_dalton,
    proteomics::post_translational_modifications::PostTranslationalModification,
};
use futures::Future;
use macpepdb::{
    mass::convert::to_int as mass_to_int,
    tools::{
        progress_monitor::ProgressMonitor,
        queue_monitor::{MonitorableQueue, QueueMonitor},
    },
};
use rustis::commands::{ListCommands, ServerCommands, StringCommands};
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::{
    functions::{
        create_search_space, create_spectrum_workdir, create_work_dir, post_process,
        run_comet_search, sanatize_string,
    },
    io::comet::configuration::Configuration as CometConfiguration,
};

/// Default start tag for a spectrum in mzML
const SPECTRUM_START_TAG: &'static [u8; 10] = b"<spectrum ";

/// Default stop tag for a spectrum in mzML
const SPECTRUM_STOP_TAG: &'static [u8; 11] = b"</spectrum>";

const CLEANUP_QUEUE_KEY: &str = "cleanup";
const GOODNESS_AND_RESCORING_QUEUE_KEY: &str = "goodness_and_rescoring";
const COMET_SEARCH_QUEUE_KEY: &str = "comet_search";
const SEARCH_SPACE_GENERATION_QUEUE_KEY: &str = "search_space_generation";
const PREPARATION_QUEUE_KEY: &str = "preparation";
const INDEX_QUEUE_KEY: &str = "index";

const QUEUE_KEYS: [&str; 6] = [
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
    comet_exe_path: PathBuf,
    threads: usize,
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

/// Central storage for configuration, PTM etc.
///
pub trait PipelineStorage: Send + Sync + Sized {
    /// Create a new storage
    ///
    /// # Arguments
    /// * `config` - Configuration for the storage
    ///
    fn new(config: &PipelineConfiguration) -> impl Future<Output = Result<Self>> + Send;

    /// Get the pipeline configuration
    ///
    fn get_configuration(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<Option<PipelineConfiguration>>> + Send;

    /// Set the pipeline configuration
    ///
    fn set_configuration(
        &mut self,
        uuid: &str,
        config: &PipelineConfiguration,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Get the PTM reader
    ///
    fn get_ptms(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<Option<Vec<PostTranslationalModification>>>> + Send;

    /// Set the PTM reader
    ///
    fn set_ptms(
        &mut self,
        uuid: &str,
        ptms: &Vec<PostTranslationalModification>,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Get comet config
    ///
    fn get_comet_config(
        &self,
        uuid: &str,
    ) -> impl Future<Output = Result<Option<CometConfiguration>>> + Send;

    /// Set comet config
    ///
    fn set_comet_config(
        &mut self,
        uuid: &str,
        config: &CometConfiguration,
    ) -> impl Future<Output = Result<()>> + Send;

    fn get_configuration_key(uuid: &str) -> String {
        format!("config:{}", uuid)
    }

    fn get_ptms_key(uuid: &str) -> String {
        format!("ptms:{}", uuid)
    }

    fn get_comet_config_key(uuid: &str) -> String {
        format!("comet_config:{}", uuid)
    }
}

/// Local storage for the pipeline to
///
pub struct LocalPipelineStorage {
    /// Pipeline configuration
    configs: HashMap<String, PipelineConfiguration>,

    /// Post translational modifications
    ptms_collections: HashMap<String, Vec<PostTranslationalModification>>,

    /// Comet configurations
    comet_configs: HashMap<String, CometConfiguration>,
}

impl PipelineStorage for LocalPipelineStorage {
    async fn new(_config: &PipelineConfiguration) -> Result<Self> {
        Ok(Self {
            configs: HashMap::new(),
            ptms_collections: HashMap::new(),
            comet_configs: HashMap::new(),
        })
    }

    async fn get_configuration(&self, uuid: &str) -> Result<Option<PipelineConfiguration>> {
        Ok(self
            .configs
            .get(&Self::get_configuration_key(uuid))
            .cloned())
    }

    async fn set_configuration(
        &mut self,
        uuid: &str,
        config: &PipelineConfiguration,
    ) -> Result<()> {
        self.configs
            .insert(Self::get_configuration_key(uuid), config.clone());
        Ok(())
    }

    async fn get_ptms(&self, uuid: &str) -> Result<Option<Vec<PostTranslationalModification>>> {
        Ok(self
            .ptms_collections
            .get(&Self::get_ptms_key(uuid))
            .cloned())
    }

    async fn set_ptms(
        &mut self,
        uuid: &str,
        ptms: &Vec<PostTranslationalModification>,
    ) -> Result<()> {
        self.ptms_collections
            .insert(Self::get_ptms_key(uuid), ptms.clone());
        Ok(())
    }

    async fn get_comet_config(&self, uuid: &str) -> Result<Option<CometConfiguration>> {
        Ok(self
            .comet_configs
            .get(&Self::get_comet_config_key(uuid))
            .cloned())
    }

    async fn set_comet_config(&mut self, uuid: &str, config: &CometConfiguration) -> Result<()> {
        self.comet_configs
            .insert(Self::get_comet_config_key(uuid), config.clone());
        Ok(())
    }
}

pub struct RedisPipelineStorage {
    client: rustis::client::Client,
}

impl PipelineStorage for RedisPipelineStorage {
    async fn new(config: &PipelineConfiguration) -> Result<Self> {
        if config.pipelines.redis_url.is_none() {
            bail!("[STORAGE] Redis URL is None")
        }

        let mut redis_client_config =
            rustis::client::Config::from_str(config.pipelines.redis_url.as_ref().unwrap())?;
        redis_client_config.retry_on_error = true;
        redis_client_config.reconnection = rustis::client::ReconnectionConfig::new_constant(0, 5);

        let client = rustis::client::Client::connect(redis_client_config)
            .await
            .context("[STORAGE] Error opening connection to Redis")?;

        Ok(Self { client })
    }

    async fn get_configuration(&self, uuid: &str) -> Result<Option<PipelineConfiguration>> {
        let config_json: String = self
            .client
            .get(Self::get_configuration_key(uuid))
            .await
            .context("[STORAGE] Error getting configuration")?;

        if config_json.is_empty() {
            return Ok(None);
        }

        let config: PipelineConfiguration = serde_json::from_str(&config_json)
            .context("[STORAGE] Error deserializing configuration")?;

        Ok(Some(config))
    }

    async fn set_configuration(
        &mut self,
        uuid: &str,
        config: &PipelineConfiguration,
    ) -> Result<()> {
        let config_json =
            serde_json::to_string(config).context("[STORAGE] Error serializing config")?;

        self.client
            .set(Self::get_configuration_key(uuid), config_json)
            .await
            .context("[STORAGE] Error setting configuration")
    }

    async fn get_ptms(&self, uuid: &str) -> Result<Option<Vec<PostTranslationalModification>>> {
        let ptms_json: String = self
            .client
            .get(Self::get_ptms_key(uuid))
            .await
            .context("[STORAGE] Error getting PTMs")?;

        if ptms_json.is_empty() {
            return Ok(None);
        }

        let ptms: Vec<PostTranslationalModification> =
            serde_json::from_str(&ptms_json).context("[STORAGE] Error deserializing PTMs")?;

        Ok(Some(ptms))
    }

    async fn set_ptms(
        &mut self,
        uuid: &str,
        ptms: &Vec<PostTranslationalModification>,
    ) -> Result<()> {
        let ptms_json = serde_json::to_string(ptms).context("[STORAGE] Error serializing PTMs")?;

        self.client
            .set(Self::get_ptms_key(uuid), ptms_json)
            .await
            .context("[STORAGE] Error setting PTMs")
    }

    async fn get_comet_config(&self, uuid: &str) -> Result<Option<CometConfiguration>> {
        let comet_params_json: String = self
            .client
            .get(Self::get_comet_config_key(uuid))
            .await
            .context("[STORAGE] Error getting PTMs")?;

        if comet_params_json.is_empty() {
            return Ok(None);
        }

        let config: CometConfiguration = serde_json::from_str(&comet_params_json)
            .context("[STORAGE] Error deserializing Comet configuration")?;

        Ok(Some(config))
    }

    async fn set_comet_config(&mut self, uuid: &str, config: &CometConfiguration) -> Result<()> {
        let comet_params_json = serde_json::to_string(config)
            .context("[STORAGE] Error serializing Comet configuration")?;

        self.client
            .set(Self::get_comet_config_key(uuid), comet_params_json)
            .await
            .context("[STORAGE] Error setting Comet configuration")
    }
}

/// Manifest for a search, storing the current state of the search
/// and serving as the message between the different tasks
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchManifest {
    /// Search UUID
    pub uuid: String,

    /// Work directory where on folder per MS run is created
    pub work_dir: PathBuf,

    /// Path to the original mzML file containing the MS run
    pub ms_run_mzml_path: PathBuf,

    /// Spectrum ID of the spectrum to be searched
    pub spectrum_id: Option<String>,

    /// mzML with the spectrum to be searched
    pub spectrum_mzml: Option<Vec<u8>>,

    /// Workdir for the spectrum
    pub spectrum_work_dir: Option<PathBuf>,

    /// Path to the spectrum mzML
    pub spectrum_mzml_path: Option<PathBuf>,

    /// Precursors for the spectrum (mz, charge)
    pub precursors: Option<Vec<(f64, Vec<u8>)>>,

    /// Path to the fasta file
    pub fasta_file_path: Option<PathBuf>,

    /// Path to the Comet params file
    pub comet_params_file_path: Option<PathBuf>,

    /// Path to the PSM file
    pub psm_file_path: Option<PathBuf>,

    /// Godness file path
    pub goodness_file_path: Option<PathBuf>,
}

impl SearchManifest {
    /// Create a new search manifest
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where on folder per MS run is created
    /// * `ms_run_mzml_path` - Path to the original mzML file containing the MS run
    ///
    pub fn new(uuid: String, work_dir: PathBuf, ms_run_mzml_path: PathBuf) -> Self {
        let sanitized_mzml_stem =
            sanatize_string(ms_run_mzml_path.file_stem().unwrap().to_str().unwrap());
        let work_dir = work_dir.join(sanitized_mzml_stem);

        Self {
            uuid,
            work_dir,
            ms_run_mzml_path,
            spectrum_id: None,
            spectrum_mzml: None,
            spectrum_work_dir: None,
            spectrum_mzml_path: None,
            precursors: None,
            fasta_file_path: None,
            comet_params_file_path: None,
            psm_file_path: None,
            goodness_file_path: None,
        }
    }
}

/// Trait defining the methods for a pipeline queue
///
pub trait PipelineQueue: Send + Sync + Sized {
    /// Create a new pipeline queue
    fn new(
        config: &PipelineQueueConfiguration,
        queue_key: &str,
    ) -> impl Future<Output = Result<Self>> + Send;

    fn get_capacity(&self) -> usize;

    /// Pop a manifest from the queue
    /// Returns None if the queue is empty
    ///
    fn pop(&self) -> impl Future<Output = Option<SearchManifest>> + Send;

    /// Push a manifest to the queue
    /// Returns the manifest if the queue is full
    ///
    /// # Arguments
    /// * `manifest` - The manifest to push to the queue
    ///
    fn push(
        &self,
        manifest: SearchManifest,
    ) -> impl std::future::Future<Output = Result<(), SearchManifest>> + Send;

    /// Get the length of the queue
    ///
    fn len(&self) -> impl Future<Output = usize> + Send;
}

/// Implementation of a local pipeline queue. useful to debug, testing, reviewing or
/// very beefy servers
///
pub struct LocalPipelineQueue {
    /// Queue for search manifests to be processed
    queue: Queue<SearchManifest>,

    /// Capacity of the queue
    size: usize,
}

impl PipelineQueue for LocalPipelineQueue {
    fn new(
        config: &PipelineQueueConfiguration,
        queue_key: &str,
    ) -> impl Future<Output = Result<Self>> + Send {
        async {
            let capacity = match config.capacities.get(queue_key) {
                Some(capacity) => *capacity,
                None => config.default_capacity,
            };

            Ok(Self {
                queue: Queue::new(capacity),
                size: capacity,
            })
        }
    }

    fn get_capacity(&self) -> usize {
        self.size
    }

    fn pop(&self) -> impl Future<Output = Option<SearchManifest>> {
        async { self.queue.try_pop() }
    }

    fn push(
        &self,
        manifest: SearchManifest,
    ) -> impl std::future::Future<Output = Result<(), SearchManifest>> + Send {
        async {
            match self.queue.try_push(manifest) {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            }
        }
    }

    fn len(&self) -> impl Future<Output = usize> + Send {
        async { self.queue.len() }
    }
}

/// Redis implementation of the pipeline queue for distributed systems
///
pub struct RedisPipelineQueue {
    /// Redis client
    client: rustis::client::Client,

    /// Name of the queue
    queue_name: String,

    /// Capacity of the queue
    capacity: usize,
}

impl PipelineQueue for RedisPipelineQueue {
    fn new(
        config: &PipelineQueueConfiguration,
        queue_key: &str,
    ) -> impl Future<Output = Result<Self>> + Send {
        async move {
            if config.redis_url.is_none() {
                bail!("Redis URL is None")
            }
            let capacity = match config.capacities.get(queue_key) {
                Some(capacity) => *capacity,
                None => config.default_capacity,
            };
            let queue_name = match config.redis_queue_names.get(queue_key) {
                Some(queue_name) => queue_name.to_string(),
                None => queue_key.to_string(),
            };

            let mut redis_client_config =
                rustis::client::Config::from_str(config.redis_url.as_ref().unwrap())?;
            redis_client_config.retry_on_error = true;
            redis_client_config.reconnection =
                rustis::client::ReconnectionConfig::new_constant(0, 5);

            let client = rustis::client::Client::connect(redis_client_config)
                .await
                .context("Error opening connection to Redis")?;
            client.flushdb(rustis::commands::FlushingMode::Sync).await?;
            Ok(Self {
                client,
                queue_name,
                capacity,
            })
        }
    }

    fn get_capacity(&self) -> usize {
        self.capacity
    }

    fn pop(&self) -> impl Future<Output = Option<SearchManifest>> {
        async {
            let serialized_manifest: String = match self
                .client
                .lpop::<_, _, Vec<String>>(&self.queue_name, 1)
                .await
            {
                Ok(response) => {
                    if !response.is_empty() {
                        response[0].clone()
                    } else {
                        String::new()
                    }
                }
                Err(e) => {
                    error!("Error popping manifest from queue: {:?}", e);
                    return None;
                }
            };

            if serialized_manifest.is_empty() {
                return None;
            }

            match serde_json::from_str(&serialized_manifest) {
                Ok(manifest) => manifest,
                Err(e) => {
                    error!(
                        "[{}] Error deserializing manifest: {:?}",
                        self.queue_name, e
                    );
                    None
                }
            }
        }
    }

    fn push(
        &self,
        manifest: SearchManifest,
    ) -> impl std::future::Future<Output = Result<(), SearchManifest>> + Send {
        async {
            // Simple mechanism to prevent overcommitment of queue
            loop {
                if self.len().await < self.get_capacity() {
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }
            let serialized_manifest = match serde_json::to_string(&manifest) {
                Ok(serialized_manifest) => serialized_manifest,
                Err(e) => {
                    error!("[{}] Error serializing manifest: {:?}", self.queue_name, e);
                    return Err(manifest);
                }
            };

            match self
                .client
                .rpush(&self.queue_name, serialized_manifest)
                .await
            {
                Ok(_) => Ok(()),
                Err(e) => {
                    error!(
                        "[{}] Error pushing manifest to queue: {:?}",
                        self.queue_name, e
                    );
                    Err(manifest)
                }
            }
        }
    }

    fn len(&self) -> impl Future<Output = usize> + Send {
        async {
            match self.client.llen(&self.queue_name).await {
                Ok(size) => size,
                Err(e) => {
                    error!("[{}] Error getting queue size: {:?}", self.queue_name, e);
                    self.capacity + 111
                }
            }
        }
    }
}

/// New Arc type to implement the MonitorableQueue trait from `macpepdb`
///
struct PipelineQueueArc<T>(Arc<T>)
where
    T: PipelineQueue;

impl<T> MonitorableQueue for PipelineQueueArc<T>
where
    T: PipelineQueue + 'static,
{
    async fn len(&self) -> usize {
        self.0.len().await
    }
}

impl<Q> From<Arc<Q>> for PipelineQueueArc<Q>
where
    Q: PipelineQueue,
{
    fn from(queue: Arc<Q>) -> Self {
        Self(queue)
    }
}

/// Pipelines to run the MaCcoyS identification pipeline
///
/// # Generics
/// * `Q` - Type of the queue to use
///
pub struct Pipeline<Q, S>
where
    Q: PipelineQueue + 'static,
    S: PipelineStorage + 'static,
{
    /// UUID of the pipeline
    uuid: String,

    /// Storage
    storage: Arc<S>,

    /// Index queue
    index_queue: Arc<Q>,

    /// Preparation queue
    preparation_queue: Arc<Q>,

    /// Search space generation queue
    search_space_generation_queue: Arc<Q>,

    /// Comet search queue
    comet_search_queue: Arc<Q>,

    /// Goodness and rescoring queue
    goodness_and_rescoreing_queue: Arc<Q>,

    /// Cleanup queue
    cleanup_queue: Arc<Q>,

    /// Flag to stop the indexing task
    index_stop_flag: Arc<AtomicBool>,

    /// Flag to stop the preparation task
    preparation_stop_flag: Arc<AtomicBool>,

    /// Flag to stop the search space generation task
    search_space_generation_stop_flag: Arc<AtomicBool>,

    /// Flag to stop the Comet search task
    comet_search_stop_flag: Arc<AtomicBool>,

    /// Flag to stop the goodness and rescoring task
    goodness_and_rescoreing_stop_flag: Arc<AtomicBool>,

    /// Flag to stop the cleanup task
    cleanup_stop_flag: Arc<AtomicBool>,

    /// Number of finished search spaces
    finished_search_spaces: Arc<AtomicUsize>,

    /// Number of finished searches
    finished_searches: Arc<AtomicUsize>,

    /// Number of finished goodness and rescoring
    finished_goodness_and_rescoring: Arc<AtomicUsize>,

    /// Number of finished cleanups
    finished_cleanups: Arc<AtomicUsize>,
}

impl<Q: PipelineQueue, S: PipelineStorage> Pipeline<Q, S> {
    /// Create a new pipeline
    ///
    /// # Arguments
    /// * `config` - Configuration for the pipeline
    ///
    pub async fn new(
        config: PipelineConfiguration,
        mut comet_config: CometConfiguration,
        ptms: Vec<PostTranslationalModification>,
    ) -> Result<Self> {
        create_work_dir(&config.general.work_dir).await?;
        let uuid = Uuid::new_v4().to_string();

        comet_config.set_ptms(&ptms, config.search.max_variable_modifications)?;
        comet_config.set_num_results(10000)?;

        let index_queue = Arc::new(Q::new(&config.pipelines, INDEX_QUEUE_KEY).await?);
        let preparation_queue = Arc::new(Q::new(&config.pipelines, PREPARATION_QUEUE_KEY).await?);
        let search_space_generation_queue =
            Arc::new(Q::new(&config.pipelines, SEARCH_SPACE_GENERATION_QUEUE_KEY).await?);
        let comet_search_queue = Arc::new(Q::new(&config.pipelines, COMET_SEARCH_QUEUE_KEY).await?);
        let goodness_and_rescoreing_queue =
            Arc::new(Q::new(&config.pipelines, GOODNESS_AND_RESCORING_QUEUE_KEY).await?);
        let cleanup_queue = Arc::new(Q::new(&config.pipelines, CLEANUP_QUEUE_KEY).await?);

        let mut storage = S::new(&config).await?;
        storage.set_configuration(&uuid, &config).await?;
        storage.set_ptms(&uuid, &ptms).await?;
        storage.set_comet_config(&uuid, &comet_config).await?;

        let storage = Arc::new(storage);

        Ok(Self {
            uuid,
            storage,
            index_queue,
            preparation_queue,
            search_space_generation_queue,
            comet_search_queue,
            goodness_and_rescoreing_queue,
            cleanup_queue,
            index_stop_flag: Arc::new(AtomicBool::new(false)),
            preparation_stop_flag: Arc::new(AtomicBool::new(false)),
            search_space_generation_stop_flag: Arc::new(AtomicBool::new(false)),
            comet_search_stop_flag: Arc::new(AtomicBool::new(false)),
            goodness_and_rescoreing_stop_flag: Arc::new(AtomicBool::new(false)),
            cleanup_stop_flag: Arc::new(AtomicBool::new(false)),
            finished_search_spaces: Arc::new(AtomicUsize::new(0)),
            finished_searches: Arc::new(AtomicUsize::new(0)),
            finished_goodness_and_rescoring: Arc::new(AtomicUsize::new(0)),
            finished_cleanups: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn get_uuid(&self) -> &str {
        &self.uuid
    }

    /// Run the pipeline for each mzML file
    ///
    /// # Arguments
    /// * `mzml_file_paths` - Paths to the mzML files to search
    pub async fn run(&self, mzml_file_paths: Vec<PathBuf>) -> Result<()> {
        let config: PipelineConfiguration = match self
            .storage
            .get_configuration(&self.uuid)
            .await
            .context(
            "Error getting configuration when starting pipleine",
        )? {
            Some(config) => config,
            None => bail!("[{}] Configuration not found", self.uuid),
        };

        let mut queue_monitor = QueueMonitor::new::<PipelineQueueArc<Q>>(
            "",
            vec![
                self.cleanup_queue.clone().into(),
                self.goodness_and_rescoreing_queue.clone().into(),
                self.comet_search_queue.clone().into(),
                self.search_space_generation_queue.clone().into(),
                self.preparation_queue.clone().into(),
                self.index_queue.clone().into(),
            ],
            vec![100, 100, 100, 100, 100, 100],
            vec![
                "Cleanup".to_owned(),
                "Goodness and Rescoring".to_owned(),
                "Comet Search".to_owned(),
                "Search Space Generation".to_owned(),
                "Preparation".to_owned(),
                "Index".to_owned(),
            ],
            None,
        )?;

        let mut metrics_monitor = ProgressMonitor::new(
            "",
            vec![
                self.finished_cleanups.clone(),
                self.finished_goodness_and_rescoring.clone(),
                self.finished_searches.clone(),
                self.finished_search_spaces.clone(),
            ],
            vec![None, None, None, None],
            vec![
                "Cleanups".to_owned(),
                "Post processing".to_owned(),
                "Searches".to_owned(),
                "Build search spaces".to_owned(),
            ],
            None,
        )?;

        let index_handler: tokio::task::JoinHandle<()> = {
            let index_queue = self.index_queue.clone();
            let preparation_queue = self.preparation_queue.clone();
            let stop_flag = self.index_stop_flag.clone();
            tokio::spawn(Self::indexing_task(
                index_queue.clone(),
                preparation_queue,
                stop_flag,
            ))
        };

        let preparation_handlers: Vec<tokio::task::JoinHandle<()>> =
            (0..config.general.num_preparation_tasks)
                .into_iter()
                .map(|_| {
                    let preparation_queue = self.preparation_queue.clone();
                    let search_space_generation_queue = self.search_space_generation_queue.clone();
                    let stop_flag = self.preparation_stop_flag.clone();
                    tokio::spawn(Self::preparation_task(
                        preparation_queue,
                        search_space_generation_queue,
                        stop_flag,
                    ))
                })
                .collect();

        let search_space_generation_handlers: Vec<tokio::task::JoinHandle<()>> =
            (0..config.general.num_search_space_generation_tasks)
                .into_iter()
                .map(|_| {
                    let search_space_generation_queue = self.search_space_generation_queue.clone();
                    let comet_search_queue = self.comet_search_queue.clone();
                    let stop_flag = self.search_space_generation_stop_flag.clone();
                    let finished_search_spaces = self.finished_search_spaces.clone();
                    tokio::spawn(Self::search_space_generation_task(
                        search_space_generation_queue,
                        comet_search_queue,
                        self.storage.clone(),
                        stop_flag,
                        finished_search_spaces,
                    ))
                })
                .collect();

        let comet_search_handlers: Vec<tokio::task::JoinHandle<()>> =
            (0..config.general.num_comet_search_tasks)
                .into_iter()
                .map(|_| {
                    let comet_search_queue = self.comet_search_queue.clone();
                    let goodness_and_rescoreing_queue = self.goodness_and_rescoreing_queue.clone();
                    let comet_exe = config.comet.comet_exe_path.clone();
                    let stop_flag = self.comet_search_stop_flag.clone();
                    let finished_searches = self.finished_searches.clone();
                    tokio::spawn(Self::comet_search_task(
                        comet_search_queue,
                        goodness_and_rescoreing_queue,
                        comet_exe,
                        stop_flag,
                        finished_searches,
                    ))
                })
                .collect();

        let goodness_and_resconfing_handlers: Vec<tokio::task::JoinHandle<()>> = (0..config
            .general
            .num_goodness_and_rescoring_tasks)
            .into_iter()
            .map(|_| {
                let goodness_and_rescoreing_queue = self.goodness_and_rescoreing_queue.clone();
                let cleanup_queue = self.cleanup_queue.clone();
                let stop_flag = self.goodness_and_rescoreing_stop_flag.clone();
                let finished_goodness_and_rescoring = self.finished_goodness_and_rescoring.clone();
                tokio::spawn(Self::goodness_and_rescoring_task(
                    goodness_and_rescoreing_queue,
                    cleanup_queue,
                    stop_flag,
                    finished_goodness_and_rescoring,
                ))
            })
            .collect();

        let cleanup_handlers: Vec<tokio::task::JoinHandle<()>> =
            (0..config.general.num_cleanup_tasks)
                .into_iter()
                .map(|_| {
                    let cleanup_queue = self.cleanup_queue.clone();
                    let finished_cleanups = self.finished_cleanups.clone();
                    let stop_flag = self.cleanup_stop_flag.clone();
                    tokio::spawn(Self::cleanup_task(
                        cleanup_queue,
                        finished_cleanups,
                        self.storage.clone(),
                        stop_flag,
                    ))
                })
                .collect();

        for mzml_file_path in mzml_file_paths {
            let manifest = SearchManifest::new(
                self.uuid.clone(),
                config.general.work_dir.clone(),
                mzml_file_path,
            );
            match self.index_queue.push(manifest).await {
                Ok(_) => (),
                Err(e) => {
                    error!(
                        "Error pushing manifest to index queue: {}",
                        e.ms_run_mzml_path.display()
                    );
                    continue;
                }
            }
        }

        self.index_stop_flag.store(true, Ordering::Relaxed);

        match index_handler.await {
            Ok(_) => (),
            Err(e) => {
                error!("Error joining index thread: {:?}", e);
            }
        }

        self.preparation_stop_flag.store(true, Ordering::Relaxed);

        for preparation_handler in preparation_handlers {
            match preparation_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining preparation thread: {:?}", e);
                }
            }
        }

        self.search_space_generation_stop_flag
            .store(true, Ordering::Relaxed);

        for search_space_generation_handler in search_space_generation_handlers {
            match search_space_generation_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining search space generation thread: {:?}", e);
                }
            }
        }

        self.comet_search_stop_flag.store(true, Ordering::Relaxed);

        for comet_search_handler in comet_search_handlers {
            match comet_search_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining comet search thread: {:?}", e);
                }
            }
        }

        self.goodness_and_rescoreing_stop_flag
            .store(true, Ordering::Relaxed);

        for goodness_and_resconfing_handler in goodness_and_resconfing_handlers {
            match goodness_and_resconfing_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining goodness and rescoring thread: {:?}", e);
                }
            }
        }

        self.cleanup_stop_flag.store(true, Ordering::Relaxed);

        for cleanup_handler in cleanup_handlers {
            match cleanup_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining cleanup thread: {:?}", e);
                }
            }
        }

        queue_monitor.stop().await?;
        metrics_monitor.stop().await?;

        Ok(())
    }

    /// Task to index and split up the mzML file
    ///
    /// # Arguments
    /// * `index_queue` - Queue for the indexing task
    /// * `preparation_queue` - Queue for the preparation task
    /// * `stop_flag` - Flag to indicate to stop once the index queue is empty
    ///
    async fn indexing_task(
        index_queue: Arc<Q>,
        preparation_queue: Arc<Q>,
        stop_flag: Arc<AtomicBool>,
    ) {
        loop {
            while let Some(manifest) = index_queue.pop().await {
                match fs::create_dir_all(manifest.work_dir.clone()) {
                    Ok(_) => (),
                    Err(e) => {
                        error!("Error creating work directory: {}", e);
                        continue;
                    }
                }

                let index = match Indexer::create_index(&manifest.ms_run_mzml_path, None) {
                    Ok(index) => index,
                    Err(e) => {
                        error!("Error creating index: {:?}", e);
                        continue;
                    }
                };

                let index_file_path = manifest.work_dir.join("index.json");
                debug!("Writing index to: {}", index_file_path.display());
                let index_json = match index.to_json() {
                    Ok(json) => json,
                    Err(e) => {
                        error!("Error serializing index: {:?}", e);
                        continue;
                    }
                };
                match std::fs::write(&index_file_path, index_json) {
                    Ok(_) => (),
                    Err(e) => {
                        error!("Error writing index: {:?}", e);
                        continue;
                    }
                };

                let uuid_path = manifest.work_dir.join("uuid.txt");
                match std::fs::write(&uuid_path, &manifest.uuid) {
                    Ok(_) => (),
                    Err(e) => {
                        error!("Error writing uuid: {:?}", e);
                        continue;
                    }
                };

                let mut reader = match IndexedReader::new(&manifest.ms_run_mzml_path, &index) {
                    Ok(reader) => reader,
                    Err(e) => {
                        error!("Error creating reader: {:?}", e);
                        continue;
                    }
                };

                for (spec_id, _) in index.get_spectra() {
                    let mzml = match reader.extract_spectrum(&spec_id) {
                        Ok(content) => content,
                        Err(e) => {
                            error!("Error extracting spectrum: {:?}", e);
                            continue;
                        }
                    };

                    let mut new_manifest = manifest.clone();
                    new_manifest.spectrum_id = Some(spec_id.clone());
                    new_manifest.spectrum_mzml = Some(mzml);

                    loop {
                        new_manifest = match preparation_queue.push(new_manifest).await {
                            Ok(_) => break,
                            Err(e) => {
                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                                e
                            }
                        }
                    }
                }
            }
            if stop_flag.load(Ordering::Relaxed) {
                break;
            }
            // wait before checking the queue again
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    /// Task to prepare the spectra work directories for the search space generation and search.
    ///
    /// # Arguments
    /// * `preparation_queue` - Queue for the preparation task
    /// * `search_space_generation_queue` - Queue for the search space generation task
    /// * `stop_flag` - Flag to indicate to stop once the preparation queue is empty
    ///
    fn preparation_task(
        preparation_queue: Arc<Q>,
        search_space_generation_queue: Arc<Q>,
        stop_flag: Arc<AtomicBool>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            loop {
                while let Some(mut manifest) = preparation_queue.pop().await {
                    debug!("Preparing {}", manifest.spectrum_id.as_ref().unwrap());
                    if manifest.spectrum_id.is_none() {
                        error!("Spectrum ID is None in spectra_dir_creation_thread");
                        continue;
                    }
                    if manifest.spectrum_mzml.is_none() {
                        error!("Spectrum mzML is None in spectra_dir_creation_thread");
                        continue;
                    }

                    let start = match manifest
                        .spectrum_mzml
                        .as_ref()
                        .unwrap()
                        .windows(SPECTRUM_START_TAG.len())
                        .position(|window| window == SPECTRUM_START_TAG)
                    {
                        Some(start) => start,
                        None => {
                            error!(
                                "No spectrum start in {}",
                                manifest.spectrum_id.as_ref().unwrap()
                            );
                            continue;
                        }
                    };
                    let stop = match manifest
                        .spectrum_mzml
                        .as_ref()
                        .unwrap()
                        .windows(SPECTRUM_STOP_TAG.len())
                        .position(|window| window == SPECTRUM_STOP_TAG)
                    {
                        Some(start) => start,
                        None => {
                            error!(
                                "No spectrum stop in {}",
                                manifest.spectrum_id.as_ref().unwrap()
                            );
                            continue;
                        }
                    };

                    let spectrum_xml = &manifest.spectrum_mzml.as_ref().unwrap()[start..stop];

                    // As this mzML is already reduced to the spectrum of interest, we can parse it directly
                    // using MzMlReader::parse_spectrum_xml
                    let spectrum = match MzMlReader::parse_spectrum_xml(spectrum_xml) {
                        Ok(spectrum) => spectrum,
                        Err(e) => {
                            error!("Error parsing spectrum: {:?}", e);
                            continue;
                        }
                    };

                    let spectrum = match spectrum {
                        Spectrum::MsNSpectrum(spectrum) => spectrum,
                        _ => {
                            // Ignore MS1
                            info!("Ignoring MS1 spectrum");
                            continue;
                        }
                    };

                    // Ignore MS3 and higher
                    if spectrum.get_ms_level() != 2 {
                        info!("Ignoring MS{} spectrum", spectrum.get_ms_level());
                        continue;
                    }

                    // Get mass to charge ratio and charges
                    let precursors: Vec<(f64, Vec<u8>)> = spectrum
                        .get_precursors()
                        .iter()
                        .map(|precursor| {
                            precursor
                                .get_ions()
                                .iter()
                                .map(|(mz, charges)| (*mz, charges.clone()))
                                .collect::<Vec<(f64, Vec<u8>)>>()
                        })
                        .flatten()
                        .collect();

                    drop(spectrum);

                    let spectrum_work_dir = match create_spectrum_workdir(
                        &manifest.work_dir,
                        manifest.spectrum_id.as_ref().unwrap(),
                    )
                    .await
                    {
                        Ok(path) => path,
                        Err(e) => {
                            error!("Error creating spectrum work directory: {}", e);
                            continue;
                        }
                    };

                    let spectrum_mzml_path = spectrum_work_dir.join("spectrum.mzML");

                    match tokio::fs::write(
                        &spectrum_mzml_path,
                        manifest.spectrum_mzml.as_ref().unwrap(),
                    )
                    .await
                    {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "Error writing spectrum.mzML for {}: {}",
                                manifest.spectrum_id.as_ref().unwrap(),
                                e
                            );
                            continue;
                        }
                    }

                    manifest.spectrum_work_dir = Some(spectrum_work_dir);
                    manifest.spectrum_mzml = None; // Free up some memory
                    manifest.precursors = Some(precursors);
                    manifest.spectrum_mzml_path = Some(spectrum_mzml_path);

                    loop {
                        manifest = match search_space_generation_queue.push(manifest).await {
                            Ok(_) => {
                                break;
                            }
                            Err(e) => {
                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                                e
                            }
                        }
                    }
                }
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                // wait before checking the queue again
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
    }

    /// Task to generate the search space for the Comet search
    ///
    /// # Arguments
    /// * `search_space_generation_queue` - Queue for the search space generation task
    /// * `comet_search_queue` - Queue for the Comet search task
    /// * `storage` - Storage to access configuration and PTMs
    /// * `stop_flag` - Flag to indicate to stop once the search space generation queue is empty
    ///
    fn search_space_generation_task(
        search_space_generation_queue: Arc<Q>,
        comet_search_queue: Arc<Q>,
        storage: Arc<S>,
        stop_flag: Arc<AtomicBool>,
        finishes_search_spaces: Arc<AtomicUsize>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            let mut last_search_uuid = String::new();
            let mut current_config: Option<PipelineConfiguration> = None;
            let mut current_ptms: Option<Vec<PostTranslationalModification>> = None;
            let mut current_comet_config: Option<CometConfiguration> = None;

            loop {
                while let Some(manifest) = search_space_generation_queue.pop().await {
                    debug!(
                        "Generating search space for {}",
                        manifest.spectrum_id.as_ref().unwrap()
                    );
                    if manifest.spectrum_work_dir.is_none() {
                        error!("Spectrum work directory is None in search_space_generation_thread");
                        continue;
                    }
                    if manifest.precursors.is_none() {
                        error!("Precursors is None in search_space_generation_thread");
                        continue;
                    }

                    if last_search_uuid != manifest.uuid {
                        current_config = match storage.get_configuration(&manifest.uuid).await {
                            Ok(config) => match config {
                                Some(config) => Some(config),
                                None => {
                                    error!("Configuration not found for {}", manifest.uuid);
                                    continue;
                                }
                            },
                            Err(e) => {
                                error!("Error reading configuration: {:?}", e);
                                return;
                            }
                        };
                        current_comet_config = match storage.get_comet_config(&manifest.uuid).await
                        {
                            Ok(Some(config)) => Some(config),
                            Ok(None) => {
                                error!("Comet configuration not found for {}", manifest.uuid);
                                continue;
                            }
                            Err(e) => {
                                error!("Error reading Comet configuration: {:?}", e);
                                continue;
                            }
                        };
                        current_ptms = match storage.get_ptms(&manifest.uuid).await {
                            Ok(ptms) => match ptms {
                                Some(ptms) => Some(ptms),
                                None => {
                                    error!("PTMs not found for {}", manifest.uuid);
                                    continue;
                                }
                            },
                            Err(e) => {
                                error!("Error reading PTMs: {:?}", e);
                                return;
                            }
                        };
                        last_search_uuid = manifest.uuid.clone();

                        let config = current_config.as_ref().unwrap();
                        let comet_config = current_comet_config.as_mut().unwrap();

                        match comet_config
                            .set_option("threads", &format!("{}", config.comet.threads))
                        {
                            Ok(_) => (),
                            Err(e) => {
                                error!("Error setting threads: {:?}", e);
                                return;
                            }
                        }
                    }

                    // Unwrap the current configuration
                    let config = current_config.as_ref().unwrap();
                    let ptms = current_ptms.as_ref().unwrap();
                    let comet_config = current_comet_config.as_mut().unwrap();

                    for (precursor_mz, precursor_charges) in
                        manifest.precursors.as_ref().unwrap().iter()
                    {
                        let precursor_charges = if precursor_charges.is_empty() {
                            (2..=config.search.max_charge).collect()
                        } else {
                            precursor_charges.clone()
                        };
                        for precursor_charge in precursor_charges {
                            let fasta_file_path = manifest
                                .spectrum_work_dir
                                .as_ref()
                                .unwrap()
                                .join(format!("{}.fasta", precursor_charge));
                            let comet_params_file_path = manifest
                                .spectrum_work_dir
                                .as_ref()
                                .unwrap()
                                .join(format!("{}.comet.params", precursor_charge));
                            let mass = mass_to_int(mass_to_charge_to_dalton(
                                *precursor_mz,
                                precursor_charge,
                            ));
                            match comet_config.set_charge(precursor_charge) {
                                Ok(_) => (),
                                Err(e) => {
                                    error!("Error setting charge: {:?}", e);
                                    continue;
                                }
                            }

                            match comet_config.async_to_file(&comet_params_file_path).await {
                                Ok(_) => (),
                                Err(e) => {
                                    error!("Error writing Comet params file: {:?}", e);
                                    continue;
                                }
                            }

                            match create_search_space(
                                &fasta_file_path,
                                ptms.as_ref(),
                                mass,
                                config.search.lower_mass_tolerance_ppm,
                                config.search.upper_mass_tolerance_ppm,
                                config.search.max_variable_modifications,
                                config.search.decoys_per_peptide,
                                config.search.target_url.to_owned(),
                                config.search.decoy_url.clone(),
                                config.search.target_lookup_url.clone(),
                                config.search.decoy_cache_url.clone(),
                            )
                            .await
                            {
                                Ok(_) => (),
                                Err(e) => {
                                    error!("Error creating search space: {:?}", e);
                                    continue;
                                }
                            };

                            let mut new_manifest = manifest.clone();
                            new_manifest.fasta_file_path = Some(fasta_file_path);
                            new_manifest.comet_params_file_path =
                                Some(comet_params_file_path.clone());
                            loop {
                                new_manifest = match comet_search_queue.push(new_manifest).await {
                                    Ok(_) => break,
                                    Err(e) => {
                                        tokio::time::sleep(tokio::time::Duration::from_millis(100))
                                            .await;
                                        e
                                    }
                                }
                            }
                            finishes_search_spaces.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                // wait before checking the queue again
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
    }

    /// Task to run the Comet search
    ///
    /// # Arguments
    /// * `comet_search_queue` - Queue for the Comet search task
    /// * `goodness_and_rescoreing_queue` - Goodness and rescoreing queue
    /// * `comet_exe` - Path to the Comet executable
    /// * `stop_flag` - Flag to indicate to stop once the Comet search queue is empty
    ///
    fn comet_search_task(
        comet_search_queue: Arc<Q>,
        goodness_and_rescoreing_queue: Arc<Q>,
        comet_exe: PathBuf,
        stop_flag: Arc<AtomicBool>,
        finished_searches: Arc<AtomicUsize>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            loop {
                while let Some(mut manifest) = comet_search_queue.pop().await {
                    debug!(
                        "Running Comet search for {}",
                        manifest.spectrum_id.as_ref().unwrap()
                    );

                    if manifest.spectrum_mzml_path.is_none() {
                        error!("Spectrum mzML is None in comet_search_thread");
                        continue;
                    }
                    if manifest.fasta_file_path.is_none() {
                        error!("Fasta file path is None in comet_search_thread");
                        continue;
                    }
                    if manifest.comet_params_file_path.is_none() {
                        error!("Comet params file path is None in comet_search_thread");
                        continue;
                    }

                    manifest.psm_file_path = Some(
                        manifest
                            .fasta_file_path
                            .as_ref()
                            .unwrap()
                            .with_extension("txt"),
                    );

                    match run_comet_search(
                        &comet_exe,
                        &manifest.comet_params_file_path.as_ref().unwrap(),
                        &manifest.fasta_file_path.as_ref().unwrap(),
                        &manifest.psm_file_path.as_ref().unwrap().with_extension(""),
                        manifest.spectrum_mzml_path.as_ref().unwrap(),
                    )
                    .await
                    {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Error running Comet search: {:?}", e);
                            continue;
                        }
                    }

                    debug!(
                        "Comet search done for {}",
                        manifest.psm_file_path.as_ref().unwrap().display()
                    );

                    match goodness_and_rescoreing_queue.push(manifest).await {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "Error pushing manifest to FDR queue: {}",
                                e.spectrum_id.as_ref().unwrap()
                            );
                            continue;
                        }
                    }
                    finished_searches.fetch_add(1, Ordering::Relaxed);
                }
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                // wait before checking the queue again
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
    }

    // fn fdr_task()

    fn goodness_and_rescoring_task(
        goodness_and_rescoreing_queue: Arc<Q>,
        cleanup_queue: Arc<Q>,
        stop_flag: Arc<AtomicBool>,
        finished_goodness_and_rescoring: Arc<AtomicUsize>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            loop {
                while let Some(mut manifest) = goodness_and_rescoreing_queue.pop().await {
                    debug!(
                        "Goodness and rescoring {}",
                        manifest.spectrum_id.as_ref().unwrap()
                    );

                    if manifest.psm_file_path.is_none() {
                        error!("PSM file path is None in goodness_and_rescoring_thread");
                        continue;
                    }

                    let goodness_file_path = manifest
                        .psm_file_path
                        .as_ref()
                        .unwrap()
                        .with_extension("goodness");

                    match post_process(
                        manifest.psm_file_path.as_ref().unwrap(),
                        &goodness_file_path,
                    )
                    .await
                    {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Error post processing: {:?}", e);
                            continue;
                        }
                    }

                    manifest.goodness_file_path = Some(goodness_file_path);

                    match cleanup_queue.push(manifest).await {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "Error pushing manifest to FDR queue: {}",
                                e.spectrum_id.as_ref().unwrap()
                            );
                            continue;
                        }
                    }
                    finished_goodness_and_rescoring.fetch_add(1, Ordering::Relaxed);
                }
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                // wait before checking the queue again
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
    }

    /// Task to cleanup the search
    ///
    /// # Arguments
    /// * `cleanup_queue` - Queue for the cleanup task
    /// * `finsihed_searches` - Number of finished searches
    /// * `storage` - Storage to access configuration
    /// * `stop_flag` - Flag to indicate to stop once the cleanup queue is empty
    ///
    fn cleanup_task(
        cleanup_queue: Arc<Q>,
        finished_cleanups: Arc<AtomicUsize>,
        storage: Arc<S>,
        stop_flag: Arc<AtomicBool>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            loop {
                let mut last_search_uuid = String::new();
                let mut current_config: Option<PipelineConfiguration> = None;

                while let Some(manifest) = cleanup_queue.pop().await {
                    debug!(
                        "Running cleanup for {}",
                        manifest.spectrum_id.as_ref().unwrap()
                    );

                    if !manifest.fasta_file_path.is_some() {
                        error!("Fasta file path is None in cleanup_task");
                        continue;
                    }

                    if last_search_uuid != manifest.uuid {
                        current_config = match storage.get_configuration(&manifest.uuid).await {
                            Ok(config) => match config {
                                Some(config) => Some(config),
                                None => {
                                    error!("Configuration not found for {}", manifest.uuid);
                                    continue;
                                }
                            },
                            Err(e) => {
                                error!("Error reading configuration: {:?}", e);
                                return;
                            }
                        };
                        last_search_uuid = manifest.uuid.clone();
                    }

                    // Unwrap the current configuration
                    let config = current_config.as_ref().unwrap();

                    if !config.general.keep_fasta_files {
                        match tokio::fs::remove_file(manifest.fasta_file_path.as_ref().unwrap())
                            .await
                        {
                            Ok(_) => (),
                            Err(e) => {
                                error!("Error removing fasta file: {}", e);
                            }
                        }
                    }
                    tokio::fs::remove_file(manifest.comet_params_file_path.as_ref().unwrap())
                        .await
                        .unwrap();

                    finished_cleanups.fetch_add(1, Ordering::Relaxed);

                    debug!(
                        "Cleanup done for {}",
                        manifest.spectrum_work_dir.as_ref().unwrap().display()
                    );
                }
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                // wait before checking the queue again
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
    }
}
