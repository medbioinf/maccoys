// std imports
use std::{
    fs,
    marker::PhantomData,
    path::PathBuf,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

// 3rd party imports
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
use polars::prelude::*;
use pyo3::{prelude::*, types::PyList};
use rustis::commands::ListCommands;
use tracing::{debug, error, info, trace};
use uuid::Uuid;

// local imports
use crate::{
    constants::{COMET_EXP_BASE_SCORE, DIST_SCORE_NAME, EXP_SCORE_NAME},
    functions::{create_search_space, create_work_dir, run_comet_search, sanatize_string},
    goodness_of_fit_record::GoodnessOfFitRecord,
    io::comet::{
        configuration::Configuration as CometConfiguration,
        peptide_spectrum_match_tsv::PeptideSpectrumMatchTsv,
    },
};

use super::{
    configuration::{
        CometSearchTaskConfiguration, PipelineConfiguration, SearchParameters,
        SearchSpaceGenerationTaskConfiguration, TaskConfiguration,
    },
    storage::PipelineStorage,
};

/// Default start tag for a spectrum in mzML
const SPECTRUM_START_TAG: &'static [u8; 10] = b"<spectrum ";

/// Default stop tag for a spectrum in mzML
const SPECTRUM_STOP_TAG: &'static [u8; 11] = b"</spectrum>";

/// Manifest for a search, storing the current state of the search
/// and serving as the message between the different tasks
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchManifest {
    /// Search UUID
    pub uuid: String,

    /// MS run path
    pub ms_run_name: String,

    /// Path to the original mzML file containing the MS run
    pub ms_run_mzml_path: PathBuf,

    /// Spectrum ID of the spectrum to be searched
    pub spectrum_id: String,

    /// mzML with the spectrum to be searched
    pub spectrum_mzml: Vec<u8>,

    /// Precursors for the spectrum (mz, charge)
    pub precursors: Vec<(f64, Vec<u8>)>,

    /// Flag indicating if the indexing has been done
    pub is_indexing_done: bool,

    /// Flag indicating if the preparation has been done
    pub is_preparation_done: bool,

    /// Flag indicating if the search space has been generated
    pub is_search_space_generated: bool,

    /// Flag indicating if the Comet search has been performed
    pub is_comet_search_done: bool,

    /// Flag indicating if the goodness and rescoring has been performed
    pub is_goodness_and_rescoring_done: bool,
}

impl SearchManifest {
    /// Create a new search manifest
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where on folder per MS run is created
    /// * `ms_run_mzml_path` - Path to the original mzML file containing the MS run
    ///
    pub fn new(uuid: String, ms_run_mzml_path: PathBuf) -> Self {
        let ms_run_name = sanatize_string(ms_run_mzml_path.file_stem().unwrap().to_str().unwrap());

        Self {
            uuid,
            ms_run_name,
            ms_run_mzml_path,
            spectrum_id: String::new(),
            spectrum_mzml: Vec::new(),
            precursors: Vec::new(),
            is_indexing_done: true,
            is_preparation_done: true,
            is_search_space_generated: true,
            is_comet_search_done: true,
            is_goodness_and_rescoring_done: true,
        }
    }

    /// Returns the path to the MS run directory
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    ///
    pub fn get_ms_run_dir_path(&self, work_dir: &PathBuf) -> PathBuf {
        work_dir.join(&self.ms_run_name)
    }

    /// Retuns the path of the spectrum directory wihtin the MS run directory
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    ///
    pub fn get_spectrum_dir_path(&self, work_dir: &PathBuf) -> PathBuf {
        self.get_ms_run_dir_path(work_dir)
            .join(sanatize_string(&self.spectrum_id))
    }

    /// Retuns the path of the spectrum mzML
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    ///
    pub fn get_spectrum_mzml_path(&self, work_dir: &PathBuf) -> PathBuf {
        self.get_spectrum_dir_path(work_dir).join("spectrum.mzML")
    }

    /// Returns the path to the fasta file for the spectrum's precursor
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    /// * `precursor_mz` - Precursor mass to charge ratio
    /// * `precursor_charge` - Precursor charge
    ///
    pub fn get_fasta_file_path(
        &self,
        work_dir: &PathBuf,
        precursor_mz: f64,
        precursor_charge: u8,
    ) -> PathBuf {
        self.get_spectrum_dir_path(work_dir)
            .join(format!("{}_{}.fasta", precursor_mz, precursor_charge))
    }

    /// Returns the path to the Comet parameter file for the spectrum's precursor
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    /// * `precursor_mz` - Precursor mass to charge ratio
    /// * `precursor_charge` - Precursor charge
    ///
    pub fn get_comet_params_path(
        &self,
        work_dir: &PathBuf,
        precursor_mz: f64,
        precursor_charge: u8,
    ) -> PathBuf {
        self.get_spectrum_dir_path(work_dir)
            .join(format!("{}_{}.comet.param", precursor_mz, precursor_charge))
    }

    /// Returns the path to the PSM file for the spectrum's precursor
    /// This alredy has the TSV file extension while Comet writes it with the extension .txt
    /// It is renamed after the search
    ///
    /// # Argumentss
    /// * `work_dir` - Work directory where the results are stored
    /// * `precursor_mz` - Precursor mass to charge ratio
    /// * `precursor_charge` - Precursor charge
    ///
    pub fn get_psms_file_path(
        &self,
        work_dir: &PathBuf,
        precursor_mz: f64,
        precursor_charge: u8,
    ) -> PathBuf {
        self.get_spectrum_dir_path(work_dir)
            .join(format!("{}_{}.tsv", precursor_mz, precursor_charge))
    }

    /// Returns the path to the goodness TSV file for the spectrum's precursor
    ///
    /// # Argumentss
    /// * `work_dir` - Work directory where the results are stored
    /// * `precursor_mz` - Precursor mass to charge ratio
    /// * `precursor_charge` - Precursor charge
    ///
    pub fn get_goodness_file_path(
        &self,
        work_dir: &PathBuf,
        precursor_mz: f64,
        precursor_charge: u8,
    ) -> PathBuf {
        self.get_spectrum_dir_path(work_dir).join(format!(
            "{}_{}.goodness.tsv",
            precursor_mz, precursor_charge
        ))
    }
}

/// Trait defining the methods for a pipeline queue
///
pub trait PipelineQueue: Send + Sync + Sized {
    /// Create a new pipeline queue
    fn new(config: &TaskConfiguration) -> impl Future<Output = Result<Self>> + Send;

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
    capacity: usize,
}

impl PipelineQueue for LocalPipelineQueue {
    fn new(config: &TaskConfiguration) -> impl Future<Output = Result<Self>> + Send {
        async {
            Ok(Self {
                queue: Queue::new(config.queue_capacity),
                capacity: config.queue_capacity,
            })
        }
    }

    fn get_capacity(&self) -> usize {
        self.capacity
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
    fn new(config: &TaskConfiguration) -> impl Future<Output = Result<Self>> + Send {
        async move {
            if config.redis_url.is_none() {
                bail!("Redis URL is None")
            }

            trace!("Storages: {:?} / {}", &config.redis_url, &config.queue_name);

            let mut redis_client_config =
                rustis::client::Config::from_str(config.redis_url.as_ref().unwrap())?;
            redis_client_config.retry_on_error = true;
            redis_client_config.reconnection =
                rustis::client::ReconnectionConfig::new_constant(0, 5);

            let client = rustis::client::Client::connect(redis_client_config)
                .await
                .context("Error opening connection to Redis")?;
            Ok(Self {
                client,
                queue_name: config.queue_name.clone(),
                capacity: config.queue_capacity,
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
    _phantom_queue: PhantomData<Q>,
    _storage: PhantomData<S>,
}

impl<Q: PipelineQueue, S: PipelineStorage> Pipeline<Q, S> {
    /// Run the pipeline locally for each mzML file
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    /// * `config` - Configuration for the pipeline
    /// * `comet_config` - Configuration for Comet
    /// * `ptms` - Post translational modifications
    /// * `mzml_file_paths` - Paths to the mzML files to search
    ///
    pub async fn run_locally(
        work_dir: PathBuf,
        config: PipelineConfiguration,
        mut comet_config: CometConfiguration,
        ptms: Vec<PostTranslationalModification>,
        mzml_file_paths: Vec<PathBuf>,
    ) -> Result<()> {
        let uuid = Uuid::new_v4().to_string();
        info!("UUID: {}", uuid);

        create_work_dir(&work_dir).await?;

        comet_config.set_ptms(&ptms, config.search_parameters.max_variable_modifications)?;
        comet_config.set_num_results(10000)?;

        let mut storage = S::new(&config.storage).await?;
        storage
            .set_search_parameters(&uuid, config.search_parameters.clone())
            .await?;
        storage.set_ptms(&uuid, &ptms).await?;
        storage.set_comet_config(&uuid, &comet_config).await?;

        let storage = Arc::new(storage);

        // Create the queues
        let index_queue = Arc::new(Q::new(&config.index).await?);
        let preparation_queue = Arc::new(Q::new(&config.preparation).await?);
        let search_space_generation_queue =
            Arc::new(Q::new(&config.search_space_generation).await?);
        let comet_search_queue = Arc::new(Q::new(&config.comet_search).await?);
        let goodness_and_rescoreing_queue = Arc::new(Q::new(&config.goodness_and_rescoring).await?);
        let cleanup_queue = Arc::new(Q::new(&config.cleanup).await?);

        // Create the stop flags
        let index_stop_flag = Arc::new(AtomicBool::new(false));
        let preparation_stop_flag = Arc::new(AtomicBool::new(false));
        let search_space_generation_stop_flag = Arc::new(AtomicBool::new(false));
        let comet_search_stop_flag = Arc::new(AtomicBool::new(false));
        let goodness_and_rescoreing_stop_flag = Arc::new(AtomicBool::new(false));
        let cleanup_stop_flag = Arc::new(AtomicBool::new(false));

        // Metrics
        let finished_search_spaces = Arc::new(AtomicUsize::new(0));
        let finished_searches = Arc::new(AtomicUsize::new(0));
        let finished_goodness_and_rescoring = Arc::new(AtomicUsize::new(0));
        let finished_cleanups = Arc::new(AtomicUsize::new(0));

        let mut queue_monitor = QueueMonitor::new::<PipelineQueueArc<Q>>(
            "",
            vec![
                cleanup_queue.clone().into(),
                goodness_and_rescoreing_queue.clone().into(),
                comet_search_queue.clone().into(),
                search_space_generation_queue.clone().into(),
                preparation_queue.clone().into(),
                index_queue.clone().into(),
            ],
            vec![
                config.cleanup.queue_capacity as u64,
                config.goodness_and_rescoring.queue_capacity as u64,
                config.comet_search.queue_capacity as u64,
                config.search_space_generation.queue_capacity as u64,
                config.preparation.queue_capacity as u64,
                config.index.queue_capacity as u64,
            ],
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
                finished_cleanups.clone(),
                finished_goodness_and_rescoring.clone(),
                finished_searches.clone(),
                finished_search_spaces.clone(),
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
            tokio::spawn(Self::indexing_task(
                work_dir.clone(),
                index_queue.clone(),
                preparation_queue.clone(),
                index_stop_flag.clone(),
            ))
        };

        let preparation_handlers: Vec<tokio::task::JoinHandle<()>> =
            (0..config.preparation.num_tasks)
                .into_iter()
                .map(|_| {
                    tokio::spawn(Self::preparation_task(
                        work_dir.clone(),
                        preparation_queue.clone(),
                        search_space_generation_queue.clone(),
                        preparation_stop_flag.clone(),
                    ))
                })
                .collect();

        let search_space_generation_handlers: Vec<tokio::task::JoinHandle<()>> =
            (0..config.search_space_generation.num_tasks)
                .into_iter()
                .map(|_| {
                    tokio::spawn(Self::search_space_generation_task(
                        work_dir.clone(),
                        Arc::new(config.search_space_generation.clone()),
                        search_space_generation_queue.clone(),
                        comet_search_queue.clone(),
                        storage.clone(),
                        search_space_generation_stop_flag.clone(),
                        finished_search_spaces.clone(),
                    ))
                })
                .collect();

        let comet_search_handlers: Vec<tokio::task::JoinHandle<()>> =
            (0..config.comet_search.num_tasks)
                .into_iter()
                .map(|_| {
                    tokio::spawn(Self::comet_search_task(
                        work_dir.clone(),
                        Arc::new(config.comet_search.clone()),
                        storage.clone(),
                        comet_search_queue.clone(),
                        goodness_and_rescoreing_queue.clone(),
                        comet_search_stop_flag.clone(),
                        finished_searches.clone(),
                    ))
                })
                .collect();

        let goodness_and_resconfing_handlers: Vec<tokio::task::JoinHandle<()>> =
            (0..config.goodness_and_rescoring.num_tasks)
                .into_iter()
                .map(|_| {
                    tokio::spawn(Self::goodness_and_rescoring_task(
                        work_dir.clone(),
                        goodness_and_rescoreing_queue.clone(),
                        cleanup_queue.clone(),
                        goodness_and_rescoreing_stop_flag.clone(),
                        finished_goodness_and_rescoring.clone(),
                    ))
                })
                .collect();

        let cleanup_handlers: Vec<tokio::task::JoinHandle<()>> = (0..config.cleanup.num_tasks)
            .into_iter()
            .map(|_| {
                tokio::spawn(Self::cleanup_task(
                    work_dir.clone(),
                    cleanup_queue.clone(),
                    finished_cleanups.clone(),
                    storage.clone(),
                    cleanup_stop_flag.clone(),
                ))
            })
            .collect();

        for mzml_file_path in mzml_file_paths {
            let manifest = SearchManifest::new(uuid.clone(), mzml_file_path);
            match index_queue.push(manifest).await {
                Ok(_) => (),
                Err(e) => {
                    error!("[{}] Error pushing manifest to index queue.", &e.uuid);
                    continue;
                }
            }
        }

        index_stop_flag.store(true, Ordering::Relaxed);

        match index_handler.await {
            Ok(_) => (),
            Err(e) => {
                error!("Error joining index thread: {:?}", e);
            }
        }

        preparation_stop_flag.store(true, Ordering::Relaxed);

        for preparation_handler in preparation_handlers {
            match preparation_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining preparation thread: {:?}", e);
                }
            }
        }

        search_space_generation_stop_flag.store(true, Ordering::Relaxed);

        for search_space_generation_handler in search_space_generation_handlers {
            match search_space_generation_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining search space generation thread: {:?}", e);
                }
            }
        }

        comet_search_stop_flag.store(true, Ordering::Relaxed);

        for comet_search_handler in comet_search_handlers {
            match comet_search_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining comet search thread: {:?}", e);
                }
            }
        }

        goodness_and_rescoreing_stop_flag.store(true, Ordering::Relaxed);

        for goodness_and_resconfing_handler in goodness_and_resconfing_handlers {
            match goodness_and_resconfing_handler.await {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining goodness and rescoring thread: {:?}", e);
                }
            }
        }

        cleanup_stop_flag.store(true, Ordering::Relaxed);

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

        let mut storage = match Arc::try_unwrap(storage) {
            Ok(storage) => storage,
            Err(_) => {
                bail!("Error unwrapping storage");
            }
        };
        storage.remove_search_params(&uuid).await?;
        storage.remove_comet_config(&uuid).await?;
        storage.remove_ptms(&uuid).await?;

        Ok(())
    }

    /// Task to index and split up the mzML file
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    /// * `index_queue` - Queue for the indexing task
    /// * `preparation_queue` - Queue for the preparation task
    /// * `stop_flag` - Flag to indicate to stop once the index queue is empty
    ///
    pub async fn indexing_task(
        work_dir: PathBuf,
        index_queue: Arc<Q>,
        preparation_queue: Arc<Q>,
        stop_flag: Arc<AtomicBool>,
    ) {
        loop {
            while let Some(manifest) = index_queue.pop().await {
                let ms_run_dir_path = manifest.get_ms_run_dir_path(&work_dir);

                match fs::create_dir_all(&ms_run_dir_path) {
                    Ok(_) => (),
                    Err(e) => {
                        error!("[{}] Error creating work directory: {}", &manifest.uuid, e);
                        continue;
                    }
                }

                let index = match Indexer::create_index(&manifest.ms_run_mzml_path, None) {
                    Ok(index) => index,
                    Err(e) => {
                        error!("[{}] Error creating index: {:?}", &manifest.uuid, e);
                        continue;
                    }
                };

                let index_file_path = ms_run_dir_path.join("index.json");
                debug!("Writing index to: {}", index_file_path.display());
                let index_json = match index.to_json() {
                    Ok(json) => json,
                    Err(e) => {
                        error!("[{}] Error serializing index: {:?}", &manifest.uuid, e);
                        continue;
                    }
                };
                match std::fs::write(&index_file_path, index_json) {
                    Ok(_) => (),
                    Err(e) => {
                        error!("[{}] Error writing index: {:?}", &manifest.uuid, e);
                        continue;
                    }
                };

                let uuid_path = ms_run_dir_path.join("uuid.txt");
                match tokio::fs::write(&uuid_path, &manifest.uuid).await {
                    Ok(_) => (),
                    Err(e) => {
                        error!("[{}] Error writing uuid: {:?}", &manifest.uuid, e);
                        continue;
                    }
                };

                let mut reader = match IndexedReader::new(&manifest.ms_run_mzml_path, &index) {
                    Ok(reader) => reader,
                    Err(e) => {
                        error!("[{} /] Error creating reader: {:?}", &manifest.uuid, e);
                        continue;
                    }
                };

                for (spec_id, _) in index.get_spectra() {
                    let mzml = match reader.extract_spectrum(&spec_id) {
                        Ok(content) => content,
                        Err(e) => {
                            error!("[{}] Error extracting spectrum: {:?}", &manifest.uuid, e);
                            continue;
                        }
                    };

                    let mut new_manifest = manifest.clone();
                    new_manifest.spectrum_id = spec_id.clone();
                    new_manifest.spectrum_mzml = mzml;
                    new_manifest.is_indexing_done = true;

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
    /// * `work_dir` - Work directory where the results are stored
    /// * `preparation_queue` - Queue for the preparation task
    /// * `search_space_generation_queue` - Queue for the search space generation task
    /// * `stop_flag` - Flag to indicate to stop once the preparation queue is empty
    ///
    fn preparation_task(
        work_dir: PathBuf,
        preparation_queue: Arc<Q>,
        search_space_generation_queue: Arc<Q>,
        stop_flag: Arc<AtomicBool>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            loop {
                while let Some(mut manifest) = preparation_queue.pop().await {
                    debug!("[{} / {}] Preparing", &manifest.uuid, &manifest.spectrum_id);

                    if manifest.spectrum_id.is_empty() {
                        error!(
                            "[{} / {}] Spectrum ID is empty in spectra_dir_creation_thread",
                            &manifest.uuid, &manifest.spectrum_id
                        );
                        continue;
                    }

                    if manifest.spectrum_mzml.is_empty() {
                        error!(
                            "[{} / {}] Spectrum mzML is empty in spectra_dir_creation_thread",
                            &manifest.uuid, &manifest.spectrum_id
                        );
                        continue;
                    }

                    if !manifest.is_indexing_done {
                        error!(
                            "[{} / {}] Indexing not done in spectra_dir_creation_thread",
                            &manifest.uuid, &manifest.spectrum_id
                        );
                        continue;
                    }

                    let start = match manifest
                        .spectrum_mzml
                        .windows(SPECTRUM_START_TAG.len())
                        .position(|window| window == SPECTRUM_START_TAG)
                    {
                        Some(start) => start,
                        None => {
                            error!(
                                "[{} / {}] No spectrum start",
                                &manifest.uuid, &manifest.spectrum_id
                            );
                            continue;
                        }
                    };

                    let stop = match manifest
                        .spectrum_mzml
                        .windows(SPECTRUM_STOP_TAG.len())
                        .position(|window| window == SPECTRUM_STOP_TAG)
                    {
                        Some(start) => start,
                        None => {
                            error!(
                                "[{} / {}] No spectrum stop",
                                &manifest.uuid, &manifest.spectrum_id
                            );
                            continue;
                        }
                    };

                    let spectrum_xml = &manifest.spectrum_mzml[start..stop];

                    // As this mzML is already reduced to the spectrum of interest, we can parse it directly
                    // using MzMlReader::parse_spectrum_xml
                    let spectrum = match MzMlReader::parse_spectrum_xml(spectrum_xml) {
                        Ok(spectrum) => spectrum,
                        Err(e) => {
                            error!(
                                "[{} / {}] Error parsing spectrum: {:?}",
                                &manifest.uuid, &manifest.spectrum_id, e
                            );
                            continue;
                        }
                    };

                    let spectrum = match spectrum {
                        Spectrum::MsNSpectrum(spectrum) => spectrum,
                        _ => {
                            // Ignore MS1
                            trace!(
                                "[{} / {}] Ignoring MS1 spectrum",
                                &manifest.uuid,
                                &manifest.spectrum_id
                            );
                            continue;
                        }
                    };

                    // Ignore MS3 and higher
                    if spectrum.get_ms_level() != 2 {
                        trace!(
                            "[{} / {}] Ignoring MS{} spectrum",
                            &manifest.uuid,
                            &manifest.spectrum_id,
                            spectrum.get_ms_level()
                        );
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

                    let spectrum_dir_path = manifest.get_spectrum_dir_path(&work_dir);

                    match fs::create_dir_all(&spectrum_dir_path) {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error creating spectrum directory: {}",
                                &manifest.uuid, manifest.spectrum_id, e
                            );
                            continue;
                        }
                    }

                    let spectrum_mzml_path = manifest.get_spectrum_mzml_path(&work_dir);

                    match tokio::fs::write(&spectrum_mzml_path, &manifest.spectrum_mzml).await {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error writing single spectrum: {}",
                                &manifest.uuid, manifest.spectrum_id, e
                            );
                            continue;
                        }
                    }

                    manifest.spectrum_mzml = Vec::with_capacity(0); // Free up some memory
                    manifest.precursors = precursors;
                    manifest.is_preparation_done = true;

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
    /// * `work_dir` - Work directory where the results are stored
    /// * `config` - Configuration for the search space generation task
    /// * `search_space_generation_queue` - Queue for the search space generation task
    /// * `comet_search_queue` - Queue for the Comet search task
    /// * `storage` - Storage to access configuration and PTMs
    /// * `stop_flag` - Flag to indicate to stop once the search space generation queue is empty
    ///
    fn search_space_generation_task(
        work_dir: PathBuf,
        config: Arc<SearchSpaceGenerationTaskConfiguration>,
        search_space_generation_queue: Arc<Q>,
        comet_search_queue: Arc<Q>,
        storage: Arc<S>,
        stop_flag: Arc<AtomicBool>,
        finished_search_spaces: Arc<AtomicUsize>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            let mut last_search_uuid = String::new();
            let mut current_search_params = SearchParameters::new();
            let mut current_ptms: Vec<PostTranslationalModification> = Vec::new();

            loop {
                while let Some(mut manifest) = search_space_generation_queue.pop().await {
                    debug!(
                        "[{} / {}] Generating search space",
                        &manifest.uuid, &manifest.spectrum_id
                    );
                    if !manifest.is_preparation_done {
                        error!(
                            "[{} / {}] Prepartion not done in search_space_generation_task",
                            &manifest.uuid, &manifest.spectrum_id
                        );
                        continue;
                    }

                    if last_search_uuid != manifest.uuid {
                        current_search_params =
                            match storage.get_search_parameters(&manifest.uuid).await {
                                Ok(Some(params)) => params,
                                Ok(None) => {
                                    error!(
                                        "[{} / {}] Search params not found",
                                        manifest.uuid, manifest.spectrum_id
                                    );
                                    continue;
                                }
                                Err(e) => {
                                    error!(
                                        "[{} / {}] Error getting search params from storage: {:?}",
                                        &manifest.uuid, &manifest.spectrum_id, e
                                    );
                                    continue;
                                }
                            };

                        current_ptms = match storage.get_ptms(&manifest.uuid).await {
                            Ok(Some(ptms)) => ptms,
                            Ok(None) => {
                                error!(
                                    "[{} / {}] PTMs not found",
                                    manifest.uuid, manifest.spectrum_id
                                );
                                continue;
                            }
                            Err(e) => {
                                error!(
                                    "[{} / {}] Error getting PTMs from storage: {:?}",
                                    &manifest.uuid, &manifest.spectrum_id, e
                                );
                                return;
                            }
                        };
                        last_search_uuid = manifest.uuid.clone();
                    }

                    // set default charges if none are provided
                    for (_, precursor_charges) in manifest.precursors.iter_mut() {
                        if precursor_charges.is_empty() {
                            precursor_charges.extend(2..=current_search_params.max_charge);
                        }
                    }

                    for (precursor_mz, precursor_charges) in manifest.precursors.iter() {
                        for precursor_charge in precursor_charges {
                            let fasta_file_path = manifest.get_fasta_file_path(
                                &work_dir,
                                *precursor_mz,
                                *precursor_charge,
                            );

                            let mass = mass_to_int(mass_to_charge_to_dalton(
                                *precursor_mz,
                                *precursor_charge,
                            ));

                            match create_search_space(
                                &fasta_file_path,
                                &current_ptms,
                                mass,
                                current_search_params.lower_mass_tolerance_ppm,
                                current_search_params.upper_mass_tolerance_ppm,
                                current_search_params.max_variable_modifications,
                                current_search_params.decoys_per_peptide,
                                config.target_url.to_owned(),
                                config.decoy_url.clone(),
                                config.target_lookup_url.clone(),
                                config.decoy_cache_url.clone(),
                            )
                            .await
                            {
                                Ok(_) => (),
                                Err(e) => {
                                    error!(
                                        "[{} / {}] Error creating search space: {:?}",
                                        &manifest.uuid, &manifest.spectrum_id, e
                                    );
                                    continue;
                                }
                            };

                            finished_search_spaces.fetch_add(1, Ordering::Relaxed);
                        }
                    }

                    manifest.is_search_space_generated = true;
                    loop {
                        manifest = match comet_search_queue.push(manifest).await {
                            Ok(_) => break,
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

    /// Task to run the Comet search
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    /// * `config` - Configuration for the Comet search task
    /// * `storage` - Storage to access params and PTMs
    /// * `comet_search_queue` - Queue for the Comet search task
    /// * `goodness_and_rescoreing_queue` - Goodness and rescoreing queue
    /// * `stop_flag` - Flag to indicate to stop once the Comet search queue is empty
    ///
    fn comet_search_task(
        work_dir: PathBuf,
        config: Arc<CometSearchTaskConfiguration>,
        storage: Arc<S>,
        comet_search_queue: Arc<Q>,
        goodness_and_rescoreing_queue: Arc<Q>,
        stop_flag: Arc<AtomicBool>,
        finished_searches: Arc<AtomicUsize>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            let mut last_search_uuid = String::new();
            let mut current_comet_config: Option<CometConfiguration> = None;

            loop {
                while let Some(mut manifest) = comet_search_queue.pop().await {
                    debug!(
                        "[{} / {}] Running Comet search",
                        &manifest.uuid, &manifest.spectrum_id
                    );

                    if !manifest.is_search_space_generated {
                        error!(
                            "[{} / {}] Search space not generated in `comet_search_task`",
                            &manifest.uuid, &manifest.spectrum_id
                        );
                        continue;
                    }

                    if last_search_uuid != manifest.uuid {
                        current_comet_config = match storage.get_comet_config(&manifest.uuid).await
                        {
                            Ok(Some(config)) => Some(config),
                            Ok(None) => {
                                error!(
                                    "[{} / {}] Comet config not found",
                                    &manifest.uuid, &manifest.spectrum_id
                                );
                                continue;
                            }
                            Err(e) => {
                                error!(
                                    "[{} / {}] Error getting comet config from storage: {:?}",
                                    &manifest.uuid, &manifest.spectrum_id, e
                                );
                                continue;
                            }
                        };

                        match current_comet_config
                            .as_mut()
                            .unwrap()
                            .set_option("threads", &format!("{}", config.threads))
                        {
                            Ok(_) => (),
                            Err(e) => {
                                error!(
                                    "[{} / {}] Error setting threads in Comet configuration: {:?}",
                                    &manifest.uuid, &manifest.spectrum_id, e
                                );
                                continue;
                            }
                        }

                        last_search_uuid = manifest.uuid.clone();
                    }

                    // Unwrap the current Comet configuration for easier access
                    let comet_config = current_comet_config.as_mut().unwrap();

                    for (precursor_mz, precursor_charges) in manifest.precursors.iter() {
                        for precursor_charge in precursor_charges {
                            let fasta_file_path = manifest.get_fasta_file_path(
                                &work_dir,
                                *precursor_mz,
                                *precursor_charge,
                            );

                            let psms_file_path = manifest.get_psms_file_path(
                                &work_dir,
                                *precursor_mz,
                                *precursor_charge,
                            );

                            let comet_params_file_path = manifest.get_comet_params_path(
                                &work_dir,
                                *precursor_mz,
                                *precursor_charge,
                            );

                            match comet_config.set_charge(*precursor_charge) {
                                Ok(_) => (),
                                Err(e) => {
                                    error!(
                                        "[{} / {}] Error setting charge: {:?}",
                                        &manifest.uuid, &manifest.spectrum_id, e
                                    );
                                    continue;
                                }
                            }

                            match comet_config.async_to_file(&comet_params_file_path).await {
                                Ok(_) => (),
                                Err(e) => {
                                    error!(
                                        "[{} / {}] Error writing Comet params: {:?}",
                                        &manifest.uuid, &manifest.spectrum_id, e
                                    );
                                    continue;
                                }
                            }

                            match run_comet_search(
                                &config.comet_exe_path,
                                &comet_params_file_path,
                                &fasta_file_path,
                                &psms_file_path.with_extension(""),
                                &manifest.get_spectrum_mzml_path(&work_dir),
                            )
                            .await
                            {
                                Ok(_) => (),
                                Err(e) => {
                                    error!("Error running Comet search: {:?}", e);
                                    continue;
                                }
                            }

                            match tokio::fs::rename(
                                &psms_file_path.with_extension("txt"),
                                &psms_file_path,
                            )
                            .await
                            {
                                Ok(_) => (),
                                Err(e) => {
                                    error!("Error renaming PSM file: {:?}", e);
                                    continue;
                                }
                            }

                            debug!(
                                "[{} / {}] Comet search done for {}",
                                &manifest.uuid,
                                &manifest.spectrum_id,
                                psms_file_path.display()
                            );
                        }
                    }

                    manifest.is_comet_search_done = true;
                    loop {
                        manifest = match goodness_and_rescoreing_queue.push(manifest).await {
                            Ok(_) => break,
                            Err(e) => {
                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                                e
                            }
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

    fn goodness_and_rescoring_task(
        work_dir: PathBuf,
        goodness_and_rescoreing_queue: Arc<Q>,
        cleanup_queue: Arc<Q>,
        stop_flag: Arc<AtomicBool>,
        finished_goodness_and_rescoring: Arc<AtomicUsize>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            let (to_python, mut from_rust) = tokio::sync::mpsc::channel::<Vec<f64>>(1);
            let (to_rust, mut from_python) =
                tokio::sync::mpsc::channel::<(Vec<GoodnessOfFitRecord>, Vec<f64>, Vec<f64>)>(1);

            let python_handle: std::thread::JoinHandle<Result<()>> =
                std::thread::spawn(move || {
                    match Python::with_gil(|py| {
                        // std imports
                        let signal = py.import_bound("signal")?;
                        // maccoys imports
                        let goodness_of_fit_mod =
                            PyModule::import_bound(py, "maccoys.goodness_of_fit")?;
                        let scoring_mod = PyModule::import_bound(py, "maccoys.scoring")?;
                        // enable CTRL-C
                        signal
                            .getattr("signal")?
                            .call1((signal.getattr("SIGINT")?, signal.getattr("SIG_DFL")?))?;

                        // Load all necessary functions
                        let calc_goodnesses_fn = goodness_of_fit_mod.getattr("calc_goodnesses")?;
                        let calculate_exp_score_fn = scoring_mod.getattr("calculate_exp_score")?;
                        let calculate_distance_score_fn =
                            scoring_mod.getattr("calculate_distance_score")?;

                        while let Some(psm_scores) = from_rust.blocking_recv() {
                            // Cast to python list
                            let psm_scores = PyList::new_bound(py, psm_scores);

                            let goodness_of_fits: Vec<GoodnessOfFitRecord> = calc_goodnesses_fn
                                .call1((&psm_scores,))?
                                .extract::<Vec<(String, String, f64, f64)>>()?
                                .into_iter()
                                .map(|row| GoodnessOfFitRecord::from(row))
                                .collect();

                            let exponential_score: Vec<f64> =
                                calculate_exp_score_fn.call1((&psm_scores,))?.extract()?;

                            let distance_score: Vec<f64> = calculate_distance_score_fn
                                .call1((&psm_scores,))?
                                .extract()?;

                            to_rust.blocking_send((
                                goodness_of_fits,
                                exponential_score,
                                distance_score,
                            ))?;
                        }

                        Ok::<_, anyhow::Error>(())
                    }) {
                        Ok(_) => (),
                        Err(e) => {
                            error!("[PYTHON] Error running Python thread: {:?}", e);
                        }
                    }
                    Ok(())
                });

            loop {
                while let Some(mut manifest) = goodness_and_rescoreing_queue.pop().await {
                    debug!(
                        "[{} / {}] Goodness and rescoring",
                        &manifest.uuid, &manifest.spectrum_id
                    );

                    if !manifest.is_comet_search_done {
                        error!(
                            "[{} / {}] Comet search not finished in goodness_and_rescoring_thread",
                            &manifest.uuid, &manifest.spectrum_id
                        );
                        continue;
                    }

                    for (precursor_mz, precursor_charges) in manifest.precursors.iter() {
                        for precursor_charge in precursor_charges {
                            let psms_file_path = manifest.get_psms_file_path(
                                &work_dir,
                                *precursor_mz,
                                *precursor_charge,
                            );

                            let goodness_file_path = manifest.get_goodness_file_path(
                                &work_dir,
                                *precursor_mz,
                                *precursor_charge,
                            );

                            let mut psms = match PeptideSpectrumMatchTsv::read(&psms_file_path) {
                                Ok(Some(psms)) => psms,
                                Ok(None) => {
                                    info!(
                                        "[{} / {}] No PSMs found in `{}`",
                                        &manifest.uuid,
                                        &manifest.spectrum_id,
                                        psms_file_path.display()
                                    );
                                    continue;
                                }
                                Err(e) => {
                                    error!(
                                        "[{} / {}] Error reading PSMs from `{}`: {:?}",
                                        &manifest.uuid,
                                        &manifest.spectrum_id,
                                        psms_file_path.display(),
                                        e
                                    );
                                    continue;
                                }
                            };

                            let psms_score_series = match psms.column(COMET_EXP_BASE_SCORE) {
                                Ok(scores) => scores,
                                Err(e) => {
                                    error!(
                                        "[{} / {}] Error selecting scores `{}` from PSMs: {:?}",
                                        &manifest.uuid,
                                        &manifest.spectrum_id,
                                        COMET_EXP_BASE_SCORE,
                                        e
                                    );
                                    continue;
                                }
                            };

                            let psms_score: Vec<f64> = match psms_score_series.f64() {
                                Ok(scores) => scores
                                    .to_vec()
                                    .into_iter()
                                    .map(|score| score.unwrap_or(-1.0))
                                    .collect(),
                                Err(e) => {
                                    error!(
                                        "[{} / {}] Error converting scores to f64: {:?}",
                                        &manifest.uuid, &manifest.spectrum_id, e
                                    );
                                    continue;
                                }
                            };

                            match to_python.send(psms_score).await {
                                Ok(_) => (),
                                Err(e) => {
                                    error!(
                                        "[{} / {}] Error sending scores to Python: {:?}",
                                        &manifest.uuid, &manifest.spectrum_id, e
                                    );
                                    continue;
                                }
                            }

                            let (goodness_of_fits, exponential_scores, dist_scores) =
                                match from_python.recv().await {
                                    Some(goodness_of_fit) => goodness_of_fit,
                                    None => {
                                        error!(
                                            "[{} / {}] No goodness of fit received from Python",
                                            &manifest.uuid, &manifest.spectrum_id
                                        );
                                        continue;
                                    }
                                };

                            let mut writer = match csv::WriterBuilder::new()
                                .delimiter(b'\t')
                                .has_headers(true)
                                .from_path(&goodness_file_path)
                            {
                                Ok(writer) => writer,
                                Err(e) => {
                                    error!(
                                        "[{} / {}] Error creating CSV writer for `{}`: {:?}",
                                        &manifest.uuid,
                                        &manifest.spectrum_id,
                                        goodness_file_path.display(),
                                        e
                                    );
                                    continue;
                                }
                            };

                            for goodness_row in goodness_of_fits {
                                match writer.serialize(goodness_row) {
                                    Ok(_) => (),
                                    Err(e) => {
                                        error!(
                                            "[{} / {}] Error writing goodness of fit to `{}`: {:?}",
                                            &manifest.uuid,
                                            &manifest.spectrum_id,
                                            goodness_file_path.display(),
                                            e
                                        );
                                        continue;
                                    }
                                }
                            }

                            match psms.with_column(Series::new(EXP_SCORE_NAME, exponential_scores))
                            {
                                Ok(_) => (),
                                Err(e) => {
                                    error!(
                                        "[{} / {}] Error adding exponential scores to PSMs: {:?}",
                                        &manifest.uuid, &manifest.spectrum_id, e
                                    );
                                    continue;
                                }
                            }

                            match psms.with_column(Series::new(DIST_SCORE_NAME, dist_scores)) {
                                Ok(_) => (),
                                Err(e) => {
                                    error!(
                                        "[{} / {}] Error adding distance scores to PSMs: {:?}",
                                        &manifest.uuid, &manifest.spectrum_id, e
                                    );
                                    continue;
                                }
                            }

                            match PeptideSpectrumMatchTsv::overwrite(psms, &psms_file_path) {
                                Ok(_) => (),
                                Err(e) => {
                                    error!(
                                        "[{} / {}] Error writing PSMs to `{}`: {:?}",
                                        &manifest.uuid,
                                        &manifest.spectrum_id,
                                        psms_file_path.display(),
                                        e
                                    );
                                    continue;
                                }
                            }

                            debug!(
                                "[{} / {}] Goodness and rescoring done for `{}`",
                                &manifest.uuid,
                                &manifest.spectrum_id,
                                goodness_file_path.display()
                            );
                        }
                    }

                    manifest.is_goodness_and_rescoring_done = true;
                    loop {
                        manifest = match cleanup_queue.push(manifest).await {
                            Ok(_) => break,
                            Err(e) => {
                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                                e
                            }
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
            drop(to_python);
            match python_handle.join() {
                Ok(_) => (),
                Err(e) => {
                    error!("Error joining Python thread: {:?}", e);
                }
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
        work_dir: PathBuf,
        cleanup_queue: Arc<Q>,
        finished_cleanups: Arc<AtomicUsize>,
        storage: Arc<S>,
        stop_flag: Arc<AtomicBool>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            loop {
                let mut last_search_uuid = String::new();
                let mut current_search_params = SearchParameters::new();

                while let Some(manifest) = cleanup_queue.pop().await {
                    debug!(
                        "[{} / {}] Running cleanup",
                        &manifest.uuid, &manifest.spectrum_id
                    );

                    if !manifest.is_goodness_and_rescoring_done {
                        error!(
                            "[{} / {}] Goodness and rescoing is not done in cleanup_task",
                            &manifest.uuid, &manifest.spectrum_id
                        );
                        continue;
                    }

                    if last_search_uuid != manifest.uuid {
                        trace!(
                            "[cleanup_task] Loading data from storage (UUIDs: old => {}, new=> {})",
                            &last_search_uuid,
                            &manifest.uuid
                        );
                        current_search_params =
                            match storage.get_search_parameters(&manifest.uuid).await {
                                Ok(Some(params)) => params,
                                Ok(None) => {
                                    error!(
                                        "[{} / {}] Search params not found`",
                                        manifest.uuid, manifest.spectrum_id
                                    );
                                    continue;
                                }
                                Err(e) => {
                                    error!(
                                        "[{} / {}] Error getting search params from storage: {:?}",
                                        &manifest.uuid, &manifest.spectrum_id, e
                                    );
                                    continue;
                                }
                            };
                        last_search_uuid = manifest.uuid.clone();
                    }

                    // Delete the mzML file
                    match tokio::fs::remove_file(manifest.get_spectrum_mzml_path(&work_dir)).await {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error removing mzML: {}",
                                &manifest.uuid, &manifest.spectrum_id, e
                            );
                        }
                    }

                    for (precursor_mz, precursor_charges) in manifest.precursors.iter() {
                        for precursor_charge in precursor_charges {
                            if !current_search_params.keep_fasta_files {
                                let psms_file_path = manifest.get_fasta_file_path(
                                    &work_dir,
                                    *precursor_mz,
                                    *precursor_charge,
                                );
                                match tokio::fs::remove_file(&psms_file_path).await {
                                    Ok(_) => (),
                                    Err(e) => {
                                        error!(
                                            "[{} / {}] Error removing fasta file `{}`: {}",
                                            &manifest.uuid,
                                            &manifest.spectrum_id,
                                            psms_file_path.display(),
                                            e
                                        );
                                    }
                                }
                            }

                            let comet_params_path = manifest.get_comet_params_path(
                                &work_dir,
                                *precursor_mz,
                                *precursor_charge,
                            );
                            match tokio::fs::remove_file(&comet_params_path).await {
                                Ok(_) => (),
                                Err(e) => {
                                    error!(
                                        "[{} / {}] Error removing Comet params file `{}`: {}",
                                        &manifest.uuid,
                                        &manifest.spectrum_id,
                                        comet_params_path.display(),
                                        e
                                    );
                                }
                            }
                        }
                    }

                    finished_cleanups.fetch_add(1, Ordering::Relaxed);

                    debug!(
                        "[{} / {}] Cleanup done in `{}`",
                        &manifest.uuid,
                        &manifest.spectrum_id,
                        manifest.get_spectrum_dir_path(&work_dir).display()
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
