use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::Result;
use crossbeam_queue::ArrayQueue;
use dihardts_omicstools::mass_spectrometry::unit_conversions::mass_to_charge_to_dalton;
use dihardts_omicstools::{
    mass_spectrometry::spectrum::{MsNSpectrum, Precursor, Spectrum as SpectrumTrait},
    proteomics::io::mzml::{
        index::Index,
        indexed_reader::IndexedReader,
        indexer::Indexer,
        reader::{Reader as MzMlReader, Spectrum},
    },
};
use futures::Future;
use macpepdb::{
    io::post_translational_modification_csv::reader::Reader as PtmReader,
    mass::convert::to_int as mass_to_int,
    tools::{
        progress_monitor::ProgressMonitor,
        queue_monitor::{MonitorableQueue, QueueMonitor},
    },
};
use tracing::{debug, error};

use crate::{
    functions::{
        create_search_space, create_spectrum_workdir, create_work_dir, run_comet_search,
        sanatize_string,
    },
    io::comet::configuration::Configuration as CometConfiguration,
};

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct PipelineCometConfiguration {
    comet_exe_path: PathBuf,
    threads: usize,
    default_comet_params_file_path: PathBuf,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct PipelineGeneralConfiguration {
    work_dir: PathBuf,
    num_preparation_tasks: usize,
    num_search_space_generation_tasks: usize,
    num_comet_search_tasks: usize,
    num_fdr_tasks: usize,
    num_goodness_and_rescoring_tasks: usize,
    num_cleanup_tasks: usize,
    keep_fasta_files: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct PipelineSearchConfiguration {
    max_charge: u8,
    lower_mass_tolerance_ppm: i64,
    upper_mass_tolerance_ppm: i64,
    max_variable_modifications: i8,
    decoys_per_peptide: usize,
    ptm_file_path: Option<PathBuf>,
    target_url: String,
    decoy_url: Option<String>,
    target_lookup_url: Option<String>,
    decoy_cache_url: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct PipelineConfiguration {
    general: PipelineGeneralConfiguration,
    search: PipelineSearchConfiguration,
    comet: PipelineCometConfiguration,
}

impl PipelineConfiguration {
    pub fn new() -> Self {
        Self {
            general: PipelineGeneralConfiguration {
                work_dir: PathBuf::from("./"),
                num_preparation_tasks: 1,
                num_search_space_generation_tasks: 1,
                num_comet_search_tasks: 1,
                num_fdr_tasks: 1,
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
                default_comet_params_file_path: PathBuf::from("./comet.params"),
            },
        }
    }
}

#[derive(Clone)]
pub struct SearchManifest {
    /// Workdir for the search
    pub work_dir: PathBuf,
    /// Path to the original mzML file containing the MS run
    pub ms_run_mzml_path: PathBuf,
    /// Index of the spectra in the mzML file
    pub ms_run_index: Option<Arc<Index>>,
    /// Spectrum ID of the spectrum to be searched
    pub spectrum_id: Option<String>,
    pub spectrum_work_dir: Option<PathBuf>,
    pub spectrum_mzml_path: Option<PathBuf>,
    /// Precursors for the spectrum (mz, charge)
    pub precursors: Option<Vec<(f64, Vec<u8>)>>,
    pub fasta_file_path: Option<PathBuf>,
    pub comet_params_file_path: Option<PathBuf>,
    pub psm_file_path: Option<PathBuf>,
}

impl SearchManifest {
    pub fn new(work_dir: PathBuf, ms_run_mzml_path: PathBuf) -> Self {
        let sanitized_mzml_stem =
            sanatize_string(ms_run_mzml_path.file_stem().unwrap().to_str().unwrap());
        let work_dir = work_dir.join(sanitized_mzml_stem);

        Self {
            work_dir,
            ms_run_mzml_path,
            ms_run_index: None,
            spectrum_id: None,
            spectrum_work_dir: None,
            spectrum_mzml_path: None,
            precursors: None,
            fasta_file_path: None,
            comet_params_file_path: None,
            psm_file_path: None,
        }
    }
}

/// Trait defining the methods for a pipeline queue
///
pub trait PipelineQueue: Send + Sync + Sized {
    /// Create a new queue
    ///
    /// # Arguments
    /// * `size` - The size of the queue
    ///
    fn new(size: usize) -> Self;

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
    queue: ArrayQueue<SearchManifest>,
}

impl PipelineQueue for LocalPipelineQueue {
    fn new(size: usize) -> Self {
        Self {
            queue: ArrayQueue::new(size),
        }
    }

    fn pop(&self) -> impl Future<Output = Option<SearchManifest>> {
        async { self.queue.pop() }
    }

    async fn push(&self, manifest: SearchManifest) -> Result<(), SearchManifest> {
        match self.queue.push(manifest) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn len(&self) -> impl Future<Output = usize> + Send {
        async { self.queue.len() }
    }
}

/// New Arc type to implement the MonitorableQueue trait
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
pub struct Pipeline<Q>
where
    Q: PipelineQueue + 'static,
{
    config: Arc<PipelineConfiguration>,
    index_queue: Arc<Q>,
    preparation_queue: Arc<Q>,
    search_space_generation_queue: Arc<Q>,
    comet_search_queue: Arc<Q>,
    // fdr_queue: Arc<Q>,
    // goodness_and_rescoreing_queue: Arc<Q>,
    cleanup_queue: Arc<Q>,
    index_stop_flag: Arc<AtomicBool>,
    preparation_stop_flag: Arc<AtomicBool>,
    search_space_generation_stop_flag: Arc<AtomicBool>,
    comet_search_stop_flag: Arc<AtomicBool>,
    // fdr_stop_flag: Arc<AtomicBool>,
    // goodness_and_rescoreing_stop_flag: Arc<AtomicBool>,
    cleanup_stop_flag: Arc<AtomicBool>,
    finshed_searches: Arc<AtomicUsize>,
}

impl<Q: PipelineQueue> Pipeline<Q> {
    pub async fn new(config: PipelineConfiguration) -> Result<Self> {
        create_work_dir(&config.general.work_dir).await?;
        let config = Arc::new(config);

        let index_queue = Arc::new(Q::new(100));
        let preparation_queue = Arc::new(Q::new(100));
        let search_space_generation_queue = Arc::new(Q::new(100));
        let comet_search_queue = Arc::new(Q::new(100));
        // let fdr_queue = Arc::new(Q::new(100));
        // let goodness_and_rescoreing_queue = Arc::new(Q::new(100));
        let cleanup_queue = Arc::new(Q::new(100));

        Ok(Self {
            config,
            index_queue,
            preparation_queue,
            search_space_generation_queue,
            comet_search_queue,
            // fdr_queue,
            // goodness_and_rescoreing_queue,
            cleanup_queue,
            index_stop_flag: Arc::new(AtomicBool::new(false)),
            preparation_stop_flag: Arc::new(AtomicBool::new(false)),
            search_space_generation_stop_flag: Arc::new(AtomicBool::new(false)),
            comet_search_stop_flag: Arc::new(AtomicBool::new(false)),
            // fdr_stop_flag: Arc::new(AtomicBool::new(false)),
            // goodness_and_rescoreing_stop_flag: Arc::new(AtomicBool::new(false)),
            cleanup_stop_flag: Arc::new(AtomicBool::new(false)),
            finshed_searches: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub async fn run(&self, mzml_file_paths: Vec<PathBuf>) -> Result<()> {
        let mut queue_monitor = QueueMonitor::new::<PipelineQueueArc<Q>>(
            "",
            vec![
                self.cleanup_queue.clone().into(),
                // self.goodness_and_rescoreing_queue.clone().into(),
                // self.fdr_queue.clone().into(),
                self.comet_search_queue.clone().into(),
                self.search_space_generation_queue.clone().into(),
                self.preparation_queue.clone().into(),
                self.index_queue.clone().into(),
            ],
            vec![100, 100, 100, 100, 100],
            vec![
                "Cleanup".to_owned(),
                // "Goodness and Rescoring".to_owned(),
                // "FDR".to_owned(),
                "Comet Search".to_owned(),
                "Search Space Generation".to_owned(),
                "Preparation".to_owned(),
                "Index".to_owned(),
            ],
            None,
        )?;

        let mut metrics_monitor = ProgressMonitor::new(
            "",
            vec![self.finshed_searches.clone()],
            vec![None],
            vec!["Finished searches".to_owned()],
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
            (0..self.config.general.num_preparation_tasks)
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
            (0..self.config.general.num_search_space_generation_tasks)
                .into_iter()
                .map(|_| {
                    let search_space_generation_queue = self.search_space_generation_queue.clone();
                    let comet_search_queue = self.comet_search_queue.clone();
                    let stop_flag = self.search_space_generation_stop_flag.clone();
                    tokio::spawn(Self::search_space_generation_task(
                        search_space_generation_queue,
                        comet_search_queue,
                        self.config.clone(),
                        stop_flag,
                    ))
                })
                .collect();

        let comet_search_handlers: Vec<tokio::task::JoinHandle<()>> =
            (0..self.config.general.num_comet_search_tasks)
                .into_iter()
                .map(|_| {
                    let comet_search_queue = self.comet_search_queue.clone();
                    let fdr_queue = self.cleanup_queue.clone(); // TODO: Change this to FDR queue once implemented
                    let comet_exe = self.config.comet.comet_exe_path.clone();
                    let stop_flag = self.comet_search_stop_flag.clone();
                    tokio::spawn(Self::comet_search_task(
                        comet_search_queue,
                        fdr_queue,
                        comet_exe,
                        stop_flag,
                    ))
                })
                .collect();

        let cleanup_handlers: Vec<tokio::task::JoinHandle<()>> =
            (0..self.config.general.num_cleanup_tasks)
                .into_iter()
                .map(|_| {
                    let cleanup_queue = self.cleanup_queue.clone();
                    let finsihed_searches = self.finshed_searches.clone();
                    let stop_flag = self.cleanup_stop_flag.clone();
                    tokio::spawn(Self::cleanup_task(
                        cleanup_queue,
                        finsihed_searches,
                        self.config.clone(),
                        stop_flag,
                    ))
                })
                .collect();

        // let fdr_handlers: Vec<tokio::task::JoinHandle<()>> = (0..self.config.general.num_fdr_tasks)
        //     .into_iter()
        //     .map(|_| {
        //         let fdr_queue = self.fdr_queue.clone();
        //         let goodness_and_rescoreing_queue = self.goodness_and_rescoreing_queue.clone();
        //         let stop_flag = fdr_stop_flag.clone();
        //         tokio::spawn(Self::fdr_task(
        //             fdr_queue,
        //             goodness_and_rescoreing_queue,
        //             stop_flag,
        //         ))
        //     })
        //     .collect();

        // let goodness_and_resconfing_handlers: Vec<tokio::task::JoinHandle<()>> =
        //     (0..self.config.general.num_goodness_and_rescoring_tasks)
        //         .into_iter()
        //         .map(|_| {
        //             let goodness_and_rescoreing_queue = self.goodness_and_rescoreing_queue.clone();
        //             let stop_flag = goodness_and_resconfing_stop_flag.clone();
        //             tokio::spawn(Self::goodness_and_rescoring_task(
        //                 goodness_and_rescoreing_queue,
        //                 stop_flag,
        //             ))
        //         })
        //         .collect();

        for mzml_file_path in mzml_file_paths {
            let manifest =
                SearchManifest::new(self.config.general.work_dir.clone(), mzml_file_path);
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

        // self.fdr_stop_flag.store(true, Ordering::Relaxed);

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
                }

                let index = Arc::new(index);
                for (spec_id, _) in index.get_spectra() {
                    let mut new_manifest = manifest.clone();
                    new_manifest.ms_run_index = Some(index.clone());
                    new_manifest.spectrum_id = Some(spec_id.clone());
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

    fn preparation_task(
        preparation_queue: Arc<Q>,
        search_space_generation_queue: Arc<Q>,
        stop_flag: Arc<AtomicBool>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            loop {
                while let Some(mut manifest) = preparation_queue.pop().await {
                    debug!("Preparing {}", manifest.spectrum_id.as_ref().unwrap());
                    if manifest.ms_run_index.is_none() {
                        error!("Index is None in search_preparation_thread");
                        continue;
                    }
                    if manifest.spectrum_id.is_none() {
                        error!("Spectrum ID is None in spectra_dir_creation_thread");
                        continue;
                    }

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

                    match tokio::fs::create_dir_all(&spectrum_work_dir).await {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Error creating spectrum work directory: {}", e);
                            continue;
                        }
                    }

                    let mut reader = match IndexedReader::new(
                        &manifest.ms_run_mzml_path,
                        manifest.ms_run_index.as_ref().unwrap(),
                    ) {
                        Ok(reader) => reader,
                        Err(e) => {
                            error!("Error creating reader: {:?}", e);
                            continue;
                        }
                    };

                    let spectrum_xml =
                        match reader.get_raw_spectrum(manifest.spectrum_id.as_ref().unwrap()) {
                            Ok(spectrum) => spectrum,
                            Err(e) => {
                                error!("Error reading spectrum: {:?}", e);
                                continue;
                            }
                        };

                    let spectrum = match MzMlReader::parse_spectrum_xml(spectrum_xml.as_slice()) {
                        Ok(spectrum) => spectrum,
                        Err(e) => {
                            error!("Error parsing spectrum: {:?}", e);
                            continue;
                        }
                    };
                    drop(spectrum_xml);

                    let spectrum = match spectrum {
                        Spectrum::MsNSpectrum(spectrum) => spectrum,
                        _ => {
                            // Ignore MS1
                            continue;
                        }
                    };

                    // Ignore MS3 and higher
                    if spectrum.get_ms_level() != 2 {
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

                    let mzml_conent =
                        match reader.extract_spectrum(manifest.spectrum_id.as_ref().unwrap()) {
                            Ok(content) => content,
                            Err(e) => {
                                error!("Error extracting spectrum: {:?}", e);
                                continue;
                            }
                        };

                    drop(reader);

                    let spectrum_mzml_path = spectrum_work_dir.join("spectrum.mzML");

                    match tokio::fs::write(&spectrum_mzml_path, mzml_conent).await {
                        Ok(_) => (),
                        Err(e) => {
                            error!("Error writing spectrum.mzML: {}", e);
                            continue;
                        }
                    }

                    manifest.spectrum_work_dir = Some(spectrum_work_dir);
                    manifest.spectrum_mzml_path = Some(spectrum_mzml_path);
                    manifest.precursors = Some(precursors);

                    loop {
                        manifest = match search_space_generation_queue.push(manifest).await {
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

    fn search_space_generation_task(
        search_space_generation_queue: Arc<Q>,
        comet_search_queue: Arc<Q>,
        config: Arc<PipelineConfiguration>,
        stop_flag: Arc<AtomicBool>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            let mut comet_config =
                match CometConfiguration::new(&config.comet.default_comet_params_file_path) {
                    Ok(config) => config,
                    Err(e) => {
                        error!("Error reading Comet configuration: {:?}", e);
                        return;
                    }
                };

            let ptms = match &config.search.ptm_file_path {
                Some(ptm_file_path) => match PtmReader::read(Path::new(&ptm_file_path)) {
                    Ok(ptms) => ptms,
                    Err(e) => {
                        error!("Error reading PTMs: {:?}", e);
                        return;
                    }
                },
                None => Vec::new(),
            };

            match comet_config.set_ptms(&ptms, config.search.max_variable_modifications) {
                Ok(_) => (),
                Err(e) => {
                    error!("Error setting PTMs: {:?}", e);
                    return;
                }
            }

            match comet_config.set_option("threads", &format!("{}", config.comet.threads)) {
                Ok(_) => (),
                Err(e) => {
                    error!("Error setting threads: {:?}", e);
                    return;
                }
            }

            match comet_config.set_num_results(10000) {
                Ok(_) => (),
                Err(e) => {
                    error!("Error setting number of results: {:?}", e);
                    return;
                }
            }

            match comet_config.set_max_variable_mods(config.search.max_variable_modifications) {
                Ok(_) => (),
                Err(e) => {
                    error!("Error setting max variable modifications: {:?}", e);
                    return;
                }
            }

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

    fn comet_search_task(
        comet_search_queue: Arc<Q>,
        fdr_queue: Arc<Q>,
        comet_exe: PathBuf,
        stop_flag: Arc<AtomicBool>,
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

                    match fdr_queue.push(manifest).await {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "Error pushing manifest to FDR queue: {}",
                                e.spectrum_id.as_ref().unwrap()
                            );
                            continue;
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

    // fn fdr_task()

    // fn goodness_and_rescoring_task()

    fn cleanup_task(
        cleanup_queue: Arc<Q>,
        finsihed_searches: Arc<AtomicUsize>,
        config: Arc<PipelineConfiguration>,
        stop_flag: Arc<AtomicBool>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            loop {
                while let Some(manifest) = cleanup_queue.pop().await {
                    debug!(
                        "Running cleanup for {}",
                        manifest.spectrum_id.as_ref().unwrap()
                    );

                    if !manifest.fasta_file_path.is_some() {
                        error!("Fasta file path is None in cleanup_task");
                        continue;
                    }

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
                    finsihed_searches.fetch_add(1, Ordering::Relaxed);

                    debug!(
                        "Cleanup done for {}",
                        manifest.psm_file_path.as_ref().unwrap().display()
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
