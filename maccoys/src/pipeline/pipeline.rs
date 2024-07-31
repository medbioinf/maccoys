// std imports
use std::{
    fs,
    io::Cursor,
    num::ParseIntError,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

// 3rd party imports
use anyhow::{anyhow, bail, Context, Result};
use axum::{
    extract::{DefaultBodyLimit, Multipart, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
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
use macpepdb::{
    mass::convert::to_int as mass_to_int,
    tools::{progress_monitor::ProgressMonitor, queue_monitor::QueueMonitor},
};
use polars::prelude::*;
use pyo3::{prelude::*, types::PyList};
use signal_hook::{consts::SIGINT, iterator::Signals};
use tokio::sync::{mpsc::error::TryRecvError, RwLock};
use tracing::{debug, error, info, trace};
use uuid::Uuid;

// local imports
use super::{
    configuration::{
        CometSearchTaskConfiguration, PipelineConfiguration, RemoteEntypointConfiguration,
        SearchParameters, SearchSpaceGenerationTaskConfiguration,
        StandaloneCometSearchConfiguration, StandaloneGoodnessAndRescoringConfiguration,
        StandaloneIndexingConfiguration, StandalonePreparationConfiguration,
        StandaloneSearchSpaceGenerationConfiguration,
    },
    convert::IntoInputOutputQueueAndStorage,
    queue::{PipelineQueue, RedisPipelineQueue},
    storage::{PipelineStorage, RedisPipelineStorage},
};
use crate::{
    constants::{COMET_EXP_BASE_SCORE, DIST_SCORE_NAME, EXP_SCORE_NAME},
    errors::axum::web_error::AnyhowWebError,
    functions::{create_search_space, create_work_dir, run_comet_search},
    goodness_of_fit_record::GoodnessOfFitRecord,
    io::axum::multipart::write_streamed_file,
    io::comet::{
        configuration::Configuration as CometConfiguration,
        peptide_spectrum_match_tsv::PeptideSpectrumMatchTsv,
    },
    pipeline::{
        queue::PipelineQueueArc,
        search_manifest::SearchManifest,
        storage::{COUNTER_LABLES, NUMBER_OF_COUNTERS},
    },
};

/// Default start tag for a spectrum in mzML
const SPECTRUM_START_TAG: &'static [u8; 10] = b"<spectrum ";

/// Default stop tag for a spectrum in mzML
const SPECTRUM_STOP_TAG: &'static [u8; 11] = b"</spectrum>";

/// Shared state for the remote entrypoint service
///
struct EntrypointServiceState {
    index_queue: RedisPipelineQueue,
    storage: RwLock<RedisPipelineStorage>,
    work_dir: PathBuf,
}

/// Pipeline to run the MaCcoyS identification pipeline
///
pub struct Pipeline;

impl Pipeline {
    /// Run the pipeline locally for each mzML file
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where each search is stored
    /// * `config` - Configuration for the pipeline
    /// * `comet_config` - Configuration for Comet
    /// * `ptms` - Post translational modifications
    /// * `mzml_file_paths` - Paths to the mzML files to search
    ///
    pub async fn run_locally<Q: PipelineQueue + 'static, S: PipelineStorage + 'static>(
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
        storage.init_counters(&uuid).await?;

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
        let metrics_stop_flag = Arc::new(AtomicBool::new(false));

        // Metrics
        let metrics = vec![Arc::new(AtomicUsize::new(0)); NUMBER_OF_COUNTERS];

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

        let metrics_poll_taks = tokio::spawn(Self::poll_store_metrics_task(
            storage.clone(),
            uuid.clone(),
            metrics.clone(),
            metrics_stop_flag.clone(),
        ));

        let mut metrics_monitor = ProgressMonitor::new(
            "",
            metrics.clone(),
            vec![None; NUMBER_OF_COUNTERS],
            COUNTER_LABLES
                .iter()
                .rev()
                .map(|label| label.to_string())
                .collect(),
            None,
        )?;

        let index_handler: tokio::task::JoinHandle<()> = {
            tokio::spawn(Self::indexing_task(
                work_dir.clone(),
                storage.clone(),
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
                        storage.clone(),
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
                        storage.clone(),
                        search_space_generation_queue.clone(),
                        comet_search_queue.clone(),
                        search_space_generation_stop_flag.clone(),
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
                    ))
                })
                .collect();

        let goodness_and_resconfing_handlers: Vec<tokio::task::JoinHandle<()>> =
            (0..config.goodness_and_rescoring.num_tasks)
                .into_iter()
                .map(|_| {
                    tokio::spawn(Self::goodness_and_rescoring_task(
                        work_dir.clone(),
                        storage.clone(),
                        goodness_and_rescoreing_queue.clone(),
                        cleanup_queue.clone(),
                        goodness_and_rescoreing_stop_flag.clone(),
                    ))
                })
                .collect();

        let cleanup_handlers: Vec<tokio::task::JoinHandle<()>> = (0..config.cleanup.num_tasks)
            .into_iter()
            .map(|_| {
                tokio::spawn(Self::cleanup_task(
                    work_dir.clone(),
                    storage.clone(),
                    cleanup_queue.clone(),
                    cleanup_stop_flag.clone(),
                ))
            })
            .collect();

        for mzml_file_path in mzml_file_paths {
            let manifest = SearchManifest::new(
                uuid.clone(),
                mzml_file_path.file_name().unwrap().to_str().unwrap(),
            );

            tokio::fs::create_dir_all(manifest.get_ms_run_dir_path(&work_dir)).await?;
            tokio::fs::copy(&mzml_file_path, manifest.get_ms_run_mzml_path(&work_dir)).await?;

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

        metrics_stop_flag.store(true, Ordering::Relaxed);
        metrics_poll_taks.await?;
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
        storage.remove_counters(&uuid).await?;

        Ok(())
    }

    /// Task to poll the metrcis from the storage to monitor them
    ///
    /// # Arguments
    /// * `storage` - Storage to access the metrics
    /// * `uuid` - UUID of the search
    /// * `metrics` - Metrics to store the values
    /// * `stop_flag` - Flag to indicate to stop polling
    ///
    async fn poll_store_metrics_task<S: PipelineStorage>(
        storage: Arc<S>,
        uuid: String,
        metrics: Vec<Arc<AtomicUsize>>,
        stop_flag: Arc<AtomicBool>,
    ) {
        loop {
            metrics[0].store(
                storage.get_started_searches_ctr(&uuid).await.unwrap_or(0),
                Ordering::Relaxed,
            );
            metrics[1].store(
                storage.get_prepared_ctr(&uuid).await.unwrap_or(0),
                Ordering::Relaxed,
            );
            metrics[2].store(
                storage
                    .get_search_space_generation_ctr(&uuid)
                    .await
                    .unwrap_or(0),
                Ordering::Relaxed,
            );
            metrics[3].store(
                storage.get_comet_search_ctr(&uuid).await.unwrap_or(0),
                Ordering::Relaxed,
            );
            metrics[4].store(
                storage
                    .get_goodness_and_rescoring_ctr(&uuid)
                    .await
                    .unwrap_or(0),
                Ordering::Relaxed,
            );
            metrics[5].store(
                storage.get_cleanup_ctr(&uuid).await.unwrap_or(0),
                Ordering::Relaxed,
            );

            tokio::time::sleep(tokio::time::Duration::from_millis(750)).await;
            if stop_flag.load(Ordering::Relaxed) {
                break;
            }
        }
    }

    /// Task to index and split up the mzML file
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    /// * `index_queue` - Queue for the indexing task
    /// * `preparation_queue` - Queue for the preparation task
    /// * `stop_flag` - Flag to indicate to stop once the index queue is empty
    ///
    pub async fn indexing_task<Q: PipelineQueue + 'static, S: PipelineStorage + 'static>(
        work_dir: PathBuf,
        storage: Arc<S>,
        index_queue: Arc<Q>,
        preparation_queue: Arc<Q>,
        stop_flag: Arc<AtomicBool>,
    ) {
        loop {
            while let Some(manifest) = index_queue.pop().await {
                let index =
                    match Indexer::create_index(&manifest.get_ms_run_mzml_path(&work_dir), None) {
                        Ok(index) => index,
                        Err(e) => {
                            error!("[{}] Error creating index: {:?}", &manifest.uuid, e);
                            continue;
                        }
                    };

                let index_file_path = manifest.get_index_path(&work_dir);
                debug!("Writing index to: {}", index_file_path.display());
                let index_json = match index.to_json() {
                    Ok(json) => json,
                    Err(e) => {
                        error!("[{}] Error serializing index: {:?}", &manifest.uuid, e);
                        continue;
                    }
                };
                match tokio::fs::write(&index_file_path, index_json).await {
                    Ok(_) => (),
                    Err(e) => {
                        error!("[{}] Error writing index: {:?}", &manifest.uuid, e);
                        continue;
                    }
                };

                let ms_run_mzml = manifest.get_ms_run_mzml_path(&work_dir);
                let mut reader = match IndexedReader::new(&ms_run_mzml, &index) {
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

                    match storage.increment_started_searches_ctr(&manifest.uuid).await {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error incrementing indexing counter: {:?}",
                                &new_manifest.uuid, &new_manifest.spectrum_id, e
                            );
                            continue;
                        }
                    }

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
    pub fn preparation_task<Q: PipelineQueue + 'static, S: PipelineStorage + 'static>(
        work_dir: PathBuf,
        storage: Arc<S>,
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

                    match storage.increment_prepared_ctr(&manifest.uuid).await {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error incrementing prepare counter: {:?}",
                                &manifest.uuid, &manifest.spectrum_id, e
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
    pub fn search_space_generation_task<
        Q: PipelineQueue + 'static,
        S: PipelineStorage + 'static,
    >(
        work_dir: PathBuf,
        config: Arc<SearchSpaceGenerationTaskConfiguration>,
        storage: Arc<S>,
        search_space_generation_queue: Arc<Q>,
        comet_search_queue: Arc<Q>,
        stop_flag: Arc<AtomicBool>,
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

                            match storage
                                .increment_search_space_generation_ctr(&manifest.uuid)
                                .await
                            {
                                Ok(_) => (),
                                Err(e) => {
                                    error!(
                                        "[{} / {}] Error incrementing search space generation counter: {:?}",
                                        &manifest.uuid, &manifest.spectrum_id, e
                                    );
                                    continue;
                                }
                            }
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
    pub fn comet_search_task<Q: PipelineQueue + 'static, S: PipelineStorage + 'static>(
        work_dir: PathBuf,
        config: Arc<CometSearchTaskConfiguration>,
        storage: Arc<S>,
        comet_search_queue: Arc<Q>,
        goodness_and_rescoreing_queue: Arc<Q>,
        stop_flag: Arc<AtomicBool>,
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
                                    error!(
                                        "Error renaming PSM file `{}`: {:?}",
                                        &psms_file_path.with_extension("txt").display(),
                                        e
                                    );
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

                    match storage.increment_comet_search_ctr(&manifest.uuid).await {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error incrementing Comet search counter: {:?}",
                                &manifest.uuid, &manifest.spectrum_id, e
                            );
                            continue;
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
                }
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                // wait before checking the queue again
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
    }

    pub fn goodness_and_rescoring_task<Q: PipelineQueue + 'static, S: PipelineStorage + 'static>(
        work_dir: PathBuf,
        storage: Arc<S>,
        goodness_and_rescoreing_queue: Arc<Q>,
        cleanup_queue: Arc<Q>,
        stop_flag: Arc<AtomicBool>,
    ) -> impl std::future::Future<Output = ()> + Send {
        async move {
            let (to_python, mut from_rust) = tokio::sync::mpsc::channel::<Vec<f64>>(1);
            let (to_rust, mut from_python) =
                tokio::sync::mpsc::channel::<(Vec<GoodnessOfFitRecord>, Vec<f64>, Vec<f64>)>(1);
            let python_stop_flag = Arc::new(AtomicBool::new(false));
            let python_thread_stop_flag = python_stop_flag.clone(); // Getting moved in to the python thread

            let python_handle: std::thread::JoinHandle<Result<()>> =
                std::thread::spawn(move || {
                    match Python::with_gil(|py| {
                        // maccoys imports
                        let goodness_of_fit_mod =
                            PyModule::import_bound(py, "maccoys.goodness_of_fit")?;
                        let scoring_mod = PyModule::import_bound(py, "maccoys.scoring")?;

                        // Load all necessary functions
                        let calc_goodnesses_fn = goodness_of_fit_mod.getattr("calc_goodnesses")?;
                        let calculate_exp_score_fn = scoring_mod.getattr("calculate_exp_score")?;
                        let calculate_distance_score_fn =
                            scoring_mod.getattr("calculate_distance_score")?;

                        loop {
                            if python_thread_stop_flag.load(Ordering::Relaxed) {
                                break;
                            }

                            let psm_scores = match from_rust.try_recv() {
                                Ok(scores) => scores,
                                Err(TryRecvError::Empty) => {
                                    std::thread::sleep(tokio::time::Duration::from_millis(100));
                                    continue;
                                }
                                Err(TryRecvError::Disconnected) => {
                                    break;
                                }
                            };

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
                    debug!("[PYTHON] Python thread stopped");
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

                    match storage
                        .increment_goodness_and_rescoring_ctr(&manifest.uuid)
                        .await
                    {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error incrementing goodness and rescoring counter: {:?}",
                                &manifest.uuid, &manifest.spectrum_id, e
                            );
                            continue;
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
                }
                if stop_flag.load(Ordering::Relaxed) {
                    python_stop_flag.store(true, Ordering::Relaxed);
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
    pub fn cleanup_task<Q: PipelineQueue + 'static, S: PipelineStorage + 'static>(
        work_dir: PathBuf,
        storage: Arc<S>,
        cleanup_queue: Arc<Q>,
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

                    match storage.increment_cleanup_ctr(&manifest.uuid).await {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "[{} / {}] Error incrementing cleanup counter: {:?}",
                                &manifest.uuid, &manifest.spectrum_id, e
                            );
                            continue;
                        }
                    }

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

    /// Enqueues a search to a remote server
    ///
    /// # Arguments
    /// * `base_url` - Base URL of the remote server
    /// * `search_parameters_path` - Path to the search parameters file
    /// * `comet_params_path` - Path to the Comet parameters file
    /// * `mzml_file_paths` - Paths to the mzML files
    /// * `ptms_path` - Optional path to the PTMs file
    ///
    pub async fn run_remotely(
        base_url: String,
        search_parameters_path: PathBuf,
        comet_params_path: PathBuf,
        mzml_file_paths: Vec<PathBuf>,
        ptms_path: Option<PathBuf>,
    ) -> Result<String> {
        let enqueue_url = format!("{}/api/pipeline/enqueue", base_url);

        let search_parameters_reader =
            reqwest::Body::wrap_stream(tokio_util::codec::FramedRead::new(
                tokio::fs::File::open(search_parameters_path).await?,
                tokio_util::codec::BytesCodec::new(),
            ));

        let comet_params_reader = reqwest::Body::wrap_stream(tokio_util::codec::FramedRead::new(
            tokio::fs::File::open(comet_params_path).await?,
            tokio_util::codec::BytesCodec::new(),
        ));

        let mut form = reqwest::multipart::Form::new()
            .part(
                "search_params",
                reqwest::multipart::Part::stream(search_parameters_reader),
            )
            .part(
                "comet_params",
                reqwest::multipart::Part::stream(comet_params_reader),
            );

        if let Some(ptms_path) = ptms_path {
            let ptms_reader = reqwest::Body::wrap_stream(tokio_util::codec::FramedRead::new(
                tokio::fs::File::open(ptms_path).await?,
                tokio_util::codec::BytesCodec::new(),
            ));

            form = form.part("ptms", reqwest::multipart::Part::stream(ptms_reader));
        }

        for (index, mzml_file_path) in mzml_file_paths.iter().enumerate() {
            let mzml_reader = reqwest::Body::wrap_stream(tokio_util::codec::FramedRead::new(
                tokio::fs::File::open(mzml_file_path).await?,
                tokio_util::codec::BytesCodec::new(),
            ));

            let file_name = match mzml_file_path.file_name() {
                Some(file_name) => file_name.to_string_lossy().to_string(),
                None => {
                    bail!(
                        "Error getting file name from path: {}",
                        mzml_file_path.display()
                    );
                }
            };

            form = form.part(
                format!("mzml_{}", index),
                reqwest::multipart::Part::stream(mzml_reader).file_name(file_name),
            );
        }

        let client = reqwest::Client::new();
        let response = client
            .post(enqueue_url)
            .multipart(form)
            .header("Connection", "close")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!(
                "Error submitting search: {:?}",
                response.text().await?
            ));
        }

        let uuid = response.text().await?;
        info!(
            "Search submitted with UUID: {}. Start search monitor (exit with CTRL-C).",
            uuid
        );

        Self::start_remote_search_monitor(base_url, &uuid).await?;

        Ok(uuid)
    }

    /// Starts a progress monitor for a remote search
    ///
    /// # Arguments
    /// * `base_url` - Base URL of the remote server
    /// * `uuid` - UUID of the search
    ///
    pub async fn start_remote_search_monitor(base_url: String, uuid: &str) -> Result<()> {
        let monitor_url = format!("{}/api/pipeline/monitor/{}", base_url, uuid);
        let metrics_stop_flag = Arc::new(AtomicBool::new(false));
        let metrics = vec![Arc::new(AtomicUsize::new(0)); NUMBER_OF_COUNTERS];

        let thread_metrics_stop_flag = metrics_stop_flag.clone();
        let thread_metrics = metrics.clone();

        // Polls the metrics from the remote server
        let metrics_poll_taks: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
            let mut next_poll = tokio::time::Instant::now();
            while !thread_metrics_stop_flag.load(Ordering::Relaxed) {
                if next_poll >= tokio::time::Instant::now() {
                    tokio::time::sleep(next_poll - tokio::time::Instant::now()).await;
                }
                next_poll = tokio::time::Instant::now() + tokio::time::Duration::from_millis(300); // Should be smaller then the monitoring interval

                let response = match reqwest::get(&monitor_url).await {
                    Ok(response) => response,
                    Err(e) => {
                        error!("Error getting metrics: {:?}", e);
                        continue;
                    }
                };

                let tsv = match response.text().await {
                    Ok(csv) => csv,
                    Err(e) => {
                        error!("Error reading metrics line: {:?}", e);
                        continue;
                    }
                };

                let mut tsv_line_iter = tsv.lines().skip(1);

                let polled_metrics: Result<Vec<usize>, ParseIntError> = match tsv_line_iter.next() {
                    Some(metrics) => metrics
                        .trim()
                        .split("\t")
                        .map(|metric| metric.parse::<usize>())
                        .collect(),
                    None => {
                        error!("No metrics found in response");
                        continue;
                    }
                };

                match polled_metrics {
                    Ok(polled_metrics) => polled_metrics
                        .into_iter()
                        .zip(thread_metrics.iter())
                        .for_each(|(polled_metric, metric)| {
                            metric.store(polled_metric, Ordering::Relaxed);
                        }),
                    Err(e) => {
                        error!("Error parsing metrics: {:?}", e);
                        continue;
                    }
                };
            }
            Ok(())
        });

        let mut metrics_monitor = ProgressMonitor::new(
            "",
            metrics.clone(),
            vec![None; NUMBER_OF_COUNTERS],
            COUNTER_LABLES
                .iter()
                .rev()
                .map(|label| label.to_string())
                .collect(),
            None,
        )?;

        tokio::signal::ctrl_c().await?;

        metrics_stop_flag.store(true, Ordering::Relaxed);
        metrics_poll_taks.await??;

        metrics_monitor.stop().await?;

        Ok(())
    }

    /// Starts a http service to submit new searches
    /// and monitor the progress of the searches.
    ///
    /// # Arguments
    /// * `interface` - Interface to bind the service to
    /// * `port` - Port to bind the service to
    /// * `work_dir` - Work directory where the results are stored
    /// * `config` - Configuration for the remote entrypoint
    ///
    pub async fn start_remote_entrypoint(
        interface: String,
        port: u16,
        work_dir: PathBuf,
        config: RemoteEntypointConfiguration,
    ) -> Result<()> {
        let index_queue = RedisPipelineQueue::new(&config.index).await?;
        let storage = RwLock::new(RedisPipelineStorage::new(&config.storage).await?);

        let state = Arc::new(EntrypointServiceState {
            index_queue,
            storage,
            work_dir,
        });

        // Build our application with route
        let app = Router::new()
            .route(
                "/api/pipeline/enqueue",
                post(Self::remote_entrypoint_enqueue_endpoint),
            )
            .route(
                "/api/pipeline/monitor/:uuid",
                get(Self::remote_entrypoint_monitor_endpoint),
            )
            .layer(DefaultBodyLimit::disable())
            .with_state(state);

        let listener = tokio::net::TcpListener::bind(format!("{}:{}", interface, port)).await?;
        tracing::info!("ready for connections, listening on {}", interface);
        axum::serve(listener, app).await.unwrap();

        Ok(())
    }

    /// Entrypoint for submitting new searches. Returns the UUID of the search.
    ///
    /// # Arguments
    /// * `state` - Application state containing the storage, work directory and index queue
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/pipeline/enqueue`
    /// * Method: `POST`
    /// * Content-Type: `multipart/form-data`
    ///
    /// ### Body
    /// * `mzml_*` - Every fields starting with `mzml_` is considered as a mzML file
    /// * `search_parameters` - Search parameters in TOML format (section `search_parameters` from the configuration file without the section name)
    /// * `comet_params` - Comet parameter file
    /// * `ptms` - CSV file containing PTMs
    ///
    /// ## Response
    /// ```
    /// ae2439d1-7940-4b43-b96e-444a1e99e78d
    /// ```
    ///
    async fn remote_entrypoint_enqueue_endpoint(
        State(state): State<Arc<EntrypointServiceState>>,
        mut payload: Multipart,
    ) -> Result<Response, AnyhowWebError> {
        // Manifest files
        let uuid = Uuid::new_v4().to_string();
        let mut ptms: Vec<PostTranslationalModification> = Vec::new();
        let mut manifests: Vec<SearchManifest> = Vec::new();

        // Mandatory file flags
        let mut is_search_params_uploaded = false;
        let mut is_comet_params_uploaded = false;

        while let Ok(Some(field)) = payload.next_field().await {
            let field_name = match field.name() {
                Some(name) => name,
                None => return Err(anyhow!("Field has no name").into()),
            };

            if field_name.starts_with("mzml_") {
                let file_name = match field.file_name() {
                    Some(file_name) => file_name.to_string(),
                    None => continue,
                };
                let manifest = SearchManifest::new(uuid.clone(), &file_name);
                tokio::fs::create_dir_all(&manifest.get_ms_run_dir_path(&state.work_dir)).await?;

                write_streamed_file(&manifest.get_ms_run_mzml_path(&state.work_dir), field).await?;
                manifests.push(manifest);
                continue;
            }

            if field_name == "search_params" {
                let search_params: SearchParameters = match &field.text().await {
                    Ok(text) => toml::from_str(text).context("Parsing search parameters TOML")?,
                    Err(err) => {
                        return Err(anyhow!("Error reading search params field: {:?}", err).into())
                    }
                };
                state
                    .storage
                    .write()
                    .await
                    .set_search_parameters(&uuid, search_params)
                    .await?;
                is_search_params_uploaded = true;
                continue;
            }

            if field_name == "comet_params" {
                let comet_params: CometConfiguration = match &field.text().await {
                    Ok(text) => CometConfiguration::new(text.to_string())?,
                    Err(err) => {
                        return Err(anyhow!("Error reading Comet params field: {:?}", err).into())
                    }
                };
                state
                    .storage
                    .write()
                    .await
                    .set_comet_config(&uuid, &comet_params)
                    .await?;
                is_comet_params_uploaded = true;
                continue;
            }

            // Optional PTMs
            if field_name == "ptms" {
                let ptm_csv = match field.bytes().await {
                    Ok(csv) => Cursor::new(csv),
                    Err(err) => return Err(anyhow!("Error reading PTMs field: {:?}", err).into()),
                };

                let reader = csv::ReaderBuilder::new()
                    .has_headers(true)
                    .delimiter(b',')
                    .from_reader(ptm_csv);

                ptms = reader
                    .into_deserialize::<PostTranslationalModification>()
                    .into_iter()
                    .map(|ptm_result| match ptm_result {
                        Ok(ptm) => Ok(ptm),
                        Err(e) => Err(anyhow::Error::new(e)),
                    })
                    .collect::<Result<Vec<PostTranslationalModification>>>()?;

                debug!("PTMs: {}", &ptms.len());
            }
        }

        if manifests.is_empty() {
            return Err(anyhow!("No mzML files uploaded").into());
        }

        if !is_search_params_uploaded {
            tokio::fs::remove_dir_all(manifests[0].get_search_dir(&state.work_dir)).await?;
            return Err(anyhow!("Search parameters not uploaded").into());
        }

        if !is_comet_params_uploaded {
            tokio::fs::remove_dir_all(manifests[0].get_search_dir(&state.work_dir)).await?;
            return Err(anyhow!("Comet parameters not uploaded").into());
        }

        // Add remaining data
        state.storage.write().await.set_ptms(&uuid, &ptms).await?;
        state.storage.write().await.init_counters(&uuid).await?;

        for mut manifest in manifests.into_iter() {
            loop {
                manifest = match state.index_queue.push(manifest).await {
                    Ok(_) => break,
                    Err(errored_manifest) => {
                        error!("Error pushing manifest to index queue");
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        errored_manifest
                    }
                }
            }
        }

        Ok((StatusCode::OK, uuid).into_response())
    }

    /// Entrypoint for monitoring the progress of a search
    ///
    /// # API
    /// ## Request
    /// * Path: `/api/pipeline/monitor/:uuid`
    /// * Method: `GET`
    ///
    /// ## Response
    /// RSV with metrics
    /// ```tsv
    /// started_searches        prepared        search_space_generation comet_search    goodness_and_rescoring  cleanup
    /// 13852   10357   286     64      47      47
    /// ```
    ///
    async fn remote_entrypoint_monitor_endpoint(
        State(state): State<Arc<EntrypointServiceState>>,
        axum::extract::Path(uuid): axum::extract::Path<String>,
    ) -> Result<Response<String>, AnyhowWebError> {
        let counters: Vec<usize> = {
            let storage = state.storage.read().await;
            vec![
                storage.get_started_searches_ctr(&uuid).await?,
                storage.get_prepared_ctr(&uuid).await?,
                storage.get_search_space_generation_ctr(&uuid).await?,
                storage.get_comet_search_ctr(&uuid).await?,
                storage.get_goodness_and_rescoring_ctr(&uuid).await?,
                storage.get_cleanup_ctr(&uuid).await?,
            ]
        };

        // Start with column names
        let mut csv = COUNTER_LABLES
            .iter()
            .map(|label| label.to_string())
            .collect::<Vec<String>>()
            .join("\t");
        // add a new line
        csv.push_str("\n");
        // Add the counters
        csv.push_str(
            &counters
                .into_iter()
                .map(|ctr| format!("{}", ctr))
                .collect::<Vec<String>>()
                .join("\t"),
        );

        let response = axum::response::Response::builder()
            .header("Content-Type", "text/tab-separated-values")
            .status(StatusCode::OK)
            .body(csv)?;

        Ok(response)
    }

    pub async fn standalone_indexing(work_dir: PathBuf, config_file_path: PathBuf) -> Result<()> {
        let config: StandaloneIndexingConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let (storage, input_queue, output_queue) =
            config.into_input_output_queue_and_storage().await?;
        let storage = Arc::new(storage);
        let input_queue = Arc::new(input_queue);
        let output_queue = Arc::new(output_queue);

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.index.num_tasks)
            .into_iter()
            .map(|_| {
                tokio::spawn(Self::indexing_task(
                    work_dir.clone(),
                    storage.clone(),
                    input_queue.clone(),
                    output_queue.clone(),
                    stop_flag.clone(),
                ))
            })
            .collect();

        for handle in handles {
            handle.await?;
        }

        Ok(())
    }

    pub async fn standalone_preparation(
        work_dir: PathBuf,
        config_file_path: PathBuf,
    ) -> Result<()> {
        let config: StandalonePreparationConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let (storage, input_queue, output_queue) =
            config.into_input_output_queue_and_storage().await?;
        let storage = Arc::new(storage);
        let input_queue = Arc::new(input_queue);
        let output_queue = Arc::new(output_queue);

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.preparation.num_tasks)
            .into_iter()
            .map(|_| {
                tokio::spawn(Self::preparation_task(
                    work_dir.clone(),
                    storage.clone(),
                    input_queue.clone(),
                    output_queue.clone(),
                    stop_flag.clone(),
                ))
            })
            .collect();

        for handle in handles {
            handle.await?;
        }

        Ok(())
    }

    pub async fn standalone_search_space_generation(
        work_dir: PathBuf,
        config_file_path: PathBuf,
    ) -> Result<()> {
        let config: StandaloneSearchSpaceGenerationConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let (storage, input_queue, output_queue) =
            config.into_input_output_queue_and_storage().await?;
        let storage = Arc::new(storage);
        let input_queue = Arc::new(input_queue);
        let output_queue = Arc::new(output_queue);

        let search_space_generation_config = Arc::new(config.search_space_generation.clone());

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> =
            (0..config.search_space_generation.num_tasks)
                .into_iter()
                .map(|_| {
                    tokio::spawn(Self::search_space_generation_task(
                        work_dir.clone(),
                        search_space_generation_config.clone(),
                        storage.clone(),
                        input_queue.clone(),
                        output_queue.clone(),
                        stop_flag.clone(),
                    ))
                })
                .collect();

        for handle in handles {
            handle.await?;
        }

        Ok(())
    }

    pub async fn standalone_comet_search(
        work_dir: PathBuf,
        config_file_path: PathBuf,
    ) -> Result<()> {
        let config: StandaloneCometSearchConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let (storage, input_queue, output_queue) =
            config.into_input_output_queue_and_storage().await?;
        let storage = Arc::new(storage);
        let input_queue = Arc::new(input_queue);
        let output_queue = Arc::new(output_queue);

        let comet_search_config = Arc::new(config.comet_search.clone());

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.comet_search.num_tasks)
            .into_iter()
            .map(|_| {
                tokio::spawn(Self::comet_search_task(
                    work_dir.clone(),
                    comet_search_config.clone(),
                    storage.clone(),
                    input_queue.clone(),
                    output_queue.clone(),
                    stop_flag.clone(),
                ))
            })
            .collect();

        for handle in handles {
            handle.await?;
        }

        Ok(())
    }

    pub async fn standalone_goodness_and_rescoring(
        work_dir: PathBuf,
        config_file_path: PathBuf,
    ) -> Result<()> {
        let config: StandaloneGoodnessAndRescoringConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let (storage, input_queue, output_queue) =
            config.into_input_output_queue_and_storage().await?;
        let storage = Arc::new(storage);
        let input_queue = Arc::new(input_queue);
        let output_queue = Arc::new(output_queue);
        let stop_flag = Arc::new(AtomicBool::new(false));

        let mut signals = Signals::new(&[SIGINT])?;

        let signal_stop_flag = stop_flag.clone();
        std::thread::spawn(move || {
            for sig in signals.forever() {
                match sig {
                    SIGINT => {
                        info!("Gracefully stopping.");
                        signal_stop_flag.store(true, Ordering::Relaxed);
                    }
                    _ => (),
                }
            }
        });

        let handles: Vec<tokio::task::JoinHandle<()>> =
            (0..config.goodness_and_rescoring.num_tasks)
                .into_iter()
                .map(|_| {
                    tokio::spawn(Self::goodness_and_rescoring_task(
                        work_dir.clone(),
                        storage.clone(),
                        input_queue.clone(),
                        output_queue.clone(),
                        stop_flag.clone(),
                    ))
                })
                .collect();

        for handle in handles {
            handle.await?;
        }

        Ok(())
    }

    pub async fn standalone_cleanup(work_dir: PathBuf, config_file_path: PathBuf) -> Result<()> {
        let config: StandaloneGoodnessAndRescoringConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;
        let storage = Arc::new(RedisPipelineStorage::new(&config.storage).await?);
        let input_queue = Arc::new(RedisPipelineQueue::new(&config.cleanup).await?);

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.cleanup.num_tasks)
            .into_iter()
            .map(|_| {
                tokio::spawn(Self::cleanup_task(
                    work_dir.clone(),
                    storage.clone(),
                    input_queue.clone(),
                    stop_flag.clone(),
                ))
            })
            .collect();

        for handle in handles {
            handle.await?;
        }

        Ok(())
    }
}
