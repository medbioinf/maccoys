use std::{
    fs,
    io::Cursor,
    path::PathBuf,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    vec,
};

use dihardts_omicstools::{
    mass_spectrometry::unit_conversions::mass_to_charge_to_dalton,
    proteomics::{
        io::mzml::{
            elements::{has_cv_params::HasCvParams, is_list::IsList},
            reader::Reader,
        },
        post_translational_modifications::PostTranslationalModification,
    },
};
use macpepdb::mass::convert::to_int as mass_to_int;
use metrics::counter;
use tokio::io::AsyncWriteExt;
use tracing::error;

use crate::{
    functions::create_search_space,
    pipeline::{
        configuration::{
            SearchParameters, SearchSpaceGenerationTaskConfiguration,
            StandaloneSearchSpaceGenerationConfiguration,
        },
        errors::{
            pipeline_error::PipelineError,
            search_space_generation_error::SearchSpaceGenerationError,
        },
        messages::{
            error_message::ErrorMessage, identification_message::IdentificationMessage,
            is_message::IsMessage, search_space_generation_message::SearchSpaceGenerationMessage,
        },
        queue::{PipelineQueue, RedisPipelineQueue},
        storage::{PipelineStorage, RedisPipelineStorage},
    },
};

use super::task::Task;

/// Prefix for the search space generation counter
///
pub const COUNTER_PREFIX: &str = "maccoys_search_space_generation";

/// Task to generate the search space for the search engine
///
pub struct SearchSpaceGenerationTask;

impl SearchSpaceGenerationTask {
    /// Start the indexing task
    ///
    /// # Arguments
    /// * `config` - Configuration for the search space generation task
    /// * `search_space_generation_queue` - Queue for the search space generation task
    /// * `comet_search_queue` - Queue for the Comet search task
    /// * `storage` - Storage to access configuration and PTMs
    /// * `stop_flag` - Flag to indicate to stop once the search space generation queue is empty
    ///
    /// # Generics
    /// * `S` - Type of the storage
    /// * `G` - Type of the search space generation queue
    /// * `I` - Type of the identification queue
    /// * `E` - Type of the error queue
    ///
    pub async fn start<S, G, I, E>(
        config: Arc<SearchSpaceGenerationTaskConfiguration>,
        storage: Arc<S>,
        search_space_generation_queue: Arc<G>,
        identification_queue: Arc<I>,
        error_queue: Arc<E>,
        stop_flag: Arc<AtomicBool>,
    ) where
        S: PipelineStorage + Send + Sync + 'static,
        G: PipelineQueue<SearchSpaceGenerationMessage> + Send + Sync + 'static,
        I: PipelineQueue<IdentificationMessage> + Send + Sync + 'static,
        E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
    {
        let mut last_search_uuid = String::new();
        let mut current_search_params = SearchParameters::new();
        let mut current_ptms: Vec<PostTranslationalModification> = Vec::new();
        let mut metrics_counter_name = COUNTER_PREFIX.to_string();

        'message_loop: loop {
            if stop_flag.load(Ordering::Relaxed) {
                break;
            }
            let (message_id, message) = match search_space_generation_queue.pop().await {
                Ok(Some(message)) => message,
                Ok(None) => {
                    // If the queue is empty, wait for a while before checking again
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    continue 'message_loop;
                }
                Err(e) => {
                    error!("{}", e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    continue 'message_loop;
                }
            };
            if last_search_uuid != *message.uuid() {
                current_search_params = match storage.get_search_parameters(message.uuid()).await {
                    Ok(Some(params)) => params,
                    Ok(None) => {
                        let error_message = message.to_error_message(
                            SearchSpaceGenerationError::SearchParametersNotFoundError().into(),
                        );
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                    Err(e) => {
                        let error_message = message.to_error_message(
                            SearchSpaceGenerationError::GetSearchParametersError(e).into(),
                        );
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                };

                current_ptms = match storage.get_ptms(message.uuid()).await {
                    Ok(Some(ptms)) => ptms,
                    Ok(None) => {
                        let error_message = message.to_error_message(
                            SearchSpaceGenerationError::PTMsNotFoundError().into(),
                        );
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                    Err(e) => {
                        let error_message = message
                            .to_error_message(SearchSpaceGenerationError::GetPTMsError(e).into());
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                };

                last_search_uuid = message.uuid().clone();
                metrics_counter_name = Self::get_counter_name(message.uuid());
            }

            let mut mzml_bytes_reader = Cursor::new(message.mzml());

            let mut mzml_file =
                match Reader::read_indexed(&mut mzml_bytes_reader, None, true, false) {
                    Ok(file) => file,
                    Err(e) => {
                        let error_message = message.to_error_message(
                            SearchSpaceGenerationError::ReadingSpectrumMzMlError(e).into(),
                        );
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                };

            let spectrum = match mzml_file.get_spectrum(message.spectrum_id()) {
                Ok(spectrum) => spectrum,
                Err(e) => {
                    let error_message = message.to_error_message(
                        SearchSpaceGenerationError::SpectrumReadError(
                            message.spectrum_id().clone(),
                            e,
                        )
                        .into(),
                    );
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, error_queue.as_ref()).await;
                    continue 'message_loop;
                }
            };

            // Free up some memory and release borrowed message
            drop(mzml_file);

            // Get all the m/z / charge pairs from the spectrum
            let precursors = match spectrum.precursor_list.as_ref() {
                Some(list) => list.iter().collect::<Vec<_>>(),
                None => {
                    let error_message = message.to_error_message(
                        SearchSpaceGenerationError::NoPrecursorListError(
                            message.spectrum_id().clone(),
                        )
                        .into(),
                    );
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, error_queue.as_ref()).await;
                    continue 'message_loop; // as this was the only MS2 spectrum we can continue with the next message
                }
            };

            let ions = precursors
                .iter()
                .flat_map(|precursor| match precursor.selected_ion_list.as_ref() {
                    Some(list) => list.iter().collect::<Vec<_>>(),
                    None => vec![],
                })
                .collect::<Vec<_>>();

            if ions.is_empty() {
                let error_message = message.to_error_message(
                    SearchSpaceGenerationError::NoSelectedIonError(message.spectrum_id().clone())
                        .into(),
                );
                error!("{}", &error_message);
                Self::enqueue_message(error_message, error_queue.as_ref()).await;
                continue 'message_loop; // as this was the only MS2 spectrum we can continue with the next message
            };

            let precursors_result: Result<Vec<Vec<(f64, u8)>>, SearchSpaceGenerationError> = ions
                .iter()
                .map(|ion| {
                    let mz: f64 = ion
                        .get_cv_param("MS:1000744")
                        .first()
                        .ok_or_else(|| {
                            SearchSpaceGenerationError::MissingSelectedIonMzError(
                                message.spectrum_id().clone(),
                            )
                        })?
                        .value
                        .parse()
                        .map_err(|err| {
                            SearchSpaceGenerationError::MzParsingError(
                                message.spectrum_id().clone(),
                                err,
                            )
                        })?;
                    // Select charge states statess
                    let mut charge_cv_params = ion.get_cv_param("MS:1000041");
                    // add possible charge states
                    charge_cv_params.extend(ion.get_cv_param("MS:1000633"));

                    let charges: Vec<u8> = if !charge_cv_params.is_empty() {
                        charge_cv_params
                            .into_iter()
                            .map(|x| {
                                x.value.parse().map_err(|err| {
                                    SearchSpaceGenerationError::ChargeParsingError(
                                        message.spectrum_id().clone(),
                                        err,
                                    )
                                })
                            })
                            .collect::<Result<Vec<u8>, SearchSpaceGenerationError>>()?
                    } else {
                        (2..=current_search_params.max_charge).collect()
                    };

                    Ok::<_, SearchSpaceGenerationError>(
                        charges
                            .into_iter()
                            .map(|charge| (mz, charge))
                            .collect::<Vec<_>>(),
                    )
                })
                .collect();

            // Check if everything was ok
            let precursors = match precursors_result {
                Ok(precursors) => precursors,
                Err(e) => {
                    let error_message = message.to_error_message(e.into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, error_queue.as_ref()).await;
                    continue 'message_loop; // as this was the only MS2 spectrum we can continue with the next message
                }
            };

            let precursors = precursors.into_iter().flatten().collect::<Vec<_>>();

            if precursors.is_empty() {
                let error_message = message.to_error_message(
                    SearchSpaceGenerationError::NoSelectedIonError(message.spectrum_id().clone())
                        .into(),
                );
                error!("{}", &error_message);
                Self::enqueue_message(error_message, error_queue.as_ref()).await;
                continue 'message_loop; // as this was the only MS2 spectrum we can continue with the next message
            }

            // Generate the search space for each precursor
            'precursor_loop: for (precursor_mz, precursor_charge) in precursors.into_iter() {
                let mass = mass_to_int(mass_to_charge_to_dalton(precursor_mz, precursor_charge));

                let mut fasta = Box::pin(Cursor::new(Vec::new()));

                match create_search_space(
                    &mut fasta,
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
                        let error_message = message.to_error_message_with_precursor(
                            SearchSpaceGenerationError::FastaGenerationError(e).into(),
                            (precursor_mz, precursor_charge),
                        );
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'precursor_loop;
                    }
                };

                match fasta.flush().await {
                    Ok(_) => (),
                    Err(e) => {
                        let error_message = message.to_error_message_with_precursor(
                            SearchSpaceGenerationError::FastaFlushError(e).into(),
                            (precursor_mz, precursor_charge),
                        );
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'precursor_loop;
                    }
                };
                fasta.set_position(0);

                // Get the fasta content
                let fasta = Pin::into_inner(fasta).into_inner();

                let identification_message =
                    message.into_identification_message(fasta, (precursor_mz, precursor_charge));

                Self::enqueue_message(identification_message, identification_queue.as_ref()).await;
                Self::ack_message(&message_id, search_space_generation_queue.as_ref()).await;
                counter!(metrics_counter_name.clone()).increment(1);
            }
        }
        // wait before checking the queue again
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    /// Run the search space generation task by itself
    ///
    /// # Arguments
    /// * `work_dir` - Working directory
    /// * `config_file_path` - Path to the configuration file
    ///
    pub async fn run_standalone(config_file_path: PathBuf) -> Result<(), PipelineError> {
        let config = &fs::read_to_string(&config_file_path).map_err(|err| {
            PipelineError::FileReadError(config_file_path.to_string_lossy().to_string(), err)
        })?;

        let config: StandaloneSearchSpaceGenerationConfiguration =
            toml::from_str(config).map_err(|err| {
                PipelineError::ConfigDeserializationError(
                    config_file_path.to_string_lossy().to_string(),
                    err,
                )
            })?;

        let search_space_generation_queue = Arc::new(
            RedisPipelineQueue::<SearchSpaceGenerationMessage>::new(
                &config.search_space_generation,
            )
            .await?,
        );

        let identification_queue = Arc::new(
            RedisPipelineQueue::<IdentificationMessage>::new(&config.identification).await?,
        );

        let error_queue = Arc::new(RedisPipelineQueue::<ErrorMessage>::new(&config.error).await?);

        let storage = Arc::new(RedisPipelineStorage::new(&config.storage).await?);

        let search_space_generation_config = Arc::new(config.search_space_generation.clone());

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> =
            (0..config.search_space_generation.num_tasks)
                .map(|_| {
                    tokio::spawn(SearchSpaceGenerationTask::start(
                        search_space_generation_config.clone(),
                        storage.clone(),
                        search_space_generation_queue.clone(),
                        identification_queue.clone(),
                        error_queue.clone(),
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

impl Task for SearchSpaceGenerationTask {
    fn get_counter_prefix() -> &'static str {
        COUNTER_PREFIX
    }
}
