use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use dihardts_omicstools::proteomics::post_translational_modifications::PostTranslationalModification;
use macpepdb::mass::convert::to_int as mass_to_int;
use metrics::counter;
use tracing::{error, info};

use crate::{
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
            is_message::IsMessage, publication_message::PublicationMessage,
            search_space_generation_message::SearchSpaceGenerationMessage,
        },
        queue::{PipelineQueue, RedisPipelineQueue},
        storage::{PipelineStorage, RedisPipelineStorage},
        utils::{create_file_path_on_precursor_level, create_metrics_file_path_on_precursor_level},
    },
    search_space::search_space_generator::{InMemorySearchSpace, SearchSpaceGenerator},
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
    pub async fn start<S, G, I, P, E>(
        config: Arc<SearchSpaceGenerationTaskConfiguration>,
        storage: Arc<S>,
        search_space_generation_queue: Arc<G>,
        identification_queue: Arc<I>,
        publication_queue: Arc<P>,
        error_queue: Arc<E>,
        stop_flag: Arc<AtomicBool>,
    ) where
        S: PipelineStorage + Send + Sync + 'static,
        G: PipelineQueue<SearchSpaceGenerationMessage> + Send + Sync + 'static,
        I: PipelineQueue<IdentificationMessage> + Send + Sync + 'static,
        P: PipelineQueue<PublicationMessage> + Send + Sync + 'static,
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
            info!(
                "[search space generation] Got message {}/{}/{} with {} precursor(s)",
                message.uuid(),
                message.ms_run_name(),
                message.spectrum_id(),
                message.precursors().len()
            );

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

            // Generate the search space for each precursor
            'precursor_loop: for precursor in message.precursors() {
                let now = std::time::Instant::now();

                let mass = mass_to_int(precursor.to_dalton());

                let mut search_space = InMemorySearchSpace::new();

                let search_space_generator = match SearchSpaceGenerator::new(
                    &config.target_url,
                    config.decoy_url.clone(),
                    config.target_lookup_url.clone(),
                    config.decoy_cache_url.clone(),
                )
                .await
                {
                    Ok(generator) => generator,
                    Err(e) => {
                        let error_message = message.to_error_message_with_precursor(
                            SearchSpaceGenerationError::FastaGenerationError(e).into(),
                            precursor.clone(),
                        );
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'precursor_loop;
                    }
                };

                match search_space_generator
                    .create(
                        &mut search_space,
                        mass,
                        current_search_params.lower_mass_tolerance_ppm,
                        current_search_params.upper_mass_tolerance_ppm,
                        current_search_params.max_variable_modifications,
                        &current_ptms,
                        current_search_params.decoys_per_peptide,
                    )
                    .await
                {
                    Ok(_) => (),
                    Err(e) => {
                        let error_message = message.to_error_message_with_precursor(
                            SearchSpaceGenerationError::FastaGenerationError(e).into(),
                            precursor.clone(),
                        );
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'precursor_loop;
                    }
                };

                let (target, decoys): (Vec<String>, Vec<String>) = search_space.into();

                let mut candidates = target;
                candidates.extend(decoys);

                let cadidates_size =
                    candidates.iter().map(|p| p.len()).sum::<usize>() * std::mem::size_of::<char>();
                let candidates_len = candidates.len();

                let elapsed = now.elapsed();

                info!(
                    "[search space generation] Took {:.4}s to process message {}/{}/{} with {} m/z & {} charge. Found {} candidate peptides",
                    elapsed.as_secs_f32(),
                    message.uuid(),
                    message.ms_run_name(),
                    message.spectrum_id(),
                    precursor.mz(),
                    precursor.charge(),
                    candidates.len(),
                );

                let identification_message =
                    message.into_identification_message(precursor.clone(), candidates);

                Self::enqueue_message(identification_message, identification_queue.as_ref()).await;
                Self::ack_message(&message_id, search_space_generation_queue.as_ref()).await;
                counter!(metrics_counter_name.clone()).increment(1);

                // sending metrics

                let metrics_path = create_metrics_file_path_on_precursor_level(
                    message.uuid(),
                    message.ms_run_name(),
                    message.spectrum_id(),
                    precursor,
                    "search_space_generation.time",
                );

                Self::enqueue_message(
                    message.into_publication_message(
                        metrics_path,
                        elapsed.as_millis().to_string().into_bytes(),
                    ),
                    publication_queue.as_ref(),
                )
                .await;

                let metrics_path = create_metrics_file_path_on_precursor_level(
                    message.uuid(),
                    message.ms_run_name(),
                    message.spectrum_id(),
                    precursor,
                    "search_space_generation.size",
                );

                Self::enqueue_message(
                    message.into_publication_message(
                        metrics_path,
                        cadidates_size.to_string().into_bytes(),
                    ),
                    publication_queue.as_ref(),
                )
                .await;

                let metrics_path = create_metrics_file_path_on_precursor_level(
                    message.uuid(),
                    message.ms_run_name(),
                    message.spectrum_id(),
                    precursor,
                    "search_space_generation.len",
                );

                Self::enqueue_message(
                    message.into_publication_message(
                        metrics_path,
                        candidates_len.to_string().into_bytes(),
                    ),
                    publication_queue.as_ref(),
                )
                .await;
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

        let publication_queue =
            Arc::new(RedisPipelineQueue::<PublicationMessage>::new(&config.publication).await?);

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
                        publication_queue.clone(),
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
