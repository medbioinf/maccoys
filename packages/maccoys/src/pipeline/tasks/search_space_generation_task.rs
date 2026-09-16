use std::{
    fs,
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use macpepdb::{
    mass::to_int as mass_to_int,
    post_translational_modification::{PTMCollection, PostTranslationalModification},
};
use metrics::counter;
use tracing::{debug, error, info};

use crate::{
    pipeline::{
        configuration::{
            SearchParameters, SearchSpaceGenerationTaskConfiguration,
            StandaloneSearchSpaceGenerationConfiguration, SEARCH_SPACE_GENERATION_QUEUE_KEY,
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
        queue::{HttpPipelineQueue, PipelineQueue},
        storage::{PipelineStorage, RedisPipelineStorage},
        utils::create_metrics_file_path_on_precursor_level,
    },
    search_space::search_space_generator::{InMemorySearchSpace, SearchSpaceGenerator},
};

use super::task::{backoff_sleep, Task, BACKOFF_START};

/// A lease held while a search-space-generation message is being processed, when
/// `max_concurrent` is configured. `Unlimited` is returned when the limiter is disabled, so
/// callers don't need to special-case the "no limit configured" case themselves.
///
pub(crate) enum SlotLease {
    Unlimited,
    Leased(String),
}

/// Prefix for the search space generation counter
///
pub const COUNTER_PREFIX: &str = "maccoys_search_space_generation";

/// Returns the metrics counter name for a search UUID. Free function mirroring
/// [`Task::get_counter_name`] for use where `SearchSpaceGenerationTask`'s generics
/// aren't in scope to infer.
///
pub fn get_counter_name(uuid: &str) -> String {
    format!("{}_{}", COUNTER_PREFIX, uuid)
}

/// Returns the Loki tracing label value for SearchSpaceGenerationTask. Free function mirroring
/// [`Task::loki_tracing_label_value`] for use where SearchSpaceGenerationTask's generics aren't
/// in scope to infer.
///
pub fn loki_tracing_label_value() -> &'static str {
    COUNTER_PREFIX
}

/// Task to generate the search space for the search engine
///
/// # Generics
/// * `S` - Type of the storage
/// * `G` - Type of the search space generation queue (this task's own queue)
/// * `I` - Type of the identification queue
/// * `P` - Type of the publication queue
/// * `E` - Type of the error queue
///
pub struct SearchSpaceGenerationTask<S, G, I, P, E>
where
    S: PipelineStorage + Send + Sync + 'static,
    G: PipelineQueue<SearchSpaceGenerationMessage> + Send + Sync + 'static,
    I: PipelineQueue<IdentificationMessage> + Send + Sync + 'static,
    P: PipelineQueue<PublicationMessage> + Send + Sync + 'static,
    E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
{
    config: Arc<SearchSpaceGenerationTaskConfiguration>,
    storage: Arc<S>,
    search_space_generation_queue: Arc<G>,
    identification_queue: Arc<I>,
    publication_queue: Arc<P>,
    error_queue: Arc<E>,
    stop_flag: Arc<AtomicBool>,
    /// UUID of the search the cached parameters/PTMs below belong to, to avoid
    /// refetching them for every message of the same search.
    last_search_uuid: String,
    current_search_params: SearchParameters,
    current_ptms: Arc<PTMCollection<Arc<PostTranslationalModification>>>,
    metrics_counter_name: String,
}

impl<S, G, I, P, E> SearchSpaceGenerationTask<S, G, I, P, E>
where
    S: PipelineStorage + Send + Sync + 'static,
    G: PipelineQueue<SearchSpaceGenerationMessage> + Send + Sync + 'static,
    I: PipelineQueue<IdentificationMessage> + Send + Sync + 'static,
    P: PipelineQueue<PublicationMessage> + Send + Sync + 'static,
    E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
{
    /// Creates a new search space generation task
    ///
    /// # Arguments
    /// * `config` - Configuration for the search space generation task
    /// * `storage` - Storage to access configuration and PTMs
    /// * `search_space_generation_queue` - Queue for the search space generation task
    /// * `identification_queue` - Queue for the identification task
    /// * `publication_queue` - Queue for the publication task
    /// * `error_queue` - Queue for the error task
    /// * `stop_flag` - Flag to indicate to stop once the search space generation queue is empty
    ///
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Arc<SearchSpaceGenerationTaskConfiguration>,
        storage: Arc<S>,
        search_space_generation_queue: Arc<G>,
        identification_queue: Arc<I>,
        publication_queue: Arc<P>,
        error_queue: Arc<E>,
        stop_flag: Arc<AtomicBool>,
    ) -> Self {
        Self {
            config,
            storage,
            search_space_generation_queue,
            identification_queue,
            publication_queue,
            error_queue,
            stop_flag,
            last_search_uuid: String::new(),
            current_search_params: SearchParameters::new(),
            current_ptms: Arc::new(PTMCollection::new(vec![]).unwrap()),
            metrics_counter_name: COUNTER_PREFIX.to_string(),
        }
    }

    /// Tries once to acquire a slot under `config.max_concurrent`. Returns
    /// `Some(SlotLease::Unlimited)` immediately (no storage round-trip) if no limit is
    /// configured. Returns `None` if the limit is configured and currently exhausted - this is
    /// routine backpressure, not an error, and callers should treat it exactly like a full
    /// downstream queue.
    ///
    pub(crate) async fn try_acquire_slot(&self) -> Option<SlotLease> {
        let max_slots = match self.config.max_concurrent {
            Some(max_slots) => max_slots,
            None => return Some(SlotLease::Unlimited),
        };

        match self
            .storage
            .try_acquire_slot(
                SEARCH_SPACE_GENERATION_QUEUE_KEY,
                max_slots.get(),
                self.config.slot_lease_timeout,
            )
            .await
        {
            Ok(Some(lease_id)) => Some(SlotLease::Leased(lease_id)),
            Ok(None) => None,
            Err(e) => {
                error!("{}", e);
                None
            }
        }
    }

    /// Releases a lease acquired via [`Self::try_acquire_slot`]. A no-op for
    /// `SlotLease::Unlimited`.
    ///
    pub(crate) async fn release_slot(&self, lease: SlotLease) {
        if let SlotLease::Leased(lease_id) = lease {
            if let Err(e) = self
                .storage
                .release_slot(SEARCH_SPACE_GENERATION_QUEUE_KEY, &lease_id)
                .await
            {
                error!("{}", e);
            }
        }
    }

    /// Same loop as [`Task::run`], except a slot under `config.max_concurrent` (if configured)
    /// is acquired before popping each message and released once it's been processed. Waiting
    /// for a slot here blocks only this task instance - each instance of this task type is
    /// always spawned as its own independent worker (never sharing a round-robin slot with
    /// other stages, unlike the grouped `processing_worker`), so blocking with backoff is fine
    /// and mirrors the existing empty-queue backoff below.
    ///
    pub(crate) async fn run_with_slot_limit(mut self)
    where
        Self: Send + 'static,
    {
        let mut pop_backoff = BACKOFF_START;
        let mut acquire_backoff = BACKOFF_START;
        'message_loop: loop {
            if self.stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }

            let lease = loop {
                match self.try_acquire_slot().await {
                    Some(lease) => {
                        acquire_backoff = BACKOFF_START;
                        break lease;
                    }
                    None => {
                        if self.stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
                            break 'message_loop;
                        }
                        acquire_backoff = backoff_sleep(acquire_backoff).await;
                    }
                }
            };

            let (message_id, message) = match self.search_space_generation_queue.pop().await {
                Ok(Some(message)) => {
                    pop_backoff = BACKOFF_START;
                    message
                }
                Ok(None) => {
                    self.release_slot(lease).await;
                    pop_backoff = BACKOFF_START;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue 'message_loop;
                }
                Err(e) => {
                    self.release_slot(lease).await;
                    error!("{}", e);
                    pop_backoff = backoff_sleep(pop_backoff).await;
                    continue 'message_loop;
                }
            };

            debug!("[{}] recv", &message_id);
            self.process_message(message_id, message).await;
            self.release_slot(lease).await;
        }
    }
}

/// Standalone entrypoint used by the `maccoys pipeline search-space-generation` CLI command;
/// always talks to the HTTP queue broker, Redis storage and Redis-backed PTM lookup, so it's
/// implemented for the one concrete instantiation rather than the generic one used by
/// [`super::super::local_pipeline`].
///
impl
    SearchSpaceGenerationTask<
        RedisPipelineStorage,
        HttpPipelineQueue<SearchSpaceGenerationMessage>,
        HttpPipelineQueue<IdentificationMessage>,
        HttpPipelineQueue<PublicationMessage>,
        HttpPipelineQueue<ErrorMessage>,
    >
{
    /// Run the search space generation task by itself
    ///
    /// # Arguments
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
            HttpPipelineQueue::<SearchSpaceGenerationMessage>::new(&config.search_space_generation)
                .await?,
        );

        let identification_queue = Arc::new(
            HttpPipelineQueue::<IdentificationMessage>::new(&config.identification).await?,
        );

        let publication_queue =
            Arc::new(HttpPipelineQueue::<PublicationMessage>::new(&config.publication).await?);

        let error_queue = Arc::new(HttpPipelineQueue::<ErrorMessage>::new(&config.error).await?);

        let storage = Arc::new(RedisPipelineStorage::new(&config.storage).await?);

        let search_space_generation_config = Arc::new(config.search_space_generation.clone());

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> =
            (0..config.search_space_generation.num_tasks)
                .map(|_| {
                    tokio::spawn(
                        SearchSpaceGenerationTask::new(
                            search_space_generation_config.clone(),
                            storage.clone(),
                            search_space_generation_queue.clone(),
                            identification_queue.clone(),
                            publication_queue.clone(),
                            error_queue.clone(),
                            stop_flag.clone(),
                        )
                        .run_with_slot_limit(),
                    )
                })
                .collect();

        for handle in handles {
            handle.await?;
        }

        Ok(())
    }
}

impl<S, G, I, P, E> Task for SearchSpaceGenerationTask<S, G, I, P, E>
where
    S: PipelineStorage + Send + Sync + 'static,
    G: PipelineQueue<SearchSpaceGenerationMessage> + Send + Sync + 'static,
    I: PipelineQueue<IdentificationMessage> + Send + Sync + 'static,
    P: PipelineQueue<PublicationMessage> + Send + Sync + 'static,
    E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
{
    type Message = SearchSpaceGenerationMessage;
    type Queue = G;

    fn get_counter_prefix() -> &'static str {
        COUNTER_PREFIX
    }

    fn queue(&self) -> &Arc<G> {
        &self.search_space_generation_queue
    }

    fn stop_flag(&self) -> &Arc<AtomicBool> {
        &self.stop_flag
    }

    async fn process_message(&mut self, message_id: String, message: SearchSpaceGenerationMessage) {
        info!(
            "[search space generation] Got message {}/{}/{} with {} precursor(s)",
            message.uuid(),
            message.ms_run_name(),
            message.spectrum_id(),
            message.precursors().len()
        );

        if self.last_search_uuid != *message.uuid() {
            self.current_search_params =
                match self.storage.get_search_parameters(message.uuid()).await {
                    Ok(Some(params)) => params,
                    Ok(None) => {
                        let error_message = message.to_error_message(
                            SearchSpaceGenerationError::SearchParametersNotFoundError().into(),
                        );
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                        return;
                    }
                    Err(e) => {
                        let error_message = message.to_error_message(
                            SearchSpaceGenerationError::GetSearchParametersError(e).into(),
                        );
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                        return;
                    }
                };

            let ptms = match self.storage.get_ptms(message.uuid()).await {
                Ok(Some(ptms)) => ptms,
                Ok(None) => {
                    let error_message = message
                        .to_error_message(SearchSpaceGenerationError::PTMsNotFoundError().into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                    return;
                }
                Err(e) => {
                    let error_message = message
                        .to_error_message(SearchSpaceGenerationError::GetPTMsError(e).into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                    return;
                }
            };

            self.current_ptms = match PTMCollection::new(ptms) {
                Ok(ptms) => Arc::new(ptms),
                Err(err) => {
                    let error_message = message.to_error_message(
                        SearchSpaceGenerationError::PTMCollection(Box::new(err)).into(),
                    );
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                    return;
                }
            };

            self.last_search_uuid = message.uuid().clone();
            self.metrics_counter_name = Self::get_counter_name(message.uuid());
        }

        let current_search_params = self.current_search_params.clone();
        let current_ptms = self.current_ptms.clone();
        let metrics_counter_name = self.metrics_counter_name.clone();

        // Generate the search space for each precursor
        'precursor_loop: for precursor in message.precursors() {
            let now = std::time::Instant::now();

            let mass = mass_to_int(precursor.to_dalton());

            let mut search_space = InMemorySearchSpace::new();

            let search_space_generator = match SearchSpaceGenerator::new(
                &self.config.target_url,
                self.config.decoy_url.clone(),
                self.config.target_lookup_url.clone(),
            )
            .await
            {
                Ok(generator) => generator,
                Err(err) => {
                    let error_message = message.to_error_message_with_precursor(
                        SearchSpaceGenerationError::from(err).into(),
                        precursor.clone(),
                    );
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
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
                    current_ptms.clone(),
                    current_search_params.decoys_per_peptide,
                    self.config.concurrent_searches,
                )
                .await
            {
                Ok(_) => (),
                Err(err) => {
                    let error_message = message.to_error_message_with_precursor(
                        SearchSpaceGenerationError::from(err).into(),
                        precursor.clone(),
                    );
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                    continue 'precursor_loop;
                }
            };

            let elapsed = now.elapsed();

            let search_space_size = search_space.size();
            let search_space_len = search_space.len();

            info!(
                "[search space generation] Took {:.4}s to process message {}/{}/{} with {} m/z & {} charge. Found {} candidate peptides",
                elapsed.as_secs_f32(),
                message.uuid(),
                message.ms_run_name(),
                message.spectrum_id(),
                precursor.mz(),
                precursor.charge(),
                search_space.len(),
            );

            let identification_message =
                message.into_identification_message(precursor.clone(), search_space);

            Self::enqueue_message(identification_message, self.identification_queue.as_ref()).await;
            Self::ack_message(&message_id, self.search_space_generation_queue.as_ref()).await;
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
                self.publication_queue.as_ref(),
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
                    search_space_size.to_string().into_bytes(),
                ),
                self.publication_queue.as_ref(),
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
                    search_space_len.to_string().into_bytes(),
                ),
                self.publication_queue.as_ref(),
            )
            .await;
        }
    }
}
