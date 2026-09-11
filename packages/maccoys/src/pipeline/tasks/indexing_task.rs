use std::{
    fs::{self, File},
    io::BufReader,
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
};

use dihardts_omicstools::proteomics::io::mzml::{
    elements::{has_cv_params::HasCvParams, is_list::IsList},
    indexer::Indexer,
    reader::Reader as MzMlReader,
};
use metrics::counter;
use tracing::{debug, error};

use crate::{
    pipeline::{
        configuration::StandaloneIndexingConfiguration,
        errors::{indexing_error::IndexingError, pipeline_error::PipelineError},
        messages::{
            error_message::ErrorMessage, indexing_message::IndexingMessage, is_message::IsMessage,
            search_space_generation_message::SearchSpaceGenerationMessage,
        },
        queue::{HttpPipelineQueue, PipelineQueue},
        storage::{PipelineStorage, RedisPipelineStorage},
        utils::{get_ms_run_index_path, get_ms_run_mzml_path},
    },
    precursor::Precursor,
};

use super::task::Task;

/// Prefix for the indexing counter
///
pub const COUNTER_PREFIX: &str = "maccoys_indexings";

/// Returns the metrics counter name for a search UUID. Free function mirroring
/// [`Task::get_counter_name`] for use where `IndexingTask`'s queue/storage generics
/// aren't in scope to infer.
///
pub fn get_counter_name(uuid: &str) -> String {
    format!("{}_{}", COUNTER_PREFIX, uuid)
}

/// Returns the Loki tracing label value for IndexingTask. Free function mirroring
/// [`Task::loki_tracing_label_value`] for use where IndexingTask's generics aren't
/// in scope to infer.
///
pub fn loki_tracing_label_value() -> &'static str {
    COUNTER_PREFIX
}

/// Task to index and split up the mzML file
///
/// # Generics
/// * `Q` - Type of the indexing queue (this task's own queue)
/// * `S` - Type of the search space generation queue
/// * `E` - Type of the error queue
/// * `T` - Type of the storage
///
pub struct IndexingTask<Q, S, E, T>
where
    Q: PipelineQueue<IndexingMessage> + Send + Sync + 'static,
    S: PipelineQueue<SearchSpaceGenerationMessage> + Send + Sync + 'static,
    E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
    T: PipelineStorage + Send + Sync + 'static,
{
    work_dir: PathBuf,
    indexing_queue: Arc<Q>,
    search_space_generation_queue: Arc<S>,
    error_queue: Arc<E>,
    storage: Arc<T>,
    stop_flag: Arc<AtomicBool>,
}

impl<Q, S, E, T> IndexingTask<Q, S, E, T>
where
    Q: PipelineQueue<IndexingMessage> + Send + Sync + 'static,
    S: PipelineQueue<SearchSpaceGenerationMessage> + Send + Sync + 'static,
    E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
    T: PipelineStorage + Send + Sync + 'static,
{
    /// Creates a new indexing task
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    /// * `indexing_queue` - Queue for the indexing task
    /// * `search_space_generation_queue` - Queue for the search space_generation task
    /// * `error_queue` - Queue for the error task
    /// * `storage` - Storage for the indexing task
    /// * `stop_flag` - Flag to indicate to stop once the index queue is empty
    ///
    pub fn new(
        work_dir: PathBuf,
        indexing_queue: Arc<Q>,
        search_space_generation_queue: Arc<S>,
        error_queue: Arc<E>,
        storage: Arc<T>,
        stop_flag: Arc<AtomicBool>,
    ) -> Self {
        Self {
            work_dir,
            indexing_queue,
            search_space_generation_queue,
            error_queue,
            storage,
            stop_flag,
        }
    }
}

/// Standalone entrypoint used by the `maccoys pipeline index` CLI command; always talks to
/// the HTTP queue broker and Redis storage, so it's implemented for the one concrete
/// instantiation of `IndexingTask` rather than the generic one used by [`super::super::local_pipeline`].
///
impl
    IndexingTask<
        HttpPipelineQueue<IndexingMessage>,
        HttpPipelineQueue<SearchSpaceGenerationMessage>,
        HttpPipelineQueue<ErrorMessage>,
        RedisPipelineStorage,
    >
{
    /// Run the indexing task by itself
    ///
    /// # Arguments
    /// * `config_file_path` - Path to the configuration file
    ///
    pub async fn run_standalone(config_file_path: PathBuf) -> Result<(), PipelineError> {
        let config = &fs::read_to_string(&config_file_path).map_err(|err| {
            PipelineError::FileReadError(config_file_path.to_string_lossy().to_string(), err)
        })?;

        let config: StandaloneIndexingConfiguration = toml::from_str(config).map_err(|err| {
            PipelineError::ConfigDeserializationError(
                config_file_path.to_string_lossy().to_string(),
                err,
            )
        })?;

        let indexing_queue =
            Arc::new(HttpPipelineQueue::<IndexingMessage>::new(&config.index).await?);

        let search_space_generation_queue = Arc::new(
            HttpPipelineQueue::<SearchSpaceGenerationMessage>::new(&config.search_space_generation)
                .await?,
        );

        let error_queue = Arc::new(HttpPipelineQueue::<ErrorMessage>::new(&config.error).await?);

        let storage = Arc::new(RedisPipelineStorage::new(&config.storage).await?);

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.index.num_tasks)
            .map(|_| {
                tokio::spawn(
                    IndexingTask::new(
                        config.work_directory.clone(),
                        indexing_queue.clone(),
                        search_space_generation_queue.clone(),
                        error_queue.clone(),
                        storage.clone(),
                        stop_flag.clone(),
                    )
                    .run(),
                )
            })
            .collect();

        for handle in handles {
            handle.await?;
        }

        Ok(())
    }
}

impl<Q, S, E, T> Task for IndexingTask<Q, S, E, T>
where
    Q: PipelineQueue<IndexingMessage> + Send + Sync + 'static,
    S: PipelineQueue<SearchSpaceGenerationMessage> + Send + Sync + 'static,
    E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
    T: PipelineStorage + Send + Sync + 'static,
{
    type Message = IndexingMessage;
    type Queue = Q;

    fn get_counter_prefix() -> &'static str {
        COUNTER_PREFIX
    }

    fn queue(&self) -> &Arc<Q> {
        &self.indexing_queue
    }

    fn stop_flag(&self) -> &Arc<AtomicBool> {
        &self.stop_flag
    }

    async fn process_message(&mut self, message_id: String, message: IndexingMessage) {
        let search_params = match self.storage.get_search_parameters(message.uuid()).await {
            Ok(Some(params)) => params,
            Ok(None) => {
                let error_message =
                    message.to_error_message(IndexingError::SearchParametersNotFoundError().into());
                error!("{}", &error_message);
                Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                return;
            }
            Err(e) => {
                let error_message =
                    message.to_error_message(IndexingError::GetSearchParametersError(e).into());
                error!("{}", &error_message);
                Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                return;
            }
        };

        let metrics_counter_name = Self::get_counter_name(message.uuid());

        let ms_run_names = message.ms_run_names().clone();

        for ms_run_name in ms_run_names.into_iter() {
            let relative_ms_run_mzml_path = get_ms_run_mzml_path(message.uuid(), &ms_run_name);
            let absolulte_ms_run_mzml_path = self.work_dir.join(&relative_ms_run_mzml_path);

            let mut mzml_bytes_reader = match File::open(&absolulte_ms_run_mzml_path) {
                Ok(file) => BufReader::new(file),
                Err(e) => {
                    let error_message =
                        message.to_error_message(IndexingError::MsRunMzMlIoError(e).into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                    return;
                }
            };

            let index = match Indexer::create_index(&mut mzml_bytes_reader, None) {
                Ok(index) => index,
                Err(e) => {
                    let error_message =
                        message.to_error_message(IndexingError::IndexCreationError(e).into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                    return;
                }
            };

            debug!("[{}] spec ctr {}", &message_id, index.get_spectra().len());

            let relative_index_file_path = get_ms_run_index_path(message.uuid(), &ms_run_name);
            let absolute_index_file_path = self.work_dir.join(&relative_index_file_path);

            let index_json = match index.to_json() {
                Ok(json) => json,
                Err(e) => {
                    let error_message =
                        message.to_error_message(IndexingError::IndexToJsonError(e).into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                    return;
                }
            };

            match tokio::fs::write(&absolute_index_file_path, index_json).await {
                Ok(_) => (),
                Err(e) => {
                    let error_message =
                        message.to_error_message(IndexingError::IndexWriteError(e).into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                    return;
                }
            };

            let mut mzml_file =
                match MzMlReader::read_pre_indexed(&mut mzml_bytes_reader, index, None, false) {
                    Ok(reader) => reader,
                    Err(e) => {
                        let error_message = message
                            .to_error_message(IndexingError::OpenMzMlWithIndexError(e).into());
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                        return;
                    }
                };

            let spec_ids = mzml_file
                .get_index()
                .get_spectra()
                .keys()
                .cloned()
                .collect::<Vec<_>>();

            'spec_id_loop: for spec_id in spec_ids.into_iter() {
                let spectrum = match mzml_file.get_spectrum(&spec_id) {
                    Ok(spectrum) => spectrum,
                    Err(e) => {
                        let error_message = message.to_error_message(
                            IndexingError::SpectrumReadError(spec_id.clone(), e).into(),
                        );
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                        continue 'spec_id_loop;
                    }
                };

                if spectrum.get_ms_level() != Some(2) {
                    continue 'spec_id_loop;
                }

                // This increases the total spectrum count in the storage by one.
                // The speach space generation task will increase the count further if the spectrum
                // has multiple precursors or charge states.
                match self
                    .storage
                    .increase_total_spectrum_count(message.uuid())
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        let error_message = message.to_error_message(e.into());
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                        return;
                    }
                }

                // Collecting precursors (mz, charge)
                let precursors = match spectrum.precursor_list.as_ref() {
                    Some(list) => list.iter().collect::<Vec<_>>(),
                    None => {
                        let error_message = message
                            .to_error_message(IndexingError::NoPrecursorListError(spec_id).into());
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                        return; // as this was the only MS2 spectrum we can continue with the next message
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
                    let error_message =
                        message.to_error_message(IndexingError::NoSelectedIonError(spec_id).into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                    return; // as this was the only MS2 spectrum we can continue with the next message
                };

                let precursors_result: Result<Vec<Vec<Precursor>>, IndexingError> = ions
                    .iter()
                    .map(|ion| {
                        let mz: f64 = ion
                            .get_cv_param("MS:1000744")
                            .first()
                            .ok_or_else(|| {
                                IndexingError::MissingSelectedIonMzError(spec_id.clone())
                            })?
                            .value
                            .parse()
                            .map_err(|err| IndexingError::MzParsingError(spec_id.clone(), err))?;
                        // Select charge states statess
                        let mut charge_cv_params = ion.get_cv_param("MS:1000041");
                        // add possible charge states
                        charge_cv_params.extend(ion.get_cv_param("MS:1000633"));

                        let charges: Vec<u8> = if !charge_cv_params.is_empty() {
                            charge_cv_params
                                .into_iter()
                                .map(|x| {
                                    x.value.parse().map_err(|err| {
                                        IndexingError::ChargeParsingError(spec_id.clone(), err)
                                    })
                                })
                                .collect::<Result<Vec<u8>, IndexingError>>()?
                        } else {
                            (2..=search_params.max_charge).collect()
                        };

                        Ok::<_, IndexingError>(
                            charges
                                .into_iter()
                                .map(|charge| Precursor::new(mz, charge))
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
                        Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                        return; // as this was the only MS2 spectrum we can continue with the next message
                    }
                };

                let precursors = precursors.into_iter().flatten().collect::<Vec<_>>();

                if precursors.is_empty() {
                    let error_message =
                        message.to_error_message(IndexingError::NoSelectedIonError(spec_id).into());
                    error!("{}", &error_message);
                    Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                    return; // as this was the only MS2 spectrum we can continue with the next message
                }

                let search_space_generation_message = match message
                    .into_search_space_generation_message(
                        ms_run_name.clone(),
                        spec_id,
                        spectrum,
                        precursors,
                    ) {
                    Ok(msg) => msg,
                    Err(e) => {
                        let error_message = message.to_error_message(e.into());
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                        return;
                    }
                };

                Self::enqueue_message(
                    search_space_generation_message,
                    self.search_space_generation_queue.as_ref(),
                )
                .await;
            }
        }

        match self.storage.set_search_fully_enqueued(message.uuid()).await {
            Ok(_) => {}
            Err(e) => {
                let error_message = message.to_error_message(e.into());
                error!("{}", &error_message);
                Self::enqueue_message(error_message, self.error_queue.as_ref()).await;
                return;
            }
        }
        Self::ack_message(&message_id, self.indexing_queue.as_ref()).await;
        debug!("[{}] ack", &message_id);
        counter!(metrics_counter_name.clone()).increment(1);
    }
}
