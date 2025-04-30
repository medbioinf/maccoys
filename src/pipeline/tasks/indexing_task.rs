use std::{
    fs::{self, File},
    io::BufReader,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{Context, Result};
use dihardts_omicstools::proteomics::io::mzml::{indexer::Indexer, reader::Reader as MzMlReader};
use metrics::counter;
use tracing::error;

use crate::pipeline::{
    configuration::StandaloneIndexingConfiguration,
    errors::indexing_error::IndexingError,
    messages::{
        error_message::ErrorMessage, indexing_message::IndexingMessage, is_message::IsMessage,
        search_space_generation_message::SearchSpaceGenerationMessage,
    },
    queue::{PipelineQueue, RedisPipelineQueue},
};

use super::task::Task;

/// Prefix for the indexing counter
///
pub const COUNTER_PREFIX: &str = "maccoys_indexings";

/// Task to index and split up the mzML file
///
pub struct IndexingTask;

impl IndexingTask {
    /// Start the indexing task
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    /// * `indexing_queue` - Queue for the indexing task
    /// * `search_space_generation_queue` - Queue for the search space_generation task
    /// * `error_queue` - Queue for the error task
    /// * `stop_flag` - Flag to indicate to stop once the index queue is empty
    ///
    /// # Generics
    /// * `I` - Type of the indexing queue
    /// * `S` - Type of the search space generation queue
    /// * `E` - Type of the error queue
    ///
    pub async fn start<I, S, E>(
        work_dir: PathBuf,
        indexing_queue: Arc<I>,
        search_space_generation_queue: Arc<S>,
        error_queue: Arc<E>,
        stop_flag: Arc<AtomicBool>,
    ) where
        I: PipelineQueue<IndexingMessage> + Send + Sync + 'static,
        S: PipelineQueue<SearchSpaceGenerationMessage> + Send + Sync + 'static,
        E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
    {
        'message_loop: loop {
            while let Some(message) = indexing_queue.pop().await {
                let metrics_counter_name = Self::get_counter_name(message.uuid());
                let ms_run_mzml =
                    Self::get_ms_run_mzml_path(&work_dir, message.ms_run_name(), message.uuid());

                let mut mzml_bytes_reader = match File::open(&ms_run_mzml) {
                    Ok(file) => BufReader::new(file),
                    Err(e) => {
                        let error_message =
                            message.to_error_message(IndexingError::MsRunMzMlIoError(e).into());
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                };

                let index = match Indexer::create_index(&mut mzml_bytes_reader, None) {
                    Ok(index) => index,
                    Err(e) => {
                        let error_message =
                            message.to_error_message(IndexingError::IndexCreationError(e).into());
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                };

                let index_file_path =
                    Self::get_index_path(&work_dir, message.uuid(), message.ms_run_name());
                let index_json = match index.to_json() {
                    Ok(json) => json,
                    Err(e) => {
                        let error_message =
                            message.to_error_message(IndexingError::IndexToJsonError(e).into());
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                };

                match tokio::fs::write(&index_file_path, index_json).await {
                    Ok(_) => (),
                    Err(e) => {
                        let error_message =
                            message.to_error_message(IndexingError::IndexWriteError(e).into());
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                };

                let mut mzml_file = match MzMlReader::read_pre_indexed(
                    &mut mzml_bytes_reader,
                    index,
                    None,
                    false,
                ) {
                    Ok(reader) => reader,
                    Err(e) => {
                        let error_message = message
                            .to_error_message(IndexingError::OpenMzMlWithIndexError(e).into());
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
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
                            Self::enqueue_message(error_message, error_queue.as_ref()).await;
                            continue 'spec_id_loop;
                        }
                    };

                    if spectrum.get_ms_level() != Some(2) {
                        continue 'spec_id_loop;
                    }

                    let mzml = {
                        match mzml_file.extract_spectrum(&spec_id, true) {
                            Ok(content) => content,
                            Err(e) => {
                                let error_message = message.to_error_message(
                                    IndexingError::SpectrumExtractionError(e).into(),
                                );
                                error!("{}", &error_message);
                                Self::enqueue_message(error_message, error_queue.as_ref()).await;
                                continue 'message_loop;
                            }
                        }
                    };

                    let search_space_generation_message =
                        message.into_search_space_generation_message(spec_id, mzml);

                    Self::enqueue_message(
                        search_space_generation_message,
                        search_space_generation_queue.as_ref(),
                    )
                    .await;

                    counter!(metrics_counter_name.clone()).increment(1);
                }
            }
            if stop_flag.load(Ordering::Relaxed) {
                break;
            }
            // wait before checking the queue again
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    /// Run the indexing task by itself
    ///
    /// # Arguments
    /// * `work_dir` - Working directory
    /// * `config_file_path` - Path to the configuration file
    ///
    pub async fn run_standalone(work_dir: PathBuf, config_file_path: PathBuf) -> Result<()> {
        let config: StandaloneIndexingConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let indexing_queue =
            Arc::new(RedisPipelineQueue::<IndexingMessage>::new(&config.index).await?);

        let search_space_generation_queue = Arc::new(
            RedisPipelineQueue::<SearchSpaceGenerationMessage>::new(
                &config.search_space_generation,
            )
            .await?,
        );

        let error_queue = Arc::new(RedisPipelineQueue::<ErrorMessage>::new(&config.error).await?);

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.index.num_tasks)
            .map(|_| {
                tokio::spawn(IndexingTask::start(
                    work_dir.clone(),
                    indexing_queue.clone(),
                    search_space_generation_queue.clone(),
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

impl Task for IndexingTask {
    fn get_counter_prefix() -> &'static str {
        COUNTER_PREFIX
    }
}
