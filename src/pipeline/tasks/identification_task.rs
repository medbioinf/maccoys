use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{Context, Result};
use metrics::counter;
use polars::frame::DataFrame;
use tokio::fs::create_dir_all;
use tracing::error;

use crate::{
    functions::run_comet_search,
    io::comet::{
        configuration::Configuration as CometConfiguration,
        peptide_spectrum_match_tsv::PeptideSpectrumMatchTsv,
    },
    pipeline::{
        configuration::{
            IdentificationTaskConfiguration, SearchParameters,
            StandaloneIdentificationConfiguration,
        },
        errors::identification_error::IdentificationError,
        messages::{
            error_message::ErrorMessage, identification_message::IdentificationMessage,
            is_message::IsMessage, publication_message::PublicationMessage,
            scoring_message::ScoringMessage,
        },
        queue::{PipelineQueue, RedisPipelineQueue},
        storage::{PipelineStorage, RedisPipelineStorage},
        utils::create_file_path_on_precursor_level,
    },
};

use super::task::Task;

/// Prefix for the identification counter
///
pub const COUNTER_PREFIX: &str = "maccoys_identifications";

/// Task to identify the spectra
///
pub struct IdentificationTask;

impl IdentificationTask {
    /// Starts the task
    ///
    /// # Arguments
    /// * `local_work_dir` -  Local work directory
    /// * `config` - Configuration for the Comet search task
    /// * `storage` - Storage to access params and PTMs
    /// * `identification_queue` - Incomming queue for the identification task
    /// * `scoring_queue` - Scoring queue
    /// * `publication_queue` - Publication queue for persisting intermediate results if needed
    /// * `error_queue` - Queue for the error task
    /// * `stop_flag` - Flag to indicate to stop once the Comet search queue is empty
    ///
    #[allow(clippy::too_many_arguments)]
    pub async fn start<S, I, C, P, E>(
        local_work_dir: PathBuf,
        config: Arc<IdentificationTaskConfiguration>,
        storage: Arc<S>,
        identification_queue: Arc<I>,
        scoring_queue: Arc<C>,
        publication_queue: Arc<P>,
        error_queue: Arc<E>,
        stop_flag: Arc<AtomicBool>,
    ) where
        S: PipelineStorage + Send + Sync + 'static,
        I: PipelineQueue<IdentificationMessage> + Send + Sync + 'static,
        C: PipelineQueue<ScoringMessage> + Send + Sync + 'static,
        P: PipelineQueue<PublicationMessage> + Send + Sync + 'static,
        E: PipelineQueue<ErrorMessage> + Send + Sync + 'static,
    {
        match create_dir_all(&local_work_dir).await {
            Ok(_) => (),
            Err(e) => {
                error!("Error creating local work directory: {:?}", e);
                return;
            }
        }
        let mut last_search_uuid = String::new();
        let mut current_comet_config: Option<CometConfiguration> = None;
        let mut current_search_params = SearchParameters::new();
        let mut metrics_counter_name = COUNTER_PREFIX.to_string();

        let comet_params_file_path = local_work_dir.join("comet.params");
        let fasta_file_path = local_work_dir.join("search_space.fasta");
        let psms_file_path = local_work_dir.join("psms.txt");
        let mzml_file_path = local_work_dir.join("ms_run.mzML");

        'message_loop: loop {
            while let Some(mut message) = identification_queue.pop().await {
                if last_search_uuid != *message.uuid() {
                    current_comet_config = match storage.get_comet_config(message.uuid()).await {
                        Ok(Some(config)) => Some(config),
                        Ok(None) => {
                            let error_message = message.to_error_message(
                                IdentificationError::NoCometCongigurationInStorageError().into(),
                            );
                            error!("{}", &error_message);
                            Self::enqueue_message(error_message, error_queue.as_ref()).await;
                            continue 'message_loop;
                        }
                        Err(e) => {
                            let error_message = message.to_error_message(
                                IdentificationError::UnableToGetCometConfigurationFromStorageError(
                                    e,
                                )
                                .into(),
                            );
                            error!("{}", &error_message);
                            Self::enqueue_message(error_message, error_queue.as_ref()).await;
                            continue 'message_loop;
                        }
                    };

                    match current_comet_config
                        .as_mut()
                        .unwrap()
                        .set_option("threads", &format!("{}", config.threads))
                    {
                        Ok(_) => (),
                        Err(e) => {
                            let error_message = message.to_error_message(
                                IdentificationError::UnableToSetCometConfigurationAttributeError(
                                    "threads".to_string(),
                                    e,
                                )
                                .into(),
                            );
                            error!("{}", &error_message);
                            Self::enqueue_message(error_message, error_queue.as_ref()).await;
                            continue 'message_loop;
                        }
                    }

                    current_search_params =
                        match storage.get_search_parameters(message.uuid()).await {
                            Ok(Some(params)) => params,
                            Ok(None) => {
                                let error_message = message.to_error_message(
                                    IdentificationError::SearchParametersNotFoundError().into(),
                                );
                                error!("{}", &error_message);
                                Self::enqueue_message(error_message, error_queue.as_ref()).await;
                                continue 'message_loop;
                            }
                            Err(e) => {
                                let error_message = message.to_error_message(
                                    IdentificationError::GetSearchParametersError(e).into(),
                                );
                                error!("{}", &error_message);
                                Self::enqueue_message(error_message, error_queue.as_ref()).await;
                                continue 'message_loop;
                            }
                        };

                    last_search_uuid = message.uuid().clone();
                    metrics_counter_name = Self::get_counter_name(message.uuid());
                }

                // Unwrap the current Comet configuration for easier access
                let comet_config = current_comet_config.as_mut().unwrap();

                match message.write_spectrum_mzml_file(&mzml_file_path).await {
                    Ok(_) => (),
                    Err(e) => {
                        let error_message = message.to_error_message(
                            IdentificationError::WriteSpectrumMzMLFileError(e).into(),
                        );
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                }

                if current_search_params.keep_fasta_files {
                    let relative_fasta_path = create_file_path_on_precursor_level(
                        message.uuid(),
                        message.ms_run_name(),
                        message.spectrum_id(),
                        message.precursor(),
                        "fasta",
                    );
                    let fasta_publication_message = message
                        .into_publication_message(relative_fasta_path, message.fasta().clone());
                    Self::enqueue_message(fasta_publication_message, publication_queue.as_ref())
                        .await;
                }

                match message.write_fasta_file(&mzml_file_path).await {
                    Ok(_) => (),
                    Err(e) => {
                        let error_message = message
                            .to_error_message(IdentificationError::WriteFastaFileError(e).into());
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                }

                match comet_config.set_charge(message.precursor().1) {
                    Ok(_) => (),
                    Err(e) => {
                        let error_message = message.to_error_message(
                            IdentificationError::CometConfigAttributeError(
                                "precursor_charge, max_fragment_charge & max_precursor_charge"
                                    .to_string(),
                                e,
                            )
                            .into(),
                        );
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                }

                match comet_config.set_num_results(10000) {
                    Ok(_) => (),
                    Err(e) => {
                        let error_message = message.to_error_message(
                            IdentificationError::CometConfigAttributeError(
                                "num_results".to_string(),
                                e,
                            )
                            .into(),
                        );
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                }

                match comet_config.async_to_file(&comet_params_file_path).await {
                    Ok(_) => (),
                    Err(e) => {
                        let error_message = message.to_error_message(
                            IdentificationError::WriteCometConfigFileError(e).into(),
                        );
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                }

                match run_comet_search(
                    &config.comet_exe_path,
                    &comet_params_file_path,
                    &fasta_file_path,
                    &psms_file_path.with_extension(""),
                    &mzml_file_path,
                )
                .await
                {
                    Ok(_) => (),
                    Err(e) => {
                        let error_message =
                            message.to_error_message(IdentificationError::CometError(e).into());
                        error!("{}", &error_message);
                        Self::enqueue_message(error_message, error_queue.as_ref()).await;
                        continue 'message_loop;
                    }
                }

                // Add PSM file to the message
                let psms = match PeptideSpectrumMatchTsv::read(&psms_file_path) {
                    Ok(Some(psms)) => psms,
                    Ok(None) => DataFrame::empty(),
                    Err(e) => {
                        error!(
                            "[{} / {}] Error reading PSMs from `{}`: {:?}",
                            &message.uuid(),
                            &message.spectrum_id(),
                            psms_file_path.display(),
                            e
                        );
                        continue;
                    }
                };

                let scoring_message = message.into_scoring_message(psms);

                Self::enqueue_message(scoring_message, scoring_queue.as_ref()).await;
                counter!(metrics_counter_name.clone()).increment(1);
            }
            if stop_flag.load(Ordering::Relaxed) {
                break;
            }
            // wait before checking the queue again
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    /// Run the identification task by itself
    ///
    /// # Arguments
    /// * `work_dir` - Working directory
    /// * `config_file_path` - Path to the configuration file
    ///
    pub async fn run_standalone(local_work_dir: PathBuf, config_file_path: PathBuf) -> Result<()> {
        let config: StandaloneIdentificationConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let identification_queue = Arc::new(
            RedisPipelineQueue::<IdentificationMessage>::new(&config.identification).await?,
        );

        let scoring_queue =
            Arc::new(RedisPipelineQueue::<ScoringMessage>::new(&config.scoring).await?);

        let publication_queue =
            Arc::new(RedisPipelineQueue::<PublicationMessage>::new(&config.publication).await?);

        let error_queue = Arc::new(RedisPipelineQueue::<ErrorMessage>::new(&config.error).await?);

        let storage = Arc::new(RedisPipelineStorage::new(&config.storage).await?);

        let identification_config = Arc::new(config.identification.clone());

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.identification.num_tasks)
            .map(|comet_proc_idx| {
                let comet_tmp_dir = local_work_dir.join(format!("comet_{}", comet_proc_idx));
                tokio::spawn(IdentificationTask::start(
                    comet_tmp_dir,
                    identification_config.clone(),
                    storage.clone(),
                    identification_queue.clone(),
                    scoring_queue.clone(),
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

impl Task for IdentificationTask {
    fn get_counter_prefix() -> &'static str {
        COUNTER_PREFIX
    }
}
