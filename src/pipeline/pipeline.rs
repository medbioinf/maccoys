// std imports
use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

// 3rd party imports
use anyhow::{Context, Result};
use signal_hook::{consts::SIGINT, iterator::Signals};
use tracing::info;

// local imports
use super::{
    configuration::{
        StandaloneCometSearchConfiguration, StandaloneGoodnessAndRescoringConfiguration,
        StandaloneIndexingConfiguration, StandalonePreparationConfiguration,
        StandaloneSearchSpaceGenerationConfiguration,
    },
    convert::AsInputOutputQueueAndStorage,
    queue::{PipelineQueue, RedisPipelineQueue},
    storage::{PipelineStorage, RedisPipelineStorage},
};
use crate::pipeline::tasks::{
    cleanup_task::CleanupTask, identification_task::IdentificationTask,
    indexing_task::IndexingTask, preparation_task::PreparationTask, scoring_task::ScoringTask,
    search_space_generation_task::SearchSpaceGenerationTask,
};

/// Pipeline to run the MaCcoyS identification pipeline
///
pub struct Pipeline;

impl Pipeline {
    pub async fn standalone_indexing(work_dir: PathBuf, config_file_path: PathBuf) -> Result<()> {
        let config: StandaloneIndexingConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let (storage, input_queue, output_queue) =
            config.as_input_output_queue_and_storage().await?;
        let storage = Arc::new(storage);
        let input_queue = Arc::new(input_queue);
        let output_queue = Arc::new(output_queue);

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.index.num_tasks)
            .map(|_| {
                tokio::spawn(IndexingTask::start(
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

    pub async fn standalone_preparation(config_file_path: PathBuf) -> Result<()> {
        let config: StandalonePreparationConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let (storage, input_queue, output_queue) =
            config.as_input_output_queue_and_storage().await?;
        let storage = Arc::new(storage);
        let input_queue = Arc::new(input_queue);
        let output_queue = Arc::new(output_queue);

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.preparation.num_tasks)
            .map(|_| {
                tokio::spawn(PreparationTask::start(
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

    pub async fn standalone_search_space_generation(config_file_path: PathBuf) -> Result<()> {
        let config: StandaloneSearchSpaceGenerationConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let (storage, input_queue, output_queue) =
            config.as_input_output_queue_and_storage().await?;
        let storage = Arc::new(storage);
        let input_queue = Arc::new(input_queue);
        let output_queue = Arc::new(output_queue);

        let search_space_generation_config = Arc::new(config.search_space_generation.clone());

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> =
            (0..config.search_space_generation.num_tasks)
                .map(|_| {
                    tokio::spawn(SearchSpaceGenerationTask::start(
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
        local_work_dir: PathBuf,
        config_file_path: PathBuf,
    ) -> Result<()> {
        let config: StandaloneCometSearchConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let (storage, input_queue, output_queue) =
            config.as_input_output_queue_and_storage().await?;
        let storage = Arc::new(storage);
        let input_queue = Arc::new(input_queue);
        let output_queue = Arc::new(output_queue);

        let comet_search_config = Arc::new(config.comet_search.clone());

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.comet_search.num_tasks)
            .map(|comet_proc_idx| {
                let comet_tmp_dir = local_work_dir.join(format!("comet_{}", comet_proc_idx));
                tokio::spawn(IdentificationTask::start(
                    comet_tmp_dir,
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

    pub async fn standalone_goodness_and_rescoring(config_file_path: PathBuf) -> Result<()> {
        let config: StandaloneGoodnessAndRescoringConfiguration =
            toml::from_str(&fs::read_to_string(&config_file_path).context("Reading config file")?)
                .context("Deserialize config")?;

        let (storage, input_queue, output_queue) =
            config.as_input_output_queue_and_storage().await?;
        let storage = Arc::new(storage);
        let input_queue = Arc::new(input_queue);
        let output_queue = Arc::new(output_queue);
        let stop_flag = Arc::new(AtomicBool::new(false));

        let mut signals = Signals::new([SIGINT])?;

        let signal_stop_flag = stop_flag.clone();
        std::thread::spawn(move || {
            for sig in signals.forever() {
                if sig == SIGINT {
                    info!("Gracefully stopping.");
                    signal_stop_flag.store(true, Ordering::Relaxed);
                }
            }
        });

        let handles: Vec<tokio::task::JoinHandle<()>> =
            (0..config.goodness_and_rescoring.num_tasks)
                .map(|_| {
                    tokio::spawn(ScoringTask::start(
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
            .map(|_| {
                tokio::spawn(CleanupTask::start(
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
