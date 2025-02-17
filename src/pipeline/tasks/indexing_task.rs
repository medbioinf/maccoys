use std::{
    fs,
    io::Cursor,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::{Context, Result};
use dihardts_omicstools::proteomics::io::mzml::{indexed_reader::IndexedReader, indexer::Indexer};
use tracing::{debug, error};

use crate::pipeline::{
    configuration::StandaloneIndexingConfiguration, convert::AsInputOutputQueueAndStorage,
    queue::PipelineQueue, storage::PipelineStorage,
};

/// Task to index and split up the mzML file
///
pub struct IndexingTask<Q: PipelineQueue + 'static, S: PipelineStorage + 'static> {
    _phantom_queue: std::marker::PhantomData<Q>,
    _phantom_storage: std::marker::PhantomData<S>,
}

impl<Q, S> IndexingTask<Q, S>
where
    Q: PipelineQueue + 'static,
    S: PipelineStorage + 'static,
{
    /// Start the indexing task
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    /// * `index_queue` - Queue for the indexing task
    /// * `preparation_queue` - Queue for the preparation task
    /// * `stop_flag` - Flag to indicate to stop once the index queue is empty
    ///
    pub async fn start(
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

                for spec_id in index.get_spectra().keys() {
                    let mzml = match reader.extract_spectrum(spec_id) {
                        Ok(content) => content,
                        Err(e) => {
                            error!("[{}] Error extracting spectrum: {:?}", &manifest.uuid, e);
                            continue;
                        }
                    };

                    let mut new_manifest = manifest.clone();
                    new_manifest.spectrum_id = spec_id.clone();
                    match new_manifest.set_spectrum_mzml(Cursor::new(mzml)) {
                        Ok(_) => (),
                        Err(e) => {
                            error!(
                                "[{}] Error putting mzML to manifest spectrum mzML: {:?}",
                                &manifest.uuid, e
                            );
                            continue;
                        }
                    };

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
}
