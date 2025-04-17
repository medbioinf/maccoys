use std::{
    fs::{self, File}, io::{BufReader, Cursor}, path::PathBuf, sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    }
};

use anyhow::{Context, Result};
use dihardts_omicstools::proteomics::io::mzml::{indexer::Indexer, reader::Reader as MzMlReader};
use metrics::counter;
use tracing::{debug, error};

use crate::pipeline::{
    configuration::StandaloneIndexingConfiguration, convert::AsInputOutputQueue,
    queue::PipelineQueue,
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
    /// * `index_queue` - Queue for the indexing task
    /// * `preparation_queue` - Queue for the preparation task
    /// * `stop_flag` - Flag to indicate to stop once the index queue is empty
    ///
    pub async fn start<Q>(
        work_dir: PathBuf,
        index_queue: Arc<Q>,
        preparation_queue: Arc<Q>,
        stop_flag: Arc<AtomicBool>,
    ) where
        Q: PipelineQueue + 'static,
    {
        loop {
            while let Some(manifest) = index_queue.pop().await {
                let metrics_counter_name = format!("{}_{}", COUNTER_PREFIX, &manifest.uuid);
                let mzml_file = match File::open(manifest.get_ms_run_mzml_path(&work_dir)) {
                    Ok(file) => file,
                    Err(e) => {
                        error!("[{}] Error opening mzML file: {:?}", &manifest.uuid, e);
                        continue;
                    }
                };
                let mut mzml_byte_reader = BufReader::new(mzml_file);
                let index =
                    match Indexer::create_index(&mut mzml_byte_reader, None) {
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

                let mzml_path = manifest.get_ms_run_mzml_path(&work_dir);
                let mut mzml_bytes_reader: BufReader<File> = BufReader::new(
                    match File::open(&mzml_path) {
                        Ok(file) => file,
                        Err(e) => {
                            error!("[{}] Error opening mzML file: {:?}", &manifest.uuid, e);
                            continue;
                        }
                    },
                );

                let mut mzml_file = match MzMlReader::read_pre_indexed(&mut mzml_bytes_reader, index, None, false) {
                    Ok(reader) => reader,
                    Err(e) => {
                        error!("[{} /] Error creating reader: {:?}", &manifest.uuid, e);
                        continue;
                    }
                };

                let spec_ids = mzml_file.get_index().get_spectra().keys().cloned().collect::<Vec<_>>();
                
                for spec_id in spec_ids.into_iter() {
                    let mzml = {
                        match mzml_file.extract_spectrum(&spec_id, true) {
                            Ok(content) => content,
                            Err(e) => {
                                error!("[{}] Error extracting spectrum: {:?}", &manifest.uuid, e);
                                continue;
                            }
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

                    counter!(metrics_counter_name.clone()).increment(1);

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

        let (input_queue, output_queue) = config.as_input_output_queue().await?;
        let input_queue = Arc::new(input_queue);
        let output_queue = Arc::new(output_queue);

        let stop_flag = Arc::new(AtomicBool::new(false));

        let handles: Vec<tokio::task::JoinHandle<()>> = (0..config.index.num_tasks)
            .map(|_| {
                tokio::spawn(IndexingTask::start(
                    work_dir.clone(),
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

impl Task for IndexingTask {
    fn get_counter_prefix() -> &'static str {
        COUNTER_PREFIX
    }
}
