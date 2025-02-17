use std::{
    io::Cursor,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use dihardts_omicstools::proteomics::io::mzml::{indexed_reader::IndexedReader, indexer::Indexer};
use tracing::{debug, error};

use crate::pipeline::{queue::PipelineQueue, storage::PipelineStorage};

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
}
