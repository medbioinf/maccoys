use std::path::{Path, PathBuf};

use tracing::error;

use crate::{
    functions::sanatize_string,
    pipeline::{
        errors::pipeline_error::PipelineError, messages::is_message::IsMessage,
        queue::PipelineQueue,
    },
};

pub trait Task {
    fn get_counter_prefix() -> &'static str;

    fn get_counter_name(uuid: &str) -> String {
        format!("{}_{}", Self::get_counter_prefix(), uuid)
    }

    /// Returns the search directory
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    ///
    fn get_search_dir(work_dir: &Path, uuid: &str) -> PathBuf {
        work_dir.join(uuid)
    }

    /// Returns the path to the MS run directory
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    ///
    fn get_ms_run_dir_path(work_dir: &Path, uuid: &str, ms_run_name: &str) -> PathBuf {
        Self::get_search_dir(work_dir, uuid).join(ms_run_name)
    }

    /// Returns the path to the MS run mzML
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    ///
    fn get_ms_run_mzml_path(work_dir: &Path, uuid: &str, ms_run_name: &str) -> PathBuf {
        Self::get_ms_run_dir_path(work_dir, uuid, ms_run_name).join("run.mzML")
    }

    /// Returns the path to the mzML index JSON
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    ///
    fn get_index_path(work_dir: &Path, uuid: &str, ms_run_name: &str) -> PathBuf {
        Self::get_ms_run_dir_path(work_dir, uuid, ms_run_name).join("index.json")
    }

    /// Retuns the path of the spectrum directory wihtin the MS run directory
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    ///
    fn get_spectrum_dir_path(
        work_dir: &Path,
        uuid: &str,
        ms_run_name: &str,
        spectrum_id: &str,
    ) -> PathBuf {
        Self::get_ms_run_dir_path(work_dir, uuid, ms_run_name).join(sanatize_string(spectrum_id))
    }

    /// Returns the path to the fasta file for the spectrum's precursor
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    /// * `precursor_mz` - Precursor mass to charge ratio
    /// * `precursor_charge` - Precursor charge
    ///
    fn get_fasta_file_path(
        work_dir: &Path,
        uuid: &str,
        ms_run_name: &str,
        spectrum_id: &str,
        precursor_mz: f64,
        precursor_charge: u8,
    ) -> PathBuf {
        Self::get_spectrum_dir_path(work_dir, uuid, ms_run_name, spectrum_id)
            .join(format!("{}_{}.fasta", precursor_mz, precursor_charge))
    }

    /// Returns the path to the Comet parameter file for the spectrum's precursor
    ///
    /// # Arguments
    /// * `work_dir` - Work directory where the results are stored
    /// * `precursor_mz` - Precursor mass to charge ratio
    /// * `precursor_charge` - Precursor charge
    ///
    fn get_comet_params_path(
        &self,
        work_dir: &Path,
        uuid: &str,
        ms_run_name: &str,
        spectrum_id: &str,
        precursor_mz: f64,
        precursor_charge: u8,
    ) -> PathBuf {
        Self::get_spectrum_dir_path(work_dir, uuid, ms_run_name, spectrum_id)
            .join(format!("{}_{}.comet.param", precursor_mz, precursor_charge))
    }

    /// Returns the path to the PSM file for the spectrum's precursor
    /// This alredy has the TSV file extension while Comet writes it with the extension .txt
    /// It is renamed after the search
    ///
    /// # Argumentss
    /// * `work_dir` - Work directory where the results are stored
    /// * `precursor_mz` - Precursor mass to charge ratio
    /// * `precursor_charge` - Precursor charge
    ///
    fn get_psms_file_path(
        &self,
        work_dir: &Path,
        uuid: &str,
        ms_run_name: &str,
        spectrum_id: &str,
        precursor_mz: f64,
        precursor_charge: u8,
    ) -> PathBuf {
        Self::get_spectrum_dir_path(work_dir, uuid, ms_run_name, spectrum_id)
            .join(format!("{}_{}.psms.tsv", precursor_mz, precursor_charge))
    }

    /// Tries to enqueue the message to the queue.
    /// If it fails, it will log the error and try again
    ///
    /// # Arguments
    /// * `message` - Message to enqueue
    /// * `queue` - Message queue
    ///
    fn enqueue_message<M, Q>(message: M, queue: &Q) -> impl std::future::Future<Output = ()> + Send
    where
        M: IsMessage,
        Q: PipelineQueue<M> + Send + Sync + 'static,
    {
        async {
            let mut message = message;
            loop {
                message = match queue.push(message).await {
                    Ok(_) => break,
                    Err(original_message) => {
                        error!(
                            "{}",
                            original_message.to_error_message(PipelineError::MessageEnqueueError())
                        );
                        original_message
                    }
                };
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }
        }
    }
}
