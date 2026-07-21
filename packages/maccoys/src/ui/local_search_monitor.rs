use std::marker::PhantomData;
// std imports
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

// 3rd party imports
use anyhow::Result;
use indicatif::ProgressStyle;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::{info_span, Instrument, Span};
use tracing_indicatif::span_ext::IndicatifSpanExt;

use crate::pipeline::storage::PipelineStorage;

/// update interval for the progress bar in ms
///
const UPDATE_INTERVAL: u64 = 1000;

/// Progress bar style. Used when a maximum value is given
///
const PROGRESS_BAR_STYLE: &str = "        {msg} {wide_bar} {pos}/{len} {per_sec} ";

// Might be come in handy in the future
// /// Progress style, used when no maximum value is given
// ///
// const PROGRESS_PLAIN_STYLE: &str = "        {msg} {pos} {per_sec} ";

/// Labels
const LABELS: [&str; 1] = ["Finished spectra"];

/// Creats a tracing span with progress bars for monitoring the storage
///
pub struct LocalSearchMonitor<S>
where
    S: PipelineStorage,
{
    thread_handle: Option<JoinHandle<()>>, // Wrapped in Option to be able to take it when await join
    stop_flag: Arc<AtomicBool>,
    _storage_phantom: PhantomData<S>,
}

impl<S> LocalSearchMonitor<S>
where
    S: PipelineStorage + 'static,
{
    /// Creates a progress view for the given progress values
    /// Make sure to call it with '.instrument(<span>)' to make sure the progress bar is displayed
    ///
    /// # Arguments
    /// * `uuid` - UUID of the search
    /// * `title` - Title of the progress bar
    /// * `storage` - Storage to use for the progress bar
    /// * `update_interval_override` - Override for the update interval (default: [UPDATE_INTERVAL])
    ///
    pub fn new(
        uuid: String,
        title: &str,
        storage: Arc<S>,
        update_interval_override: Option<u64>,
    ) -> Result<Self> {
        let stop_flag = Arc::new(AtomicBool::new(false));
        let progress_span = info_span!("");
        progress_span.pb_set_message(title);
        let thread_handle = Some(tokio::spawn(
            Self::view(uuid, storage, stop_flag.clone(), update_interval_override)
                .instrument(progress_span),
        ));

        Ok(Self {
            thread_handle,
            stop_flag,
            _storage_phantom: PhantomData,
        })
    }

    /// Creates a progress bar for the given progress values
    /// Make sure to call it with '.instrument(<span>)' to make sure the progress bar is displayed
    ///
    /// # Arguments
    /// * `uuid` - UUID of the search
    /// * `storage` - Storage to use for the progress bars
    /// * `stop_flag` - Flag to stop the progress bars
    /// * `update_interval_override` - Override for the update interval (default: [UPDATE_INTERVAL])
    ///
    ///
    async fn view(
        uuid: String,
        storage: Arc<S>,
        stop_flag: Arc<AtomicBool>,
        update_interval_override: Option<u64>,
    ) {
        let _ = Span::current().enter();
        let update_interval = update_interval_override.unwrap_or(UPDATE_INTERVAL);

        let spectra_span = info_span!("");
        spectra_span.pb_set_message(LABELS[0]);
        spectra_span.pb_set_style(&ProgressStyle::with_template(PROGRESS_BAR_STYLE).unwrap());

        while !stop_flag.load(std::sync::atomic::Ordering::Relaxed) {
            let next_deadline =
                tokio::time::Instant::now() + Duration::from_millis(update_interval);
            Self::update_finshed_spectra(&uuid, &storage, &spectra_span).await;
            tokio::time::sleep_until(next_deadline).await;
        }
    }

    pub async fn update_finshed_spectra(uuid: &str, storage: &S, span: &Span) {
        let _ = span.enter();
        let total = match storage.get_total_spectrum_count(uuid).await {
            Ok(total) => total,
            Err(_) => {
                span.pb_set_message("Error getting total spectrum count");
                return;
            }
        };
        let finished = match storage.get_finished_spectrum_count(uuid).await {
            Ok(finished) => finished,
            Err(_) => {
                span.pb_set_message("Error getting finished spectrum count");
                return;
            }
        };
        span.pb_set_length(total);
        span.pb_set_position(finished);
    }

    /// Stops the progress bar
    ///
    pub async fn stop(&mut self) -> Result<()> {
        self.stop_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(handle) = self.thread_handle.take() {
            handle.await?
        }
        Ok(())
    }
}
