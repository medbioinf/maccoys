use std::{
    sync::{atomic::AtomicBool, atomic::Ordering, Arc},
    time::Duration,
};

use tracing::{debug, error};

use crate::pipeline::{
    errors::queue_error::QueueError, messages::is_message::IsMessage, queue::PipelineQueue,
};

/// Initial delay for the retry loops below
const BACKOFF_START: Duration = Duration::from_millis(100);

/// Upper bound the retry loops' delay is capped at, so a sustained outage produces
/// periodic log lines instead of a tight, ever-shrinking-value-free busy loop
const BACKOFF_MAX: Duration = Duration::from_secs(5);

/// Sleeps for `interval`, then returns the next (doubled, capped) interval to use
/// if the same operation fails again
async fn backoff_sleep(interval: Duration) -> Duration {
    tokio::time::sleep(interval).await;
    (interval * 2).min(BACKOFF_MAX)
}

/// Common functionality shared by every pipeline stage: polling its own queue,
/// backing off when the queue is empty or erroring, and dispatching to
/// [`Task::process_message`] for the actual, stage-specific work.
///
pub trait Task: Sized {
    /// Message type consumed from this task's own queue
    type Message: IsMessage;

    /// Queue implementation backing this task's own queue
    type Queue: PipelineQueue<Self::Message> + Send + Sync + 'static;

    fn get_counter_prefix() -> &'static str;

    fn get_counter_name(uuid: &str) -> String {
        format!("{}_{}", Self::get_counter_prefix(), uuid)
    }

    /// Returns the Loki tracing label value for this task.
    /// By default, it is the same as the counter prefix.
    ///
    fn loki_tracing_label_value() -> &'static str {
        Self::get_counter_prefix()
    }

    /// This task's own queue, polled by [`Task::run`]
    ///
    fn queue(&self) -> &Arc<Self::Queue>;

    /// Flag which, once set, stops [`Task::run`] after the in-flight message finishes
    ///
    fn stop_flag(&self) -> &Arc<AtomicBool>;

    /// Processes a single message popped from [`Task::queue`]: the stage's actual work,
    /// including forwarding to the next stage(s) or the error queue, and acknowledging
    /// the message / incrementing metrics on success.
    ///
    /// # Arguments
    /// * `message_id` - ID of the popped message, as returned by [`PipelineQueue::pop`]
    /// * `message` - The popped message
    ///
    fn process_message(
        &mut self,
        message_id: String,
        message: Self::Message,
    ) -> impl std::future::Future<Output = ()> + Send;

    /// Runs the task: pop from [`Task::queue`], call [`Task::process_message`], repeat,
    /// until [`Task::stop_flag`] is set.
    ///
    fn run(mut self) -> impl std::future::Future<Output = ()> + Send
    where
        Self: Send + 'static,
    {
        async move {
            let mut pop_backoff = BACKOFF_START;
            'message_loop: loop {
                if self.stop_flag().load(Ordering::Relaxed) {
                    break;
                }
                let (message_id, message) = match self.queue().pop().await {
                    Ok(Some(message)) => {
                        pop_backoff = BACKOFF_START;
                        message
                    }
                    Ok(None) => {
                        // If the queue is empty, wait for a while before checking again
                        pop_backoff = BACKOFF_START;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue 'message_loop;
                    }
                    Err(e) => {
                        error!("{}", e);
                        pop_backoff = backoff_sleep(pop_backoff).await;
                        continue 'message_loop;
                    }
                };
                debug!("[{}] recv", &message_id);
                self.process_message(message_id, message).await;
            }
        }
    }

    /// Tries to enqueue the message to the queue.
    /// Retries until it succeeds so a message is never silently dropped; logs
    /// every failure except a full queue, which is routine backpressure.
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
            let mut backoff = BACKOFF_START;
            loop {
                message = match queue.push(message).await {
                    Ok(_) => break,
                    Err((err, errored_message)) => {
                        if !matches!(err, QueueError::QueueFullError) {
                            error!("{}", err);
                        }
                        *errored_message
                    }
                };
                backoff = backoff_sleep(backoff).await;
            }
        }
    }

    /// Loops until the message is successfully acknowledged by the queue.
    ///
    /// # Arguments
    ///
    /// * `message_id` - ID of the message to acknowledge
    /// * `queue` - Message queue
    ///
    fn ack_message<M, Q>(
        message_id: &str,
        queue: &Q,
    ) -> impl std::future::Future<Output = ()> + Send
    where
        M: IsMessage,
        Q: PipelineQueue<M> + Send + Sync + 'static,
    {
        async {
            let mut backoff = BACKOFF_START;
            loop {
                match queue.ack(message_id).await {
                    Ok(()) => break,
                    Err(e) => {
                        error!("{}", e);
                        backoff = backoff_sleep(backoff).await;
                        continue;
                    }
                }
            }
        }
    }
}
