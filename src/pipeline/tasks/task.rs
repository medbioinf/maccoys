use tracing::error;

use crate::pipeline::{
    errors::queue_error::QueueError, messages::is_message::IsMessage, queue::PipelineQueue,
};

pub trait Task {
    fn get_counter_prefix() -> &'static str;

    fn get_counter_name(uuid: &str) -> String {
        format!("{}_{}", Self::get_counter_prefix(), uuid)
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
                    Err((err, errored_message)) => match err {
                        QueueError::QueueFullError => *errored_message,
                        _ => {
                            error!("{}", err);
                            break;
                        }
                    },
                };
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }
        }
    }

    fn ack_message<M, Q>(
        message_id: &str,
        queue: &Q,
    ) -> impl std::future::Future<Output = ()> + Send
    where
        M: IsMessage,
        Q: PipelineQueue<M> + Send + Sync + 'static,
    {
        async {
            loop {
                match queue.ack(message_id).await {
                    Ok(()) => break,
                    Err(e) => {
                        error!("{}", e);
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        continue;
                    }
                }
            }
        }
    }
}
