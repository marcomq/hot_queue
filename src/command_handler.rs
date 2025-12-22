//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

use crate::traits::{send_batch_helper, CommandHandler, MessagePublisher};
use crate::traits::{HandlerError, HandlerOutcome};
use crate::CanonicalMessage;
use async_trait::async_trait;
use std::any::Any;
use std::future::Future;
use std::sync::Arc;

use crate::traits::{PublisherError, SendBatchOutcome, SendOutcome};
#[async_trait]
impl<F, Fut> CommandHandler for F
where
    F: Fn(CanonicalMessage) -> Fut + Send + Sync,
    Fut: Future<Output = Result<HandlerOutcome, HandlerError>> + Send,
{
    async fn handle(&self, msg: CanonicalMessage) -> Result<HandlerOutcome, HandlerError> {
        self(msg).await
    }
}

/// A publisher middleware that intercepts messages and passes them to a `CommandHandler`.
/// If the handler returns a new message, it is passed to the inner publisher.
pub struct CommandHandlerPublisher {
    inner: Box<dyn MessagePublisher>,
    handler: Arc<dyn CommandHandler>,
}

impl CommandHandlerPublisher {
    pub fn new(inner: Box<dyn MessagePublisher>, handler: Arc<dyn CommandHandler>) -> Self {
        Self { inner, handler }
    }
}

#[async_trait]
impl MessagePublisher for CommandHandlerPublisher {
    async fn send(&self, message: CanonicalMessage) -> Result<SendOutcome, PublisherError> {
        match self.handler.handle(message).await {
            Ok(HandlerOutcome::Publish(response_msg)) => self.inner.send(response_msg).await, // Propagate result
            Ok(HandlerOutcome::Ack) => Ok(SendOutcome::Ack),
            Err(e) => Err(e.into()), // Converts HandlerError to PublisherError
        }
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SendBatchOutcome, PublisherError> {
        send_batch_helper(self, messages, |publisher, message| {
            Box::pin(publisher.send(message))
        })
        .await
    }

    async fn flush(&self) -> anyhow::Result<()> {
        self.inner.flush().await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoints::memory::MemoryPublisher;
    use crate::models::MemoryConfig;

    #[tokio::test]
    async fn test_command_handler_produces_response() {
        let config = MemoryConfig {
            topic: "test_command_out".to_string(),
            capacity: Some(10),
        };
        let memory_publisher = MemoryPublisher::new(&config).unwrap();
        let channel = memory_publisher.channel();

        let handler = Arc::new(|msg: CanonicalMessage| async move {
            let response_payload = format!("response_to_{}", String::from_utf8_lossy(&msg.payload));
            Ok(HandlerOutcome::Publish(CanonicalMessage::new(
                response_payload.into_bytes(),
                None,
            )))
        });

        let publisher = CommandHandlerPublisher::new(Box::new(memory_publisher), handler);

        let msg = CanonicalMessage::new(b"command1".to_vec(), None);
        publisher.send(msg).await.unwrap();

        let received = channel.drain_messages();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].payload, "response_to_command1".as_bytes());
    }

    #[tokio::test]
    async fn test_command_handler_acks() {
        let config = MemoryConfig {
            topic: "test_command_out".to_string(),
            capacity: Some(10),
        };
        let memory_publisher = MemoryPublisher::new(&config).unwrap();
        let channel = memory_publisher.channel();

        let handler = Arc::new(|_msg: CanonicalMessage| async move { Ok(HandlerOutcome::Ack) });

        let publisher = CommandHandlerPublisher::new(Box::new(memory_publisher), handler);

        let msg = CanonicalMessage::new(b"command1".to_vec(), None);
        let result = publisher.send(msg).await.unwrap();

        assert!(matches!(result, SendOutcome::Ack));
        let received = channel.drain_messages();
        assert_eq!(received.len(), 0);
    }

    #[tokio::test]
    async fn test_command_handler_retryable_error() {
        let config = MemoryConfig {
            topic: "test_command_out".to_string(),
            capacity: Some(10),
        };
        let memory_publisher = MemoryPublisher::new(&config).unwrap();

        let handler = Arc::new(|_msg: CanonicalMessage| async move {
            Err(HandlerError::Retryable(anyhow::anyhow!("db is down")))
        });

        let publisher = CommandHandlerPublisher::new(Box::new(memory_publisher), handler);

        let msg = CanonicalMessage::new(b"command1".to_vec(), None);
        let result = publisher.send(msg).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        // The HandlerError is converted into a PublisherError
        assert!(matches!(err, PublisherError::Retryable(_)));
    }
}
