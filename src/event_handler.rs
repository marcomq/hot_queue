//  mq-bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq-bridge

use crate::traits::{EventHandler, MessagePublisher, send_batch_helper};
use crate::CanonicalMessage;
use async_trait::async_trait;
use std::any::Any;
use std::future::Future;
use std::sync::Arc;

#[async_trait]
impl<F, Fut> EventHandler for F
where
    F: Fn(CanonicalMessage) -> Fut + Send + Sync,
    Fut: Future<Output = anyhow::Result<()>> + Send,
{
    async fn handle(&self, msg: CanonicalMessage) -> anyhow::Result<()> {
        self(msg).await
    }
}

/// A publisher middleware that intercepts messages and passes them to an `EventHandler`.
/// This middleware is terminal; it consumes the message and does not pass it to an inner publisher.
pub struct EventHandlerPublisher {
    // The inner publisher is stored to maintain the middleware chain structure, but it is not used.
    _inner: Box<dyn MessagePublisher>,
    handler: Arc<dyn EventHandler>,
}

impl EventHandlerPublisher {
    pub fn new(inner: Box<dyn MessagePublisher>, handler: Arc<dyn EventHandler>) -> Self {
        Self {
            _inner: inner,
            handler,
        }
    }
}

#[async_trait]
impl MessagePublisher for EventHandlerPublisher {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<Option<CanonicalMessage>> {
        self.handler.handle(message).await?;
        // Event handlers do not produce a response, so we return Ok(None).
        Ok(None)
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> anyhow::Result<(Option<Vec<CanonicalMessage>>, Vec<CanonicalMessage>)> {
        send_batch_helper(self, messages, |publisher, message| {
            Box::pin(publisher.send(message))
        })
        .await
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
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn test_event_handler() {
        let memory_publisher = MemoryPublisher::new(&MemoryConfig {
            topic: "unused".to_string(),
            capacity: Some(1),
        })
        .unwrap();
        let event_handled = Arc::new(AtomicBool::new(false));
        let handler = Arc::new({
            let flag = event_handled.clone();
            move |_msg: CanonicalMessage| {
                let flag_clone = flag.clone();
                async move {
                    flag_clone.store(true, Ordering::SeqCst);
                    Ok(())
                }
            }
        });
        let publisher = EventHandlerPublisher::new(Box::new(memory_publisher), handler);
        publisher
            .send(CanonicalMessage::new(b"event1".to_vec(), None))
            .await
            .unwrap();
        assert!(event_handled.load(Ordering::SeqCst));
    }
}