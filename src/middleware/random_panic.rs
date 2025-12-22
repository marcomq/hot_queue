use crate::models::RandomPanicMiddleware;
use crate::traits::{
    ConsumerError, MessageConsumer, MessagePublisher, PublisherError, Received, ReceivedBatch,
    SendBatchOutcome, SendOutcome,
};
use crate::CanonicalMessage;
use async_trait::async_trait;
use rand::Rng;
use std::any::Any;

pub struct RandomPanicConsumer {
    inner: Box<dyn MessageConsumer>,
    probability: f64,
}

impl RandomPanicConsumer {
    pub fn new(inner: Box<dyn MessageConsumer>, config: &RandomPanicMiddleware) -> Self {
        if !(0.0..=1.0).contains(&config.probability) {
            panic!(
                "RandomPanicMiddleware: probability must be between 0.0 and 1.0, got {}",
                config.probability
            );
        }
        Self {
            inner,
            probability: config.probability,
        }
    }

    fn maybe_panic(&self) {
        if rand::rng().random_bool(self.probability) {
            panic!("RandomPanicMiddleware: Consumer panic triggered!");
        }
    }
}

#[async_trait]
impl MessageConsumer for RandomPanicConsumer {
    async fn receive(&mut self) -> Result<Received, ConsumerError> {
        self.maybe_panic();
        self.inner.receive().await
    }

    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        self.maybe_panic();
        self.inner.receive_batch(max_messages).await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct RandomPanicPublisher {
    inner: Box<dyn MessagePublisher>,
    probability: f64,
}

impl RandomPanicPublisher {
    pub fn new(inner: Box<dyn MessagePublisher>, config: &RandomPanicMiddleware) -> Self {
        if !(0.0..=1.0).contains(&config.probability) {
            panic!(
                "RandomPanicMiddleware: probability must be between 0.0 and 1.0, got {}",
                config.probability
            );
        }
        Self {
            inner,
            probability: config.probability,
        }
    }

    fn maybe_panic(&self) {
        if rand::rng().random_bool(self.probability) {
            panic!("RandomPanicMiddleware: Publisher panic triggered!");
        }
    }
}

#[async_trait]
impl MessagePublisher for RandomPanicPublisher {
    async fn send(&self, message: CanonicalMessage) -> Result<SendOutcome, PublisherError> {
        self.maybe_panic();
        self.inner.send(message).await
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SendBatchOutcome, PublisherError> {
        self.maybe_panic();
        self.inner.send_batch(messages).await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
