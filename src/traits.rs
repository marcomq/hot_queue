use crate::model::CanonicalMessage;
use anyhow::anyhow;
use async_channel::{bounded, Receiver, RecvError};
use async_trait::async_trait;
pub use futures::future::BoxFuture;
use std::any::Any;
use thiserror::Error;
use tokio::task::JoinHandle;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum ConsumerError {
    #[error("BufferedConsumer channel has been closed.")]
    ChannelClosed,
}

/// A closure that can be called to commit the message.
/// It returns a `BoxFuture` to allow for async commit operations.
pub type CommitFunc =
    Box<dyn FnOnce(Option<CanonicalMessage>) -> BoxFuture<'static, ()> + Send + 'static>;

#[async_trait]
pub trait MessageConsumer: Send + Sync {
    /// Receives a single message.
    async fn receive(&mut self) -> anyhow::Result<(CanonicalMessage, CommitFunc)>;
    fn as_any(&self) -> &dyn Any;
}


#[async_trait]
pub trait MessagePublisher: Send + Sync + 'static {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<Option<CanonicalMessage>>;
    async fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }
    fn as_any(&self) -> &dyn Any;
}
