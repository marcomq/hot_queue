use crate::models::RetryMiddleware;
use crate::traits::MessagePublisher;
use crate::CanonicalMessage;
use async_trait::async_trait;
use std::any::Any;
use std::time::Duration;
use tracing::warn;

pub struct RetryPublisher {
    inner: Box<dyn MessagePublisher>,
    config: RetryMiddleware,
}

impl RetryPublisher {
    pub fn new(inner: Box<dyn MessagePublisher>, config: RetryMiddleware) -> Self {
        Self { inner, config }
    }

    async fn retry_op<F, Fut, T>(&self, operation: F) -> anyhow::Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<T>>,
    {
        let mut attempt = 0;
        let mut interval = self.config.initial_interval_ms;

        loop {
            attempt += 1;
            match operation().await {
                Ok(val) => return Ok(val),
                Err(e) => {
                    if attempt >= self.config.max_attempts {
                        return Err(e);
                    }
                    warn!(
                        "Operation failed (attempt {}/{}): {}. Retrying in {}ms...",
                        attempt, self.config.max_attempts, e, interval
                    );
                    self.sleep_and_backoff(&mut interval).await;
                }
            }
        }
    }

    async fn sleep_and_backoff(&self, interval: &mut u64) {
        tokio::time::sleep(Duration::from_millis(*interval)).await;
        *interval = (*interval as f64 * self.config.multiplier) as u64;
        if *interval > self.config.max_interval_ms {
            *interval = self.config.max_interval_ms;
        }
    }
}

#[async_trait]
impl MessagePublisher for RetryPublisher {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<Option<CanonicalMessage>> {
        self.retry_op(|| {
            let msg = message.clone();
            async { self.inner.send(msg).await }
        })
        .await
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> anyhow::Result<(Option<Vec<CanonicalMessage>>, Vec<CanonicalMessage>)> {
        let mut current_messages = messages;
        let mut all_responses = Vec::new();

        // We reuse the retry_op logic manually here because the state (current_messages) changes
        let mut attempt = 0;
        let mut interval = self.config.initial_interval_ms;

        loop {
            attempt += 1;
            match self.inner.send_batch(current_messages.clone()).await {
                Ok((responses, failed)) => {
                    if let Some(resps) = responses {
                        all_responses.extend(resps);
                    }
                    if failed.is_empty() {
                        return Ok((Some(all_responses), Vec::new()));
                    }
                    if attempt >= self.config.max_attempts {
                        return Ok((Some(all_responses), failed));
                    }
                    warn!("Batch send partially failed (attempt {}/{}): {} messages failed. Retrying...", attempt, self.config.max_attempts, failed.len());
                    current_messages = failed;
                }
                Err(e) => {
                    if attempt >= self.config.max_attempts {
                        return Err(e);
                    }
                    warn!(
                        "Batch send failed (attempt {}/{}): {}. Retrying...",
                        attempt, self.config.max_attempts, e
                    );
                }
            }
            self.sleep_and_backoff(&mut interval).await;
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::MessagePublisher;
    use crate::CanonicalMessage;
    use anyhow::anyhow;
    use async_trait::async_trait;
    use std::any::Any;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct MockPublisher {
        attempts: Arc<Mutex<usize>>,
        succeed_after: usize,
    }

    #[async_trait]
    impl MessagePublisher for MockPublisher {
        async fn send(&self, _msg: CanonicalMessage) -> anyhow::Result<Option<CanonicalMessage>> {
            let mut attempts = self.attempts.lock().unwrap();
            *attempts += 1;
            if *attempts > self.succeed_after {
                Ok(None)
            } else {
                Err(anyhow!("Simulated error"))
            }
        }

        async fn send_batch(
            &self,
            _messages: Vec<CanonicalMessage>,
        ) -> anyhow::Result<(Option<Vec<CanonicalMessage>>, Vec<CanonicalMessage>)> {
            let mut attempts = self.attempts.lock().unwrap();
            *attempts += 1;
            if *attempts > self.succeed_after {
                Ok((None, Vec::new()))
            } else {
                Err(anyhow!("Simulated batch error"))
            }
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[tokio::test]
    async fn test_retry_success() {
        let attempts = Arc::new(Mutex::new(0));
        let mock = MockPublisher {
            attempts: attempts.clone(),
            succeed_after: 2, // Fails 2 times, succeeds on 3rd
        };

        let config = RetryMiddleware {
            max_attempts: 5,
            initial_interval_ms: 1,
            max_interval_ms: 10,
            multiplier: 1.0,
        };

        let retry_publisher = RetryPublisher::new(Box::new(mock), config);
        let msg = CanonicalMessage::new(vec![], None);

        let result = retry_publisher.send(msg).await;
        assert!(result.is_ok());
        assert_eq!(*attempts.lock().unwrap(), 3);
    }

    #[tokio::test]
    async fn test_retry_exhaustion() {
        let attempts = Arc::new(Mutex::new(0));
        let mock = MockPublisher {
            attempts: attempts.clone(),
            succeed_after: 10,
        };

        let config = RetryMiddleware {
            max_attempts: 3,
            initial_interval_ms: 1,
            max_interval_ms: 10,
            multiplier: 1.0,
        };

        let retry_publisher = RetryPublisher::new(Box::new(mock), config);
        let msg = CanonicalMessage::new(vec![], None);

        let result = retry_publisher.send(msg).await;
        assert!(result.is_err());
        assert_eq!(*attempts.lock().unwrap(), 3);
    }
}
