//  hot_queue
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/hot_queue

use crate::traits::{BulkCommitFunc, CommitFunc, MessageConsumer};
use crate::models::MetricsMiddleware;
use crate::traits::MessagePublisher;
use crate::CanonicalMessage;
use async_trait::async_trait;
use std::any::Any;
use std::time::Instant;

pub struct MetricsPublisher {
    inner: Box<dyn MessagePublisher>,
    route_name: String,
    endpoint_direction: String,
}

impl MetricsPublisher {
    pub fn new(
        inner: Box<dyn MessagePublisher>,
        _config: &MetricsMiddleware,
        route_name: &str,
        endpoint_direction: &str,
    ) -> Self {
        Self {
            inner,
            route_name: route_name.to_string(),
            endpoint_direction: endpoint_direction.to_string(),
        }
    }
}

#[async_trait]
impl MessagePublisher for MetricsPublisher {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<Option<CanonicalMessage>> {
        let start = Instant::now();
        let result = self.inner.send(message).await;
        let duration = start.elapsed();

        metrics::counter!("hot_queue_messages_processed_total", "route" => self.route_name.clone(), "endpoint" => self.endpoint_direction.clone()).increment(1);
        metrics::histogram!("hot_queue_message_processing_duration_seconds", "route" => self.route_name.clone(), "endpoint" => self.endpoint_direction.clone()).record(duration.as_secs_f64());

        result
    }

    async fn send_bulk(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> anyhow::Result<Option<Vec<CanonicalMessage>>> {
        let count = messages.len() as u64;
        let start = Instant::now();
        let result = self.inner.send_bulk(messages).await;
        let duration = start.elapsed();

        metrics::counter!("hot_queue_messages_processed_total", "route" => self.route_name.clone(), "endpoint" => self.endpoint_direction.clone()).increment(count);
        metrics::histogram!("hot_queue_message_processing_duration_seconds", "route" => self.route_name.clone(), "endpoint" => self.endpoint_direction.clone()).record(duration.as_secs_f64());

        result
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct MetricsConsumer {
    inner: Box<dyn MessageConsumer>,
    route_name: String,
    endpoint_direction: String,
}

impl MetricsConsumer {
    pub fn new(
        inner: Box<dyn MessageConsumer>,
        _config: &MetricsMiddleware,
        route_name: &str,
        endpoint_direction: &str,
    ) -> Self {
        Self {
            inner,
            route_name: route_name.to_string(),
            endpoint_direction: endpoint_direction.to_string(),
        }
    }
}

#[async_trait]
impl MessageConsumer for MetricsConsumer {
    async fn receive(&mut self) -> anyhow::Result<(CanonicalMessage, CommitFunc)> {
        let start = Instant::now();
        let result = self.inner.receive().await;
        let duration = start.elapsed();

        if result.is_ok() {
            metrics::counter!("hot_queue_messages_processed_total", "route" => self.route_name.clone(), "endpoint" => self.endpoint_direction.clone()).increment(1);
            metrics::histogram!("hot_queue_message_processing_duration_seconds", "route" => self.route_name.clone(), "endpoint" => self.endpoint_direction.clone()).record(duration.as_secs_f64());
        }

        result
    }

    async fn receive_bulk(&mut self, max_messages: usize) -> anyhow::Result<(Vec<CanonicalMessage>, BulkCommitFunc)> {
        let start = Instant::now();
        let result = self.inner.receive_bulk(max_messages).await;
        let duration = start.elapsed();

        if let Ok((messages, _)) = &result {
            metrics::counter!("hot_queue_messages_processed_total", "route" => self.route_name.clone(), "endpoint" => self.endpoint_direction.clone()).increment(messages.len() as u64);
            metrics::histogram!("hot_queue_message_processing_duration_seconds", "route" => self.route_name.clone(), "endpoint" => self.endpoint_direction.clone()).record(duration.as_secs_f64());
        }

        result
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
