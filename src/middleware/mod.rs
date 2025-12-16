//  hot_queue
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/hot_queue

use crate::models::{Endpoint, Middleware};
use crate::traits::{MessageConsumer, MessagePublisher};
use anyhow::Result;
use std::sync::Arc;

mod deduplication;
mod dlq;
mod metrics;

use deduplication::DeduplicationConsumer;
use dlq::DlqPublisher;
use metrics::{MetricsConsumer, MetricsPublisher};

/// Wraps a `MessageConsumer` with the middlewares specified in the endpoint configuration.
///
/// Middlewares are applied in reverse order of the configuration list.
/// This means the first middleware in the config is the outermost layer, executed first.
pub async fn apply_middlewares_to_consumer(
    mut consumer: Box<dyn MessageConsumer>,
    endpoint: &Endpoint,
    route_name: &str,
) -> Result<Box<dyn MessageConsumer>> {
    for middleware in endpoint.middlewares.iter().rev() {
        consumer = match middleware {
            Middleware::Deduplication(cfg) => {
                Box::new(DeduplicationConsumer::new(consumer, cfg, route_name)?)
            }
            Middleware::Metrics(cfg) => {
                Box::new(MetricsConsumer::new(consumer, cfg, route_name, "input"))
            }
            Middleware::Dlq(_) => consumer, // DLQ is a publisher-only middleware
        };
    }
    Ok(consumer)
}

/// Wraps a `MessagePublisher` with the middlewares specified in the endpoint configuration.
///
/// Middlewares are applied in the order of the configuration list.
/// This means the first middleware in the config is the outermost layer, executed first.
pub async fn apply_middlewares_to_publisher(
    mut publisher: Box<dyn MessagePublisher>,
    endpoint: &Endpoint,
    route_name: &str,
) -> Result<Arc<dyn MessagePublisher>> {
    for middleware in &endpoint.middlewares {
        publisher = match middleware {
            Middleware::Dlq(cfg) => Box::new(DlqPublisher::new(publisher, cfg, route_name).await?),
            Middleware::Metrics(cfg) => {
                Box::new(MetricsPublisher::new(publisher, cfg, route_name, "output"))
            }
            // This middleware is consumer-only
            Middleware::Deduplication(_) => publisher,
        };
    }
    Ok(publisher.into())
}
