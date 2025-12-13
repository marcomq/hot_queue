//  streamqueue
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/streamqueue

#[cfg(feature = "amqp")]
pub mod amqp;
pub mod file;
#[cfg(feature = "http")]
pub mod http;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod memory;
#[cfg(feature = "mongodb")]
pub mod mongodb;
#[cfg(feature = "mqtt")]
pub mod mqtt;
#[cfg(feature = "nats")]
pub mod nats;
pub mod static_endpoint;

use crate::config::{
    ConsumerEndpoint, PublisherEndpoint, Route,
};
use crate::consumers::MessageConsumer;
use crate::publishers::MessagePublisher;
use anyhow::{anyhow, Result};
use std::sync::Arc;

/// Creates a `MessageConsumer` based on the route's "in" configuration.
pub async fn create_consumer_from_route(
    route_name: &str,
    endpoint: &ConsumerEndpoint,
) -> Result<Box<dyn MessageConsumer>> {
    match endpoint {
        #[cfg(feature = "kafka")]
        ConsumerEndpoint::Kafka(cfg) => {
            let topic = cfg.topic.as_deref().unwrap_or(route_name);
            Ok(Box::new(kafka::KafkaConsumer::new(&cfg.config, topic)?))
        }
        #[cfg(feature = "nats")]
        ConsumerEndpoint::Nats(cfg) => {
            let subject = cfg.subject.as_deref().unwrap_or(route_name);
            let stream_name = cfg
                .stream
                .as_deref()
                .or(cfg.config.default_stream.as_deref())
                .ok_or_else(|| {
                    anyhow!(
                        "[route:{}] NATS consumer must specify a 'stream' or have a 'default_stream'",
                        route_name
                    )
                })?;
            Ok(Box::new(
                nats::NatsConsumer::new(&cfg.config, stream_name, subject).await?,
            ))
        }
        #[cfg(feature = "amqp")]
        ConsumerEndpoint::Amqp(cfg) => {
            let queue = cfg.queue.as_deref().unwrap_or(route_name);
            Ok(Box::new(amqp::AmqpConsumer::new(&cfg.config, queue).await?))
        }
        #[cfg(feature = "mqtt")]
        ConsumerEndpoint::Mqtt(cfg) => {
            let topic = cfg.topic.as_deref().unwrap_or(route_name);
            Ok(Box::new(
                mqtt::MqttConsumer::new(&cfg.config, topic, route_name).await?,
            ))
        }
        ConsumerEndpoint::File(cfg) => {
            Ok(Box::new(file::FileConsumer::new(cfg).await?))
        }
        #[cfg(feature = "http")]
        ConsumerEndpoint::Http(cfg) => {
            Ok(Box::new(http::HttpConsumer::new(&cfg.config).await?))
        }
        ConsumerEndpoint::Static(cfg) => {
            Ok(Box::new(static_endpoint::StaticRequestConsumer::new(cfg)?))
        }
        ConsumerEndpoint::Memory(cfg) => {
            Ok(Box::new(memory::MemoryConsumer::new(&cfg.config)?))
        }
        #[cfg(feature = "mongodb")]
        ConsumerEndpoint::MongoDb(cfg) => {
            let collection = cfg.collection.as_deref().unwrap_or(route_name);
            Ok(Box::new(mongodb::MongoDbConsumer::new(&cfg.config, collection).await?))
        }
        #[allow(unreachable_patterns)]
        _ => Err(anyhow!(
            "[route:{}] Unsupported consumer endpoint type",
            route_name
        )),
    }
}

/// Creates a `MessagePublisher` based on the route's "out" configuration.
pub async fn create_publisher_from_route(
    route_name: &str,
    endpoint: &PublisherEndpoint,
) -> Result<Arc<dyn MessagePublisher>> {
    match endpoint {
        #[cfg(feature = "kafka")]
        PublisherEndpoint::Kafka(cfg) => {
            let topic = cfg.topic.as_deref().unwrap_or(route_name);
            Ok(Arc::new(kafka::KafkaPublisher::new(&cfg.config, topic).await?))
        }
        #[cfg(feature = "nats")]
        PublisherEndpoint::Nats(cfg) => {
            let subject = cfg.subject.as_deref().unwrap_or(route_name);
            Ok(Arc::new(
                nats::NatsPublisher::new(&cfg.config, subject, cfg.stream.as_deref()).await?,
            ))
        }
        #[cfg(feature = "amqp")]
        PublisherEndpoint::Amqp(cfg) => {
            let queue = cfg.queue.as_deref().unwrap_or(route_name);
            Ok(Arc::new(amqp::AmqpPublisher::new(&cfg.config, queue).await?))
        }
        #[cfg(feature = "mqtt")]
        PublisherEndpoint::Mqtt(cfg) => {
            let topic = cfg.topic.as_deref().unwrap_or(route_name);
            Ok(Arc::new(
                mqtt::MqttPublisher::new(&cfg.config, topic, route_name).await?,
            ))
        }
        PublisherEndpoint::File(cfg) => {
            Ok(Arc::new(file::FilePublisher::new(cfg).await?))
        }
        #[cfg(feature = "http")]
        PublisherEndpoint::Http(cfg) => {
            let mut sink = http::HttpPublisher::new(&cfg.config).await?;
            if let Some(url) = &cfg.url {
                sink = sink.with_url(url);
            }
            Ok(Arc::new(sink))
        }
        PublisherEndpoint::Static(cfg) => {
            Ok(Arc::new(static_endpoint::StaticEndpointPublisher::new(cfg)?))
        }
        PublisherEndpoint::Memory(cfg) => {
            Ok(Arc::new(memory::MemoryPublisher::new(&cfg.config)?))
        }
        #[cfg(feature = "mongodb")]
        PublisherEndpoint::MongoDb(cfg) => {
            let collection = cfg.collection.as_deref().unwrap_or(route_name);
            Ok(Arc::new(
                mongodb::MongoDbPublisher::new(&cfg.config, collection).await?,
            ))
        }
        #[allow(unreachable_patterns)]
        _ => Err(anyhow!(
            "[route:{}] Unsupported publisher endpoint type",
            route_name
        )),
    }
}

/// Creates a `MessagePublisher` for the DLQ if configured.
#[allow(unused_variables)]
pub async fn create_dlq_from_route(
    route: &Route,
    route_name: &str,
) -> Result<Option<Arc<dyn MessagePublisher>>> {
    if let Some(dlq_endpoint) = &route.dlq {
        tracing::info!("DLQ configured for route {}", route_name);
        let publisher = create_publisher_from_route(route_name, dlq_endpoint).await?;
        return Ok(Some(publisher));
    }
    Ok(None)
}

impl ConsumerEndpoint {
    pub fn channel(&self) -> Result<memory::MemoryChannel> {
        match self {
            ConsumerEndpoint::Memory(cfg) => Ok(memory::get_or_create_channel(&cfg.config)),
            _ => Err(anyhow!("channel() called on non-memory ConsumerEndpoint")),
        }
    }
}

impl PublisherEndpoint {
    pub fn channel(&self) -> Result<memory::MemoryChannel> {
        match self {
            PublisherEndpoint::Memory(cfg) => Ok(memory::get_or_create_channel(&cfg.config)),
            _ => Err(anyhow!("channel() called on non-memory ConsumerEndpoint")),
        }
    }
}
