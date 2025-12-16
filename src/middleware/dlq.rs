//  hot_queue
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/hot_queue

use crate::endpoints::create_publisher_from_route;
use crate::models::DeadLetterQueueMiddleware;
use crate::traits::MessagePublisher;
use crate::CanonicalMessage;
use async_trait::async_trait;
use std::any::Any;
use std::sync::Arc;
use tracing::{error, info};

pub struct DlqPublisher {
    inner: Box<dyn MessagePublisher>,
    dlq_publisher: Arc<dyn MessagePublisher>,
}

impl DlqPublisher {
    pub async fn new(
        inner: Box<dyn MessagePublisher>,
        config: &DeadLetterQueueMiddleware,
        route_name: &str,
    ) -> anyhow::Result<Self> {
        info!("DLQ Middleware enabled for route '{}'", route_name);
        // Box::pin is used here to break the recursive async type definition.
        // create_publisher_from_route -> apply_middlewares -> DlqPublisher::new -> create_publisher_from_route
        let dlq_publisher = Box::pin(create_publisher_from_route(route_name, &config.endpoint)).await?;
        Ok(Self {
            inner,
            dlq_publisher,
        })
    }
}

#[async_trait]
impl MessagePublisher for DlqPublisher {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<Option<CanonicalMessage>> {
        match self.inner.send(message.clone()).await {
            Ok(response) => Ok(response),
            Err(e) => {
                error!("Failed to send message, forwarding to DLQ: {}", e);
                self.dlq_publisher.send(message).await?;
                Ok(None)
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
