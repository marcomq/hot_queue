//  hot_queue
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/hot_queue

use crate::models::DeduplicationMiddleware;
use crate::traits::{CommitFunc, MessageConsumer};
use crate::CanonicalMessage;
use async_trait::async_trait;
use sled::Db;
use std::any::Any;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, instrument};

pub struct DeduplicationConsumer {
    inner: Box<dyn MessageConsumer>,
    db: Arc<Db>,
    ttl_seconds: u64,
}

impl DeduplicationConsumer {
    pub fn new(
        inner: Box<dyn MessageConsumer>,
        config: &DeduplicationMiddleware,
        route_name: &str,
    ) -> anyhow::Result<Self> {
        info!(
            "Deduplication Middleware enabled for route '{}' with TTL {}s",
            route_name, config.ttl_seconds
        );
        let db = sled::open(&config.sled_path)?;
        Ok(Self {
            inner,
            db: Arc::new(db),
            ttl_seconds: config.ttl_seconds,
        })
    }
}

#[async_trait]
impl MessageConsumer for DeduplicationConsumer {
    #[instrument(skip_all)]
    async fn receive(&mut self) -> anyhow::Result<(CanonicalMessage, CommitFunc)> {
        loop {
            let (message, commit) = self.inner.receive().await?;
            let key = match message.message_id {
                Some(id) => id.to_be_bytes().to_vec(),
                None => {
                    // If there's no message_id, we can't deduplicate. Pass it through.
                    return Ok((message, commit));
                }
            };

            if self.db.get(&key)?.is_some() {
                info!("Duplicate message detected and skipped");
                commit(None).await;
                continue;
            }

            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            self.db.insert(&key, &now.to_be_bytes())?;

            if rand::random::<u8>() < 5 {
                // ~2% chance
                let db = self.db.clone();
                let ttl = self.ttl_seconds;
                tokio::spawn(async move {
                    let cutoff = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs()
                        - ttl;
                    for item in db.iter() {
                        if let Ok((key, val)) = item {
                            let timestamp = u64::from_be_bytes(val.as_ref().try_into().unwrap());
                            if timestamp < cutoff {
                                let _ = db.remove(key);
                            }
                        }
                    }
                });
            }

            return Ok((message, commit));
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
