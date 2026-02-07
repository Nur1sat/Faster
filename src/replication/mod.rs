use crate::{
    config::Config,
    error::AppError,
    metrics::Metrics,
    types::{ReplicateDeleteRequest, ReplicateObjectRequest},
};
use reqwest::StatusCode;
use std::{sync::Arc, time::Duration};
use tracing::{error, warn};

#[derive(Clone)]
pub struct ReplicationManager {
    peers: Vec<String>,
    advertise_addr: String,
    internal_token: String,
    client: reqwest::Client,
}

impl ReplicationManager {
    pub fn new(cfg: &Config) -> Result<Self, AppError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(cfg.request_timeout_secs))
            .build()
            .map_err(|e| {
                AppError::Internal(format!("failed to create replication HTTP client: {e}"))
            })?;

        Ok(Self {
            peers: cfg.replication_peers.clone(),
            advertise_addr: cfg.advertise_addr.clone(),
            internal_token: cfg.internal_token.clone(),
            client,
        })
    }

    pub fn enabled(&self) -> bool {
        !self.peers.is_empty()
    }

    pub fn replicate_object_async(
        &self,
        object: crate::types::ObjectMetadata,
        metrics: Arc<Metrics>,
    ) {
        if self.peers.is_empty() {
            return;
        }

        let this = self.clone();
        tokio::spawn(async move {
            if let Err(err) = this.replicate_object(object).await {
                metrics.replication_failures_total.inc();
                error!(error = %err, "object replication failed");
            }
        });
    }

    pub fn replicate_delete_async(&self, bucket: String, key: String, metrics: Arc<Metrics>) {
        if self.peers.is_empty() {
            return;
        }

        let this = self.clone();
        tokio::spawn(async move {
            if let Err(err) = this.replicate_delete(bucket, key).await {
                metrics.replication_failures_total.inc();
                error!(error = %err, "delete replication failed");
            }
        });
    }

    async fn replicate_object(&self, object: crate::types::ObjectMetadata) -> Result<(), AppError> {
        let req = ReplicateObjectRequest {
            source: self.advertise_addr.clone(),
            object,
        };

        for peer in &self.peers {
            let url = format!("{}/_internal/replicate/object", peer.trim_end_matches('/'));
            let res = self
                .client
                .post(url)
                .header("x-fs-internal-token", &self.internal_token)
                .json(&req)
                .send()
                .await
                .map_err(|e| AppError::Replication(e.to_string()))?;

            if res.status() != StatusCode::OK {
                warn!(status = %res.status(), peer, "replication peer rejected object event");
            }
        }

        Ok(())
    }

    async fn replicate_delete(&self, bucket: String, key: String) -> Result<(), AppError> {
        let req = ReplicateDeleteRequest { bucket, key };

        for peer in &self.peers {
            let url = format!("{}/_internal/replicate/delete", peer.trim_end_matches('/'));
            let res = self
                .client
                .post(url)
                .header("x-fs-internal-token", &self.internal_token)
                .json(&req)
                .send()
                .await
                .map_err(|e| AppError::Replication(e.to_string()))?;

            if res.status() != StatusCode::OK {
                warn!(status = %res.status(), peer, "replication peer rejected delete event");
            }
        }

        Ok(())
    }

    pub async fn fetch_blob_from_source(
        &self,
        source: &str,
        hash: &str,
    ) -> Result<bytes::Bytes, AppError> {
        let url = format!("{}/_internal/blob/{}", source.trim_end_matches('/'), hash);
        let response = self
            .client
            .get(url)
            .header("x-fs-internal-token", &self.internal_token)
            .send()
            .await
            .map_err(|e| AppError::Replication(format!("fetch blob from source failed: {e}")))?;

        if !response.status().is_success() {
            return Err(AppError::Replication(format!(
                "source node returned {} while fetching blob",
                response.status()
            )));
        }

        response
            .bytes()
            .await
            .map_err(|e| AppError::Replication(format!("failed to read source blob body: {e}")))
    }
}
