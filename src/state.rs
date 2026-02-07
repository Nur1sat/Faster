use crate::{
    auth::{AuthContext, AuthManager, RateLimiter},
    config::Config,
    error::AppError,
    metadata::MetadataEngine,
    metrics::Metrics,
    replication::ReplicationManager,
    storage::StorageEngine,
    types::ObjectMetadata,
};
use bytes::Bytes;
use http::{HeaderMap, Method, Uri};
use moka::sync::Cache;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub storage: Arc<StorageEngine>,
    pub metadata: Arc<MetadataEngine>,
    pub auth: Arc<AuthManager>,
    pub rate_limiter: RateLimiter,
    pub metrics: Arc<Metrics>,
    pub replication: Arc<ReplicationManager>,
    pub object_cache: Cache<String, Bytes>,
    ready: Arc<AtomicBool>,
}

impl AppState {
    pub fn new(
        config: Arc<Config>,
        storage: Arc<StorageEngine>,
        metadata: Arc<MetadataEngine>,
        auth: Arc<AuthManager>,
        metrics: Arc<Metrics>,
        replication: Arc<ReplicationManager>,
    ) -> Self {
        Self {
            rate_limiter: RateLimiter::new(config.rate_limit_per_sec),
            object_cache: Cache::builder()
                .max_capacity(config.cache_capacity_bytes())
                .weigher(|_k: &String, v: &Bytes| -> u32 { v.len() as u32 })
                .build(),
            config,
            storage,
            metadata,
            auth,
            metrics,
            replication,
            ready: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Relaxed);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }

    pub async fn authorize_object_action(
        &self,
        headers: &HeaderMap,
        method: &Method,
        uri: &Uri,
        bucket: &str,
        key: &str,
        action: &str,
    ) -> Result<AuthContext, AppError> {
        let auth_ctx = self
            .auth
            .verify_signed_request(headers, method, uri.path(), uri.query())?;

        if !self.rate_limiter.allow(&auth_ctx.access_key) {
            return Err(AppError::RateLimited);
        }

        if !self
            .metadata
            .is_allowed(bucket, key, &auth_ctx.access_key, action)
            .await
        {
            return Err(AppError::Forbidden(
                "bucket policy denied the operation".to_string(),
            ));
        }

        Ok(auth_ctx)
    }

    pub fn replicate_object(&self, object: ObjectMetadata) {
        if self.replication.enabled() {
            self.replication
                .replicate_object_async(object, self.metrics.clone());
        }
    }

    pub fn replicate_delete(&self, bucket: String, key: String) {
        if self.replication.enabled() {
            self.replication
                .replicate_delete_async(bucket, key, self.metrics.clone());
        }
    }
}
