mod api;
mod auth;
mod config;
mod error;
mod metadata;
mod metrics;
mod replication;
mod state;
mod storage;
mod types;

use crate::{
    auth::AuthManager, config::Config, error::AppError, metadata::MetadataEngine, metrics::Metrics,
    replication::ReplicationManager, state::AppState, storage::StorageEngine,
};
use axum_server::tls_rustls::RustlsConfig;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::time::interval;
use tower::ServiceBuilder;
use tower_http::{compression::CompressionLayer, trace::TraceLayer};
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let config = Arc::new(Config::from_env());
    init_tracing(config.log_json);

    let storage = Arc::new(StorageEngine::new(&config).await?);
    let metadata = Arc::new(MetadataEngine::new(&config.data_dir.join("metadata")).await?);
    let auth = Arc::new(AuthManager::new(&config));
    let metrics = Arc::new(Metrics::new());
    let replication = Arc::new(ReplicationManager::new(&config)?);

    let state = AppState::new(
        config.clone(),
        storage.clone(),
        metadata.clone(),
        auth,
        metrics,
        replication,
    );

    spawn_background_workers(state.clone());

    let app = api::build_router(state.clone()).layer(
        ServiceBuilder::new()
            .layer(CompressionLayer::new())
            .layer(TraceLayer::new_for_http()),
    );

    state.mark_ready();

    let addr: SocketAddr = config.bind_addr.parse().map_err(|e| {
        AppError::BadRequest(format!("invalid bind address {}: {e}", config.bind_addr))
    })?;

    info!(bind_addr = %config.bind_addr, advertise_addr = %config.advertise_addr, "starting fasterstore");

    let handle = axum_server::Handle::new();
    let shutdown_handle = handle.clone();

    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            info!("received shutdown signal");
            shutdown_handle.graceful_shutdown(Some(Duration::from_secs(10)));
        }
    });

    if let (Some(cert_path), Some(key_path)) = (&config.tls_cert_path, &config.tls_key_path) {
        let rustls = RustlsConfig::from_pem_file(cert_path, key_path)
            .await
            .map_err(|e| AppError::Internal(format!("failed to load TLS cert/key: {e}")))?;

        axum_server::bind_rustls(addr, rustls)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .map_err(|e| AppError::Internal(format!("server error: {e}")))?;
    } else {
        axum_server::bind(addr)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .map_err(|e| AppError::Internal(format!("server error: {e}")))?;
    }

    Ok(())
}

fn init_tracing(json_logs: bool) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = fmt().with_env_filter(env_filter).with_target(false);

    if json_logs {
        builder.json().init();
    } else {
        builder.compact().init();
    }
}

fn spawn_background_workers(state: AppState) {
    let metadata_state = state.clone();
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(
            metadata_state.config.metadata_compact_interval_secs.max(5),
        ));

        loop {
            ticker.tick().await;
            if let Err(err) = metadata_state.metadata.compact_snapshot().await {
                warn!(error = %err, "metadata compaction failed");
            }
        }
    });

    let housekeeping_state = state.clone();
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(
            housekeeping_state
                .config
                .storage_compact_interval_secs
                .max(10),
        ));

        loop {
            ticker.tick().await;

            match housekeeping_state
                .metadata
                .cleanup_stale_multipart(housekeeping_state.config.multipart_ttl_secs)
                .await
            {
                Ok(cleaned) if cleaned > 0 => {
                    info!(cleaned, "cleaned stale multipart uploads");
                }
                Ok(_) => {}
                Err(err) => warn!(error = %err, "stale multipart cleanup failed"),
            }

            housekeeping_state
                .metrics
                .multipart_uploads_active
                .set(housekeeping_state.metadata.multipart_count().await as i64);

            if let Err(err) = compact_storage_once(&housekeeping_state).await {
                error!(error = %err, "storage compaction pass failed");
            }
        }
    });
}

async fn compact_storage_once(state: &AppState) -> Result<(), AppError> {
    let live = state.metadata.live_blobs_by_segment().await;
    let active_segment = state.storage.active_segment_id().await;

    for (segment_id, blobs) in live {
        if segment_id == active_segment {
            continue;
        }

        let path = state.storage.segment_path(segment_id);
        let Ok(meta) = std::fs::metadata(&path) else {
            continue;
        };

        let total_bytes = meta.len();
        if total_bytes == 0 {
            continue;
        }

        let live_bytes = blobs.iter().map(|b| b.location.length).sum::<u64>();
        let live_ratio = live_bytes as f64 / total_bytes as f64;

        if live_ratio >= 0.80 {
            continue;
        }

        let mut relocations = Vec::with_capacity(blobs.len());
        for blob in blobs {
            let new_location = state
                .storage
                .rewrite_blob(&blob.hash, &blob.location)
                .await?;
            relocations.push((blob.hash.clone(), new_location));
        }

        if !relocations.is_empty() {
            let updated = state.metadata.update_blob_locations(relocations).await?;
            info!(segment_id, updated, live_ratio, "compacted cold segment");
            state.storage.remove_segment(segment_id).await?;
            state.metrics.storage_compactions_total.inc();
        }
    }

    Ok(())
}
