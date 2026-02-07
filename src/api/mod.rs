use crate::{
    error::AppError,
    state::AppState,
    types::{BucketPolicy, ObjectSegment, ReplicateDeleteRequest, ReplicateObjectRequest},
};
use async_stream::try_stream;
use axum::{
    Router,
    body::Body,
    extract::{Json, MatchedPath, OriginalUri, Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, Uri},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use futures_util::StreamExt;
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tracing::{error, warn};

#[derive(Debug, Deserialize)]
struct ObjectPath {
    bucket: String,
    key: String,
}

#[derive(Debug, Default, Deserialize)]
struct MultipartParams {
    #[serde(rename = "uploadId")]
    upload_id: Option<String>,
    #[serde(rename = "partNumber")]
    part_number: Option<u32>,
}

#[derive(Debug)]
struct MultipartTarget {
    bucket: String,
    key: String,
    upload_id: String,
    part_number: u32,
}

#[derive(Debug, Serialize)]
struct HealthResponse<'a> {
    status: &'a str,
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route(
            "/_admin/policy/{bucket}",
            get(get_bucket_policy).put(set_bucket_policy),
        )
        .route("/_internal/blob/{hash}", get(internal_get_blob))
        .route(
            "/_internal/replicate/object",
            post(internal_replicate_object),
        )
        .route(
            "/_internal/replicate/delete",
            post(internal_replicate_delete),
        )
        .route(
            "/{bucket}/{*key}",
            put(put_object_or_part)
                .get(get_object)
                .delete(delete_object_or_abort)
                .post(post_object),
        )
        .layer(middleware::from_fn_with_state(
            state.clone(),
            metrics_middleware,
        ))
        .with_state(state)
}

async fn metrics_middleware(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let started = Instant::now();
    let method = req.method().clone();
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_string())
        .unwrap_or_else(|| "<unknown>".to_string());

    let response = next.run(req).await;
    let status = response.status();

    state
        .metrics
        .requests_total
        .with_label_values(&[method.as_str(), &route, status.as_str()])
        .inc();
    state
        .metrics
        .request_latency_seconds
        .with_label_values(&[method.as_str(), &route])
        .observe(started.elapsed().as_secs_f64());

    response
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, Json(HealthResponse { status: "ok" }))
}

async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    if state.is_ready() {
        (StatusCode::OK, Json(HealthResponse { status: "ready" })).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthResponse {
                status: "initializing",
            }),
        )
            .into_response()
    }
}

async fn metrics(State(state): State<AppState>) -> Result<Response, AppError> {
    let text = state
        .metrics
        .encode()
        .map_err(|e| AppError::Internal(format!("metrics encode failed: {e}")))?;

    let mut response = Response::new(Body::from(text));
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4"),
    );
    Ok(response)
}

async fn put_object_or_part(
    State(state): State<AppState>,
    Path(path): Path<ObjectPath>,
    Query(params): Query<MultipartParams>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    body: Body,
) -> Result<Response, AppError> {
    let bucket = path.bucket;
    let key = decode_key(&path.key);

    match (params.upload_id.as_deref(), params.part_number) {
        (Some(upload_id), Some(part_number)) => {
            put_multipart_part(
                state,
                headers,
                uri,
                MultipartTarget {
                    bucket,
                    key,
                    upload_id: upload_id.to_string(),
                    part_number,
                },
                body,
            )
            .await
        }
        _ => put_object(state, headers, uri, bucket, key, body).await,
    }
}

async fn post_object(
    State(state): State<AppState>,
    Path(path): Path<ObjectPath>,
    Query(params): Query<MultipartParams>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    body: Body,
) -> Result<Response, AppError> {
    let bucket = path.bucket;
    let key = decode_key(&path.key);

    if query_has_flag(&uri, "uploads") {
        return initiate_multipart(state, headers, uri, bucket, key).await;
    }

    if let Some(upload_id) = params.upload_id {
        return complete_multipart(state, headers, uri, bucket, key, upload_id).await;
    }

    let _ = body;
    Err(AppError::BadRequest(
        "invalid POST operation for object path".to_string(),
    ))
}

async fn delete_object_or_abort(
    State(state): State<AppState>,
    Path(path): Path<ObjectPath>,
    Query(params): Query<MultipartParams>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, AppError> {
    let bucket = path.bucket;
    let key = decode_key(&path.key);

    if let Some(upload_id) = params.upload_id {
        return abort_multipart(state, headers, uri, bucket, key, upload_id).await;
    }

    delete_object(state, headers, uri, bucket, key).await
}

async fn get_object(
    State(state): State<AppState>,
    Path(path): Path<ObjectPath>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, AppError> {
    let bucket = path.bucket;
    let key = decode_key(&path.key);

    state
        .authorize_object_action(&headers, &Method::GET, &uri, &bucket, &key, "GetObject")
        .await?;

    let object = state
        .metadata
        .object(&bucket, &key)
        .await
        .ok_or_else(|| AppError::NotFound("object not found".to_string()))?;

    let mut blob_locations = Vec::with_capacity(object.segments.len());
    for segment in &object.segments {
        let blob = state.metadata.blob(&segment.hash).await.ok_or_else(|| {
            AppError::Internal(format!("blob {} missing for object", segment.hash))
        })?;
        blob_locations.push((segment.hash.clone(), blob.location));
    }

    if object.segments.len() == 1 {
        let (hash, location) = &blob_locations[0];

        if let Some(cached) = state.object_cache.get(hash) {
            state.metrics.cache_hits_total.inc();
            state.metrics.bytes_out_total.inc_by(cached.len() as u64);
            return Ok(object_response(
                Body::from(cached),
                StatusCode::OK,
                &object.etag,
                object.size,
                object.content_type.as_deref(),
            ));
        }

        state.metrics.cache_misses_total.inc();

        if !location.encrypted {
            let stream = state
                .storage
                .stream_blob_raw(location.clone())
                .map(map_stream_error);
            state.metrics.bytes_out_total.inc_by(object.size);
            return Ok(object_response(
                Body::from_stream(stream),
                StatusCode::OK,
                &object.etag,
                object.size,
                object.content_type.as_deref(),
            ));
        }
    }

    let storage = state.storage.clone();
    let stream = try_stream! {
        for (_, location) in blob_locations {
            let bytes = storage.read_blob_plain(&location).await?;
            yield bytes;
        }
    }
    .map(map_stream_error);

    state.metrics.bytes_out_total.inc_by(object.size);

    Ok(object_response(
        Body::from_stream(stream),
        StatusCode::OK,
        &object.etag,
        object.size,
        object.content_type.as_deref(),
    ))
}

async fn put_object(
    state: AppState,
    headers: HeaderMap,
    uri: Uri,
    bucket: String,
    key: String,
    body: Body,
) -> Result<Response, AppError> {
    let auth = state
        .authorize_object_action(&headers, &Method::PUT, &uri, &bucket, &key, "PutObject")
        .await?;

    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(ToOwned::to_owned);

    let ingested = ingest_body(&state, body).await?;
    verify_payload_hash(&auth.payload_hash, &ingested.body_sha256)?;

    let blob_hash = ingested.hash.clone();
    let blob_size = ingested.size;
    let cache_candidate = ingested.cache_candidate.clone();

    if !state.metadata.blob_exists(&blob_hash).await {
        let location = state.storage.append_ingested(ingested).await?;
        state
            .metadata
            .upsert_blob_if_absent(blob_hash.clone(), blob_size, location)
            .await?;
    } else {
        cleanup_ingested_if_needed(ingested).await;
    }

    if let Some(bytes) = cache_candidate {
        maybe_cache(&state, &blob_hash, bytes);
    }

    let object = state
        .metadata
        .put_object(
            bucket,
            key,
            content_type,
            blob_hash.clone(),
            vec![ObjectSegment {
                hash: blob_hash.clone(),
                size: blob_size,
            }],
        )
        .await?;

    state.replicate_object(object.clone());

    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        "etag",
        HeaderValue::from_str(&format!("\"{}\"", object.etag))
            .unwrap_or_else(|_| HeaderValue::from_static("invalid-etag")),
    );
    Ok(response)
}

async fn delete_object(
    state: AppState,
    headers: HeaderMap,
    uri: Uri,
    bucket: String,
    key: String,
) -> Result<Response, AppError> {
    state
        .authorize_object_action(
            &headers,
            &Method::DELETE,
            &uri,
            &bucket,
            &key,
            "DeleteObject",
        )
        .await?;

    let deleted = state.metadata.delete_object(&bucket, &key).await?;
    if deleted.is_none() {
        return Err(AppError::NotFound("object not found".to_string()));
    }

    state.replicate_delete(bucket, key);

    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NO_CONTENT;
    Ok(response)
}

async fn initiate_multipart(
    state: AppState,
    headers: HeaderMap,
    uri: Uri,
    bucket: String,
    key: String,
) -> Result<Response, AppError> {
    let auth = state
        .authorize_object_action(
            &headers,
            &Method::POST,
            &uri,
            &bucket,
            &key,
            "CreateMultipartUpload",
        )
        .await?;

    let upload = state
        .metadata
        .create_multipart(bucket.clone(), key.clone(), auth.access_key)
        .await?;

    state
        .metrics
        .multipart_uploads_active
        .set(state.metadata.multipart_count().await as i64);

    let xml = format!(
        "<InitiateMultipartUploadResult><Bucket>{bucket}</Bucket><Key>{key}</Key><UploadId>{}</UploadId></InitiateMultipartUploadResult>",
        upload.upload_id
    );

    xml_response(StatusCode::OK, xml)
}

async fn put_multipart_part(
    state: AppState,
    headers: HeaderMap,
    uri: Uri,
    target: MultipartTarget,
    body: Body,
) -> Result<Response, AppError> {
    let bucket = target.bucket;
    let key = target.key;
    let upload_id = target.upload_id;
    let part_number = target.part_number;

    let auth = state
        .authorize_object_action(&headers, &Method::PUT, &uri, &bucket, &key, "UploadPart")
        .await?;

    let ingested = ingest_body(&state, body).await?;
    verify_payload_hash(&auth.payload_hash, &ingested.body_sha256)?;

    let blob_hash = ingested.hash.clone();
    let blob_size = ingested.size;

    if !state.metadata.blob_exists(&blob_hash).await {
        let location = state.storage.append_ingested(ingested).await?;
        state
            .metadata
            .upsert_blob_if_absent(blob_hash.clone(), blob_size, location)
            .await?;
    } else {
        cleanup_ingested_if_needed(ingested).await;
    }

    let part = state
        .metadata
        .upload_part(
            &upload_id,
            part_number,
            blob_hash.clone(),
            blob_size,
            blob_hash.clone(),
        )
        .await?;

    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        "etag",
        HeaderValue::from_str(&format!("\"{}\"", part.etag))
            .unwrap_or_else(|_| HeaderValue::from_static("invalid-etag")),
    );
    Ok(response)
}

async fn complete_multipart(
    state: AppState,
    headers: HeaderMap,
    uri: Uri,
    bucket: String,
    key: String,
    upload_id: String,
) -> Result<Response, AppError> {
    state
        .authorize_object_action(
            &headers,
            &Method::POST,
            &uri,
            &bucket,
            &key,
            "CompleteMultipartUpload",
        )
        .await?;

    let object = state
        .metadata
        .complete_multipart(&upload_id, Some("application/octet-stream".to_string()))
        .await?;

    state
        .metrics
        .multipart_uploads_active
        .set(state.metadata.multipart_count().await as i64);

    state.replicate_object(object.clone());

    let xml = format!(
        "<CompleteMultipartUploadResult><Bucket>{}</Bucket><Key>{}</Key><ETag>\"{}\"</ETag></CompleteMultipartUploadResult>",
        object.bucket, object.key, object.etag
    );

    xml_response(StatusCode::OK, xml)
}

async fn abort_multipart(
    state: AppState,
    headers: HeaderMap,
    uri: Uri,
    bucket: String,
    key: String,
    upload_id: String,
) -> Result<Response, AppError> {
    state
        .authorize_object_action(
            &headers,
            &Method::DELETE,
            &uri,
            &bucket,
            &key,
            "AbortMultipartUpload",
        )
        .await?;

    if !state.metadata.abort_multipart(&upload_id).await? {
        return Err(AppError::NotFound("upload_id not found".to_string()));
    }

    state
        .metrics
        .multipart_uploads_active
        .set(state.metadata.multipart_count().await as i64);

    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NO_CONTENT;
    Ok(response)
}

async fn set_bucket_policy(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    Json(policy): Json<BucketPolicy>,
) -> Result<Response, AppError> {
    let auth = state
        .auth
        .verify_signed_request(&headers, &Method::PUT, uri.path(), uri.query())?;

    if !auth.is_admin {
        return Err(AppError::Forbidden("admin access required".to_string()));
    }

    if !state.rate_limiter.allow(&auth.access_key) {
        return Err(AppError::RateLimited);
    }

    state.metadata.set_policy(bucket, policy).await?;
    Ok(Response::new(Body::empty()))
}

async fn get_bucket_policy(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Result<Response, AppError> {
    let auth = state
        .auth
        .verify_signed_request(&headers, &Method::GET, uri.path(), uri.query())?;

    if !auth.is_admin {
        return Err(AppError::Forbidden("admin access required".to_string()));
    }

    if !state.rate_limiter.allow(&auth.access_key) {
        return Err(AppError::RateLimited);
    }

    let policy = state.metadata.policy(&bucket).await.unwrap_or_default();

    let body = serde_json::to_vec_pretty(&policy)?;
    let mut response = Response::new(Body::from(body));
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    Ok(response)
}

async fn internal_get_blob(
    State(state): State<AppState>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    state.auth.verify_internal(&headers)?;

    let blob = state
        .metadata
        .blob(&hash)
        .await
        .ok_or_else(|| AppError::NotFound("blob not found".to_string()))?;

    let bytes = state.storage.read_blob_plain(&blob.location).await?;

    let mut response = Response::new(Body::from(bytes));
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    Ok(response)
}

async fn internal_replicate_object(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ReplicateObjectRequest>,
) -> Result<Response, AppError> {
    state.auth.verify_internal(&headers)?;

    for segment in &req.object.segments {
        if state.metadata.blob_exists(&segment.hash).await {
            continue;
        }

        let bytes = state
            .replication
            .fetch_blob_from_source(&req.source, &segment.hash)
            .await?;

        let computed_hash = blake3::hash(&bytes).to_hex().to_string();
        if computed_hash != segment.hash {
            return Err(AppError::Replication(format!(
                "source blob hash mismatch for {}",
                segment.hash
            )));
        }

        let location = state
            .storage
            .append_plain_blob(&segment.hash, bytes)
            .await?;

        state
            .metadata
            .upsert_blob_if_absent(segment.hash.clone(), segment.size, location)
            .await?;
    }

    state
        .metadata
        .put_object(
            req.object.bucket,
            req.object.key,
            req.object.content_type,
            req.object.etag,
            req.object.segments,
        )
        .await?;

    Ok(Response::new(Body::empty()))
}

async fn internal_replicate_delete(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ReplicateDeleteRequest>,
) -> Result<Response, AppError> {
    state.auth.verify_internal(&headers)?;
    let _ = state.metadata.delete_object(&req.bucket, &req.key).await?;
    Ok(Response::new(Body::empty()))
}

async fn ingest_body(
    state: &AppState,
    body: Body,
) -> Result<crate::storage::IngestedBlob, AppError> {
    let mut ingest = state
        .storage
        .begin_ingest(state.config.small_object_threshold_bytes);

    let mut stream = body.into_data_stream();
    let mut total: usize = 0;

    while let Some(next) = stream.next().await {
        let bytes =
            next.map_err(|e| AppError::BadRequest(format!("request body read error: {e}")))?;
        total += bytes.len();

        if total > state.config.max_request_bytes {
            return Err(AppError::BadRequest(format!(
                "request exceeds configured max payload of {} bytes",
                state.config.max_request_bytes
            )));
        }

        ingest.push(&bytes).await?;
    }

    state.metrics.bytes_in_total.inc_by(total as u64);

    ingest.finish().await
}

fn verify_payload_hash(expected: &str, actual: &str) -> Result<(), AppError> {
    if expected.eq_ignore_ascii_case("UNSIGNED-PAYLOAD") {
        return Ok(());
    }

    if expected.eq_ignore_ascii_case(actual) {
        Ok(())
    } else {
        Err(AppError::Unauthorized(
            "x-fs-content-sha256 does not match payload".to_string(),
        ))
    }
}

fn maybe_cache(state: &AppState, hash: &str, bytes: bytes::Bytes) {
    if bytes.len() <= state.config.cache_object_max_bytes {
        state.object_cache.insert(hash.to_string(), bytes);
    }
}

async fn cleanup_ingested_if_needed(ingested: crate::storage::IngestedBlob) {
    if let crate::storage::IngestedPayload::TempFile(path) = ingested.payload
        && let Err(err) = tokio::fs::remove_file(&path).await
    {
        warn!(error = %err, path = %path.display(), "failed to remove temp ingest file");
    }
}

fn object_response(
    body: Body,
    status: StatusCode,
    etag: &str,
    size: u64,
    content_type: Option<&str>,
) -> Response {
    let mut response = Response::new(body);
    *response.status_mut() = status;

    response.headers_mut().insert(
        "etag",
        HeaderValue::from_str(&format!("\"{}\"", etag))
            .unwrap_or_else(|_| HeaderValue::from_static("invalid-etag")),
    );
    response.headers_mut().insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_str(&size.to_string()).unwrap_or_else(|_| HeaderValue::from_static("0")),
    );

    if let Some(ct) = content_type
        && let Ok(header) = HeaderValue::from_str(ct)
    {
        response
            .headers_mut()
            .insert(axum::http::header::CONTENT_TYPE, header);
    }

    response
}

fn xml_response(status: StatusCode, xml: String) -> Result<Response, AppError> {
    let mut response = Response::new(Body::from(xml));
    *response.status_mut() = status;
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    Ok(response)
}

fn decode_key(raw: &str) -> String {
    let trimmed = raw.trim_start_matches('/');
    percent_decode_str(trimmed).decode_utf8_lossy().into_owned()
}

fn query_has_flag(uri: &Uri, key: &str) -> bool {
    let Some(q) = uri.query() else {
        return false;
    };
    q.split('&')
        .any(|part| part == key || part.starts_with(&format!("{key}=")))
}

fn map_stream_error(item: Result<bytes::Bytes, AppError>) -> Result<bytes::Bytes, std::io::Error> {
    item.map_err(|e| {
        error!(error = %e, "streaming response failed");
        std::io::Error::other(e.to_string())
    })
}
