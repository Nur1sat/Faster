use crate::{
    error::AppError,
    types::{
        BlobLocation, BlobMetadata, BucketPolicy, MultipartPart, MultipartUpload, ObjectMetadata,
        ObjectSegment, PolicyEffect, object_id,
    },
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    fs::{self, File, OpenOptions},
    io::AsyncWriteExt,
    sync::{Mutex, RwLock},
};
use tracing::{info, warn};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct MetadataState {
    objects: HashMap<String, ObjectMetadata>,
    blobs: HashMap<String, BlobMetadata>,
    multiparts: HashMap<String, MultipartUpload>,
    policies: HashMap<String, BucketPolicy>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TxnEvent {
    ts_unix: i64,
    changes: Vec<Change>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Change {
    UpsertObject {
        key: String,
        object: ObjectMetadata,
    },
    DeleteObject {
        key: String,
    },
    UpsertBlob {
        hash: String,
        blob: BlobMetadata,
    },
    DeleteBlob {
        hash: String,
    },
    UpsertMultipart {
        upload_id: String,
        upload: MultipartUpload,
    },
    DeleteMultipart {
        upload_id: String,
    },
    UpsertPolicy {
        bucket: String,
        policy: BucketPolicy,
    },
    DeletePolicy {
        bucket: String,
    },
}

#[derive(Clone)]
pub struct MetadataEngine {
    state: Arc<RwLock<MetadataState>>,
    wal_path: PathBuf,
    snapshot_path: PathBuf,
    wal_file: Arc<Mutex<File>>,
    commit_lock: Arc<Mutex<()>>,
}

impl MetadataEngine {
    pub async fn new(root: &Path) -> Result<Self, AppError> {
        fs::create_dir_all(root).await?;
        let wal_path = root.join("wal.log");
        let snapshot_path = root.join("snapshot.json");

        let mut state = if snapshot_path.exists() {
            let bytes = fs::read(&snapshot_path).await?;
            serde_json::from_slice::<MetadataState>(&bytes).unwrap_or_default()
        } else {
            MetadataState::default()
        };

        if wal_path.exists() {
            let wal_text = fs::read_to_string(&wal_path).await.unwrap_or_default();
            for (i, line) in wal_text.lines().enumerate() {
                if line.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<TxnEvent>(line) {
                    Ok(txn) => apply_changes(&mut state, txn.changes),
                    Err(err) => {
                        warn!(line_no = i + 1, error = %err, "failed to parse WAL line; skipping")
                    }
                }
            }
        }

        let wal_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&wal_path)
            .await?;

        info!(
            objects = state.objects.len(),
            blobs = state.blobs.len(),
            multiparts = state.multiparts.len(),
            "metadata engine loaded"
        );

        Ok(Self {
            state: Arc::new(RwLock::new(state)),
            wal_path,
            snapshot_path,
            wal_file: Arc::new(Mutex::new(wal_file)),
            commit_lock: Arc::new(Mutex::new(())),
        })
    }

    pub async fn object(&self, bucket: &str, key: &str) -> Option<ObjectMetadata> {
        let state = self.state.read().await;
        state.objects.get(&object_id(bucket, key)).cloned()
    }

    pub async fn blob(&self, hash: &str) -> Option<BlobMetadata> {
        let state = self.state.read().await;
        state.blobs.get(hash).cloned()
    }

    pub async fn blob_exists(&self, hash: &str) -> bool {
        let state = self.state.read().await;
        state.blobs.contains_key(hash)
    }

    pub async fn upsert_blob_if_absent(
        &self,
        hash: String,
        size: u64,
        location: BlobLocation,
    ) -> Result<(), AppError> {
        self.commit(|state| {
            if state.blobs.contains_key(&hash) {
                return Ok(((), Vec::new()));
            }

            let blob = BlobMetadata {
                hash: hash.clone(),
                size,
                location,
                ref_count: 0,
                created_at: Utc::now(),
            };

            state.blobs.insert(hash.clone(), blob.clone());
            Ok(((), vec![Change::UpsertBlob { hash, blob }]))
        })
        .await
    }

    pub async fn put_object(
        &self,
        bucket: String,
        key: String,
        content_type: Option<String>,
        etag: String,
        segments: Vec<ObjectSegment>,
    ) -> Result<ObjectMetadata, AppError> {
        self.commit(move |state| {
            if segments.is_empty() {
                return Err(AppError::BadRequest(
                    "object must contain at least one segment".to_string(),
                ));
            }

            let size = segments.iter().map(|s| s.size).sum::<u64>();
            let obj = ObjectMetadata {
                bucket: bucket.clone(),
                key: key.clone(),
                size,
                etag,
                content_type,
                last_modified: Utc::now(),
                segments: segments.clone(),
            };

            let k = object_id(&bucket, &key);
            let mut changes = Vec::new();

            if let Some(previous) = state.objects.get(&k).cloned() {
                for segment in previous.segments {
                    dec_blob_ref(state, &segment.hash, &mut changes);
                }
            }

            for segment in &segments {
                inc_blob_ref(state, &segment.hash, &mut changes)?;
            }

            state.objects.insert(k.clone(), obj.clone());
            changes.push(Change::UpsertObject {
                key: k,
                object: obj.clone(),
            });

            Ok((obj, changes))
        })
        .await
    }

    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<ObjectMetadata>, AppError> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        self.commit(move |state| {
            let k = object_id(&bucket, &key);
            let mut changes = Vec::new();
            let previous = state.objects.remove(&k);

            if let Some(obj) = &previous {
                for segment in &obj.segments {
                    dec_blob_ref(state, &segment.hash, &mut changes);
                }
                changes.push(Change::DeleteObject { key: k });
            }

            Ok((previous, changes))
        })
        .await
    }

    pub async fn create_multipart(
        &self,
        bucket: String,
        key: String,
        initiated_by: String,
    ) -> Result<MultipartUpload, AppError> {
        self.commit(move |state| {
            let upload_id = uuid::Uuid::new_v4().to_string();
            let upload = MultipartUpload {
                upload_id: upload_id.clone(),
                bucket,
                key,
                created_at: Utc::now(),
                parts: Default::default(),
                initiated_by,
            };
            state.multiparts.insert(upload_id.clone(), upload.clone());
            Ok((
                upload.clone(),
                vec![Change::UpsertMultipart { upload_id, upload }],
            ))
        })
        .await
    }

    pub async fn upload_part(
        &self,
        upload_id: &str,
        part_number: u32,
        hash: String,
        size: u64,
        etag: String,
    ) -> Result<MultipartPart, AppError> {
        let upload_id = upload_id.to_string();

        self.commit(move |state| {
            let mut upload = state
                .multiparts
                .remove(&upload_id)
                .ok_or_else(|| AppError::NotFound("upload_id not found".to_string()))?;

            inc_blob_ref(state, &hash, &mut Vec::new())?;

            let mut changes = Vec::new();

            let part = MultipartPart {
                part_number,
                hash: hash.clone(),
                size,
                etag,
                uploaded_at: Utc::now(),
            };

            if let Some(previous) = upload.parts.insert(part_number, part.clone()) {
                dec_blob_ref(state, &previous.hash, &mut changes);
            }

            if let Some(blob) = state.blobs.get(&hash).cloned() {
                changes.push(Change::UpsertBlob {
                    hash: hash.clone(),
                    blob,
                });
            }

            state.multiparts.insert(upload_id.clone(), upload.clone());
            changes.push(Change::UpsertMultipart { upload_id, upload });

            Ok((part, changes))
        })
        .await
    }

    pub async fn complete_multipart(
        &self,
        upload_id: &str,
        content_type: Option<String>,
    ) -> Result<ObjectMetadata, AppError> {
        let upload_id = upload_id.to_string();
        self.commit(move |state| {
            let upload = state
                .multiparts
                .remove(&upload_id)
                .ok_or_else(|| AppError::NotFound("upload_id not found".to_string()))?;

            if upload.parts.is_empty() {
                return Err(AppError::BadRequest(
                    "multipart upload has no parts".to_string(),
                ));
            }

            let segments = upload
                .parts
                .values()
                .map(|p| ObjectSegment {
                    hash: p.hash.clone(),
                    size: p.size,
                })
                .collect::<Vec<_>>();

            let mut etag_hasher = blake3::Hasher::new();
            for part in upload.parts.values() {
                etag_hasher.update(part.etag.as_bytes());
            }
            let etag = etag_hasher.finalize().to_hex().to_string();

            let mut changes = vec![Change::DeleteMultipart {
                upload_id: upload_id.clone(),
            }];

            let object = apply_put_object(
                state,
                upload.bucket,
                upload.key,
                content_type,
                etag,
                segments,
                &mut changes,
            )?;

            for part in upload.parts.values() {
                dec_blob_ref(state, &part.hash, &mut changes);
            }

            Ok((object, changes))
        })
        .await
    }

    pub async fn abort_multipart(&self, upload_id: &str) -> Result<bool, AppError> {
        let upload_id = upload_id.to_string();
        self.commit(move |state| {
            let mut changes = Vec::new();
            let upload = state.multiparts.remove(&upload_id);
            if let Some(upload) = upload {
                for part in upload.parts.values() {
                    dec_blob_ref(state, &part.hash, &mut changes);
                }
                changes.push(Change::DeleteMultipart { upload_id });
                Ok((true, changes))
            } else {
                Ok((false, changes))
            }
        })
        .await
    }

    pub async fn set_policy(&self, bucket: String, policy: BucketPolicy) -> Result<(), AppError> {
        self.commit(move |state| {
            state.policies.insert(bucket.clone(), policy.clone());
            Ok(((), vec![Change::UpsertPolicy { bucket, policy }]))
        })
        .await
    }

    pub async fn policy(&self, bucket: &str) -> Option<BucketPolicy> {
        let state = self.state.read().await;
        state.policies.get(bucket).cloned()
    }

    pub async fn is_allowed(&self, bucket: &str, key: &str, principal: &str, action: &str) -> bool {
        let state = self.state.read().await;
        let Some(policy) = state.policies.get(bucket) else {
            return true;
        };

        let mut explicit_allow = false;
        for rule in &policy.rules {
            let principal_match = rule.principal == "*" || rule.principal == principal;
            let action_match = rule.action == "*" || rule.action.eq_ignore_ascii_case(action);
            let prefix_match = key.starts_with(&rule.prefix);

            if !(principal_match && action_match && prefix_match) {
                continue;
            }

            match rule.effect {
                PolicyEffect::Deny => return false,
                PolicyEffect::Allow => explicit_allow = true,
            }
        }

        if explicit_allow {
            return true;
        }

        matches!(policy.default_effect, PolicyEffect::Allow)
    }

    pub async fn live_blobs_by_segment(&self) -> HashMap<u64, Vec<BlobMetadata>> {
        let state = self.state.read().await;
        let mut grouped: HashMap<u64, Vec<BlobMetadata>> = HashMap::new();
        for blob in state.blobs.values() {
            if blob.ref_count == 0 {
                continue;
            }
            grouped
                .entry(blob.location.segment_id)
                .or_default()
                .push(blob.clone());
        }
        grouped
    }

    pub async fn update_blob_locations(
        &self,
        relocations: Vec<(String, BlobLocation)>,
    ) -> Result<usize, AppError> {
        self.commit(move |state| {
            let mut changes = Vec::new();
            let mut count = 0_usize;

            for (hash, location) in relocations {
                if let Some(blob) = state.blobs.get_mut(&hash) {
                    blob.location = location;
                    changes.push(Change::UpsertBlob {
                        hash: hash.clone(),
                        blob: blob.clone(),
                    });
                    count += 1;
                }
            }

            Ok((count, changes))
        })
        .await
    }

    pub async fn cleanup_stale_multipart(&self, ttl_secs: u64) -> Result<usize, AppError> {
        let cutoff = Utc::now() - chrono::Duration::seconds(ttl_secs as i64);
        self.commit(move |state| {
            let mut changes = Vec::new();
            let stale_ids = state
                .multiparts
                .iter()
                .filter(|(_, upload)| upload.created_at < cutoff)
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();

            for id in &stale_ids {
                if let Some(upload) = state.multiparts.remove(id) {
                    for part in upload.parts.values() {
                        dec_blob_ref(state, &part.hash, &mut changes);
                    }
                    changes.push(Change::DeleteMultipart {
                        upload_id: id.clone(),
                    });
                }
            }

            Ok((stale_ids.len(), changes))
        })
        .await
    }

    pub async fn multipart_count(&self) -> usize {
        self.state.read().await.multiparts.len()
    }

    pub async fn compact_snapshot(&self) -> Result<(), AppError> {
        let _commit_guard = self.commit_lock.lock().await;

        let snapshot = {
            let state = self.state.read().await;
            state.clone()
        };

        let tmp_path = self.snapshot_path.with_extension("json.tmp");
        let bytes = serde_json::to_vec(&snapshot)?;
        fs::write(&tmp_path, bytes).await?;
        fs::rename(&tmp_path, &self.snapshot_path).await?;

        {
            let mut wal = self.wal_file.lock().await;
            *wal = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&self.wal_path)
                .await?;
        }

        info!("metadata snapshot compaction complete");
        Ok(())
    }

    async fn commit<F, R>(&self, f: F) -> Result<R, AppError>
    where
        F: FnOnce(&mut MetadataState) -> Result<(R, Vec<Change>), AppError>,
    {
        let _commit_guard = self.commit_lock.lock().await;
        let mut state = self.state.write().await;
        let (result, changes) = f(&mut state)?;

        if !changes.is_empty() {
            let event = TxnEvent {
                ts_unix: Utc::now().timestamp(),
                changes,
            };
            let line = serde_json::to_vec(&event)?;
            let mut wal = self.wal_file.lock().await;
            wal.write_all(&line).await?;
            wal.write_all(b"\n").await?;
            wal.flush().await?;
        }

        Ok(result)
    }
}

fn apply_put_object(
    state: &mut MetadataState,
    bucket: String,
    key: String,
    content_type: Option<String>,
    etag: String,
    segments: Vec<ObjectSegment>,
    changes: &mut Vec<Change>,
) -> Result<ObjectMetadata, AppError> {
    if segments.is_empty() {
        return Err(AppError::BadRequest(
            "object must contain at least one segment".to_string(),
        ));
    }

    let size = segments.iter().map(|s| s.size).sum::<u64>();
    let object_key = object_id(&bucket, &key);

    if let Some(previous) = state.objects.get(&object_key).cloned() {
        for segment in previous.segments {
            dec_blob_ref(state, &segment.hash, changes);
        }
    }

    for segment in &segments {
        inc_blob_ref(state, &segment.hash, changes)?;
    }

    let object = ObjectMetadata {
        bucket,
        key,
        size,
        etag,
        content_type,
        last_modified: Utc::now(),
        segments,
    };

    state.objects.insert(object_key.clone(), object.clone());
    changes.push(Change::UpsertObject {
        key: object_key,
        object: object.clone(),
    });

    Ok(object)
}

fn inc_blob_ref(
    state: &mut MetadataState,
    hash: &str,
    changes: &mut Vec<Change>,
) -> Result<(), AppError> {
    let Some(blob) = state.blobs.get_mut(hash) else {
        return Err(AppError::NotFound(format!(
            "segment blob {} is missing",
            hash
        )));
    };

    blob.ref_count += 1;
    changes.push(Change::UpsertBlob {
        hash: hash.to_string(),
        blob: blob.clone(),
    });

    Ok(())
}

fn dec_blob_ref(state: &mut MetadataState, hash: &str, changes: &mut Vec<Change>) {
    let Some(existing) = state.blobs.get(hash).cloned() else {
        return;
    };

    if existing.ref_count <= 1 {
        state.blobs.remove(hash);
        changes.push(Change::DeleteBlob {
            hash: hash.to_string(),
        });
    } else if let Some(blob) = state.blobs.get_mut(hash) {
        blob.ref_count -= 1;
        changes.push(Change::UpsertBlob {
            hash: hash.to_string(),
            blob: blob.clone(),
        });
    }
}

fn apply_changes(state: &mut MetadataState, changes: Vec<Change>) {
    for change in changes {
        match change {
            Change::UpsertObject { key, object } => {
                state.objects.insert(key, object);
            }
            Change::DeleteObject { key } => {
                state.objects.remove(&key);
            }
            Change::UpsertBlob { hash, blob } => {
                state.blobs.insert(hash, blob);
            }
            Change::DeleteBlob { hash } => {
                state.blobs.remove(&hash);
            }
            Change::UpsertMultipart { upload_id, upload } => {
                state.multiparts.insert(upload_id, upload);
            }
            Change::DeleteMultipart { upload_id } => {
                state.multiparts.remove(&upload_id);
            }
            Change::UpsertPolicy { bucket, policy } => {
                state.policies.insert(bucket, policy);
            }
            Change::DeletePolicy { bucket } => {
                state.policies.remove(&bucket);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn location(offset: u64) -> BlobLocation {
        BlobLocation {
            segment_id: 0,
            offset,
            length: 16,
            encrypted: false,
            nonce: None,
        }
    }

    fn hash(seed: &str) -> String {
        blake3::hash(seed.as_bytes()).to_hex().to_string()
    }

    #[tokio::test]
    async fn object_refcount_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let engine = MetadataEngine::new(dir.path()).await.unwrap();
        let h = hash("blob-a");

        engine
            .upsert_blob_if_absent(h.clone(), 16, location(0))
            .await
            .unwrap();

        engine
            .put_object(
                "b".to_string(),
                "k".to_string(),
                None,
                "etag".to_string(),
                vec![ObjectSegment {
                    hash: h.clone(),
                    size: 16,
                }],
            )
            .await
            .unwrap();

        assert_eq!(engine.blob(&h).await.unwrap().ref_count, 1);

        engine.delete_object("b", "k").await.unwrap();
        assert!(engine.blob(&h).await.is_none());
    }

    #[tokio::test]
    async fn multipart_complete_keeps_blob_refs() {
        let dir = tempfile::tempdir().unwrap();
        let engine = MetadataEngine::new(dir.path()).await.unwrap();
        let h1 = hash("part-1");
        let h2 = hash("part-2");

        engine
            .upsert_blob_if_absent(h1.clone(), 16, location(0))
            .await
            .unwrap();
        engine
            .upsert_blob_if_absent(h2.clone(), 16, location(16))
            .await
            .unwrap();

        let upload = engine
            .create_multipart("b".to_string(), "k".to_string(), "ak".to_string())
            .await
            .unwrap();

        engine
            .upload_part(&upload.upload_id, 1, h1.clone(), 16, "e1".to_string())
            .await
            .unwrap();
        engine
            .upload_part(&upload.upload_id, 2, h2.clone(), 16, "e2".to_string())
            .await
            .unwrap();

        assert_eq!(engine.blob(&h1).await.unwrap().ref_count, 1);
        assert_eq!(engine.blob(&h2).await.unwrap().ref_count, 1);

        let object = engine
            .complete_multipart(&upload.upload_id, None)
            .await
            .unwrap();

        assert_eq!(object.segments.len(), 2);
        assert_eq!(engine.blob(&h1).await.unwrap().ref_count, 1);
        assert_eq!(engine.blob(&h2).await.unwrap().ref_count, 1);
        assert_eq!(engine.multipart_count().await, 0);
    }
}
