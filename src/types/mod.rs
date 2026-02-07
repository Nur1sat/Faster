use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlobLocation {
    pub segment_id: u64,
    pub offset: u64,
    pub length: u64,
    pub encrypted: bool,
    pub nonce: Option<[u8; 12]>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlobMetadata {
    pub hash: String,
    pub size: u64,
    pub location: BlobLocation,
    pub ref_count: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObjectSegment {
    pub hash: String,
    pub size: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub bucket: String,
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub content_type: Option<String>,
    pub last_modified: DateTime<Utc>,
    pub segments: Vec<ObjectSegment>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MultipartPart {
    pub part_number: u32,
    pub hash: String,
    pub size: u64,
    pub etag: String,
    pub uploaded_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MultipartUpload {
    pub upload_id: String,
    pub bucket: String,
    pub key: String,
    pub created_at: DateTime<Utc>,
    pub parts: BTreeMap<u32, MultipartPart>,
    pub initiated_by: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum PolicyEffect {
    Allow,
    Deny,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PolicyRule {
    pub principal: String,
    pub action: String,
    pub prefix: String,
    pub effect: PolicyEffect,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BucketPolicy {
    pub default_effect: PolicyEffect,
    pub rules: Vec<PolicyRule>,
}

impl Default for BucketPolicy {
    fn default() -> Self {
        Self {
            default_effect: PolicyEffect::Allow,
            rules: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicateObjectRequest {
    pub source: String,
    pub object: ObjectMetadata,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicateDeleteRequest {
    pub bucket: String,
    pub key: String,
}

pub fn object_id(bucket: &str, key: &str) -> String {
    format!("{bucket}/{key}")
}
