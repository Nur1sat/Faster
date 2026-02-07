use crate::{config::Config, error::AppError, types::BlobLocation};
use async_stream::try_stream;
use blake3::Hasher;
use bytes::{Bytes, BytesMut};
use futures_util::Stream;
use sha2::{Digest, Sha256};
use std::{
    cmp::min,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, SeekFrom},
    sync::Mutex,
};
use tracing::info;

use super::crypto::AtRestCipher;

const RECORD_MAGIC: u32 = 0x4653_5452;
const RECORD_VERSION: u8 = 1;
const HEADER_SIZE: usize = 60;

#[derive(Clone, Debug)]
pub enum IngestedPayload {
    Memory(Bytes),
    TempFile(PathBuf),
}

#[derive(Clone, Debug)]
pub struct IngestedBlob {
    pub hash: String,
    pub body_sha256: String,
    pub size: u64,
    pub payload: IngestedPayload,
    pub cache_candidate: Option<Bytes>,
}

pub struct IngestSession {
    threshold: usize,
    tmp_dir: PathBuf,
    hasher: Hasher,
    sha256: Sha256,
    size: u64,
    small_buf: BytesMut,
    temp_file: Option<File>,
    temp_path: Option<PathBuf>,
}

impl IngestSession {
    pub fn new(threshold: usize, tmp_dir: PathBuf) -> Self {
        Self {
            threshold,
            tmp_dir,
            hasher: Hasher::new(),
            sha256: Sha256::new(),
            size: 0,
            small_buf: BytesMut::new(),
            temp_file: None,
            temp_path: None,
        }
    }

    pub async fn push(&mut self, chunk: &[u8]) -> Result<(), AppError> {
        self.hasher.update(chunk);
        self.sha256.update(chunk);
        self.size += chunk.len() as u64;

        if self.temp_file.is_none() && self.small_buf.len() + chunk.len() <= self.threshold {
            self.small_buf.extend_from_slice(chunk);
            return Ok(());
        }

        if self.temp_file.is_none() {
            let path = self
                .tmp_dir
                .join(format!("ingest-{}.tmp", uuid::Uuid::new_v4()));
            let mut f = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&path)
                .await?;
            if !self.small_buf.is_empty() {
                f.write_all(&self.small_buf).await?;
                self.small_buf.clear();
            }
            self.temp_path = Some(path);
            self.temp_file = Some(f);
        }

        if let Some(f) = &mut self.temp_file {
            f.write_all(chunk).await?;
        }

        Ok(())
    }

    pub async fn finish(mut self) -> Result<IngestedBlob, AppError> {
        let hash = self.hasher.finalize().to_hex().to_string();
        let body_sha256 = hex::encode(self.sha256.finalize());

        if let Some(mut file) = self.temp_file.take() {
            file.flush().await?;
            let path = self
                .temp_path
                .take()
                .ok_or_else(|| AppError::Storage("missing temporary path".to_string()))?;
            Ok(IngestedBlob {
                hash,
                body_sha256,
                size: self.size,
                payload: IngestedPayload::TempFile(path),
                cache_candidate: None,
            })
        } else {
            let bytes = self.small_buf.freeze();
            Ok(IngestedBlob {
                hash,
                body_sha256,
                size: self.size,
                payload: IngestedPayload::Memory(bytes.clone()),
                cache_candidate: Some(bytes),
            })
        }
    }
}

struct ActiveSegment {
    id: u64,
    offset: u64,
    file: File,
}

#[derive(Clone)]
pub struct StorageEngine {
    segments_dir: PathBuf,
    tmp_dir: PathBuf,
    segment_target_bytes: u64,
    read_chunk_size: usize,
    cipher: Option<Arc<AtRestCipher>>,
    active: Arc<Mutex<ActiveSegment>>,
}

impl StorageEngine {
    pub async fn new(cfg: &Config) -> Result<Self, AppError> {
        let segments_dir = cfg.data_dir.join("segments");
        let tmp_dir = cfg.data_dir.join("tmp");

        fs::create_dir_all(&segments_dir).await?;
        fs::create_dir_all(&tmp_dir).await?;

        let active_id = detect_latest_segment_id(&segments_dir)?;
        let active_path = segment_path(&segments_dir, active_id);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&active_path)
            .await?;
        let offset = file.metadata().await?.len();

        let cipher = cfg
            .encryption_key_base64
            .as_deref()
            .map(AtRestCipher::from_base64_key)
            .transpose()?
            .map(Arc::new);

        info!(
            segment_id = active_id,
            offset,
            encrypted = cipher.is_some(),
            "storage engine initialized"
        );

        Ok(Self {
            segments_dir,
            tmp_dir,
            segment_target_bytes: cfg.segment_target_bytes(),
            read_chunk_size: 128 * 1024,
            cipher,
            active: Arc::new(Mutex::new(ActiveSegment {
                id: active_id,
                offset,
                file,
            })),
        })
    }

    pub fn begin_ingest(&self, threshold: usize) -> IngestSession {
        IngestSession::new(threshold, self.tmp_dir.clone())
    }

    pub async fn append_ingested(&self, ingested: IngestedBlob) -> Result<BlobLocation, AppError> {
        match ingested.payload {
            IngestedPayload::Memory(bytes) => {
                let (payload, encrypted, nonce) = if let Some(cipher) = &self.cipher {
                    let (ciphertext, nonce) = cipher.encrypt(&bytes)?;
                    (Bytes::from(ciphertext), true, Some(nonce))
                } else {
                    (bytes, false, None)
                };

                self.append_bytes(&ingested.hash, payload, encrypted, nonce)
                    .await
            }
            IngestedPayload::TempFile(path) => {
                let result = if let Some(cipher) = &self.cipher {
                    // Encryption is optional and off by default. In encryption mode, we materialize
                    // temporary payloads once to keep the on-disk format simple and predictable.
                    let plaintext = fs::read(&path).await?;
                    let (ciphertext, nonce) = cipher.encrypt(&plaintext)?;
                    self.append_bytes(&ingested.hash, Bytes::from(ciphertext), true, Some(nonce))
                        .await
                } else {
                    self.append_file(&ingested.hash, &path, ingested.size, false, None)
                        .await
                };

                let _ = fs::remove_file(&path).await;
                result
            }
        }
    }

    pub async fn append_plain_blob(
        &self,
        hash: &str,
        plaintext: Bytes,
    ) -> Result<BlobLocation, AppError> {
        let (payload, encrypted, nonce) = if let Some(cipher) = &self.cipher {
            let (ciphertext, nonce) = cipher.encrypt(&plaintext)?;
            (Bytes::from(ciphertext), true, Some(nonce))
        } else {
            (plaintext, false, None)
        };

        self.append_bytes(hash, payload, encrypted, nonce).await
    }

    pub async fn read_blob_plain(&self, location: &BlobLocation) -> Result<Bytes, AppError> {
        let ciphertext_or_plain = self.read_blob_raw(location).await?;
        if !location.encrypted {
            return Ok(ciphertext_or_plain);
        }

        let cipher = self.cipher.as_ref().ok_or_else(|| {
            AppError::Storage("encrypted blob found but encryption key is unavailable".to_string())
        })?;

        let nonce = location
            .nonce
            .ok_or_else(|| AppError::Storage("encrypted blob missing nonce".to_string()))?;

        let plaintext = cipher.decrypt(&ciphertext_or_plain, nonce)?;
        Ok(Bytes::from(plaintext))
    }

    pub async fn read_blob_raw(&self, location: &BlobLocation) -> Result<Bytes, AppError> {
        let path = segment_path(&self.segments_dir, location.segment_id);
        let mut f = File::open(&path).await?;
        f.seek(SeekFrom::Start(location.offset)).await?;

        let mut buf = vec![0_u8; location.length as usize];
        f.read_exact(&mut buf).await?;
        Ok(Bytes::from(buf))
    }

    pub fn stream_blob_raw(
        &self,
        location: BlobLocation,
    ) -> impl Stream<Item = Result<Bytes, AppError>> + Send + 'static {
        let path = segment_path(&self.segments_dir, location.segment_id);
        let chunk_size = self.read_chunk_size;
        try_stream! {
            let mut f = File::open(path).await?;
            f.seek(SeekFrom::Start(location.offset)).await?;

            let mut remaining = location.length;
            let mut buf = vec![0_u8; chunk_size];

            while remaining > 0 {
                let read_len = min(chunk_size, remaining as usize);
                let n = f.read(&mut buf[..read_len]).await?;
                if n == 0 {
                    Err(AppError::Storage("short read while streaming blob".to_string()))?;
                }
                remaining -= n as u64;
                yield Bytes::copy_from_slice(&buf[..n]);
            }
        }
    }

    pub async fn rewrite_blob(
        &self,
        hash: &str,
        location: &BlobLocation,
    ) -> Result<BlobLocation, AppError> {
        let source_path = segment_path(&self.segments_dir, location.segment_id);
        let mut source = File::open(&source_path).await?;
        source.seek(SeekFrom::Start(location.offset)).await?;

        let mut active = self.active.lock().await;
        self.rotate_if_needed(&mut active, location.length).await?;

        let hash_bytes = decode_hash(hash)?;
        let header_offset = active.offset;
        let payload_offset = header_offset + HEADER_SIZE as u64;

        let header = encode_header(
            location.length,
            hash_bytes,
            location.encrypted,
            location.nonce,
        );
        active.file.write_all(&header).await?;

        let mut reader = BufReader::new(source.take(location.length));
        let copied = tokio::io::copy(&mut reader, &mut active.file).await?;
        if copied != location.length {
            return Err(AppError::Storage(
                "blob rewrite copied unexpected size".to_string(),
            ));
        }

        active.offset += HEADER_SIZE as u64 + copied;
        active.file.flush().await?;

        Ok(BlobLocation {
            segment_id: active.id,
            offset: payload_offset,
            length: location.length,
            encrypted: location.encrypted,
            nonce: location.nonce,
        })
    }

    pub async fn active_segment_id(&self) -> u64 {
        self.active.lock().await.id
    }

    pub fn segment_path(&self, segment_id: u64) -> PathBuf {
        segment_path(&self.segments_dir, segment_id)
    }

    pub async fn remove_segment(&self, segment_id: u64) -> Result<(), AppError> {
        let active = self.active.lock().await;
        if active.id == segment_id {
            return Ok(());
        }
        drop(active);

        let path = segment_path(&self.segments_dir, segment_id);
        if path.exists() {
            fs::remove_file(path).await?;
        }
        Ok(())
    }

    async fn append_bytes(
        &self,
        hash: &str,
        payload: Bytes,
        encrypted: bool,
        nonce: Option<[u8; 12]>,
    ) -> Result<BlobLocation, AppError> {
        let payload_len = payload.len() as u64;
        let mut active = self.active.lock().await;
        self.rotate_if_needed(&mut active, payload_len).await?;

        let hash_bytes = decode_hash(hash)?;
        let header_offset = active.offset;
        let payload_offset = header_offset + HEADER_SIZE as u64;
        let header = encode_header(payload_len, hash_bytes, encrypted, nonce);

        active.file.write_all(&header).await?;
        active.file.write_all(&payload).await?;
        active.file.flush().await?;

        active.offset += HEADER_SIZE as u64 + payload_len;

        Ok(BlobLocation {
            segment_id: active.id,
            offset: payload_offset,
            length: payload_len,
            encrypted,
            nonce,
        })
    }

    async fn append_file(
        &self,
        hash: &str,
        path: &Path,
        size: u64,
        encrypted: bool,
        nonce: Option<[u8; 12]>,
    ) -> Result<BlobLocation, AppError> {
        let mut active = self.active.lock().await;
        self.rotate_if_needed(&mut active, size).await?;

        let hash_bytes = decode_hash(hash)?;
        let header_offset = active.offset;
        let payload_offset = header_offset + HEADER_SIZE as u64;
        let header = encode_header(size, hash_bytes, encrypted, nonce);

        active.file.write_all(&header).await?;

        let file = File::open(path).await?;
        let mut reader = BufReader::new(file);
        let copied = tokio::io::copy(&mut reader, &mut active.file).await?;
        if copied != size {
            return Err(AppError::Storage(
                "file ingest copied unexpected size".to_string(),
            ));
        }

        active.file.flush().await?;
        active.offset += HEADER_SIZE as u64 + copied;

        Ok(BlobLocation {
            segment_id: active.id,
            offset: payload_offset,
            length: size,
            encrypted,
            nonce,
        })
    }

    async fn rotate_if_needed(
        &self,
        active: &mut ActiveSegment,
        incoming_payload_size: u64,
    ) -> Result<(), AppError> {
        let next_size = active.offset + HEADER_SIZE as u64 + incoming_payload_size;
        if next_size <= self.segment_target_bytes {
            return Ok(());
        }

        let next_id = active.id + 1;
        let path = segment_path(&self.segments_dir, next_id);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .await?;

        info!(
            old_segment = active.id,
            new_segment = next_id,
            "rotating append segment"
        );

        active.id = next_id;
        active.offset = 0;
        active.file = file;

        Ok(())
    }
}

fn decode_hash(hash: &str) -> Result<[u8; 32], AppError> {
    let raw = hex::decode(hash)
        .map_err(|_| AppError::Storage("blob hash is not valid hex".to_string()))?;

    if raw.len() != 32 {
        return Err(AppError::Storage("blob hash must be 32 bytes".to_string()));
    }

    let mut out = [0_u8; 32];
    out.copy_from_slice(&raw);
    Ok(out)
}

fn encode_header(
    payload_len: u64,
    hash: [u8; 32],
    encrypted: bool,
    nonce: Option<[u8; 12]>,
) -> [u8; HEADER_SIZE] {
    let mut buf = [0_u8; HEADER_SIZE];
    buf[0..4].copy_from_slice(&RECORD_MAGIC.to_le_bytes());
    buf[4] = RECORD_VERSION;
    buf[5] = if encrypted { 0b0000_0001 } else { 0 };
    buf[6..8].copy_from_slice(&0_u16.to_le_bytes());
    buf[8..16].copy_from_slice(&payload_len.to_le_bytes());
    buf[16..48].copy_from_slice(&hash);
    let nonce = nonce.unwrap_or([0_u8; 12]);
    buf[48..60].copy_from_slice(&nonce);
    buf
}

fn segment_path(root: &Path, segment_id: u64) -> PathBuf {
    root.join(format!("segment-{segment_id:020}.dat"))
}

fn parse_segment_id(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let without_prefix = name.strip_prefix("segment-")?;
    let without_suffix = without_prefix.strip_suffix(".dat")?;
    without_suffix.parse::<u64>().ok()
}

fn detect_latest_segment_id(root: &Path) -> Result<u64, AppError> {
    let mut max_id = 0_u64;
    let mut found = false;
    let entries = std::fs::read_dir(root)
        .map_err(|e| AppError::Storage(format!("list segments failed: {e}")))?;

    for entry in entries {
        let entry = entry.map_err(|e| AppError::Storage(e.to_string()))?;
        if let Some(id) = parse_segment_id(&entry.path()) {
            found = true;
            if id > max_id {
                max_id = id;
            }
        }
    }

    if !found { Ok(0) } else { Ok(max_id) }
}
