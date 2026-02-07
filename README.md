# FasterStore

FasterStore is a high-performance, single-binary, cloud-native object storage server inspired by MinIO and optimized for lower latency, lower overhead, and predictable behavior under load.

It provides S3-style object APIs, multipart uploads, custom metadata persistence (no external database), background compaction, optional at-rest encryption, built-in observability, and horizontal replication.

## Highlights

- Rust-first, async/non-blocking I/O (`tokio` + `axum`)
- Content-addressable blobs (BLAKE3 hash)
- Append-only segment files for object payloads
- WAL + snapshot metadata engine (fast startup, no segment scans)
- Multipart uploads implemented as segment manifests (no full-object rewrite on complete)
- Built-in in-memory hot-object cache
- Access key / secret key signing validation
- Built-in rate limiting
- Internal node-to-node replication over authenticated control API
- Prometheus metrics + health/readiness endpoints
- Optional TLS and optional at-rest encryption (AES-256-GCM)

## High-Level Architecture

FasterStore is a single process with strongly separated internal modules:

1. API Layer (`src/api`)
- S3-like object endpoints (`PUT`/`GET`/`DELETE`)
- Multipart upload flow (`initiate`, `upload part`, `complete`, `abort`)
- Admin policy endpoints
- Internal replication endpoints

2. Storage Engine (`src/storage`)
- Append-only segment writer
- Blob record format with deterministic offsets
- Content-addressable storage keyed by hash
- Segment rotation + background compaction hooks
- Optional at-rest encryption

3. Metadata Engine (`src/metadata`)
- Custom metadata state (objects, blobs, multipart sessions, policies)
- Write-ahead log (transaction events)
- Snapshot compaction + WAL rotation
- Reference counting for CAS blobs

4. Replication Module (`src/replication`)
- Peer fanout for object/delete events
- Pull-based blob hydration from source nodes
- Internal token-authenticated node RPC

5. Observability + Security
- `src/metrics`: Prometheus metrics registry and exporter
- `src/auth`: signing validation + rate limiting

## Folder Structure

```text
.
├── .env.example
├── Cargo.toml
├── Dockerfile
├── Makefile
├── README.md
├── docs/
│   └── minio-comparison.md
├── examples/
│   └── basic_benchmark.rs
├── scripts/
│   ├── fs-common.sh
│   ├── fs-delete.sh
│   ├── fs-get.sh
│   ├── fs-health.sh
│   └── fs-put.sh
└── src/
    ├── api/
    │   └── mod.rs
    ├── auth/
    │   └── mod.rs
    ├── config.rs
    ├── error.rs
    ├── main.rs
    ├── metadata/
    │   ├── engine.rs
    │   └── mod.rs
    ├── metrics/
    │   └── mod.rs
    ├── replication/
    │   └── mod.rs
    ├── state.rs
    ├── storage/
    │   ├── crypto.rs
    │   ├── engine.rs
    │   └── mod.rs
    └── types/
        └── mod.rs
```

## Core Data Flow Diagram (Text)

```text
Client
  |
  | 1) Signed PUT /bucket/key
  v
[API Layer] --auth+rate+policy--> [Storage Ingest Session]
  |                                   |
  |                                   | stream hash + spool threshold
  |                                   v
  |                             [Append Segment Writer]
  |                                   |
  | 2) blob location/hash ------------+
  v
[Metadata Engine]
  |  - Upsert blob (if new)
  |  - Refcount updates
  |  - Upsert object manifest
  |  - WAL append (transaction)
  v
Response (ETag)
  |
  | async
  v
[Replication Module] --> peer /_internal/replicate/object

GET /bucket/key:
API -> metadata lookup -> cache check -> storage stream/read -> response
```

## Implemented S3-Compatible Core APIs

### Object APIs

- `PUT /:bucket/*key` : upload object
- `GET /:bucket/*key` : download object
- `DELETE /:bucket/*key` : delete object

### Multipart APIs

- `POST /:bucket/*key?uploads` : initiate multipart
- `PUT /:bucket/*key?uploadId=<id>&partNumber=<n>` : upload part
- `POST /:bucket/*key?uploadId=<id>` : complete multipart
- `DELETE /:bucket/*key?uploadId=<id>` : abort multipart

### Bucket Policy APIs

- `PUT /_admin/policy/:bucket`
- `GET /_admin/policy/:bucket`

### Observability APIs

- `GET /metrics`
- `GET /healthz`
- `GET /readyz`

### Internal Replication APIs

- `GET /_internal/blob/:hash`
- `POST /_internal/replicate/object`
- `POST /_internal/replicate/delete`

## Authentication and Signing

Requests are validated using access key / secret key HMAC signing:

- `x-fs-access-key`
- `x-fs-timestamp` (unix seconds)
- `x-fs-content-sha256` (payload hash or `UNSIGNED-PAYLOAD`)
- `x-fs-signature`

Canonical string:

```text
<METHOD>\n<PATH>\n<QUERY>\n<TIMESTAMP>\n<PAYLOAD_SHA256>
```

Signature:

```text
hex(HMAC_SHA256(secret_key, canonical_string))
```

Internal replication calls use:

- `x-fs-internal-token`

## Performance-Critical Design Choices

1. Small-object fast path
- Ingest keeps small payloads in memory up to threshold and appends directly.
- Avoids temp-file overhead for common small-file workloads.

2. Zero-copy-friendly response path
- Unencrypted single-segment reads stream from disk to response body in chunks.

3. Content-addressable deduplication
- Hash-based blob reuse avoids duplicate writes and reduces disk pressure.

4. Multipart as manifest
- Complete-multipart does not rewrite full object payload; it commits metadata references.

5. Fast startup
- Snapshot + WAL replay restores metadata without scanning all segment files.

6. Background compaction
- Metadata WAL compaction and cold-segment data relocation reduce storage bloat while keeping foreground writes append-only.

7. Bounded memory behavior
- Configurable cache capacity and per-object cache size caps.
- Streamed ingest with spool threshold.

## Comparison Snapshot: FasterStore vs MinIO (As of February 7, 2026)

| Dimension | FasterStore | MinIO (baseline) |
|---|---|---|
| Core runtime | Rust single binary | Go single binary |
| Metadata dependency | Embedded custom WAL+snapshot (no external DB) | Internal distributed metadata path |
| Small-file path | In-memory threshold + append-only CAS path tuned for low overhead | General-purpose path |
| Multipart complete | Manifest commit (no full object rewrite) | Multipart support (implementation differs) |
| Startup behavior | Snapshot + WAL replay, no payload scans | Deployment-dependent |
| Resource profile | Designed for lower steady-state memory with bounded cache | Good but often tuned per deployment |
| Replication | Built-in async peer control API | Built-in distributed/replication features |
| Config simplicity | Env-driven minimal surface | Broader feature surface |

Detailed, source-checked comparison and roadmap:
`docs/minio-comparison.md`

## Configuration (Environment Variables)

| Variable | Default | Description |
|---|---|---|
| `FS_BIND_ADDR` | `0.0.0.0:9000` | Listen address |
| `FS_ADVERTISE_ADDR` | `http://127.0.0.1:9000` | Node URL advertised to peers |
| `FS_DATA_DIR` | `./data` | Data root |
| `FS_AUTH_ENABLED` | `true` | Enable signed auth |
| `FS_ACCESS_KEY` | `faster` | Access key |
| `FS_SECRET_KEY` | `faster-secret-key` | Secret key |
| `FS_ADMIN_ACCESS_KEY` | `<FS_ACCESS_KEY>` | Admin key for policy endpoints |
| `FS_INTERNAL_TOKEN` | `internal-replication-token` | Internal replication token |
| `FS_REPLICATION_PEERS` | empty | Comma-separated peer URLs |
| `FS_RATE_LIMIT_PER_SEC` | `20000` | Per-access-key request cap per second |
| `FS_CACHE_CAPACITY_MB` | `256` | Total cache size |
| `FS_CACHE_OBJECT_MAX_BYTES` | `2097152` | Max object size allowed in cache |
| `FS_SMALL_OBJECT_THRESHOLD_BYTES` | `262144` | Small-object ingest cutoff |
| `FS_SEGMENT_TARGET_SIZE_MB` | `512` | Segment rollover size |
| `FS_METADATA_COMPACT_INTERVAL_SECS` | `60` | Snapshot interval |
| `FS_STORAGE_COMPACT_INTERVAL_SECS` | `120` | Storage compaction cadence |
| `FS_MULTIPART_TTL_SECS` | `86400` | Stale multipart cleanup TTL |
| `FS_MAX_REQUEST_BYTES` | `134217728` | Max upload payload size |
| `FS_REQUEST_TIMEOUT_SECS` | `60` | Request/replication timeout |
| `FS_LOG_JSON` | `true` | Structured JSON logs |
| `FS_ENCRYPTION_KEY_BASE64` | unset | Optional 32-byte AES key (base64) |
| `FS_TLS_CERT_PATH` | unset | Optional TLS cert path |
| `FS_TLS_KEY_PATH` | unset | Optional TLS key path |

## Run Locally

```bash
cargo run --release
```

Server starts on `http://127.0.0.1:9000` unless overridden.

## Effortless Local Connect

These files are included to make local usage one-command:

- `.env.example` - defaults for endpoint and credentials
- `Makefile` - shortcuts for run/check/test/health/object operations
- `scripts/fs-*.sh` - signed API helpers for PUT/GET/DELETE

Quick start:

```bash
cp .env.example .env
make run
```

In another terminal:

```bash
make health
make put FILE=README.md BUCKET=bench KEY=hello.txt
make get BUCKET=bench KEY=hello.txt OUT=/tmp/hello.txt
make delete BUCKET=bench KEY=hello.txt
```

## Docker

```bash
docker build -t fasterstore:latest .
docker run --rm -p 9000:9000 -v $(pwd)/data:/data fasterstore:latest
```

## Docker Example Setup

### 1. Single Node (docker compose)

```bash
docker compose --env-file examples/docker/.env.example -f examples/docker/docker-compose.single.yml up --build -d
```

Endpoints:

- API: `http://127.0.0.1:9000`
- Metrics: `http://127.0.0.1:9000/metrics`
- Health: `http://127.0.0.1:9000/healthz`

Stop:

```bash
docker compose -f examples/docker/docker-compose.single.yml down -v
```

### 2. Two-Node Replication Example

```bash
docker compose --env-file examples/docker/.env.example -f examples/docker/docker-compose.cluster.yml up --build -d
```

Endpoints:

- Node 1: `http://127.0.0.1:9001`
- Node 2: `http://127.0.0.1:9002`

In this setup, each node is configured with the other as a replication peer through
`FS_REPLICATION_PEERS`.

Stop:

```bash
docker compose -f examples/docker/docker-compose.cluster.yml down -v
```

## Basic Benchmark Example

A benchmark client is included:

```bash
cargo run --release --example basic_benchmark
```

Useful overrides:

```bash
FS_ENDPOINT=http://127.0.0.1:9000 \
FS_ACCESS_KEY=faster \
FS_SECRET_KEY=faster-secret-key \
FS_BUCKET=bench \
FS_OBJECTS=2000 \
FS_SIZE_BYTES=4096 \
FS_CONCURRENCY=128 \
cargo run --release --example basic_benchmark
```

## Production Notes

- Enable TLS in production (`FS_TLS_CERT_PATH`, `FS_TLS_KEY_PATH`).
- Set strong access/secret/internal tokens.
- Configure replication peers for horizontal durability.
- Set dedicated persistent volume for `FS_DATA_DIR`.
- Tune `FS_SMALL_OBJECT_THRESHOLD_BYTES`, cache, and segment size based on workload.

## Current Scope

This repository implements the core object path and operational primitives with a performance-first architecture. Additional S3 surface area (broader bucket management semantics, compatibility edge cases, and optional gRPC control plane) can be layered on top without changing the storage/metadata core.
