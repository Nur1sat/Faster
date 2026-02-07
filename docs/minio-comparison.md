# FasterStore vs MinIO: Detailed Comparison

As of **February 7, 2026**, this document compares FasterStore to the current public MinIO upstream and defines what to add next.

## Primary Upstream References Checked

- MinIO upstream repository: [minio/minio](https://github.com/minio/minio)
- MinIO README (S3 object store positioning, maintenance-mode notice): [README.md](https://github.com/minio/minio/blob/master/README.md)
- MinIO releases page (latest server release metadata): [Releases](https://github.com/minio/minio/releases)
- Replication requirements (versioning/object-lock constraints): [Bucket Replication](https://docs.min.io/community/minio-object-store/administration/bucket-replication.html)
- Server-side encryption modes and KES integration: [Server-Side Encryption](https://docs.min.io/enterprise/aistor-object-store/reference/aistor-server/settings/security-server-side-encryption.html)
- Erasure coding defaults and stripe-set guidance: [Erasure Coding](https://docs.min.io/enterprise/aistor-object-store/operations/core-concepts/erasure-coding.html)
- MinIO strong consistency statement: [Blog: Strict Consistency](https://blog.min.io/strict-consistency-hard-requirement-for-primary-storage/)

## Important Context from Upstream

- The `minio/minio` README currently states the project is in **maintenance mode** and points users to MinIO AIStor for latest features, with source-available distribution details in that notice.
- MinIO docs currently use AIStor branding for many pages, but these pages still describe the current MinIO object-storage feature set and operational model.

## Detailed Capability Matrix

| Area | MinIO (upstream/docs) | FasterStore (current repo) | Gap / Add Next |
|---|---|---|---|
| Core API compatibility | Broad S3-compatible platform | Core object APIs + multipart + bucket policy endpoints | Expand to wider S3 API surface and edge-case parity |
| Auth/signing model | AWS ecosystem-compatible model and policy stack | Custom HMAC headers (`x-fs-*`) | Add full SigV4 request validation path |
| Consistency model | Advertises strict read-after-write/list consistency | Single-node immediate, replicated async eventual | Add explicit consistency model doc + stronger replica consistency options |
| Data durability model | Erasure-coded distributed sets | Append-only local segments + background compaction | Add pluggable erasure coding module (required for MinIO-class durability) |
| Replication | Bucket/site replication with versioning constraints | Async peer fanout + blob pull | Add version-aware replication, lag tracking, replay/idempotency logs |
| Versioning | Native S3 versioning workflows | Not implemented | Add per-object version chains + delete markers |
| Object locking / WORM | Documented object-lock semantics | Not implemented | Add retention/legal-hold model tied to versioning |
| Encryption at rest | SSE modes with KES/external key workflows | Optional AES-256-GCM with single local key | Add key-rotation, per-bucket key mapping, KMS/KES plugin interface |
| Metadata architecture | Internal distributed metadata path | Embedded WAL + snapshot metadata engine | Add crash-invariant metadata tests + schema migration strategy |
| Healing/recovery | Mature distributed repair flows | Fast startup via WAL/snapshot; no shard healing | Add scrub/heal jobs and corruption detection |
| Observability | Rich metrics/admin tooling | Prometheus endpoint + structured logs + health checks | Add cardinality-safe, per-bucket and replication-lag metrics |
| Identity integration | Enterprise-grade ecosystem integrations | Static access/secret + internal token | Add pluggable identity providers (OIDC/LDAP/STS equivalent path) |
| Lifecycle/ILM | Lifecycle management features | Not implemented | Add TTL/transition/expiry policy engine |
| Multi-tenancy operations | Mature operational controls | Single-process service | Add tenant boundary model + quotas/rate-shaping per tenant |
| Operational UX | Extensive MinIO ecosystem tooling | Minimal env-driven ops | Add admin API for runtime diagnostics and safe maintenance commands |

## Where FasterStore Already Has a Strong Direction

- Very small and understandable codebase.
- Single-binary deployment with no external metadata database.
- Clear performance-oriented storage path (append-only + CAS + small-object fast path).
- Fast restart behavior from metadata snapshot + WAL replay.

## Priority Additions (Concrete)

### P0: Compatibility and correctness

1. Implement AWS SigV4 validation (while keeping current fast internal auth for trusted internal traffic).
2. Add S3 object versioning and delete markers.
3. Add object lock retention/legal-hold semantics on top of versioning.
4. Add conformance tests for S3-compatible behaviors on overwrite, conditional headers, and multipart corner cases.

### P1: Durability and distributed behavior

1. Add pluggable erasure coding with stripe-set abstraction.
2. Add replication journal with durable replay and idempotency keys.
3. Add healing/scrubbing workers and on-read checksum verification policy.
4. Add stronger replication observability (lag, queue depth, failure reason labels).

### P2: Security and operations

1. Add external KMS/KES plugin interface and key rotation support.
2. Add lifecycle policies (expiration and cleanup jobs).
3. Add tenant-scoped quotas and policy isolation.
4. Add richer admin/diagnostic endpoints (rebalance, compaction status, replication status).

## Performance Differentiation Plan vs MinIO

To claim "faster than MinIO" credibly, publish repeatable benchmarks with the same hardware and workload shapes:

1. Small object PUT/GET latency at p50/p95/p99 (`1KiB`, `4KiB`, `32KiB`).
2. Multipart ingest throughput (`64MiB` to `5GiB` objects).
3. CPU cycles/GB and memory overhead under mixed load.
4. Recovery time after unclean restart and after node rejoin.

Use this repo's `examples/basic_benchmark.rs` as a seed, then add MinIO side-by-side runs and publish raw CSV + plots.
