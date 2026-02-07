use hmac::{Hmac, Mac};
use reqwest::Client;
use sha2::{Digest, Sha256};
use std::{
    env,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Semaphore;

type HmacSha256 = Hmac<Sha256>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = env_var("FS_ENDPOINT", "http://127.0.0.1:9000");
    let access_key = env_var("FS_ACCESS_KEY", "faster");
    let secret_key = env_var("FS_SECRET_KEY", "faster-secret-key");
    let bucket = env_var("FS_BUCKET", "bench");

    let objects: usize = env_var("FS_OBJECTS", "2000").parse()?;
    let object_size: usize = env_var("FS_SIZE_BYTES", "4096").parse()?;
    let concurrency: usize = env_var("FS_CONCURRENCY", "128").parse()?;

    let payload = vec![b'x'; object_size];

    let client = Client::builder()
        .pool_max_idle_per_host(concurrency)
        .build()?;
    let limiter = Arc::new(Semaphore::new(concurrency));

    let put_start = Instant::now();
    let mut put_tasks = Vec::with_capacity(objects);
    for i in 0..objects {
        let permit = limiter.clone().acquire_owned().await?;
        let client = client.clone();
        let endpoint = endpoint.clone();
        let bucket = bucket.clone();
        let access_key = access_key.clone();
        let secret_key = secret_key.clone();
        let payload = payload.clone();

        put_tasks.push(tokio::spawn(async move {
            let _permit = permit;
            let key = format!("obj-{i:08}");
            let path = format!("/{bucket}/{key}");
            let payload_hash = hex::encode(Sha256::digest(&payload));
            let timestamp = now_ts();
            let signature = sign(
                &secret_key,
                &canonical_string("PUT", &path, "", timestamp, &payload_hash),
            );

            let res = client
                .put(format!("{endpoint}{path}"))
                .header("x-fs-access-key", access_key)
                .header("x-fs-timestamp", timestamp)
                .header("x-fs-content-sha256", payload_hash)
                .header("x-fs-signature", signature)
                .body(payload)
                .send()
                .await?;

            if !res.status().is_success() {
                anyhow::bail!("PUT failed with status {}", res.status());
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    for task in put_tasks {
        task.await??;
    }
    let put_elapsed = put_start.elapsed();

    let get_start = Instant::now();
    let mut get_tasks = Vec::with_capacity(objects);
    for i in 0..objects {
        let permit = limiter.clone().acquire_owned().await?;
        let client = client.clone();
        let endpoint = endpoint.clone();
        let bucket = bucket.clone();
        let access_key = access_key.clone();
        let secret_key = secret_key.clone();

        get_tasks.push(tokio::spawn(async move {
            let _permit = permit;
            let key = format!("obj-{i:08}");
            let path = format!("/{bucket}/{key}");
            let timestamp = now_ts();
            let payload_hash = "UNSIGNED-PAYLOAD";
            let signature = sign(
                &secret_key,
                &canonical_string("GET", &path, "", timestamp, payload_hash),
            );

            let res = client
                .get(format!("{endpoint}{path}"))
                .header("x-fs-access-key", access_key)
                .header("x-fs-timestamp", timestamp)
                .header("x-fs-content-sha256", payload_hash)
                .header("x-fs-signature", signature)
                .send()
                .await?;

            if !res.status().is_success() {
                anyhow::bail!("GET failed with status {}", res.status());
            }

            let body = res.bytes().await?;
            if body.len() != object_size {
                anyhow::bail!(
                    "GET size mismatch: expected {}, got {}",
                    object_size,
                    body.len()
                );
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    for task in get_tasks {
        task.await??;
    }
    let get_elapsed = get_start.elapsed();

    let total_bytes = (objects * object_size) as f64;
    let put_throughput_mb_s = (total_bytes / (1024.0 * 1024.0)) / put_elapsed.as_secs_f64();
    let get_throughput_mb_s = (total_bytes / (1024.0 * 1024.0)) / get_elapsed.as_secs_f64();

    println!("FasterStore Benchmark");
    println!(
        "objects={} size={}B concurrency={}",
        objects, object_size, concurrency
    );
    println!(
        "PUT: {:?} total, {:.2} ops/s, {:.2} MiB/s",
        put_elapsed,
        objects as f64 / put_elapsed.as_secs_f64(),
        put_throughput_mb_s
    );
    println!(
        "GET: {:?} total, {:.2} ops/s, {:.2} MiB/s",
        get_elapsed,
        objects as f64 / get_elapsed.as_secs_f64(),
        get_throughput_mb_s
    );

    Ok(())
}

fn env_var(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn canonical_string(
    method: &str,
    path: &str,
    query: &str,
    timestamp: i64,
    payload_hash: &str,
) -> String {
    format!("{method}\n{path}\n{query}\n{timestamp}\n{payload_hash}")
}

fn sign(secret: &str, canonical: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("hmac key");
    mac.update(canonical.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
