use std::{env, path::PathBuf, str::FromStr};

#[derive(Clone, Debug)]
pub struct Config {
    pub bind_addr: String,
    pub advertise_addr: String,
    pub data_dir: PathBuf,
    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,
    pub auth_enabled: bool,
    pub access_key: String,
    pub secret_key: String,
    pub admin_access_key: String,
    pub internal_token: String,
    pub rate_limit_per_sec: u64,
    pub cache_capacity_mb: u64,
    pub cache_object_max_bytes: usize,
    pub small_object_threshold_bytes: usize,
    pub segment_target_size_mb: u64,
    pub metadata_compact_interval_secs: u64,
    pub storage_compact_interval_secs: u64,
    pub multipart_ttl_secs: u64,
    pub max_request_bytes: usize,
    pub request_timeout_secs: u64,
    pub log_json: bool,
    pub replication_peers: Vec<String>,
    pub encryption_key_base64: Option<String>,
}

impl Config {
    pub fn from_env() -> Self {
        let bind_addr = env_or("FS_BIND_ADDR", "0.0.0.0:9000");
        let advertise_addr = env_or("FS_ADVERTISE_ADDR", "http://127.0.0.1:9000");

        let data_dir = PathBuf::from(env_or("FS_DATA_DIR", "./data"));

        let tls_cert_path = env::var("FS_TLS_CERT_PATH").ok().map(PathBuf::from);
        let tls_key_path = env::var("FS_TLS_KEY_PATH").ok().map(PathBuf::from);

        let auth_enabled = env_bool("FS_AUTH_ENABLED", true);
        let access_key = env_or("FS_ACCESS_KEY", "faster");
        let secret_key = env_or("FS_SECRET_KEY", "faster-secret-key");
        let admin_access_key = env_or("FS_ADMIN_ACCESS_KEY", &access_key);
        let internal_token = env_or("FS_INTERNAL_TOKEN", "internal-replication-token");

        let rate_limit_per_sec = env_parse("FS_RATE_LIMIT_PER_SEC", 20_000_u64);
        let cache_capacity_mb = env_parse("FS_CACHE_CAPACITY_MB", 256_u64);
        let cache_object_max_bytes = env_parse("FS_CACHE_OBJECT_MAX_BYTES", 2 * 1024 * 1024_usize);
        let small_object_threshold_bytes =
            env_parse("FS_SMALL_OBJECT_THRESHOLD_BYTES", 256 * 1024_usize);
        let segment_target_size_mb = env_parse("FS_SEGMENT_TARGET_SIZE_MB", 512_u64);
        let metadata_compact_interval_secs = env_parse("FS_METADATA_COMPACT_INTERVAL_SECS", 60_u64);
        let storage_compact_interval_secs = env_parse("FS_STORAGE_COMPACT_INTERVAL_SECS", 120_u64);
        let multipart_ttl_secs = env_parse("FS_MULTIPART_TTL_SECS", 86_400_u64);
        let max_request_bytes = env_parse("FS_MAX_REQUEST_BYTES", 128 * 1024 * 1024_usize);
        let request_timeout_secs = env_parse("FS_REQUEST_TIMEOUT_SECS", 60_u64);
        let log_json = env_bool("FS_LOG_JSON", true);

        let replication_peers = env::var("FS_REPLICATION_PEERS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();

        let encryption_key_base64 = env::var("FS_ENCRYPTION_KEY_BASE64")
            .ok()
            .filter(|v| !v.trim().is_empty());

        Self {
            bind_addr,
            advertise_addr,
            data_dir,
            tls_cert_path,
            tls_key_path,
            auth_enabled,
            access_key,
            secret_key,
            admin_access_key,
            internal_token,
            rate_limit_per_sec,
            cache_capacity_mb,
            cache_object_max_bytes,
            small_object_threshold_bytes,
            segment_target_size_mb,
            metadata_compact_interval_secs,
            storage_compact_interval_secs,
            multipart_ttl_secs,
            max_request_bytes,
            request_timeout_secs,
            log_json,
            replication_peers,
            encryption_key_base64,
        }
    }

    pub fn segment_target_bytes(&self) -> u64 {
        self.segment_target_size_mb * 1024 * 1024
    }

    pub fn cache_capacity_bytes(&self) -> u64 {
        self.cache_capacity_mb * 1024 * 1024
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .and_then(|v| match v.to_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn env_parse<T>(key: &str, default: T) -> T
where
    T: FromStr + Copy,
{
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<T>().ok())
        .unwrap_or(default)
}
