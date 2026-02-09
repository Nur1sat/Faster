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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct EnvGuard {
        originals: Vec<(String, Option<String>)>,
    }

    impl EnvGuard {
        fn apply(changes: &[(&str, Option<&str>)]) -> Self {
            let mut originals = Vec::with_capacity(changes.len());
            for (key, value) in changes {
                originals.push(((*key).to_string(), env::var(key).ok()));
                match value {
                    Some(v) => {
                        // SAFETY: tests serialize all env mutations via `env_lock`.
                        unsafe { env::set_var(key, v) };
                    }
                    None => {
                        // SAFETY: tests serialize all env mutations via `env_lock`.
                        unsafe { env::remove_var(key) };
                    }
                }
            }
            Self { originals }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, value) in self.originals.iter().rev() {
                match value {
                    Some(v) => {
                        // SAFETY: tests serialize all env mutations via `env_lock`.
                        unsafe { env::set_var(key, v) };
                    }
                    None => {
                        // SAFETY: tests serialize all env mutations via `env_lock`.
                        unsafe { env::remove_var(key) };
                    }
                }
            }
        }
    }

    #[test]
    fn from_env_uses_defaults() {
        let _lock = env_lock().lock().expect("env test lock poisoned");
        let _guard = EnvGuard::apply(&[
            ("FS_BIND_ADDR", None),
            ("FS_ADVERTISE_ADDR", None),
            ("FS_AUTH_ENABLED", None),
            ("FS_ACCESS_KEY", None),
            ("FS_SECRET_KEY", None),
            ("FS_ADMIN_ACCESS_KEY", None),
            ("FS_REPLICATION_PEERS", None),
            ("FS_ENCRYPTION_KEY_BASE64", None),
            ("FS_SEGMENT_TARGET_SIZE_MB", None),
            ("FS_CACHE_CAPACITY_MB", None),
        ]);

        let cfg = Config::from_env();
        assert_eq!(cfg.bind_addr, "0.0.0.0:9000");
        assert_eq!(cfg.advertise_addr, "http://127.0.0.1:9000");
        assert!(cfg.auth_enabled);
        assert_eq!(cfg.access_key, "faster");
        assert_eq!(cfg.secret_key, "faster-secret-key");
        assert_eq!(cfg.admin_access_key, "faster");
        assert!(cfg.replication_peers.is_empty());
        assert_eq!(cfg.encryption_key_base64, None);
        assert_eq!(cfg.segment_target_bytes(), 512 * 1024 * 1024);
        assert_eq!(cfg.cache_capacity_bytes(), 256 * 1024 * 1024);
    }

    #[test]
    fn from_env_parses_overrides() {
        let _lock = env_lock().lock().expect("env test lock poisoned");
        let _guard = EnvGuard::apply(&[
            ("FS_BIND_ADDR", Some("127.0.0.1:9999")),
            ("FS_ADVERTISE_ADDR", Some("http://node-1:9999")),
            ("FS_AUTH_ENABLED", Some("off")),
            ("FS_ACCESS_KEY", Some("access-a")),
            ("FS_SECRET_KEY", Some("secret-b")),
            ("FS_ADMIN_ACCESS_KEY", Some("admin-c")),
            ("FS_RATE_LIMIT_PER_SEC", Some("321")),
            ("FS_CACHE_CAPACITY_MB", Some("512")),
            ("FS_CACHE_OBJECT_MAX_BYTES", Some("4096")),
            ("FS_SMALL_OBJECT_THRESHOLD_BYTES", Some("2048")),
            ("FS_SEGMENT_TARGET_SIZE_MB", Some("8")),
            ("FS_METADATA_COMPACT_INTERVAL_SECS", Some("30")),
            ("FS_STORAGE_COMPACT_INTERVAL_SECS", Some("90")),
            ("FS_MULTIPART_TTL_SECS", Some("120")),
            ("FS_MAX_REQUEST_BYTES", Some("8192")),
            ("FS_REQUEST_TIMEOUT_SECS", Some("15")),
            ("FS_LOG_JSON", Some("false")),
            (
                "FS_REPLICATION_PEERS",
                Some("http://n1:9000, http://n2:9000, ,"),
            ),
            ("FS_ENCRYPTION_KEY_BASE64", Some("Zm9vYmFy")),
        ]);

        let cfg = Config::from_env();
        assert_eq!(cfg.bind_addr, "127.0.0.1:9999");
        assert_eq!(cfg.advertise_addr, "http://node-1:9999");
        assert!(!cfg.auth_enabled);
        assert_eq!(cfg.access_key, "access-a");
        assert_eq!(cfg.secret_key, "secret-b");
        assert_eq!(cfg.admin_access_key, "admin-c");
        assert_eq!(cfg.rate_limit_per_sec, 321);
        assert_eq!(cfg.cache_capacity_mb, 512);
        assert_eq!(cfg.cache_object_max_bytes, 4096);
        assert_eq!(cfg.small_object_threshold_bytes, 2048);
        assert_eq!(cfg.segment_target_size_mb, 8);
        assert_eq!(cfg.metadata_compact_interval_secs, 30);
        assert_eq!(cfg.storage_compact_interval_secs, 90);
        assert_eq!(cfg.multipart_ttl_secs, 120);
        assert_eq!(cfg.max_request_bytes, 8192);
        assert_eq!(cfg.request_timeout_secs, 15);
        assert!(!cfg.log_json);
        assert_eq!(
            cfg.replication_peers,
            vec!["http://n1:9000".to_string(), "http://n2:9000".to_string()]
        );
        assert_eq!(cfg.encryption_key_base64, Some("Zm9vYmFy".to_string()));
        assert_eq!(cfg.segment_target_bytes(), 8 * 1024 * 1024);
        assert_eq!(cfg.cache_capacity_bytes(), 512 * 1024 * 1024);
    }

    #[test]
    fn from_env_falls_back_for_invalid_values() {
        let _lock = env_lock().lock().expect("env test lock poisoned");
        let _guard = EnvGuard::apply(&[
            ("FS_AUTH_ENABLED", Some("not-a-bool")),
            ("FS_RATE_LIMIT_PER_SEC", Some("not-a-number")),
            ("FS_ENCRYPTION_KEY_BASE64", Some("   ")),
        ]);

        let cfg = Config::from_env();
        assert!(cfg.auth_enabled);
        assert_eq!(cfg.rate_limit_per_sec, 20_000);
        assert_eq!(cfg.encryption_key_base64, None);
    }
}
