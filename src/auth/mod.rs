use crate::{config::Config, error::AppError};
use dashmap::DashMap;
use hmac::{Hmac, Mac};
use http::{HeaderMap, Method};
use sha2::Sha256;
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug)]
pub struct AuthContext {
    pub access_key: String,
    pub is_admin: bool,
    pub payload_hash: String,
}

#[derive(Clone)]
pub struct AuthManager {
    enabled: bool,
    access_key: String,
    secret_key: String,
    admin_access_key: String,
    internal_token: String,
}

#[derive(Clone)]
pub struct RateLimiter {
    limit_per_sec: u64,
    counters: Arc<DashMap<String, (u64, u64)>>,
}

impl RateLimiter {
    pub fn new(limit_per_sec: u64) -> Self {
        Self {
            limit_per_sec,
            counters: Arc::new(DashMap::new()),
        }
    }

    pub fn allow(&self, principal: &str) -> bool {
        if self.limit_per_sec == 0 {
            return true;
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let mut entry = self
            .counters
            .entry(principal.to_owned())
            .or_insert((now, 0_u64));

        if entry.0 != now {
            *entry = (now, 0);
        }

        if entry.1 >= self.limit_per_sec {
            return false;
        }

        entry.1 += 1;
        true
    }
}

impl AuthManager {
    pub fn new(cfg: &Config) -> Self {
        Self {
            enabled: cfg.auth_enabled,
            access_key: cfg.access_key.clone(),
            secret_key: cfg.secret_key.clone(),
            admin_access_key: cfg.admin_access_key.clone(),
            internal_token: cfg.internal_token.clone(),
        }
    }

    pub fn verify_signed_request(
        &self,
        headers: &HeaderMap,
        method: &Method,
        path: &str,
        query: Option<&str>,
    ) -> Result<AuthContext, AppError> {
        if !self.enabled {
            return Ok(AuthContext {
                access_key: "anonymous".to_string(),
                is_admin: true,
                payload_hash: headers
                    .get("x-fs-content-sha256")
                    .and_then(|h| h.to_str().ok())
                    .unwrap_or("UNSIGNED-PAYLOAD")
                    .to_string(),
            });
        }

        let key = header_str(headers, "x-fs-access-key")?;
        if key != self.access_key {
            return Err(AppError::Unauthorized("unknown access key".to_string()));
        }

        let timestamp = header_str(headers, "x-fs-timestamp").and_then(|v| {
            v.parse::<i64>()
                .map_err(|_| AppError::Unauthorized("invalid timestamp".to_string()))
        })?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        if (now - timestamp).abs() > 300 {
            return Err(AppError::Unauthorized(
                "signature timestamp skew too high".to_string(),
            ));
        }

        let payload_hash = headers
            .get("x-fs-content-sha256")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("UNSIGNED-PAYLOAD");

        let supplied_signature = header_str(headers, "x-fs-signature")?;
        let canonical = canonical_signing_string(
            method.as_str(),
            path,
            query.unwrap_or(""),
            timestamp,
            payload_hash,
        );

        let mut mac = HmacSha256::new_from_slice(self.secret_key.as_bytes())
            .map_err(|e| AppError::Internal(e.to_string()))?;
        mac.update(canonical.as_bytes());

        let supplied_bytes = hex::decode(supplied_signature)
            .map_err(|_| AppError::Unauthorized("invalid signature encoding".to_string()))?;

        mac.verify_slice(&supplied_bytes)
            .map_err(|_| AppError::Unauthorized("invalid signature".to_string()))?;

        Ok(AuthContext {
            access_key: key.to_string(),
            is_admin: key == self.admin_access_key,
            payload_hash: payload_hash.to_string(),
        })
    }

    pub fn verify_internal(&self, headers: &HeaderMap) -> Result<(), AppError> {
        let token = header_str(headers, "x-fs-internal-token")?;
        if token != self.internal_token {
            return Err(AppError::Unauthorized("invalid internal token".to_string()));
        }
        Ok(())
    }
}

pub fn canonical_signing_string(
    method: &str,
    path: &str,
    query: &str,
    timestamp: i64,
    payload_hash: &str,
) -> String {
    format!("{method}\n{path}\n{query}\n{timestamp}\n{payload_hash}")
}

fn header_str<'a>(headers: &'a HeaderMap, key: &str) -> Result<&'a str, AppError> {
    headers
        .get(key)
        .ok_or_else(|| AppError::Unauthorized(format!("missing header {key}")))?
        .to_str()
        .map_err(|_| AppError::Unauthorized(format!("invalid header {key}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    fn build_auth_manager() -> AuthManager {
        AuthManager {
            enabled: true,
            access_key: "test-ak".to_string(),
            secret_key: "test-sk".to_string(),
            admin_access_key: "test-ak".to_string(),
            internal_token: "token".to_string(),
        }
    }

    fn sign(secret: &str, canonical: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("hmac");
        mac.update(canonical.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    #[test]
    fn verifies_valid_signature() {
        let auth = build_auth_manager();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let payload_hash = "UNSIGNED-PAYLOAD";
        let canonical = canonical_signing_string("GET", "/b/k", "", now, payload_hash);
        let signature = sign("test-sk", &canonical);

        let mut headers = HeaderMap::new();
        headers.insert("x-fs-access-key", HeaderValue::from_static("test-ak"));
        headers.insert(
            "x-fs-timestamp",
            HeaderValue::from_str(&now.to_string()).unwrap(),
        );
        headers.insert(
            "x-fs-content-sha256",
            HeaderValue::from_static(payload_hash),
        );
        headers.insert("x-fs-signature", HeaderValue::from_str(&signature).unwrap());

        let ctx = auth
            .verify_signed_request(&headers, &Method::GET, "/b/k", None)
            .expect("valid auth");

        assert_eq!(ctx.access_key, "test-ak");
        assert!(ctx.is_admin);
    }

    #[test]
    fn rejects_stale_timestamp() {
        let auth = build_auth_manager();
        let stale = 1_i64;
        let payload_hash = "UNSIGNED-PAYLOAD";
        let canonical = canonical_signing_string("GET", "/b/k", "", stale, payload_hash);
        let signature = sign("test-sk", &canonical);

        let mut headers = HeaderMap::new();
        headers.insert("x-fs-access-key", HeaderValue::from_static("test-ak"));
        headers.insert(
            "x-fs-timestamp",
            HeaderValue::from_str(&stale.to_string()).unwrap(),
        );
        headers.insert(
            "x-fs-content-sha256",
            HeaderValue::from_static(payload_hash),
        );
        headers.insert("x-fs-signature", HeaderValue::from_str(&signature).unwrap());

        let err = auth
            .verify_signed_request(&headers, &Method::GET, "/b/k", None)
            .expect_err("must reject stale signature");

        assert!(matches!(err, AppError::Unauthorized(_)));
    }
}
