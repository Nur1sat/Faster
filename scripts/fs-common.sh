#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Preserve external overrides passed via environment.
FS_ENDPOINT_OVERRIDE="${FS_ENDPOINT-}"
FS_ACCESS_KEY_OVERRIDE="${FS_ACCESS_KEY-}"
FS_SECRET_KEY_OVERRIDE="${FS_SECRET_KEY-}"

if [[ -f "$ROOT_DIR/.env" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
elif [[ -f "$ROOT_DIR/.env.example" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env.example"
fi

if [[ -n "$FS_ENDPOINT_OVERRIDE" ]]; then
  FS_ENDPOINT="$FS_ENDPOINT_OVERRIDE"
fi

if [[ -n "$FS_ACCESS_KEY_OVERRIDE" ]]; then
  FS_ACCESS_KEY="$FS_ACCESS_KEY_OVERRIDE"
fi

if [[ -n "$FS_SECRET_KEY_OVERRIDE" ]]; then
  FS_SECRET_KEY="$FS_SECRET_KEY_OVERRIDE"
fi

FS_ENDPOINT="${FS_ENDPOINT:-http://127.0.0.1:9000}"
FS_ACCESS_KEY="${FS_ACCESS_KEY:-faster}"
FS_SECRET_KEY="${FS_SECRET_KEY:-faster-secret-key}"

trim_trailing_slash() {
  local input="$1"
  echo "${input%/}"
}

hmac_sha256_hex() {
  local key="$1"
  local message="$2"
  printf '%s' "$message" | openssl dgst -sha256 -hmac "$key" -binary | xxd -p -c 256
}

sha256_hex_file() {
  local file="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file" | awk '{print $1}'
    return
  fi

  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file" | awk '{print $1}'
    return
  fi

  openssl dgst -sha256 "$file" | awk '{print $NF}'
}

canonical_string() {
  local method="$1"
  local path="$2"
  local query="$3"
  local timestamp="$4"
  local payload_hash="$5"
  printf '%s\n%s\n%s\n%s\n%s' "$method" "$path" "$query" "$timestamp" "$payload_hash"
}

signed_headers() {
  local method="$1"
  local path="$2"
  local query="$3"
  local payload_hash="$4"

  local timestamp
  timestamp="$(date +%s)"

  local canonical
  canonical="$(canonical_string "$method" "$path" "$query" "$timestamp" "$payload_hash")"

  local signature
  signature="$(hmac_sha256_hex "$FS_SECRET_KEY" "$canonical")"

  cat <<HDR
x-fs-access-key: $FS_ACCESS_KEY
x-fs-timestamp: $timestamp
x-fs-content-sha256: $payload_hash
x-fs-signature: $signature
HDR
}

build_url() {
  local path="$1"
  local query="${2:-}"
  local endpoint
  endpoint="$(trim_trailing_slash "$FS_ENDPOINT")"

  if [[ -n "$query" ]]; then
    printf '%s%s?%s' "$endpoint" "$path" "$query"
  else
    printf '%s%s' "$endpoint" "$path"
  fi
}
