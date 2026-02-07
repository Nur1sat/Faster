#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 || $# -gt 3 ]]; then
  echo "Usage: $0 <bucket> <key> <output-file>" >&2
  exit 1
fi

BUCKET="$1"
KEY="$2"
OUT_FILE="$3"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/fs-common.sh"

PATH_ONLY="/${BUCKET}/${KEY}"
QUERY=""
PAYLOAD_HASH="UNSIGNED-PAYLOAD"
URL="$(build_url "$PATH_ONLY" "$QUERY")"

HEADERS="$(signed_headers "GET" "$PATH_ONLY" "$QUERY" "$PAYLOAD_HASH")"

curl -fsS -X GET "$URL" \
  -H "$(echo "$HEADERS" | sed -n '1p')" \
  -H "$(echo "$HEADERS" | sed -n '2p')" \
  -H "$(echo "$HEADERS" | sed -n '3p')" \
  -H "$(echo "$HEADERS" | sed -n '4p')" \
  -D /tmp/fasterstore-get-headers.out \
  -o "$OUT_FILE"

ETAG="$(awk '/^etag:/ {print $2}' /tmp/fasterstore-get-headers.out | tr -d '\r\n\"')"
SIZE="$(wc -c < "$OUT_FILE" | tr -d ' ')"
echo "downloaded bucket=${BUCKET} key=${KEY} out=${OUT_FILE} size=${SIZE} etag=${ETAG}"
