#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 || $# -gt 4 ]]; then
  echo "Usage: $0 <bucket> <key> <file> [content-type]" >&2
  exit 1
fi

BUCKET="$1"
KEY="$2"
FILE="$3"
CONTENT_TYPE="${4:-application/octet-stream}"

if [[ ! -f "$FILE" ]]; then
  echo "file not found: $FILE" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/fs-common.sh"

PATH_ONLY="/${BUCKET}/${KEY}"
QUERY=""
PAYLOAD_HASH="$(sha256_hex_file "$FILE")"
URL="$(build_url "$PATH_ONLY" "$QUERY")"

HEADERS="$(signed_headers "PUT" "$PATH_ONLY" "$QUERY" "$PAYLOAD_HASH")"

curl -fsS -X PUT "$URL" \
  -H "$(echo "$HEADERS" | sed -n '1p')" \
  -H "$(echo "$HEADERS" | sed -n '2p')" \
  -H "$(echo "$HEADERS" | sed -n '3p')" \
  -H "$(echo "$HEADERS" | sed -n '4p')" \
  -H "content-type: ${CONTENT_TYPE}" \
  --data-binary "@$FILE" \
  -D /tmp/fasterstore-put-headers.out \
  -o /tmp/fasterstore-put-body.out

ETAG="$(awk '/^etag:/ {print $2}' /tmp/fasterstore-put-headers.out | tr -d '\r\n\"')"
echo "uploaded bucket=${BUCKET} key=${KEY} etag=${ETAG}"
