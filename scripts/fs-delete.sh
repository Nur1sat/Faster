#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 2 ]]; then
  echo "Usage: $0 <bucket> <key>" >&2
  exit 1
fi

BUCKET="$1"
KEY="$2"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/fs-common.sh"

PATH_ONLY="/${BUCKET}/${KEY}"
QUERY=""
PAYLOAD_HASH="UNSIGNED-PAYLOAD"
URL="$(build_url "$PATH_ONLY" "$QUERY")"

HEADERS="$(signed_headers "DELETE" "$PATH_ONLY" "$QUERY" "$PAYLOAD_HASH")"

curl -fsS -X DELETE "$URL" \
  -H "$(echo "$HEADERS" | sed -n '1p')" \
  -H "$(echo "$HEADERS" | sed -n '2p')" \
  -H "$(echo "$HEADERS" | sed -n '3p')" \
  -H "$(echo "$HEADERS" | sed -n '4p')" \
  -o /tmp/fasterstore-delete-body.out

echo "deleted bucket=${BUCKET} key=${KEY}"
