#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/fs-common.sh"

curl -fsS "$(trim_trailing_slash "$FS_ENDPOINT")/healthz"
echo
