SHELL := /bin/bash

-include .env

FS_ENDPOINT ?= http://127.0.0.1:9000
FS_DEFAULT_BUCKET ?= bench
FS_DEFAULT_KEY ?= hello.txt
FILE ?= README.md
OUT ?= /tmp/fasterstore-download.out

.PHONY: help run check test clippy fmt health put get delete docker-up docker-down smoke

help:
	@echo "FasterStore shortcuts"
	@echo "  make run            - run server locally"
	@echo "  make check          - cargo check"
	@echo "  make test           - cargo test"
	@echo "  make clippy         - strict clippy"
	@echo "  make fmt            - cargo fmt"
	@echo "  make health         - call /healthz"
	@echo "  make put            - upload FILE to BUCKET/KEY"
	@echo "  make get            - download BUCKET/KEY to OUT"
	@echo "  make delete         - delete BUCKET/KEY"
	@echo "  make smoke          - put + get + delete workflow"
	@echo "  make docker-up      - single-node docker compose"
	@echo "  make docker-down    - stop single-node docker compose"
	@echo "Variables: BUCKET=<name> KEY=<path> FILE=<file> OUT=<file>"

run:
	cargo run --release

check:
	cargo check

test:
	cargo test

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

fmt:
	cargo fmt

health:
	@FS_ENDPOINT="$(FS_ENDPOINT)" ./scripts/fs-health.sh

put:
	@FS_ENDPOINT="$(FS_ENDPOINT)" ./scripts/fs-put.sh "$${BUCKET:-$(FS_DEFAULT_BUCKET)}" "$${KEY:-$(FS_DEFAULT_KEY)}" "$(FILE)"

get:
	@FS_ENDPOINT="$(FS_ENDPOINT)" ./scripts/fs-get.sh "$${BUCKET:-$(FS_DEFAULT_BUCKET)}" "$${KEY:-$(FS_DEFAULT_KEY)}" "$(OUT)"

delete:
	@FS_ENDPOINT="$(FS_ENDPOINT)" ./scripts/fs-delete.sh "$${BUCKET:-$(FS_DEFAULT_BUCKET)}" "$${KEY:-$(FS_DEFAULT_KEY)}"

smoke:
	@FS_ENDPOINT="$(FS_ENDPOINT)" ./scripts/fs-put.sh "$${BUCKET:-$(FS_DEFAULT_BUCKET)}" "$${KEY:-$(FS_DEFAULT_KEY)}" "$(FILE)"
	@FS_ENDPOINT="$(FS_ENDPOINT)" ./scripts/fs-get.sh "$${BUCKET:-$(FS_DEFAULT_BUCKET)}" "$${KEY:-$(FS_DEFAULT_KEY)}" "$(OUT)"
	@FS_ENDPOINT="$(FS_ENDPOINT)" ./scripts/fs-delete.sh "$${BUCKET:-$(FS_DEFAULT_BUCKET)}" "$${KEY:-$(FS_DEFAULT_KEY)}"

docker-up:
	docker compose --env-file examples/docker/.env.example -f examples/docker/docker-compose.single.yml up --build -d

docker-down:
	docker compose -f examples/docker/docker-compose.single.yml down -v
