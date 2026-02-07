# syntax=docker/dockerfile:1

FROM rust:1.89-bookworm AS builder
WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples ./examples

RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /build/target/release/fasterstore /usr/local/bin/fasterstore

ENV FS_BIND_ADDR=0.0.0.0:9000
ENV FS_ADVERTISE_ADDR=http://127.0.0.1:9000
ENV FS_DATA_DIR=/data

EXPOSE 9000
VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/fasterstore"]
