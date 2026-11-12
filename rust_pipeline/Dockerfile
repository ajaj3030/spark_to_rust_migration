# ── Builder stage ──────────────────────────────────────────────
FROM rust:1.85-slim-bookworm AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y \
    cmake g++ libssl-dev pkg-config libsasl2-dev libcurl4-openssl-dev libzstd-dev \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock* ./
# Create dummy main to cache dependency builds
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release 2>/dev/null || true

# Copy real source and rebuild
COPY src/ ./src/
RUN touch src/main.rs && cargo build --release

# ── Runtime stage ─────────────────────────────────────────────
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
    libssl3 libsasl2-2 ca-certificates sqlite3 procps \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/rust-pipeline /usr/local/bin/rust-pipeline
