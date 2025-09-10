# syntax=docker/dockerfile:1.7
FROM rust:1.89-slim as builder

WORKDIR /usr/src/app

# Install build dependencies
RUN --mount=type=cache,target=/var/cache/apt \
    --mount=type=cache,target=/var/lib/apt/lists \
    apt-get -yq update && apt-get -yq install --no-install-recommends \
      pkg-config libssl-dev ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Create non-root user
RUN useradd -r -s /bin/false sidecar

# Create necessary directories
RUN mkdir -p /wasm /var/run
RUN chown -R sidecar:sidecar /wasm

# Copy the binary
COPY --from=builder /usr/src/app/target/release/light-node /usr/local/bin/
COPY --from=builder /usr/src/app/light-node/config.toml /etc/config.toml
COPY --from=builder /usr/src/app/compiled/* /wasm/

# Switch to non-root user
USER sidecar

# Set environment variables
ENV LOG_LEVEL=info
ENV SIDECAR_CONFIG_FILE=/etc/config.toml

# Expose WASM directory
VOLUME ["/wasm"]

# Run the sidecar
CMD ["/usr/local/bin/light-node"] 