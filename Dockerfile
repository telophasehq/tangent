# Multi-stage build for Rust sidecar
FROM rust:1.75-slim as builder

WORKDIR /usr/src/app
COPY . .

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Build the application
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -r -s /bin/false sidecar

# Create necessary directories
RUN mkdir -p /wasm /var/run/docker.sock
RUN chown -R sidecar:sidecar /wasm

# Copy the binary
COPY --from=builder /usr/src/app/target/release/rustagent /usr/local/bin/
COPY --from=builder /usr/src/app/config.toml /etc/config.toml

# Switch to non-root user
USER sidecar

# Set environment variables
ENV RUST_LOG=info
ENV SIDECAR_CONFIG_FILE=/etc/config.toml

# Expose WASM directory
VOLUME ["/wasm"]

# Run the sidecar
CMD ["/usr/local/bin/rustagent"] 