# syntax=docker/dockerfile:1.7
FROM debian:stable-slim

ARG GO_VERSION=1.24.7
ARG TINYGO_VERSION=0.33.0
ARG GH_ORG=telophasehq
ARG GH_REPO=tangent
ARG TANGENT_VERSION=v0.1.9
ARG TARGETOS
ARG TARGETARCH

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl git xz-utils build-essential \
    python3 python3-venv python3-pip \
  && rm -rf /var/lib/apt/lists/*

RUN curl -fL "https://go.dev/dl/go${GO_VERSION}.linux-${TARGETARCH}.tar.gz" -o /tmp/go.tgz \
 && tar -C /usr/local -xzf /tmp/go.tgz \
 && rm /tmp/go.tgz
ENV PATH=/usr/local/go/bin:$PATH

RUN curl -fL "https://github.com/tinygo-org/tinygo/releases/download/v${TINYGO_VERSION}/tinygo_${TINYGO_VERSION}_${TARGETARCH}.deb" -o /tmp/tinygo.deb \
 && apt-get update && apt-get install -y /tmp/tinygo.deb \
 && rm -rf /var/lib/apt/lists/* /tmp/tinygo.deb

RUN python3 -m venv /opt/venv \
 && /opt/venv/bin/pip install --upgrade pip \
 && /opt/venv/bin/pip install componentize-py
ENV PATH=/opt/venv/bin:$PATH

# wit-bindgen-go (for go:generate etc.)
RUN GOFLAGS="-buildvcs=false" go install github.com/bytecodealliance/wasm-tools-go/cmd/wit-bindgen-go@latest
ENV PATH=/root/go/bin:$PATH

RUN set -e; \
  case "$TARGETARCH" in \
    amd64)  ARCH=x86_64 ;; \
    arm64)  ARCH=aarch64 ;; \
    *) echo "Unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;; \
  esac; \
  PKG=tangent-cli; \
  ASSET="${PKG}-${ARCH}-unknown-linux-gnu.tar.xz"; \
  URL="https://github.com/${GH_ORG}/${GH_REPO}/releases/download/${TANGENT_VERSION}/${ASSET}"; \
  echo "Downloading $URL"; \
  curl -fsSL "$URL" -o /tmp/${ASSET}; \
  mkdir -p /tmp/tangent-cli && tar -C /tmp/tangent-cli -xJf /tmp/${ASSET}; \
  BIN="$(find /tmp/tangent-cli -type f -name tangent -perm -u+x | head -n1)"; \
  install -Dm755 "$BIN" /usr/local/bin/tangent; \
  rm -rf /tmp/${ASSET} /tmp/tangent-cli

RUN useradd -m builder
USER builder
WORKDIR /work

ENTRYPOINT ["tangent"]
CMD ["--help"]
