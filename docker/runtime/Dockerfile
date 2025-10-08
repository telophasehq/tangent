FROM ghcr.io/Santiago-Labs/tangent:toolchain

WORKDIR /work
COPY . .

RUN tangent compile-wasm --config /work/tangent.yaml --wit /work/.tangent/wit

ENTRYPOINT ["/usr/local/bin/tangent", "run", "--config", "tangent.yaml"]
CMD ["--help"]
