FROM ghcr.io/telophasehq/tangent-toolchain:latest AS build
WORKDIR /src
COPY . .
RUN ./setup.sh
RUN tangent plugin compile --config tangent.yaml

FROM debian:stable-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/tangent /usr/local/bin/tangent

RUN useradd -m -u 10001 app
USER app
WORKDIR /opt/tangent

COPY --chown=app:app tangent.yaml ./tangent.yaml
COPY --from=build --chown=app:app /src/plugins ./plugins

ENTRYPOINT ["tangent"]
CMD ["run", "--config", "/opt/tangent/tangent.yaml"]