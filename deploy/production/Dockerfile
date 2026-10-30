# Rust v1 runtime image for the isolated production canary.
FROM rust:1.99.1-bookworm AS build
WORKDIR /build
COPY rust/Cargo.toml rust/Cargo.lock ./rust/
COPY rust/src ./rust/src
COPY rust/migrations ./rust/migrations
COPY contracts/v1 ./contracts/v1
RUN cargo build --locked --release --manifest-path rust/Cargo.toml --bin relayhub-server

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --no-install-recommends -y ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/rust/target/release/relayhub-server /usr/local/bin/relayhub-server
USER 65532:65532
WORKDIR /tmp
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=3s --retries=6 CMD curl --fail --silent --output /dev/null http://127.0.0.1:8080/api/v1/openapi.json
ENTRYPOINT ["/usr/local/bin/relayhub-server"]
