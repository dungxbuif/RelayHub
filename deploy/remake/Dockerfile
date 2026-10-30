# Release rehearsal only. Exact compiler version matches the locked crate.
# This image contains the one-shot contract harness, CLI, and the disposable
# release-rehearsal API host. Store recovery runs on the host via script.
FROM rust:1.99.1-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./rust/
COPY src ./rust/src
COPY migrations ./rust/migrations
COPY tests/contracts.rs ./rust/tests/contracts.rs
COPY --from=contracts . ./contracts/v1
RUN cargo build --locked --manifest-path rust/Cargo.toml --bin relayhub \
    && cargo build --locked --manifest-path rust/Cargo.toml --bin relayhub-server \
    && cargo test --locked --manifest-path rust/Cargo.toml --test contracts --no-run \
    && mkdir /out \
    && cp rust/target/debug/relayhub /out/relayhub \
    && cp rust/target/debug/relayhub-server /out/relayhub-server \
    && find rust/target/debug/deps -maxdepth 1 -type f -name 'contracts-*' -executable -exec cp '{}' /out/contracts ';'

FROM rust:1.99.1-bookworm
RUN apt-get update \
    && apt-get install --no-install-recommends -y curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/relayhub /usr/local/bin/relayhub
COPY --from=build /out/relayhub-server /usr/local/bin/relayhub-server
COPY --from=build /out/contracts /usr/local/bin/contracts
USER 65532:65532
WORKDIR /tmp
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=5s --timeout=3s --retries=3 CMD ["/usr/local/bin/relayhub", "version"]
ENTRYPOINT ["/usr/local/bin/contracts"]
CMD ["--test-threads=1"]
