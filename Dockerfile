FROM --platform=$BUILDPLATFORM golang:1.27.1-alpine AS build
ARG TARGETOS=linux
ARG TARGETARCH
RUN apk add --no-cache ca-certificates python3 py3-pip nodejs \
    && python3 -m venv /opt/docs-tools \
    && /opt/docs-tools/bin/pip install --no-cache-dir jsonschema==4.26.0 openapi-spec-validator==0.9.0
ENV PYTHON=/opt/docs-tools/bin/python
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
# Copy build inputs explicitly: never copy .env, backups or arbitrary workspace files.
COPY cmd ./cmd
COPY internal ./internal
COPY web ./web
COPY public-docs ./public-docs
COPY scripts ./scripts
COPY .github ./.github
COPY compose.yaml .env.example Dockerfile ./
RUN sh scripts/build-skill.sh --check \
    && sh scripts/build-llms.sh --check \
    && sh scripts/check-contracts.sh --static
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/relayhub ./cmd/relayhub

# static includes trusted CA roots for outbound HTTPS, with no shell/package manager.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/relayhub /relayhub
USER 65532:65532
ENTRYPOINT ["/relayhub"]
