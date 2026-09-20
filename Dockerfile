FROM node:20.19-alpine AS admin
WORKDIR /src/web/admin
COPY web/admin/package.json web/admin/package-lock.json ./
RUN npm ci
COPY web/admin/index.html web/admin/tsconfig.json web/admin/vite.config.ts ./
COPY web/admin/src ./src
COPY web/admin/tests ./tests
RUN npm run typecheck && npm test && npm run build

FROM --platform=$BUILDPLATFORM golang:1.27.1-alpine AS build
ARG TARGETOS=linux
ARG TARGETARCH
RUN apk add --no-cache ca-certificates python3 py3-pip nodejs \
    && python3 -m venv /opt/docs-tools \
    && /opt/docs-tools/bin/pip install --no-cache-dir jsonschema==4.26.0 openapi-spec-validator==0.9.0
ENV PYTHON=/opt/docs-tools/bin/python
WORKDIR /src/backend
COPY backend/go.mod backend/go.sum ./
RUN go mod download
# Copy build inputs explicitly: never copy .env, backups or arbitrary workspace files.
COPY backend/cmd ./cmd
COPY backend/internal ./internal
COPY backend/web ./web
COPY backend/scripts ./scripts
COPY --from=admin /src/web/admin/dist /src/web/admin/dist
COPY web/docs /src/web/docs
COPY docs/developer/streaming-protocol.md /src/docs/developer/streaming-protocol.md
COPY compose.yaml .env.example Dockerfile .dockerignore /src/
RUN sh scripts/build-skill.sh --check \
    && sh scripts/build-llms.sh --check \
    && sh scripts/check-contracts.sh --static
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/relayhub ./cmd/relayhub

# static includes trusted CA roots for outbound HTTPS, with no shell/package manager.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/relayhub /relayhub
USER 65532:65532
ENTRYPOINT ["/relayhub"]
