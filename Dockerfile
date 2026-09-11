FROM golang:1.24-alpine AS build

RUN apk add --no-cache python3 py3-pip nodejs \
    && python3 -m venv /opt/docs-tools \
    && /opt/docs-tools/bin/pip install --no-cache-dir jsonschema==4.26.0 openapi-spec-validator==0.9.0
ENV PYTHON=/opt/docs-tools/bin/python
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN sh scripts/build-skill.sh --check \
    && sh scripts/build-llms.sh --check \
    && sh scripts/check-contracts.sh --static
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/relayhub ./cmd/relayhub

FROM alpine:3.22

RUN apk add --no-cache ca-certificates \
    && addgroup -S relayhub \
    && adduser -S -G relayhub relayhub
COPY --from=build /out/relayhub /usr/local/bin/relayhub

USER relayhub
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/relayhub"]
