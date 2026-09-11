FROM golang:1.24-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go test ./web
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/relayhub ./cmd/relayhub

FROM alpine:3.22

RUN apk add --no-cache ca-certificates \
    && addgroup -S relayhub \
    && adduser -S -G relayhub relayhub
COPY --from=build /out/relayhub /usr/local/bin/relayhub

USER relayhub
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/relayhub"]
