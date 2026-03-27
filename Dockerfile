# Build stage

FROM golang:1.26-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build fully static binary - strip debug symbols to reduce size
# BIFRACT_VERSION can be passed as a build arg (e.g. docker build --build-arg BIFRACT_VERSION=v1.0.0)
ARG BIFRACT_VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
-a \
-installsuffix cgo \
-ldflags="-w -s -X main.Version=${BIFRACT_VERSION}" \
-o bifract-server ./cmd/bifract-server

# Pull CA certs to copy into scratch (no apk in scratch to fetch these)
FROM alpine:latest AS certs
RUN apk --no-cache add ca-certificates

# Runtime stage
FROM scratch

# CA certs needed for any outbound HTTPS calls your app makes
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy binary and assets
COPY --from=builder /app/bifract-server /bifract-server
COPY --from=builder /app/web /web

# Writable temp directory for feed repo cloning (go-git)
COPY --from=builder --chown=65534:65534 /tmp /tmp

# Writable archives directory (overlaid by Docker volume at runtime)
COPY --from=builder --chown=65534:65534 /tmp /archives

# Nobody user — scratch has no useradd, so reference by UID directly
# This prevents the process running as root (uid 0)
USER 65534:65534

EXPOSE 8080

ENTRYPOINT ["/bifract-server"]
