# Build stage

FROM golang:1.27.0-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build fully static binary - strip debug symbols to reduce size.
# CGO_ENABLED=0 alone produces a static binary; -a/-installsuffix would force a
# full stdlib rebuild on every build (~100s each) for an identical result.
# The go-build cache mount makes rebuilds incremental across docker builds.
# BIFRACT_VERSION can be passed as a build arg (e.g. docker build --build-arg BIFRACT_VERSION=v1.0.0)
ARG BIFRACT_VERSION=dev
RUN --mount=type=cache,target=/root/.cache/go-build \
CGO_ENABLED=0 GOOS=linux go build \
-ldflags="-w -s -X main.Version=${BIFRACT_VERSION}" \
-o bifract-server ./cmd/bifract-server

# Build the archive sidecar binary from the same module (version lockstep with
# the server: they share the pkg/spool on-disk format). Shipped in the same
# scratch image and run as a sidecar via `command: ["/bifract-archiver"]`.
RUN --mount=type=cache,target=/root/.cache/go-build \
CGO_ENABLED=0 GOOS=linux go build \
-ldflags="-w -s -X main.Version=${BIFRACT_VERSION}" \
-o bifract-archiver ./cmd/bifract-archiver

# Pull CA certs to copy into scratch (no apk in scratch to fetch these)
FROM alpine:latest AS certs
RUN apk --no-cache add ca-certificates

# Runtime stage
FROM scratch

# CA certs needed for any outbound HTTPS calls your app makes
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy binaries and assets
COPY --from=builder /app/bifract-server /bifract-server
COPY --from=builder /app/bifract-archiver /bifract-archiver
COPY --from=builder /app/web /web

# Writable temp directory for feed repo cloning (go-git)
COPY --from=builder --chown=65534:65534 /tmp /tmp

# Writable archives directory (overlaid by Docker volume at runtime)
COPY --from=builder --chown=65534:65534 /tmp /archives

# Writable Iceberg archive spool + data dirs (overlaid by shared volumes at
# runtime). Pre-created owned by 65534 so a mounted named volume inherits that
# ownership and the non-root process can write. The server tees to the spool;
# the archiver sidecar drains it and writes Parquet to the archive dir.
COPY --from=builder --chown=65534:65534 /tmp /var/lib/bifract/spool
COPY --from=builder --chown=65534:65534 /tmp /var/lib/bifract/archive

# Nobody user — scratch has no useradd, so reference by UID directly
# This prevents the process running as root (uid 0)
USER 65534:65534

EXPOSE 8080

ENTRYPOINT ["/bifract-server"]
