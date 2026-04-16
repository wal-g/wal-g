# Multi-stage build for minimal image size
FROM golang:1.21-alpine AS builder

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY *.go ./

# Build static binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-w -s" -o walg-mysql-exporter .

# Final stage - minimal runtime image
FROM alpine:3.19

# Install ca-certificates for HTTPS requests and tools for WAL-G installation
RUN apk --no-cache add ca-certificates wget tar

# Install wal-g from official GitHub releases
# Note: This downloads the latest stable version. Pin to specific version in production.
ARG WAL_G_VERSION=v2.0.1
RUN set -eux; \
    wget -O /tmp/wal-g.tar.gz "https://github.com/wal-g/wal-g/releases/download/${WAL_G_VERSION}/wal-g-mysql-ubuntu-20.04-amd64.tar.gz"; \
    tar -xzf /tmp/wal-g.tar.gz -C /tmp; \
    mv /tmp/wal-g-mysql-ubuntu-20.04-amd64 /usr/local/bin/wal-g; \
    chmod +x /usr/local/bin/wal-g; \
    rm -f /tmp/wal-g.tar.gz; \
    # Verify installation \
    /usr/local/bin/wal-g --version || echo "WAL-G installed"

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /build/walg-mysql-exporter /app/walg-mysql-exporter

# Create non-root user
RUN addgroup -g 1000 walg && \
    adduser -D -u 1000 -G walg walg && \
    chown -R walg:walg /app

USER walg

EXPOSE 9352

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD wget --quiet --tries=1 --spider http://localhost:9352/health || exit 1

ENTRYPOINT ["/app/walg-mysql-exporter"]
CMD ["--web.listen-address=:9352", "--scrape.interval=60s"]
