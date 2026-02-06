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

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates wget

# Install wal-g (you should replace this with your actual wal-g installation method)
# This is just a placeholder - in production, you'd copy from your wal-g build
# or download from releases
# COPY --from=walg-builder /usr/local/bin/wal-g /usr/local/bin/wal-g

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
