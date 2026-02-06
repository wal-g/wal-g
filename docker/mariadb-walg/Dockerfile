# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  MariaDB + WAL-G: Production-Ready Docker Image                            ║
# ║  Supports: Automatic restore, PITR, graceful shutdown                      ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

FROM mariadb:11.0

# Install dependencies
RUN apt-get update && \
    apt-get install -y \
        mariadb-backup \
        curl \
        tini \
        gosu \
    && rm -rf /var/lib/apt/lists/*

# Copy wal-g binary (must be built for linux/amd64)
COPY wal-g /usr/local/bin/wal-g
RUN chmod +x /usr/local/bin/wal-g

# Copy entrypoint wrapper
COPY docker-entrypoint-walg.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint-walg.sh

# Copy binlog replay helper
COPY binlog-replay-helper.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/binlog-replay-helper.sh

# Use tini as PID 1 for proper signal handling
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/docker-entrypoint-walg.sh"]

# Default to running mariadbd
CMD ["mariadbd"]
