FROM ghcr.io/wal-g/minio:RELEASE.2024-06-13T22-53-53Z

ENV MINIO_ACCESS_KEY=S3_ACCESS_KEY
ENV MINIO_SECRET_KEY=S3_SECRET_KEY

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

HEALTHCHECK --interval=30s --timeout=5s \
    CMD /usr/bin/healthcheck.sh

CMD ["server", "/export"]
