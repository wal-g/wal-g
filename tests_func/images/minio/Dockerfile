FROM minio/mc:RELEASE.2025-01-17T23-25-50Z AS minio_client
FROM minio/minio:RELEASE.2025-01-20T14-49-07Z

COPY --from=minio_client /usr/bin/mc /usr/bin/mc

ENV MINIO_ROOT_USER=S3_ACCESS_KEY
ENV MINIO_ROOT_PASSWORD=S3_SECRET_KEY

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

HEALTHCHECK --interval=30s --timeout=5s \
    CMD /usr/bin/healthcheck.sh

EXPOSE $S3_PORT

CMD ["server", "/export"]
