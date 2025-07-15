FROM minio/mc:RELEASE.2024-06-12T14-34-03Z as minio_client
FROM minio/minio:RELEASE.2024-06-13T22-53-53Z

COPY --from=minio_client /usr/bin/mc /usr/bin/mc

ENV MINIO_ACCESS_KEY S3_ACCESS_KEY
ENV MINIO_SECRET_KEY S3_SECRET_KEY

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

HEALTHCHECK --interval=30s --timeout=5s \
    CMD /usr/bin/healthcheck.sh

EXPOSE $S3_PORT

CMD ["server", "/export"]
