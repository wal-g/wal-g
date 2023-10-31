FROM minio/mc:RELEASE.2021-06-08T01-29-37Z
FROM minio/minio:RELEASE.2021-06-07T21-40-51Z

COPY --from=0 /usr/bin/mc /usr/bin/mc

ENV MINIO_ACCESS_KEY MINIO_ROOT_USER
ENV MINIO_SECRET_KEY MINIO_ROOT_PASSWORD
ENV MINIO_API_REQUESTS_MAX 1
ENV MINIO_API_REQUESTS_DEADLINE 2m

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

HEALTHCHECK --interval=30s --timeout=5s \
    CMD /usr/bin/healthcheck.sh

EXPOSE $S3_PORT

CMD ["server", "/export"]
