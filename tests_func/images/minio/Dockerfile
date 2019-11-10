FROM minio/mc:RELEASE.2019-01-30T19-57-22Z
FROM minio/minio:RELEASE.2019-01-31T00-31-19Z

COPY --from=0 /usr/bin/mc /usr/bin/mc

ENV MINIO_ACCESS_KEY {{MINIO_ACCESS_KEY}}
ENV MINIO_SECRET_KEY {{MINIO_SECRET_KEY}}

ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

HEALTHCHECK --interval=30s --timeout=5s \
    CMD /usr/bin/healthcheck.sh

EXPOSE 9000

CMD ["server", "/export"]
