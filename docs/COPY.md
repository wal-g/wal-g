# Exporting copy file lists for rclone

PostgreSQL, MySQL, Greenplum, MongoDB, and Redis/Valkey support exporting the `copy` plan instead of transferring objects:

```bash
wal-g copy --from=source.yaml --backup-name=LATEST \
  --output=rclone --output-file=files.txt
```

`--output rclone` writes a list compatible with `rclone --files-from-raw`: one source-relative object path per line, without headers, quoting, or comments. Spaces and names starting with `#` or `;` are preserved. Names containing line breaks or NUL cannot be represented and cause an error.

`--output-file` is required with `--output`. The output directory must already exist. The output file is replaced only after the complete list has been written successfully.

`--to` is optional in this mode and, if provided, is ignored. WAL-G reads source metadata to build and validate the plan; it does not configure or access the destination, transfer payload objects, or exclude files already present at the destination. Normal copying still requires `--to`.

The listing uses the same backup selection and dependency planning as `copy`. Omit `--backup-name` to select all backups. For PostgreSQL, MySQL, Greenplum, and MongoDB, add `--with-history` to include the database's archived recovery history. Greenplum also honors `--wal-wait-timeout`.

Use source and destination roots corresponding to the WAL-G storage prefixes:

```bash
rclone copy source:bucket/prefix destination:bucket/prefix \
  --files-from-raw=files.txt --no-traverse
```

The list is ordered by WAL-G's publication phases, but it does not encode barriers between phases. A single rclone invocation does not preserve WAL-G's guarantee that backup sentinels are published only after their dependencies. For that guarantee, use normal `wal-g copy` or transfer phase-specific lists separately.

The list contains object names, not snapshots of their contents. In particular, MySQL's binlog sentinel is read from the source when rclone runs, so it may have changed since planning. Generated metadata without an existing source object, and plans requiring object renames, cannot be exported as a plain file list.

Rclone does not report missing source files from `--files-from-raw` as errors; check the resulting transfer separately. See the [rclone file-list documentation](https://rclone.org/filtering/#files-from-raw-read-list-of-source-file-names-without-any-processing).
