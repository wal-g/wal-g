# Testing Tools

WAL-G offers three prototyping programs to assist with testing and development:

* [compress](#compress)
* [extract](#extract)
* [generate](#generate)
* [delta](#delta)


#### compress

`compress` takes in a directory and minimum part size and creates a compressed tarball.

Example use cases:

Compress a local directory to disk.

```
./compress -out=$HOME/directory/to/be/compressed 1000000 directory/to/compress
```

Connect to Postgres and upload the compressed tarball to S3.

```
./compress -s 1000000 /dat/9.6/data
```

#### extract

`extract` takes in a target out directory and however many files/urls. It is often used in conjunction with `generate` to test the accuracy and speed of decompression and extraction.

Example use cases:

Extract local files.

```
./extract /target/out/directory file1 file2 
```

Extract local files and randomly generated data.

```
./extract /out/directory file1                     \
https://localhost:8080/stride-10.bytes-100.tar.lzo \
https://localhost:8080/stride-100.bytes-1000.tar.lzo
```

#### generate

`generate` outputs randomly compressed tarballs hosted on `localhost:8080`. To configure the stride length `N` and size of the data `M`, use: `https://localhost:8080/stride-N.bytes-M.tar.lzo`

Randomly generated data can be downloaded using:

```
curl -k https://localhost:8080/stride-1.bytes-1.tar.lzo \
-o /path/to/new/file
```
The URLs can also be fed directly to `extract`. Currently, `generate` only supports LZOP compression.


**NOTE:** `compress` and `extract` support profiling options using the flags `-p` and `-m`. The first generates a CPU profile to `cpu.prof` while the latter generates a memory profile to `mem.prof`.

To access the profiles, use:

```
go tool pprof wal-g FILENAME
```

#### delta

`delta` runs concurrent testing of delta backups:
- init pg_bench
- make base backup
- run bp_bench
- run pg_bench and delta backup in parallel
- make delta backup
- restore delta chain
- compare restoragtion results and highlight but difference
- if critical differences found diff will exit with panic

`delta` must be invoked with env variables necessary for wal-g `backup-push`