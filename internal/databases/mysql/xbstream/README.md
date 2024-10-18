## xbstream format

xbstream - is an archive format created by Percona(c) team to store MySQL backups. Main feature of this format - it consist of blocks of different files, and this allows writers (backup tool) to write several files (almost) concurrenlty (in MySQL case, it allows to write to the archive redo-log while writing huge tablespaces, so redo logs won't be rotated).

It is simple format that doesn't support compression/encryption by itself. It is unix-philosophy archiver.

xbstream archive consist of Chunks. There are 3 types of Chunks:
 * 'P' - Payload chunk - chunk that consist of raw file content. We just copy its content to destination file.
 * 'S' - Sparse chunks - special kind of chunks that also contains SparseMap - instructions of how we should unpack data of the chunk. (see below)
 * 'E' - End Of File chunk - special kind of chunk that signals that this file ended. No more data expected for this file.
 * '\0' - Unknown chunk type. We can ignore it when `StreamFlagIgnorable` flag is set. In other case whe should end archive processing with an error.

Chunk header format:
```
* Magic:         8 bytes = "XBSTCK01"
* Flags:         1 byte
* ChunkType:     1 byte
* PathLen:       4 bytes
* Path: 	     $PathLen bytes
* SparseMapLen:  4 bytes (only for ChunkType = Sparse)
* PayloadLen:    8 bytes (for ChunkType = Payload | Sparse)
* PayloadOffset: 8 bytes (for ChunkType = Payload | Sparse)
* Checksum:      4 bytes (for ChunkType = Payload | Sparse)
* SparseMap:     $SparseMapLen x (4 + 4) bytes
    * SkipBytes:      4 bytes
    * WriteBytes:     4 bytes
* Payload:		 $PayloadLen bytes
```

**Sparse chunks**

Each SparseMap's entry consist of pair (SkipBytes; WriteBytes). For each entry we should firstly skip $SkipBytes then copy $WriteBytes from payload to destination files. xtrabackup uses this type of chunks to archive Innodb compressed pages, so it is supposed that we will use `fallocate (2)` with `mode` = `FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE`. In this case punch holes will be released back to Linux, and it will be able to optimise disk usage.

Sparse messages are processed as follows:
```python
for schunk in sparse:
    seek(schunk.SkipBytes, SeekCurrent)
    write(schunk.WriteBytes, data from payload)
```

### How compression / encryption handled

From xbstream point of view - compressed MySQL backup is just archive of compressed files. In other words - `Path` field will end with `.lz4` (or `.zstd` or deprecated qpress format `.qp`) and its payload will be regular compressed file. That's why you can end up with a directory of compressed files when you don't pass `--decrypt` option to `xbstream` tool.

Encryption ("NONE", "AES128", "AES192","AES256") is done in similar way with suffix `.xbcrypt`.

When both options used - xtrabackup firstly compress files, then encrypts it (because encryption is slower). So, suffix will be something like `.zstd.xbcrypt`

### How delta-backup handled

During delta backup creation `xtrabackup` knows previous backup LSN. So, it can skip InnoDB pages that wasn't modified since LSN.
For every `.idb` file `xtrabackup` will create two 'files' in archive:

`xxx.meta` file which contains information required to parse InnoDB pages from this archive (e.g. 'page_size) and 'space_id' to deal with file renaming.
```
page_size = 16384
zip_size = 0
space_id = 8
space_flags = 33
```

And file with InnoDB pages `xxx.delta`. Unarchiver should leverage InnoDB page format knowledge to unarchive file properly: it should put page at correct offset (by looking at PageSize and PageNumber)

It is Ok to expect that `.meta` file will precede `.delta` file in archive.

non-InnoDB files archived AS IS. xbstream doesn't support 'delete' file chunks, so unarchiver should remove all non-InnoDB files before applying diff. 
