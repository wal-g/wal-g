# Greenplum Append-Optimized file format

This package contains low-level parsing and repair helpers for Greenplum Append-Optimized (AO) files. It currently understands the checksummed **AO Row** block format needed to find a trustworthy prefix after a failed restore. It is not a general AO reader.

## File layout

An AO file is a sequence of storage blocks. A regular block starts with an 8-byte header made of two little-endian 32-bit words:

| Byte offset | Length | Field |
| ---: | ---: | --- |
| 0 | 4 bytes | Word 0: header kind, flags, executor block kind, row count, and the high part of `dataLength`. |
| 4 | 4 bytes | Word 1: `compressedLength` and the low part of `dataLength`. |

Within each word, fields are packed and identified by masks. In Word 0, `0x80000000` is the reserved flag and must be zero; `0x70000000` is the header kind (`1` Small Content, `2` Large Content); `0x08000000` is `hasFirstRowNum`; `0x07000000` is the executor block kind; and `0x00FFFC00` is the row count. The upper 10 bits of `dataLength` are in Word 0's low 10 bits. In Word 1, the upper 11 bits hold the remaining `dataLength`, and the low 21 bits (`0x001FFFFF`) hold `compressedLength`.

`dataLength` is the uncompressed payload length, in bytes. A `compressedLength` of zero means the payload is uncompressed and its stored length is `dataLength`; otherwise its stored length is `compressedLength`. The stored payload is padded to the file format's alignment (4 bytes in older formats, 8 bytes in newer formats).

For checksummed files, two little-endian 32-bit CRC32C values follow the 8-byte header:

1. **Block checksum**: covers bytes after the checksum fields through the end of the block, including optional `firstRowNum`, payload, and padding. Greenplum stores this CRC32C without the final inversion.
2. **Header checksum**: covers the 8-byte header and block checksum, and protects the fields used to calculate the block extent.

Checksums are controlled by relation metadata (`WITH (checksum=...)`); they are not identified by a file-level magic value. Greenplum defaults AO checksums to enabled, but files can have checksums disabled. The recovery scanner in this package requires both checksums and therefore conservatively accepts no blocks from an unchecked file.

## Small and Large Content

Small Content blocks contain rows whose encoded content fits in one block. A Large Content header describes a row whose encoded data is split across one or more following Small Content fragment blocks. The Large Content header and its fragments form one logical row; accepting only some fragments could preserve corrupt data.

The doctor scanner accepts a contiguous prefix of complete, checksummed Small Content blocks. It stops on an incomplete or invalid block, checksum failure, unsupported header kind, or a Large Content header. It does not search later bytes for another possible header: recovery starts at a known block boundary and never skips over corruption.

## AO Column

AO Column Storage (AOCS) uses additional column and datum-stream structure. This package does not parse or salvage AOCS data. In `backup-fetch --allow-dataloss`, a damaged AOCS file can only be repaired by zero-filling the missing range; `--doctor` does not change that behavior.

## Recovery behavior

`CompleteRowPrefix` and `CompleteRowPrefixReaderAt` return the end offset of the largest valid Small Content prefix. `RepairFile` truncates damaged bytes after the trusted append offset, optionally keeps such a validated prefix, then extends the file with zeroes to the logical EOF from WAL-G backup metadata.

The append offset is taken from the AO incremental-file header. Incremental AO files are expected to begin appending at a block boundary. For a full-file restore, scanning starts at byte zero. Zero-filling restores the expected file length; it cannot recreate missing tuples, indexes, or visibility information.

## Greenplum implementation references

The bit layout and block checksums follow Greenplum's `src/include/cdb/cdbappendonlystorage_int.h` and `src/backend/cdb/cdbappendonlystorageformat.c`. Alignment rules are in `src/include/cdb/cdbappendonlystorage.h`.
