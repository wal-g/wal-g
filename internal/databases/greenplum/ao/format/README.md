# Greenplum Append-Optimized file format

This package contains low-level parsing and repair helpers for Greenplum Append-Optimized (AO) files. It currently understands the checksummed **AO Row** block format needed to find a trustworthy prefix after a failed restore. It is not a general AO reader.

## File layout

An AO file is a sequence of storage blocks. A regular block starts with an 8-byte header (64 bits), represented as two little-endian 32-bit words. The bit numbers below are numbered from 0 (the least significant bit) to 31 within each word.

**Word 0** (file byte offsets 0–3):

| Bits | Length | Field | Meaning |
| ---: | ---: | --- | --- |
| 31 | 1 bit | reserved | Must be zero. |
| 30–28 | 3 bits | header kind | `1` is Small Content; `2` is Large Content. |
| 27 | 1 bit | `hasFirstRowNum` | If set, an extra 8-byte first-row number follows the checksums. |
| 26–24 | 3 bits | executor block kind | Describes the stored row representation. |
| 23–10 | 14 bits | row count | Number of rows represented by the block. |
| 9–0 | 10 bits | `dataLength` (high part) | The upper 10 bits of the uncompressed payload length. |

**Word 1** (file byte offsets 4–7):

| Bits | Length | Field | Meaning |
| ---: | ---: | --- | --- |
| 31–21 | 11 bits | `dataLength` (low part) | The lower 11 bits of the uncompressed payload length. |
| 20–0 | 21 bits | `compressedLength` | The compressed payload length, or zero if the payload is uncompressed. |

The 21-bit `dataLength` is split across both words: Word 0 bits 9–0 contain its high 10 bits, and Word 1 bits 31–21 contain its low 11 bits. `dataLength` is measured in bytes. When `compressedLength` is zero, the stored payload length is `dataLength`; otherwise it is `compressedLength`. The stored payload is padded to the file format's alignment (4 bytes in older formats, 8 bytes in newer formats).

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
