# Greenplum Append-Optimized file format

This document describes the on-disk format of Greenplum Append-Optimized (AO) files. WAL-G's data-loss recovery does not parse these headers; it preserves the plaintext prefix produced by decryption and zero-fills the rest of the file to its metadata EOF.

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

Small Content blocks contain rows whose encoded content fits in one block. A Large Content block is a metadata-only header for a row that is too large to fit in one Small Content block. It uses the same 8-byte header and checksums, but its fields differ:

| Word | Bits | Length | Field | Meaning |
| --- | ---: | ---: | --- | --- |
| 0 | 31 | 1 bit | reserved | Must be zero. |
| 0 | 30–28 | 3 bits | header kind | `2` for Large Content. |
| 0 | 27 | 1 bit | `hasFirstRowNum` | If set, an extra 8-byte first-row number follows the checksums. |
| 0 | 26–24 | 3 bits | executor block kind | Row representation of the large content. |
| 0 | 23 | 1 bit | reserved | Must be zero. |
| 0 bits 22–0, then Word 1 bits 31–30 | 25 bits | `largeRowCount` | Number of rows represented by this large content (normally one large row). |
| 1 | 29–0 | 30 bits | `largeContentLength` | Total uncompressed byte length of the large content. |

The header is followed by one or more Small Content blocks that carry fragments of the large row. Greenplum reads fragments until their uncompressed lengths sum to `largeContentLength`; fragment headers use `rowCount = 0`. The header and every fragment are checksummed independently when AO checksums are enabled. A fragment sequence is one logical row: preserving only some fragments can leave unusable or corrupt tuple data.

WAL-G does not check block checksums or align the recovered prefix to AO block boundaries. If decryption ends in the middle of a Small Content block, its decrypted bytes are retained and the remaining file bytes are zero-filled. If the truncation is inside a Large Content record, the zero-filled tail does not reconstruct the missing fragments or tuple payload; reading that relation may report a storage error or encounter invalid tuple data.

## AO Column

AO Column Storage (AOCS) uses additional column and datum-stream structure. `backup-fetch --allow-dataloss` treats AO Row and AOCS files the same way: it retains the decrypted prefix and zero-fills the rest to the recorded EOF.

## Recovery behavior

For an incremental AO backup, WAL-G applies the decrypted increment on top of the already restored file. If that application fails, the base bytes and bytes written from the decrypted increment are retained; the file is extended with zeroes to the logical EOF from AO backup metadata. For a full-file restore, the bytes decrypted from the file are retained and padded to the same metadata EOF.

Zero-filling restores the expected file length. It cannot recreate missing tuples, indexes, or visibility information, and it does not guarantee that queries reading damaged data will succeed.

## Greenplum implementation references

The bit layout and block checksums follow Greenplum's `src/include/cdb/cdbappendonlystorage_int.h` and `src/backend/cdb/cdbappendonlystorageformat.c`. Alignment rules are in `src/include/cdb/cdbappendonlystorage.h`.
