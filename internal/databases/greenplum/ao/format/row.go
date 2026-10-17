// Package format contains low-level helpers for parsing Greenplum AO files.
package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const regularHeaderSize = 8

// CompleteRowPrefix returns the longest prefix made of complete AO Row
// SmallContent blocks. The source must start at an AO block boundary. Large
// Content records are deliberately not recovered because their data spans
// multiple storage blocks and must be kept as one logical row.
//
// GPDB stores the checksum setting in relation metadata, not in each file
// header. AO format versions align payloads to either 4 or 8 bytes. Only blocks
// with valid AO checksums are accepted as recoverable.
func CompleteRowPrefix(data []byte) int {
	prefix, _ := CompleteRowPrefixReaderAt(bytes.NewReader(data), int64(len(data)))
	return int(prefix)
}

// CompleteRowPrefixReaderAt scans AO Row block boundaries without loading the
// complete file into memory. The source must start at an AO block boundary.
func CompleteRowPrefixReaderAt(source io.ReaderAt, size int64) (int64, error) {
	if size < 0 {
		return 0, io.ErrUnexpectedEOF
	}
	prefix8 := scanRowBlocks(source, size, 8)
	prefix4 := scanRowBlocks(source, size, 4)
	if prefix4 > prefix8 {
		return prefix4, nil
	}
	return prefix8, nil
}

// RepairFile truncates an AO Row file at the append offset, optionally keeps
// its checksum-verified complete blocks, and zero-fills it to logicalEOF.
func RepairFile(filePath string, appendOffset, logicalEOF int64, preserveCompleteRows bool, mode os.FileMode, fsync bool) error {
	if appendOffset < 0 || logicalEOF < 0 || appendOffset > logicalEOF {
		return fmt.Errorf("invalid AO repair range: append offset %d, logical EOF %d", appendOffset, logicalEOF)
	}
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, mode)
	if err != nil {
		return err
	}
	defer file.Close()

	prefixEOF := appendOffset
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if preserveCompleteRows && info.Size() >= appendOffset {
		scanSize := info.Size()
		if scanSize > logicalEOF {
			scanSize = logicalEOF
		}
		prefixLen, err := CompleteRowPrefixReaderAt(
			io.NewSectionReader(file, appendOffset, scanSize-appendOffset), scanSize-appendOffset)
		if err != nil {
			return err
		}
		prefixEOF += prefixLen
	}
	if err := file.Truncate(prefixEOF); err != nil {
		return err
	}
	if err := file.Truncate(logicalEOF); err != nil {
		return err
	}
	if err := file.Chmod(mode); err != nil {
		return err
	}
	if fsync {
		return file.Sync()
	}
	return nil
}

func scanRowBlocks(source io.ReaderAt, size int64, alignment int) int64 {
	var pos int64
	for pos < size {
		blockLen, ok := smallContentBlockLen(source, pos, size-pos, alignment)
		if !ok {
			return pos
		}
		pos += blockLen
	}
	return pos
}

func smallContentBlockLen(source io.ReaderAt, pos, remaining int64, alignment int) (int64, bool) {
	if remaining < regularHeaderSize {
		return 0, false
	}
	var baseHeader [regularHeaderSize]byte
	if _, err := source.ReadAt(baseHeader[:], pos); err != nil {
		return 0, false
	}
	word0 := binary.LittleEndian.Uint32(baseHeader[0:4])
	word1 := binary.LittleEndian.Uint32(baseHeader[4:8])
	if word0&0x80000000 != 0 || (word0&0x70000000)>>28 != 1 {
		return 0, false
	}
	executorKind := (word0 & 0x07000000) >> 24
	rowCount := (word0 & 0x00FFFC00) >> 10
	dataLen := ((word0 & 0x3ff) << 11) | ((word1 & 0xffe00000) >> 21)
	compressedLen := word1 & 0x001fffff
	if (executorKind != 1 && executorKind != 2) || rowCount == 0 || dataLen == 0 || compressedLen > dataLen {
		return 0, false
	}

	headerLen := regularHeaderSize + 8 // block checksum and header checksum
	if word0&0x08000000 != 0 {
		headerLen += 8 // firstRowNum
	}
	if remaining < int64(headerLen) {
		return 0, false
	}
	payloadLen := uint64(dataLen)
	if compressedLen != 0 {
		payloadLen = uint64(compressedLen)
	}
	blockLen := uint64(headerLen) + ((payloadLen + uint64(alignment-1)) / uint64(alignment) * uint64(alignment))
	if blockLen > uint64(remaining) {
		return 0, false
	}
	if !validChecksums(source, pos, int64(blockLen), headerLen) {
		return 0, false
	}
	return int64(blockLen), true
}

func validChecksums(source io.ReaderAt, blockOffset, blockLen int64, headerLen int) bool {
	// GPDB's AO CRC32C uses an initial value of all ones and deliberately
	// omits the final inversion. Go's checksum API returns the inverted value.
	table := crc32.MakeTable(crc32.Castagnoli)
	header := make([]byte, headerLen)
	if _, err := source.ReadAt(header, blockOffset); err != nil {
		return false
	}
	storedBlockChecksum := binary.LittleEndian.Uint32(header[8:12])
	storedHeaderChecksum := binary.LittleEndian.Uint32(header[12:16])
	if ^crc32.Checksum(header[:12], table) != storedHeaderChecksum {
		return false
	}
	hasher := crc32.New(table)
	if _, err := io.CopyN(hasher, io.NewSectionReader(source, blockOffset+16, blockLen-16), blockLen-16); err != nil {
		return false
	}
	return ^hasher.Sum32() == storedBlockChecksum
}
