package xbstream

import (
	"hash"
	"io"
)

/*
		xbstream file format: whole file consists of Chunks.

		Chunk format:
		(numbers are Little Endian)
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

		Sparse messages are processed as follows:
		for schunk in sparse:
			seek(schunk.SkipBytes, SeekCurrent)
			write(schunk.WriteBytes, data from payload)
*/
var chunkMagic = []uint8("XBSTCK01")

type ChunkFlag uint8

const StreamFlagIgnorable ChunkFlag = 0x01
const MaxPathLen = 512

type ChunkType uint8

const (
	ChunkTypeUnknown = ChunkType(0)
	ChunkTypePayload = ChunkType('P')
	ChunkTypeSparse  = ChunkType('S')
	ChunkTypeEOF     = ChunkType('E')
)

type Chunk struct {
	ChunkHeader
	io.Reader

	crc32 hash.Hash32
}

type ChunkHeader struct {
	Magic []uint8
	Flags ChunkFlag
	Type  ChunkType
	// compression/encryption are xtrabackup primitives, xbstream knows nothing about it.
	// so, all other fields - just instructions how to write chunk's content to disk (without any knowledge of the content)
	Path       string
	PayloadLen uint64 // payload len
	Offset     uint64 // offset in file
	SparseMap  []SparseChunk
	Checksum   uint32
}

type SparseChunk struct {
	SkipBytes  uint32
	WriteBytes uint32
}
