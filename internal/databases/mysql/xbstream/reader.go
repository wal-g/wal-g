package xbstream

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/wal-g/wal-g/utility"
	"hash/crc32"
	"io"
)

type Reader struct {
	reader       io.Reader
	readPosition int64
	withChecksum bool
}

func NewReader(reader io.Reader, withChecksum bool) *Reader {
	var result = &Reader{
		withChecksum: withChecksum,
	}
	result.reader = utility.NewWithSizeReader(reader, &result.readPosition)
	return result
}

// nolint: funlen,gocyclo
func (xbs *Reader) Next() (*Chunk, error) {
	var chunk = &Chunk{}

	if xbs.withChecksum {
		chunk.crc32 = crc32.NewIEEE()
	}

	// Chunk Magic
	chunk.Magic = make([]uint8, len(chunkMagic))
	_, err := io.ReadFull(xbs.reader, chunk.Magic)
	if err != nil {
		return nil, err // note: io.EOF is valid error here
	}

	if !bytes.Equal(chunk.Magic, chunkMagic) {
		return nil, fmt.Errorf("wrong chunk magic at offset %v", xbs.readPosition)
	}

	// Chunk Flags
	err = binary.Read(xbs.reader, binary.LittleEndian, &chunk.Flags)
	if err != nil {
		return nil, io.ErrUnexpectedEOF
	}

	// Chunk Type
	err = binary.Read(xbs.reader, binary.LittleEndian, &chunk.Type)
	if err != nil {
		return nil, io.ErrUnexpectedEOF
	}
	chunk.Type = validateChunkType(chunk.Type)
	if chunk.Type == ChunkTypeUnknown && !(chunk.Flags&StreamFlagIgnorable == 1) {
		return nil, errors.New("unknown chunk type")
	}

	// Path Length
	var pathLen uint32
	err = binary.Read(xbs.reader, binary.LittleEndian, &pathLen)
	if err != nil {
		return nil, io.ErrUnexpectedEOF
	}
	if pathLen > MaxPathLen {
		return nil, fmt.Errorf("path length %v is too large at offset %v", pathLen, xbs.readPosition)
	}

	// Path
	if pathLen > 0 {
		path := make([]uint8, pathLen)
		_, err = io.ReadFull(xbs.reader, path)
		if err != nil {
			return nil, io.ErrUnexpectedEOF
		}
		chunk.Path = string(path)
	}

	if chunk.Type == ChunkTypeEOF {
		return chunk, nil
	}

	var sparseMapLen int32
	if chunk.Type == ChunkTypeSparse {
		err = binary.Read(xbs.reader, binary.LittleEndian, &sparseMapLen)
		if err != nil {
			return nil, io.ErrUnexpectedEOF
		}
	}

	err = binary.Read(xbs.reader, binary.LittleEndian, &chunk.PayloadLen)
	if err != nil {
		return nil, io.ErrUnexpectedEOF
	}
	// SIZE_T_MAX is 2^64-1 on LP64 platforms
	// however it should be 2^32-1 on 32-bit platforms
	// => we should be conservative here
	//if chunk.PayloadLen > SIZE_T_MAX {
	//	return nil, errors.New(fmt.Sprintf(" chunk length %v is too large at offset %v", chunk.PayloadLen, xbs.readPosition))
	//}

	err = binary.Read(xbs.reader, binary.LittleEndian, &chunk.Offset)
	if err != nil {
		return nil, io.ErrUnexpectedEOF
	}

	err = binary.Read(xbs.reader, binary.LittleEndian, &chunk.Checksum)
	if err != nil {
		return nil, io.ErrUnexpectedEOF
	}

	if sparseMapLen > 0 {
		chunk.SparseMap = make([]SparseChunk, sparseMapLen)
		var raw = make([]byte, 8)
		for i := 0; i < int(sparseMapLen); i++ {
			_, err = io.ReadFull(xbs.reader, raw)
			if err != nil {
				return nil, io.ErrUnexpectedEOF
			}
			chunk.SparseMap[i].SkipBytes = binary.LittleEndian.Uint32(raw[:4])
			chunk.SparseMap[i].WriteBytes = binary.LittleEndian.Uint32(raw[4:])
			if xbs.withChecksum {
				chunk.crc32.Write(raw)
			}
		}
	}

	if chunk.PayloadLen > 0 {
		chunk.Reader = io.LimitReader(xbs.reader, int64(chunk.PayloadLen))
		if xbs.withChecksum {
			chunk.Reader = io.TeeReader(chunk.Reader, chunk.crc32)
		}
	} else {
		chunk.Reader = bytes.NewReader(nil)
	}

	return chunk, nil
}

func (ch *Chunk) ValidateCheckSum() error {
	if ch.Checksum != binary.BigEndian.Uint32(ch.crc32.Sum(nil)) {
		return errors.New("chunk checksum did not match")
	}
	return nil
}

func validateChunkType(p ChunkType) ChunkType {
	if p == ChunkTypeSparse || p == ChunkTypePayload || p == ChunkTypeEOF {
		return p
	}
	return ChunkTypeUnknown
}
