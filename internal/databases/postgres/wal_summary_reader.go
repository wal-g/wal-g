package postgres

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math/bits"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/wal-g/wal-g/internal/walparser"
)

// PostgreSQL src/common/blkreftable.c: native-endian magic, 24-byte relation
// entries, uint16 chunk sizes/data, zero entry, CRC32C. A chunk represents
// 65536 blocks, either as offsets or as a 4096-word bitmap.
func readWalSummary(src io.Reader, delta PagedFileDeltaMap) error {
	raw := bufio.NewReader(src)
	checksum := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	in := io.TeeReader(raw, checksum)
	var magic [4]byte
	if _, err := io.ReadFull(in, magic[:]); err != nil {
		return err
	}
	var order binary.ByteOrder = binary.LittleEndian
	if order.Uint32(magic[:]) != 0x652b137b {
		order = binary.BigEndian
		if order.Uint32(magic[:]) != 0x652b137b {
			return fmt.Errorf("invalid WAL summary magic")
		}
	}
	for {
		var entry [6]uint32
		if err := binary.Read(in, order, &entry); err != nil {
			return err
		}
		if entry == [6]uint32{} {
			break
		}
		if err := readWalSummaryEntry(in, order, entry, delta); err != nil {
			return err
		}
	}
	var stored uint32
	if err := binary.Read(raw, order, &stored); err != nil {
		return err
	}
	if stored != checksum.Sum32() {
		return fmt.Errorf("WAL summary checksum mismatch")
	}
	// Also consume the compression/encryption footer before accepting the map.
	if _, err := raw.ReadByte(); err != io.EOF {
		return fmt.Errorf("invalid WAL summary end: %v", err)
	}
	return nil
}

func readWalSummaryEntry(in io.Reader, order binary.ByteOrder, entry [6]uint32, delta PagedFileDeltaMap) error {
	if entry[3] > 3 || entry[5] > 65536 {
		return fmt.Errorf("invalid WAL summary fork/chunk count")
	}
	node := walparser.RelFileNode{SpcNode: walparser.Oid(entry[0]), DBNode: walparser.Oid(entry[1]), RelNode: walparser.Oid(entry[2])}
	var bitmap *roaring.Bitmap
	if entry[3] == 0 { // Only main forks use WAL-G's page-delta format.
		var exists bool
		bitmap, exists = delta[node]
		if entry[4] != ^uint32(0) {
			// A finite limit means create/drop/truncate. Conservatively copy
			// the whole relation; nil is a full-copy marker, not an empty map.
			bitmap = nil
		} else if !exists {
			bitmap = roaring.New()
		}
		delta[node] = bitmap
	}
	chunks := make([]uint16, entry[5])
	if err := binary.Read(in, order, chunks); err != nil {
		return err
	}
	for chunk, size := range chunks {
		if size > 4096 {
			return fmt.Errorf("invalid WAL summary chunk size %d", size)
		}
		words := make([]uint16, size)
		if err := binary.Read(in, order, words); err != nil {
			return err
		}
		if bitmap != nil {
			addWalSummaryChunk(bitmap, uint32(chunk)*65536, words)
		}
	}
	return nil
}

func addWalSummaryChunk(bitmap *roaring.Bitmap, base uint32, words []uint16) {
	for index, word := range words {
		if len(words) != 4096 {
			bitmap.Add(base + uint32(word))
			continue
		}
		for word != 0 {
			bitmap.Add(base + uint32(index*16+bits.TrailingZeros16(word)))
			word &= word - 1
		}
	}
}
