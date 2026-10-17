package format

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func smallContentBlock(payload []byte) []byte {
	block := make([]byte, regularHeaderSize+8+((len(payload)+7)/8*8))
	word0 := uint32(1<<28) | uint32(1<<24) | uint32(1<<10) // small content, varblock, one row
	word0 |= uint32(len(payload)>>11) & 0x3ff
	word1 := (uint32(len(payload)) & 0x7ff) << 21
	binary.LittleEndian.PutUint32(block[:4], word0)
	binary.LittleEndian.PutUint32(block[4:8], word1)
	copy(block[16:], payload)
	table := crc32.MakeTable(crc32.Castagnoli)
	binary.LittleEndian.PutUint32(block[8:12], ^crc32.Checksum(block[16:], table))
	binary.LittleEndian.PutUint32(block[12:16], ^crc32.Checksum(block[:12], table))
	return block
}

func TestCompleteRowPrefix(t *testing.T) {
	first := smallContentBlock([]byte("first"))
	second := smallContentBlock([]byte("next"))
	stream := append(append([]byte{}, first...), second...)
	stream = append(stream, 0, 0, 0, 0)

	if got := CompleteRowPrefix(stream); got != len(first)+len(second) {
		t.Fatalf("CompleteRowPrefix() = %d, want %d", got, len(first)+len(second))
	}
}

func TestCompleteRowPrefixDropsIncompleteBlock(t *testing.T) {
	first := smallContentBlock([]byte("first"))
	second := smallContentBlock([]byte("second"))
	stream := append(append([]byte{}, first...), second[:len(second)-1]...)

	if got := CompleteRowPrefix(stream); got != len(first) {
		t.Fatalf("CompleteRowPrefix() = %d, want %d", got, len(first))
	}
}

func TestCompleteRowPrefixStopsAtLargeContent(t *testing.T) {
	first := smallContentBlock([]byte("first"))
	large := make([]byte, regularHeaderSize)
	binary.LittleEndian.PutUint32(large[:4], uint32(2<<28))
	stream := append(append([]byte{}, first...), large...)

	if got := CompleteRowPrefix(stream); got != len(first) {
		t.Fatalf("CompleteRowPrefix() = %d, want %d", got, len(first))
	}
}

func TestCompleteRowPrefixChecksChecksummedBlocks(t *testing.T) {
	block := smallContentBlock([]byte("row data"))
	if got := CompleteRowPrefix(block); got != len(block) {
		t.Fatalf("CompleteRowPrefix() = %d, want %d", got, len(block))
	}
	block[len(block)-1] ^= 0xff
	if got := CompleteRowPrefix(block); got != 0 {
		t.Fatalf("CompleteRowPrefix() after checksum mismatch = %d, want 0", got)
	}
}

func TestRepairFilePreservesVerifiedRowsAndZeroFillsRemainder(t *testing.T) {
	base := []byte("base")
	block := smallContentBlock([]byte("valid row"))
	partial := smallContentBlock([]byte("partial row"))
	data := append(append(append([]byte{}, base...), block...), partial[:len(partial)-1]...)
	fileName := filepath.Join(t.TempDir(), "ao")
	if err := os.WriteFile(fileName, data, 0600); err != nil {
		t.Fatal(err)
	}
	logicalEOF := int64(len(base) + len(block) + len(partial) + 16)
	if err := RepairFile(fileName, int64(len(base)), logicalEOF, true, 0640, false); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(fileName)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, int(logicalEOF))
	copy(want, base)
	copy(want[len(base):], block)
	if !bytes.Equal(got, want) {
		t.Fatalf("repaired bytes do not match expected verified prefix and zero tail")
	}
}

func TestRepairFileZeroFillsWholeMissingFile(t *testing.T) {
	fileName := filepath.Join(t.TempDir(), "missing-ao")
	if err := RepairFile(fileName, 0, 32, false, 0600, false); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(fileName)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, make([]byte, 32)) {
		t.Fatal("repaired missing file is not zero-filled")
	}
}
