package postgres

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/walparser"
)

func TestParseWalSummaryFilename(t *testing.T) {
	cases := []struct {
		name     string
		ok       bool
		timeline uint32
		start    LSN
		end      LSN
	}{
		{"0000000100000001000000000000000200000000.summary", true, 1, 0x0000000100000000, 0x0000000200000000},
		// Lowercase hex is accepted.
		{"0000000200000000000000ff00000000000001ff.summary", true, 2, 0xff, 0x1ff},
		{"not_a_summary.txt", false, 0, 0, 0},
		{"0000000100000000.summary", false, 0, 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f, ok := parseWalSummaryFilename(c.name)
			require.Equal(t, c.ok, ok)
			if !ok {
				return
			}
			assert.Equal(t, c.timeline, f.timeline)
			assert.Equal(t, c.start, f.startLSN)
			assert.Equal(t, c.end, f.endLSN)
		})
	}
}

func TestSelectWalSummariesForRange_CoverageAndGaps(t *testing.T) {
	files := []walSummaryFile{
		{timeline: 1, startLSN: 0x100, endLSN: 0x200},
		{timeline: 1, startLSN: 0x200, endLSN: 0x300},
		{timeline: 1, startLSN: 0x300, endLSN: 0x400},
		{timeline: 2, startLSN: 0x000, endLSN: 0x500}, // wrong timeline
	}

	// Full coverage across [0x150, 0x350).
	got, err := selectWalSummariesForRange(files, 1, 0x150, 0x350)
	require.NoError(t, err)
	require.Len(t, got, 3)

	// Request before any file.
	_, err = selectWalSummariesForRange(files, 1, 0x0, 0x50)
	require.Error(t, err)

	// Gap in the middle.
	withGap := []walSummaryFile{
		{timeline: 1, startLSN: 0x100, endLSN: 0x200},
		{timeline: 1, startLSN: 0x280, endLSN: 0x300}, // gap 0x200..0x280
	}
	_, err = selectWalSummariesForRange(withGap, 1, 0x150, 0x280)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "gap")

	// Tail missing.
	tail := []walSummaryFile{
		{timeline: 1, startLSN: 0x100, endLSN: 0x200},
	}
	_, err = selectWalSummariesForRange(tail, 1, 0x150, 0x300)
	require.Error(t, err)

	// Head missing.
	_, err = selectWalSummariesForRange(tail, 1, 0x50, 0x150)
	require.Error(t, err)

	// Empty range is rejected.
	_, err = selectWalSummariesForRange(files, 1, 0x200, 0x200)
	require.Error(t, err)
}

// summaryBuilder assembles a minimal BlockRefTable on-disk byte stream that
// matches src/common/blkreftable.c's WriteBlockRefTable output.
type summaryBuilder struct {
	buf bytes.Buffer
}

func (b *summaryBuilder) writeMagic() {
	_ = binary.Write(&b.buf, binary.LittleEndian, blockRefTableMagic)
}

type summaryEntryChunk struct {
	// Array representation: one uint16 offset per modified block.
	array []uint16
	// Bitmap representation: exactly 4096 uint16 words. Mutually exclusive with array.
	bitmap []uint16
}

func (b *summaryBuilder) writeEntry(spc, db, rel uint32, fork int32, limitBlock uint32, chunks []summaryEntryChunk) {
	// Serialized entry header: 24 bytes.
	_ = binary.Write(&b.buf, binary.LittleEndian, spc)
	_ = binary.Write(&b.buf, binary.LittleEndian, db)
	_ = binary.Write(&b.buf, binary.LittleEndian, rel)
	_ = binary.Write(&b.buf, binary.LittleEndian, uint32(fork))
	_ = binary.Write(&b.buf, binary.LittleEndian, limitBlock)
	_ = binary.Write(&b.buf, binary.LittleEndian, uint32(len(chunks)))

	// chunk_usage array.
	for _, c := range chunks {
		var used uint16
		switch {
		case c.bitmap != nil:
			if len(c.bitmap) != maxEntriesPerChunk {
				panic("bitmap must be exactly 4096 uint16 words")
			}
			used = maxEntriesPerChunk
		default:
			used = uint16(len(c.array))
		}
		_ = binary.Write(&b.buf, binary.LittleEndian, used)
	}
	// chunk payloads (skip empty chunks).
	for _, c := range chunks {
		if c.bitmap != nil {
			for _, w := range c.bitmap {
				_ = binary.Write(&b.buf, binary.LittleEndian, w)
			}
			continue
		}
		for _, off := range c.array {
			_ = binary.Write(&b.buf, binary.LittleEndian, off)
		}
	}
}

// finish writes the 24-byte zero sentinel and the CRC-32C footer.
func (b *summaryBuilder) finish() []byte {
	zero := make([]byte, serializedEntrySize)
	b.buf.Write(zero)
	crc := crc32.Checksum(b.buf.Bytes(), castagnoliTable)
	_ = binary.Write(&b.buf, binary.LittleEndian, crc)
	return b.buf.Bytes()
}

func writeTempSummary(t *testing.T, data []byte) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "*.summary")
	require.NoError(t, err)
	_, err = f.Write(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

func TestParseWalSummaryFile_ArrayChunk(t *testing.T) {
	b := &summaryBuilder{}
	b.writeMagic()
	// rel (spc=1663, db=16385, rel=100) main fork, no truncation, one chunk
	// with blocks {10, 20, 30}.
	b.writeEntry(1663, 16385, 100, mainForkNum, invalidBlockNumber,
		[]summaryEntryChunk{{array: []uint16{10, 20, 30}}})
	path := writeTempSummary(t, b.finish())

	state := make(map[relForkKey]*roaring.Bitmap)
	require.NoError(t, parseWalSummaryFile(path, state))

	key := relForkKey{
		rel:     walparser.RelFileNode{SpcNode: 1663, DBNode: 16385, RelNode: 100},
		forkNum: mainForkNum,
	}
	require.Contains(t, state, key)
	got := state[key].ToArray()
	assert.Equal(t, []uint32{10, 20, 30}, got)
}

func TestParseWalSummaryFile_BitmapChunk(t *testing.T) {
	b := &summaryBuilder{}
	b.writeMagic()
	bitmap := make([]uint16, maxEntriesPerChunk)
	// Set bits for blocks 0, 1, 15, 16, 17. Within a uint16, bit 0 is the
	// lowest block number.
	bitmap[0] = 1<<0 | 1<<1 | 1<<15
	bitmap[1] = 1<<0 | 1<<1 // blocks 16 and 17
	b.writeEntry(1663, 16385, 42, mainForkNum, invalidBlockNumber,
		[]summaryEntryChunk{{bitmap: bitmap}})
	path := writeTempSummary(t, b.finish())

	state := make(map[relForkKey]*roaring.Bitmap)
	require.NoError(t, parseWalSummaryFile(path, state))

	key := relForkKey{
		rel:     walparser.RelFileNode{SpcNode: 1663, DBNode: 16385, RelNode: 42},
		forkNum: mainForkNum,
	}
	require.Contains(t, state, key)
	assert.Equal(t, []uint32{0, 1, 15, 16, 17}, state[key].ToArray())
}

func TestParseWalSummaryFile_LimitBlockPrunesEarlierBlocks(t *testing.T) {
	// First summary records blocks {5, 10, 20}. Second summary announces a
	// truncation to 15, then adds nothing. Combined view: {5, 10}.
	b1 := &summaryBuilder{}
	b1.writeMagic()
	b1.writeEntry(1663, 16385, 7, mainForkNum, invalidBlockNumber,
		[]summaryEntryChunk{{array: []uint16{5, 10, 20}}})
	p1 := writeTempSummary(t, b1.finish())

	b2 := &summaryBuilder{}
	b2.writeMagic()
	b2.writeEntry(1663, 16385, 7, mainForkNum, 15, nil)
	p2 := writeTempSummary(t, b2.finish())

	state := make(map[relForkKey]*roaring.Bitmap)
	require.NoError(t, parseWalSummaryFile(p1, state))
	require.NoError(t, parseWalSummaryFile(p2, state))

	key := relForkKey{
		rel:     walparser.RelFileNode{SpcNode: 1663, DBNode: 16385, RelNode: 7},
		forkNum: mainForkNum,
	}
	require.Contains(t, state, key)
	assert.Equal(t, []uint32{5, 10}, state[key].ToArray())
}

func TestParseWalSummaryFile_BadMagic(t *testing.T) {
	data := []byte{0xde, 0xad, 0xbe, 0xef}
	path := writeTempSummary(t, data)
	err := parseWalSummaryFile(path, make(map[relForkKey]*roaring.Bitmap))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "magic")
}

func TestParseWalSummaryFile_BadCRC(t *testing.T) {
	b := &summaryBuilder{}
	b.writeMagic()
	b.writeEntry(1663, 16385, 100, mainForkNum, invalidBlockNumber,
		[]summaryEntryChunk{{array: []uint16{10}}})
	data := b.finish()
	// Flip the last byte of the CRC footer.
	data[len(data)-1] ^= 0xFF
	path := writeTempSummary(t, data)
	err := parseWalSummaryFile(path, make(map[relForkKey]*roaring.Bitmap))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CRC")
}

func TestListWalSummaryFiles_SkipsNonSummaryFiles(t *testing.T) {
	dir := t.TempDir()
	// One valid summary and two unrelated files.
	name := "0000000100000001000000000000000200000000.summary"
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte{}, 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "README"), []byte{}, 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "temp.summary"), []byte{}, 0644))
	files, err := listWalSummaryFiles(dir)
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.Equal(t, uint32(1), files[0].timeline)
}
