package postgres

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/walparser"
)

func summaryFixture(order binary.ByteOrder, limit uint32, dense bool) []byte {
	var data bytes.Buffer
	_ = binary.Write(&data, order, uint32(0x652b137b))
	chunks := []uint16{2, 1}
	if dense {
		chunks = []uint16{4096}
	}
	_ = binary.Write(&data, order, [6]uint32{1663, 5, 123, 0, limit, uint32(len(chunks))})
	_ = binary.Write(&data, order, chunks)
	if dense {
		bits := make([]uint16, 4096)
		bits[0], bits[4095] = 3, 1<<15
		_ = binary.Write(&data, order, bits)
	} else {
		_ = binary.Write(&data, order, []uint16{9, 4, 3})
	}
	_ = binary.Write(&data, order, [6]uint32{})
	_ = binary.Write(&data, order, crc32.Checksum(data.Bytes(), crc32.MakeTable(crc32.Castagnoli)))
	return data.Bytes()
}

func TestWalSummaryReader(t *testing.T) {
	node := walparser.RelFileNode{SpcNode: 1663, DBNode: 5, RelNode: 123}
	for _, order := range []binary.ByteOrder{binary.LittleEndian, binary.BigEndian} {
		for _, dense := range []bool{false, true} {
			m := NewPagedFileDeltaMap()
			require.NoError(t, readWalSummary(bytes.NewReader(summaryFixture(order, ^uint32(0), dense)), m))
			want := []uint32{4, 9, 65539}
			if dense {
				want = []uint32{0, 1, 65535}
			}
			require.Equal(t, want, m[node].ToArray())
			// A finite limit means creation/drop/truncation: copy the relation
			// fully, even when an earlier/later summary also marks single blocks.
			require.NoError(t, readWalSummary(bytes.NewReader(summaryFixture(order, 8, dense)), m))
			require.Nil(t, m[node])
			require.NoError(t, readWalSummary(bytes.NewReader(summaryFixture(order, ^uint32(0), dense)), m))
			require.Nil(t, m[node])
		}
	}
}

func TestWalSummaryReaderRejectsDamage(t *testing.T) {
	data := summaryFixture(binary.LittleEndian, ^uint32(0), false)
	for end := 0; end < len(data); end++ {
		require.Error(t, readWalSummary(bytes.NewReader(data[:end]), NewPagedFileDeltaMap()))
	}
	data[len(data)-1] ^= 1
	require.ErrorContains(t, readWalSummary(bytes.NewReader(data), NewPagedFileDeltaMap()), "checksum")
}

func TestWalSummaryCoverage(t *testing.T) {
	files := []walSummaryFile{{start: 10, end: 20}, {start: 19, end: 30}, {start: 30, end: 40}}
	selected, err := selectWalSummaries(files, 15, 35)
	require.NoError(t, err)
	require.Len(t, selected, 3)
	_, err = selectWalSummaries(files, 9, 35)
	require.Error(t, err)
	_, err = selectWalSummaries(files, 15, 41)
	require.Error(t, err)
	_, err = selectWalSummaries([]walSummaryFile{files[0], files[2]}, 15, 35)
	require.Error(t, err)
}

func TestWalSummaryFullCopyMarkers(t *testing.T) {
	m := NewPagedFileDeltaMap()
	bundle := &Bundle{DeltaMap: m}
	require.False(t, bundle.walSummaryRequiresFullCopy("base/5/123.2"))
	node := walparser.RelFileNode{SpcNode: 1663, DBNode: 5, RelNode: 123}
	m[node] = nil
	require.True(t, bundle.walSummaryRequiresFullCopy("base/5/123.2"))
	delete(m, node)
	node.RelNode = 0
	m[node] = nil
	require.True(t, bundle.walSummaryRequiresFullCopy("base/5/456"))
	require.False(t, bundle.walSummaryRequiresFullCopy("base/6/456"))
}
