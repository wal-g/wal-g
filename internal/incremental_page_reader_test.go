package internal_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"testing"

	"github.com/wal-g/wal-g/utility"

	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/testtools"
)

func TestDeltaBitmapInitialize(t *testing.T) {
	pageReader := internal.IncrementalPageReader{
		FileSize: internal.DatabasePageSize * 5,
		Blocks:   make([]uint32, 0),
	}
	deltaBitmap := roaring.BitmapOf(0, 2, 3, 12, 14)
	pageReader.DeltaBitmapInitialize(deltaBitmap)
	assert.Equal(t, pageReader.Blocks, []uint32{0, 2, 3})
}

func TestSelectNewValidPage_ZeroPage(t *testing.T) {
	pageReader := internal.IncrementalPageReader{
		Blocks: make([]uint32, 0),
	}
	pageData := make([]byte, internal.DatabasePageSize)
	var blockNo uint32 = 10
	valid := pageReader.SelectNewValidPage(pageData, blockNo)
	assert.True(t, valid)
	assert.Equal(t, []uint32{blockNo}, pageReader.Blocks)
}

func TestSelectNewValidPage_InvalidPage(t *testing.T) {
	pageReader := internal.IncrementalPageReader{
		Blocks: make([]uint32, 0),
	}
	pageData := make([]byte, internal.DatabasePageSize)
	for i := byte(0); i < 24; i++ {
		pageData[i] = i
	}
	pageData[2134] = 100
	var blockNo uint32 = 10
	valid := pageReader.SelectNewValidPage(pageData, blockNo)
	assert.False(t, valid)
	assert.Equal(t, []uint32{}, pageReader.Blocks)
}

func TestSelectNewValidPage_ValidPageLowLsn(t *testing.T) {
	pageReader := internal.IncrementalPageReader{
		Blocks: make([]uint32, 0),
	}
	var blockNo uint32 = 10
	pageFile, err := os.Open(pagedFileName)
	assert.NoError(t, err)
	defer utility.LoggedClose(pageFile, "")
	pageData := make([]byte, internal.DatabasePageSize)
	_, err = io.ReadFull(pageFile, pageData)
	assert.NoError(t, err)
	assert.NoError(t, err)
	valid := pageReader.SelectNewValidPage(pageData, blockNo)
	assert.True(t, valid)
	assert.Equal(t, []uint32{blockNo}, pageReader.Blocks)
}

func TestSelectNewValidPage_ValidPageHighLsn(t *testing.T) {
	pageReader := internal.IncrementalPageReader{
		Blocks: make([]uint32, 0),
		Lsn:    uint64(1) << 62,
	}
	var blockNo uint32 = 10
	pageFile, err := os.Open(pagedFileName)
	assert.NoError(t, err)
	defer utility.LoggedClose(pageFile, "")
	pageData := make([]byte, internal.DatabasePageSize)
	_, err = io.ReadFull(pageFile, pageData)
	assert.NoError(t, err)
	assert.NoError(t, err)
	valid := pageReader.SelectNewValidPage(pageData, blockNo)
	assert.True(t, valid)
	assert.Equal(t, []uint32{}, pageReader.Blocks)
}

func TestWriteDiffMapToHeader(t *testing.T) {
	pageReader := internal.IncrementalPageReader{
		Blocks: []uint32{1, 2, 33},
	}
	var header bytes.Buffer
	pageReader.WriteDiffMapToHeader(&header)
	var diffBlockCount uint32
	err := binary.Read(&header, binary.LittleEndian, &diffBlockCount)
	assert.NoError(t, err)
	actualBlocks := make([]uint32, 0)
	for i := 0; i < int(diffBlockCount); i++ {
		var blockNo uint32
		err := binary.Read(&header, binary.LittleEndian, &blockNo)
		assert.NoError(t, err)
		actualBlocks = append(actualBlocks, blockNo)
	}
	testtools.AssertReaderIsEmpty(t, &header)
	assert.Equal(t, pageReader.Blocks, actualBlocks)
}

func TestFullScanInitialize(t *testing.T) {
	pageFile, err := os.Open(pagedFileName)
	defer utility.LoggedClose(pageFile, "")
	assert.NoError(t, err)
	pageReader := internal.IncrementalPageReader{
		PagedFile: pageFile,
		Blocks:    make([]uint32, 0),
		Lsn:       sampeLSN,
	}
	err = pageReader.FullScanInitialize()
	assert.NoError(t, err)
	assert.Equal(t, []uint32{3, 4, 5, 6, 7}, pageReader.Blocks)
}

func makePageDataReader() ioextensions.ReadSeekCloser {
	pageCount := int64(8)
	pageData := make([]byte, pageCount*internal.DatabasePageSize)
	for i := int64(0); i < pageCount; i++ {
		for j := i * internal.DatabasePageSize; j < (i+1)*internal.DatabasePageSize; j++ {
			pageData[j] = byte(i)
		}
	}
	pageDataReader := bytes.NewReader(pageData)
	return &ioextensions.ReadSeekCloserImpl{Reader: pageDataReader, Seeker: pageDataReader, Closer: &testtools.NopCloser{}}
}

func TestRead(t *testing.T) {
	blocks := []uint32{1, 2, 4}
	header := []byte{12, 13, 14}
	expectedRead := make([]byte, 3+3*internal.DatabasePageSize)
	copy(expectedRead, header)
	for id, i := range blocks {
		for j := 3 + int64(id)*internal.DatabasePageSize; j < 3+(int64(id)+1)*internal.DatabasePageSize; j++ {
			expectedRead[j] = byte(i)
		}
	}

	pageReader := internal.IncrementalPageReader{
		PagedFile: makePageDataReader(),
		Blocks:    blocks,
		Next:      header,
	}

	actualRead := make([]byte, 3+3*internal.DatabasePageSize)
	_, err := io.ReadFull(&pageReader, actualRead)
	assert.NoError(t, err)
	assert.Equal(t, expectedRead, actualRead)
	testtools.AssertReaderIsEmpty(t, &pageReader)
}

func TestAdvanceFileReader(t *testing.T) {
	pageReader := internal.IncrementalPageReader{
		PagedFile: makePageDataReader(),
		Blocks:    []uint32{5, 9},
	}
	err := pageReader.AdvanceFileReader()
	assert.NoError(t, err)
	assert.Equal(t, []uint32{9}, pageReader.Blocks)
	expectedNext := make([]byte, internal.DatabasePageSize)
	for i := int64(0); i < internal.DatabasePageSize; i++ {
		expectedNext[i] = 5
	}
	assert.Equal(t, expectedNext, pageReader.Next)
}

func TestDrainMoreData_NoBlocks(t *testing.T) {
	pageReader := internal.IncrementalPageReader{}
	succeed, err := pageReader.DrainMoreData()
	assert.NoError(t, err)
	assert.False(t, succeed)
}

func TestDrainMoreData_HasBlocks(t *testing.T) {
	pageReader := internal.IncrementalPageReader{
		PagedFile: makePageDataReader(),
		Blocks:    []uint32{3, 6},
	}
	succeed, err := pageReader.DrainMoreData()
	assert.NoError(t, err)
	assert.True(t, succeed)
	assert.Equal(t, []uint32{6}, pageReader.Blocks)
	expectedNext := make([]byte, internal.DatabasePageSize)
	for i := int64(0); i < internal.DatabasePageSize; i++ {
		expectedNext[i] = 3
	}
	assert.Equal(t, expectedNext, pageReader.Next)
}
