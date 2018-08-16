package walg_test

import (
	"bytes"
	"encoding/binary"
	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g"
	"github.com/wal-g/wal-g/testtools"
	"io"
	"os"
	"testing"
)

func TestDeltaBitmapInitialize(t *testing.T) {
	pageReader := walg.IncrementalPageReader{
		FileSize: int64(walg.WalPageSize * 5),
		Blocks:   make([]uint32, 0),
	}
	deltaBitmap := roaring.BitmapOf(0, 2, 3, 12, 14)
	pageReader.DeltaBitmapInitialize(deltaBitmap)
	assert.Equal(t, pageReader.Blocks, []uint32{0, 2, 3})
}

func TestSelectNewValidPage_ZeroPage(t *testing.T) {
	pageReader := walg.IncrementalPageReader{
		Blocks: make([]uint32, 0),
	}
	pageData := make([]byte, walg.WalPageSize)
	var blockNo uint32 = 10
	valid := pageReader.SelectNewValidPage(pageData, blockNo)
	assert.True(t, valid)
	assert.Equal(t, []uint32{blockNo}, pageReader.Blocks)
}

func TestSelectNewValidPage_InvalidPage(t *testing.T) {
	pageReader := walg.IncrementalPageReader{
		Blocks: make([]uint32, 0),
	}
	pageData := make([]byte, walg.WalPageSize)
	pageData[2134] = 100
	var blockNo uint32 = 10
	valid := pageReader.SelectNewValidPage(pageData, blockNo)
	assert.False(t, valid)
	assert.Equal(t, []uint32{}, pageReader.Blocks)
}

func TestSelectNewValidPage_ValidPageLowLsn(t *testing.T) {
	pageReader := walg.IncrementalPageReader{
		Blocks: make([]uint32, 0),
	}
	var blockNo uint32 = 10
	pageFile, err := os.Open(pagedFileName)
	defer pageFile.Close()
	pageData := make([]byte, walg.WalPageSize)
	_, err = io.ReadFull(pageFile, pageData)
	assert.NoError(t, err)
	assert.NoError(t, err)
	valid := pageReader.SelectNewValidPage(pageData, blockNo)
	assert.True(t, valid)
	assert.Equal(t, []uint32{blockNo}, pageReader.Blocks)
}

func TestSelectNewValidPage_ValidPageHighLsn(t *testing.T) {
	pageReader := walg.IncrementalPageReader{
		Blocks: make([]uint32, 0),
		Lsn:    uint64(1) << 62,
	}
	var blockNo uint32 = 10
	pageFile, err := os.Open(pagedFileName)
	defer pageFile.Close()
	pageData := make([]byte, walg.WalPageSize)
	_, err = io.ReadFull(pageFile, pageData)
	assert.NoError(t, err)
	assert.NoError(t, err)
	valid := pageReader.SelectNewValidPage(pageData, blockNo)
	assert.True(t, valid)
	assert.Equal(t, []uint32{}, pageReader.Blocks)
}

func TestWriteDiffMapToHeader(t *testing.T) {
	pageReader := walg.IncrementalPageReader{
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
	defer pageFile.Close()
	assert.NoError(t, err)
	pageReader := walg.IncrementalPageReader{
		PagedFile: pageFile,
		Blocks:    make([]uint32, 0),
		Lsn:       sampeLSN,
	}
	err = pageReader.FullScanInitialize()
	assert.NoError(t, err)
	assert.Equal(t, []uint32{3, 4, 5, 6, 7}, pageReader.Blocks)
}

func makePageDataReader() walg.ReadSeekCloser {
	pageCount := 8
	pageData := make([]byte, pageCount*int(walg.WalPageSize))
	for i := 0; i < pageCount; i++ {
		for j := i * int(walg.WalPageSize); j < (i+1)*int(walg.WalPageSize); j++ {
			pageData[j] = byte(i)
		}
	}
	pageDataReader := bytes.NewReader(pageData)
	return &walg.ReadSeekCloserImpl{Reader: pageDataReader, Seeker: pageDataReader, Closer: &testtools.NopCloser{}}
}

func TestRead(t *testing.T) {
	blocks := []uint32{1, 2, 4}
	header := []byte{12, 13, 14}
	expectedRead := make([]byte, 3+3*walg.WalPageSize)
	copy(expectedRead, header)
	for id, i := range blocks {
		for j := 3 + id*int(walg.WalPageSize); j < 3+(id+1)*int(walg.WalPageSize); j++ {
			expectedRead[j] = byte(i)
		}
	}

	pageReader := walg.IncrementalPageReader{
		PagedFile: makePageDataReader(),
		Blocks:    blocks,
		Next:      header,
	}

	actualRead := make([]byte, 3+3*walg.WalPageSize)
	_, err := io.ReadFull(&pageReader, actualRead)
	assert.NoError(t, err)
	assert.Equal(t, expectedRead, actualRead)
	testtools.AssertReaderIsEmpty(t, &pageReader)
}

func TestAdvanceFileReader(t *testing.T) {
	pageReader := walg.IncrementalPageReader{
		PagedFile: makePageDataReader(),
		Blocks:    []uint32{5, 9},
	}
	err := pageReader.AdvanceFileReader()
	assert.NoError(t, err)
	assert.Equal(t, []uint32{9}, pageReader.Blocks)
	expectedNext := make([]byte, walg.WalPageSize)
	for i := 0; i < int(walg.WalPageSize); i++ {
		expectedNext[i] = 5
	}
	assert.Equal(t, expectedNext, pageReader.Next)
}

func TestDrainMoreData_NoBlocks(t *testing.T) {
	pageReader := walg.IncrementalPageReader{}
	succeed, err := pageReader.DrainMoreData()
	assert.NoError(t, err)
	assert.False(t, succeed)
}

func TestDrainMoreData_HasBlocks(t *testing.T) {
	pageReader := walg.IncrementalPageReader{
		PagedFile: makePageDataReader(),
		Blocks:    []uint32{3, 6},
	}
	succeed, err := pageReader.DrainMoreData()
	assert.NoError(t, err)
	assert.True(t, succeed)
	assert.Equal(t, []uint32{6}, pageReader.Blocks)
	expectedNext := make([]byte, walg.WalPageSize)
	for i := 0; i < int(walg.WalPageSize); i++ {
		expectedNext[i] = 3
	}
	assert.Equal(t, expectedNext, pageReader.Next)
}
