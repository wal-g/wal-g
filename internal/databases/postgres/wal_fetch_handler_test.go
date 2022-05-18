package postgres_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/compression/lz4"
	"github.com/wal-g/wal-g/internal/compression/lzma"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func TestWallFetchCachesLastDecompressor(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder().GetSubFolder(utility.WalPath)

	type TestData struct {
		filename     string
		decompressor compression.Decompressor
		compressor   compression.Compressor
	}

	testData := []TestData{
		{"00000001000000000000007C", lz4.Decompressor{}, lz4.Compressor{}},
		{"00000001000000000000007F", lzma.Decompressor{}, lzma.Compressor{}},
	}

	for _, data := range testData {
		walFilename, decompressor, compressor := data.filename, data.decompressor, data.compressor

		data := bytes.Buffer{}
		cw := compressor.NewWriter(&data)
		_, err := io.WriteString(cw, "dest data")
		assert.NoError(t, err)
		err = cw.Close()
		assert.NoError(t, err)

		assert.NoError(t, folder.PutObject(walFilename+"."+decompressor.FileExtension(), &data))

		_, err = internal.DownloadAndDecompressStorageFile(folder, walFilename)
		assert.NoError(t, err)

		last, err := internal.GetLastDecompressor()

		assert.NoError(t, err)
		assert.Equal(t, decompressor, last)
	}
}

func TestSetLastDecompressorWorkWell(t *testing.T) {
	for _, decompressor := range compression.Decompressors {
		_ = internal.SetLastDecompressor(decompressor)
		last, err := internal.GetLastDecompressor()

		assert.NoError(t, err)
		assert.Equal(t, last, decompressor)
	}
}

func TestTryDownloadWALFile_Exist(t *testing.T) {
	expectedData := []byte("mock")
	folder := testtools.MakeDefaultInMemoryStorageFolder().GetSubFolder(utility.WalPath)
	folder.PutObject(WalFilename, bytes.NewBuffer(expectedData))
	archiveReader, exist, err := internal.TryDownloadFile(folder, WalFilename)
	assert.NoError(t, err)
	assert.True(t, exist)
	actualData, err := io.ReadAll(archiveReader)
	assert.NoError(t, err)
	assert.Equal(t, expectedData, actualData)
}

func TestTryDownloadWALFile_NotExist(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	reader, exist, err := internal.TryDownloadFile(folder, WalFilename)
	assert.Nil(t, reader)
	assert.False(t, exist)
	assert.NoError(t, err)
}
