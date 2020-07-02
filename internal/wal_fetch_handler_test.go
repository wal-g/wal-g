package internal_test

import (
	"bytes"
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
	}

	testData := []TestData{{"00000001000000000000007C", lz4.Decompressor{}},
		{"00000001000000000000007F", lzma.Decompressor{}}}

	for _, data := range testData {
		walFilename, decompressor := data.filename, data.decompressor

		assert.NoError(t, folder.PutObject(walFilename+"."+decompressor.FileExtension(),
			bytes.NewReader([]byte("test data"))))

		_, err := internal.DownloadAndDecompressWALFile(folder, walFilename)
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
