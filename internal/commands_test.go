package internal_test

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func TestTryDownloadWALFile_Exist(t *testing.T) {
	expectedData := []byte("mock")
	folder := testtools.MakeDefaultInMemoryStorageFolder().GetSubFolder(utility.WalPath)
	folder.PutObject(WalFilename, bytes.NewBuffer(expectedData))
	archiveReader, exist, err := internal.TryDownloadFile(folder, WalFilename)
	assert.NoError(t, err)
	assert.True(t, exist)
	actualData, err := ioutil.ReadAll(archiveReader)
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
