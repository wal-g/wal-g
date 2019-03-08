package test

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
	"io/ioutil"
	"testing"
)

func TestTryDownloadWALFile_Exist(t *testing.T) {
	expectedData := []byte("mock")
	folder := testtools.MakeDefaultInMemoryStorageFolder().GetSubFolder(internal.WalPath)
	folder.PutObject("00000001000000000000007C", bytes.NewBuffer(expectedData))
	archiveReader, exist, err := internal.TryDownloadWALFile(folder, "00000001000000000000007C")
	assert.NoError(t, err)
	assert.True(t, exist)
	actualData, err := ioutil.ReadAll(archiveReader)
	assert.NoError(t, err)
	assert.Equal(t, expectedData, actualData)
}

func TestTryDownloadWALFile_NotExist(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	reader, exist, err := internal.TryDownloadWALFile(folder, "00000001000000000000007C")
	assert.Nil(t, reader)
	assert.False(t, exist)
	assert.NoError(t, err)
}
