package internal_test

import (
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/asm"
	"github.com/wal-g/wal-g/testtools"
)

func generateAndUploadWalFile(t *testing.T, fileFormat string) (internal.WalUploader, *asm.FakeASM, string, string) {
	defer cleanup(t, internal.GetDataFolderPath())
	dir, _ := setupArchiveStatus(t, "")
	addTestDataFile(t, dir, fileFormat)
	testFileName := testFilename(fileFormat)
	uploader := testtools.NewMockWalDirUploader(false, false)
	fakeASM := asm.NewFakeASM()
	uploader.ArchiveStatusManager = fakeASM
	internal.HandleWALPush(uploader, filepath.Join(dir, testFileName))
	return *uploader, fakeASM, dir, testFileName
}


func TestWalPush_HandleWALPush(t *testing.T) {
	viper.Set(internal.UploadWalMetadata, "NOMETADATA")
	_, fakeASM, dir, testFileName := generateAndUploadWalFile(t, "1")
	defer cleanup(t, dir)
	wasUploaded := fakeASM.WalAlreadyUploaded(testFileName)
	assert.True(t, wasUploaded, testFileName+" was not marked as uploaded")
}

func TestWalPush_IndividualMetadataUploader(t *testing.T) {
	viper.Set(internal.UploadWalMetadata, "INDIVIDUAL")
	uploader, _, dir, testFileName := generateAndUploadWalFile(t, "1")
	defer cleanup(t, dir)
	_, err := uploader.UploadingFolder.ReadObject(testFileName + ".json")
	assert.NoError(t, err)
}

func TestWalPush_BulkMetadataUploader(t *testing.T) {
	viper.Set(internal.UploadWalMetadata, "BULK")
	uploader, _, dir, testFileName := generateAndUploadWalFile(t, "F")
	defer cleanup(t, dir)
	_, err := uploader.UploadingFolder.ReadObject(testFileName[0:len(testFileName)-1] + ".json")
	assert.NoError(t, err)
}

func TestWalPush_NoMetataNoUploader(t *testing.T) {
	viper.Set(internal.UploadWalMetadata, "NOMETADATA")
	uploader, _, dir, testFileName := generateAndUploadWalFile(t, "1")
	defer cleanup(t, dir)
	_, err := uploader.UploadingFolder.ReadObject(testFileName + ".json")
	assert.Error(t, err)
}

func TestWalPush_BulkMetadataUploaderWithUploadConcurrency(t *testing.T) {
	viper.Set(internal.UploadWalMetadata, "BULK")
	viper.Set(internal.UploadConcurrencySetting,4)
	uploader, _, dir, testFileName := generateAndUploadWalFile(t, "F")
	defer cleanup(t, dir)
	_, err := uploader.UploadingFolder.ReadObject(testFileName[0:len(testFileName)-1] + ".json")
	assert.NoError(t, err)
}