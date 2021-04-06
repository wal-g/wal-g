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

func init() {
	internal.InitConfig()
	internal.Configure()
}

func generateAndUploadWalFile(t *testing.T, fileFormat string) (internal.WalUploader, *asm.FakeASM, string, string) {
	dir, _ := setupArchiveStatus(t, "")
	dirName := filepath.Join(dir, "pg_wal")
	defer cleanup(t, dir)
	addTestDataFile(t, dirName, fileFormat)
	viper.Set(internal.PgDataSetting, dir)
	testFileName := testFilename(fileFormat)
	uploader := testtools.NewMockWalDirUploader(false, false)
	fakeASM := asm.NewFakeASM()
	uploader.ArchiveStatusManager = fakeASM
	internal.HandleWALPush(uploader, filepath.Join(dirName, testFileName))
	return *uploader, fakeASM, dir, testFileName
}

func TestWalPush_HandleWALPush(t *testing.T) {
	uploader, _, dir, testFileName := generateAndUploadWalFile(t, "1")
	defer cleanup(t, dir)
	// ".mock" suffix is the MockCompressor file extension
	_, err := uploader.UploadingFolder.ReadObject(testFileName + ".mock")
	assert.NoError(t, err)
}

func TestWalPush_IndividualMetadataUploader(t *testing.T) {
	viper.Set(internal.UploadWalMetadata, internal.WalIndividualMetadataLevel)
	uploader, _, dir, testFileName := generateAndUploadWalFile(t, "1")
	defer cleanup(t, dir)
	_, err := uploader.UploadingFolder.ReadObject(testFileName + ".json")
	assert.NoError(t, err)
}

func TestWalPush_BulkMetadataUploader(t *testing.T) {
	viper.Set(internal.UploadWalMetadata, internal.WalBulkMetadataLevel)
	uploader, _, dir, testFileName := generateAndUploadWalFile(t, "F")
	defer cleanup(t, dir)
	_, err := uploader.UploadingFolder.ReadObject(testFileName[0:len(testFileName)-1] + ".json")
	assert.NoError(t, err)
}

func TestWalPush_NoMetataNoUploader(t *testing.T) {
	viper.Set(internal.UploadWalMetadata, internal.WalNoMetadataLevel)
	uploader, _, dir, testFileName := generateAndUploadWalFile(t, "1")
	defer cleanup(t, dir)
	_, err := uploader.UploadingFolder.ReadObject(testFileName + ".json")
	assert.Error(t, err)
}

func TestWalPush_BulkMetadataUploaderWithUploadConcurrency(t *testing.T) {
	viper.Set(internal.UploadWalMetadata, internal.WalBulkMetadataLevel)
	viper.Set(internal.UploadConcurrencySetting, 4)
	uploader, _, dir, testFileName := generateAndUploadWalFile(t, "F")
	defer cleanup(t, dir)
	_, err := uploader.UploadingFolder.ReadObject(testFileName[0:len(testFileName)-1] + ".json")
	assert.NoError(t, err)
}
