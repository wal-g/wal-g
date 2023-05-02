package postgres_test

import (
	"path/filepath"
	"testing"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/asm"
	"github.com/wal-g/wal-g/testtools"
)

func init() {
	internal.ConfigureSettings(internal.PG)
	internal.InitConfig()
	internal.Configure()
}

func generateAndUploadWalFile(t *testing.T, fileFormat string) (postgres.WalUploader, *asm.FakeASM, string, string) {
	dir, _ := setupArchiveStatus(t, "")
	dirName := filepath.Join(dir, "pg_wal")
	defer testtools.Cleanup(t, dir)
	addTestDataFile(t, dirName, fileFormat)
	viper.Set(internal.PgDataSetting, dir)
	testFileName := testFilename(fileFormat)
	uploader := testtools.NewMockWalDirUploader(false, false)
	fakeASM := asm.NewFakeASM()
	uploader.ArchiveStatusManager = fakeASM
	postgres.HandleWALPush(uploader, filepath.Join(dirName, testFileName))
	return *uploader, fakeASM, dir, testFileName
}

func TestWalPush_HandleWALPush(t *testing.T) {
	uploader, _, dir, testFileName := generateAndUploadWalFile(t, "1")
	defer testtools.Cleanup(t, dir)
	// ".mock" suffix is the MockCompressor file extension
	_, err := uploader.Folder().ReadObject(testFileName + ".mock")
	assert.NoError(t, err)
}

func TestWalPush_IndividualMetadataUploader(t *testing.T) {
	viper.Set(internal.UploadWalMetadata, postgres.WalIndividualMetadataLevel)
	uploader, _, dir, testFileName := generateAndUploadWalFile(t, "1")
	defer testtools.Cleanup(t, dir)
	_, err := uploader.Folder().ReadObject(testFileName + ".json")
	assert.NoError(t, err)
}

func TestWalPush_BulkMetadataUploader(t *testing.T) {
	viper.Set(internal.UploadWalMetadata, postgres.WalBulkMetadataLevel)
	uploader, _, dir, testFileName := generateAndUploadWalFile(t, "F")
	defer testtools.Cleanup(t, dir)
	_, err := uploader.Folder().ReadObject(testFileName[0:len(testFileName)-1] + ".json")
	assert.NoError(t, err)
}

func TestWalPush_NoMetataNoUploader(t *testing.T) {
	viper.Set(internal.UploadWalMetadata, postgres.WalNoMetadataLevel)
	uploader, _, dir, testFileName := generateAndUploadWalFile(t, "1")
	defer testtools.Cleanup(t, dir)
	_, err := uploader.Folder().ReadObject(testFileName + ".json")
	assert.Error(t, err)
}

func TestWalPush_BulkMetadataUploaderWithUploadConcurrency(t *testing.T) {
	viper.Set(internal.UploadWalMetadata, postgres.WalBulkMetadataLevel)
	viper.Set(internal.UploadConcurrencySetting, 4)
	uploader, _, dir, testFileName := generateAndUploadWalFile(t, "F")
	defer testtools.Cleanup(t, dir)
	_, err := uploader.Folder().ReadObject(testFileName[0:len(testFileName)-1] + ".json")
	assert.NoError(t, err)
}
