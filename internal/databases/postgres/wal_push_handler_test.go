package postgres_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/asm"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/testtools"
)

func init() {
	internal.ConfigureSettings(conf.PG)
	conf.InitConfig()
	conf.Configure()
}

func generateAndUploadWalFile(t *testing.T, fileFormat string) (postgres.WalUploader, *asm.FakeASM, string, string) {
	dir, _ := setupArchiveStatus(t, "")
	dirName := path.Join(dir, "pg_wal")
	defer testtools.Cleanup(t, dir)
	addTestDataFile(t, dirName, fileFormat)
	viper.Set(conf.PgDataSetting, dir)
	testFileName := testFilename(fileFormat)
	uploader := testtools.NewMockWalDirUploader(false, false)
	fakeASM := asm.NewFakeASM()
	uploader.ArchiveStatusManager = fakeASM
	postgres.HandleWALPush(t.Context(), uploader, path.Join(dirName, testFileName))
	return *uploader, fakeASM, dir, testFileName
}

func TestWalPush_HandleWALPush(t *testing.T) {
	uploader, _, dir, testFileName := generateAndUploadWalFile(t, "1")
	defer testtools.Cleanup(t, dir)
	// ".mock" suffix is the MockCompressor file extension
	_, err := uploader.Folder().ReadObject(t.Context(), testFileName+".mock")
	assert.NoError(t, err)
}

func TestWalPush_IndividualMetadataUploader(t *testing.T) {
	viper.Set(conf.UploadWalMetadata, postgres.WalIndividualMetadataLevel)
	uploader, _, dir, testFileName := generateAndUploadWalFile(t, "1")
	defer testtools.Cleanup(t, dir)
	_, err := uploader.Folder().ReadObject(t.Context(), testFileName+".json")
	assert.NoError(t, err)
}

func TestWalPush_BulkMetadataUploader(t *testing.T) {
	viper.Set(conf.UploadWalMetadata, postgres.WalBulkMetadataLevel)
	uploader, _, dir, testFileName := generateAndUploadWalFile(t, "F")
	defer testtools.Cleanup(t, dir)
	_, err := uploader.Folder().ReadObject(t.Context(), testFileName[0:len(testFileName)-1]+".json")
	assert.NoError(t, err)
}

func TestWalPush_NoMetataNoUploader(t *testing.T) {
	viper.Set(conf.UploadWalMetadata, postgres.WalNoMetadataLevel)
	uploader, _, dir, testFileName := generateAndUploadWalFile(t, "1")
	defer testtools.Cleanup(t, dir)
	_, err := uploader.Folder().ReadObject(t.Context(), testFileName+".json")
	assert.Error(t, err)
}

func TestWalPush_BulkMetadataUploaderWithUploadConcurrency(t *testing.T) {
	viper.Set(conf.UploadWalMetadata, postgres.WalBulkMetadataLevel)
	viper.Set(conf.UploadConcurrencySetting, 4)
	uploader, _, dir, testFileName := generateAndUploadWalFile(t, "F")
	defer testtools.Cleanup(t, dir)
	_, err := uploader.Folder().ReadObject(t.Context(), testFileName[0:len(testFileName)-1]+".json")
	assert.NoError(t, err)
}

// The storage refuses connections: a listed file can only succeed by not needing it.
func TestWalPushCLISkipList(t *testing.T) {
	walg := os.Getenv("WALG_TEST_WALG_BIN")
	if walg == "" {
		t.Skip("set WALG_TEST_WALG_BIN to run the CLI test")
	}
	dir := t.TempDir()
	listed, unlisted := "000000010000000000000001", "000000010000000000000002"
	for _, name := range []string{listed, unlisted} {
		assert.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("test WAL"), 0600))
	}
	listPath := filepath.Join(dir, "skip_list")
	assert.NoError(t, os.WriteFile(listPath, []byte(listed+"\n"), 0640))
	config, err := json.Marshal(map[string]string{
		"AWS_ACCESS_KEY_ID":                    "test",
		"AWS_ENDPOINT":                         "http://127.0.0.1:9",
		"AWS_REGION":                           "us-east-1",
		"AWS_S3_FORCE_PATH_STYLE":              "true",
		"AWS_SECRET_ACCESS_KEY":                "test",
		"PGDATA":                               dir,
		"WALG_FAILOVER_STORAGES_CHECK":         "true",
		"WALG_FAILOVER_STORAGES_CHECK_TIMEOUT": "2s",
		"WALG_PG_ARCHIVE_SKIP_LIST":            listPath,
		"WALG_S3_PREFIX":                       "s3://unreachable/prefix",
	})
	assert.NoError(t, err)
	configPath := filepath.Join(dir, "walg.json")
	assert.NoError(t, os.WriteFile(configPath, config, 0600))
	push := func(name string) (string, error) {
		cmd := exec.CommandContext(t.Context(), walg, "--config", configPath, "wal-push", filepath.Join(dir, name))
		// Package test initializers export Viper defaults that would override the config.
		cmd.Env = []string{"HOME=" + dir, "PATH=" + os.Getenv("PATH")}
		output, err := cmd.CombinedOutput()
		return string(output), err
	}

	output, err := push(listed)
	assert.NoError(t, err, output)
	assert.Contains(t, output, "skipping archiving of "+listed)

	output, err = push(unlisted)
	assert.Error(t, err)
	assert.Contains(t, output, "no alive storages")
}
