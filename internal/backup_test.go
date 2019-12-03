package internal_test

import (
	"bytes"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func getMockBackupFromFiles(files internal.BackupFileList) internal.Backup {
	return internal.Backup{
		SentinelDto: &internal.BackupSentinelDto{
			Files: files,
		},
	}
}

func TestGetFilesToUnwrap_SimpleFile(t *testing.T) {
	backup := getMockBackupFromFiles(testtools.NewBackupFileListBuilder().WithSimple().Build())

	files, _ := backup.GetFilesToUnwrap("")
	assert.Contains(t, files, testtools.SimplePath)
}

func TestGetFilesToUnwrap_IncrementedFile(t *testing.T) {
	backup := getMockBackupFromFiles(testtools.NewBackupFileListBuilder().WithIncremented().Build())

	files, _ := backup.GetFilesToUnwrap("")
	assert.Contains(t, files, testtools.IncrementedPath)
}

func TestGetFilesToUnwrap_SkippedFile(t *testing.T) {
	backup := getMockBackupFromFiles(testtools.NewBackupFileListBuilder().WithSkipped().Build())

	files, _ := backup.GetFilesToUnwrap("")
	assert.Contains(t, files, testtools.SkippedPath)
}

func TestGetFilesToUnwrap_UtilityFiles(t *testing.T) {
	backup := getMockBackupFromFiles(testtools.NewBackupFileListBuilder().Build())

	files, _ := backup.GetFilesToUnwrap("")
	assert.Equal(t, internal.UtilityFilePaths, files)
}

func TestGetFilesToUnwrap_NoMoreFiles(t *testing.T) {
	backup := getMockBackupFromFiles(testtools.NewBackupFileListBuilder().
		WithSimple().
		WithIncremented().
		WithSkipped().
		Build())

	files, _ := backup.GetFilesToUnwrap("")
	expected := map[string]bool{
		testtools.SimplePath:      true,
		testtools.IncrementedPath: true,
		testtools.SkippedPath:     true,
	}
	for utilityPath := range internal.UtilityFilePaths {
		expected[utilityPath] = true
	}
	assert.Equal(t, expected, files)
}

func TestCheckExistenceWhenBackupExists(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	backup := internal.NewBackup(folder.GetSubFolder(utility.BaseBackupPath), "base_000")
	exists, err := backup.CheckExistence()
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestCheckExistenceWhenBackupNotExists(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	backup := internal.NewBackup(folder.GetSubFolder(utility.BaseBackupPath), "base_321")
	exists, err := backup.CheckExistence()
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestGetTarNames(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	backup := internal.NewBackup(folder.GetSubFolder(utility.BaseBackupPath), "base_456")
	tarNames, err := backup.GetTarNames()
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"1", "2", "3"}, tarNames)
}

func TestIsPgControlRequired(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	backup := internal.NewBackup(folder.GetSubFolder(utility.BaseBackupPath), "base_456")
	dto, err := backup.GetSentinel()
	assert.NoError(t, err)
	assert.True(t, internal.IsPgControlRequired(backup, dto))
}

func TestIsPgControlNotRequiredForWALEBackups(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	backup := internal.NewBackup(folder.GetSubFolder(utility.BaseBackupPath), "base_000000010000DD170000000C_00743784")
	assert.False(t, internal.IsPgControlRequired(backup, internal.BackupSentinelDto{}))
}

func TestFetchSentinel(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	expectedSentinel := internal.BackupSentinelDto{}
	expectedSentinelJson, _ := json.Marshal(expectedSentinel)
	_ = folder.PutObject("base_789454598_backup_stop_sentinel.json", bytes.NewReader(expectedSentinelJson))
	backup := internal.NewBackup(folder, "base_789454598")

	actualSentinel, err := backup.GetSentinel()

	assert.NoError(t, err)
	assert.Equal(t, expectedSentinel, actualSentinel)
}

func TestFetchSentinelReturnErrorWhenSentinelNotExist(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	backup := internal.NewBackup(folder.GetSubFolder(utility.BaseBackupPath), "base_78934085033849")

	_, err := backup.GetSentinel()

	assert.Error(t, err)
}

func TestFetchSentinelReturnErrorWhenSentinelUnmarshallable(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	backup := internal.NewBackup(folder.GetSubFolder(utility.BaseBackupPath), "base_000")
	errorMessage := "failed to unmarshal sentinel"

	_, err := backup.GetSentinel()

	assert.Error(t, err)
	assert.Equal(t, errorMessage, err.Error()[:len(errorMessage)])
}

func createTempDir(prefix string) (name string, err error) {
	cwd, err := filepath.Abs("./")
	if err != nil {
		return "", err
	}

	dir, err := ioutil.TempDir(cwd, prefix)
	if err != nil {
		return "", err
	}

	return dir, nil
}

func TestIsDirectoryEmpty_ReturnsTrue_WhenDirectoryIsEmpty(t *testing.T) {
	dir, err := createTempDir("empty")
	if err != nil {
		t.Log(err)
	}

	actual, err := internal.IsDirectoryEmpty(dir)

	assert.True(t, actual)
}

func TestIsDirectoryEmpty_ReturnsFalse_WhenDirectoryIsNotEmpty(t *testing.T) {
	dir, err := createTempDir("not_empty")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(dir)

	file, err := ioutil.TempFile(dir, "file")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())

	actual, err := internal.IsDirectoryEmpty(dir)

	assert.False(t, actual)
}