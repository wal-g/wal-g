package test

import (
	"bytes"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"testing"
)

func TestCheckExistence_Exists(t *testing.T) {
	folder := createMockStorageFolder()
	backup := internal.NewBackup(folder.GetSubFolder(internal.BaseBackupPath), "base_000")
	exists, err := backup.CheckExistence()
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestCheckExistence_NotExists(t *testing.T) {
	folder := createMockStorageFolder()
	backup := internal.NewBackup(folder.GetSubFolder(internal.BaseBackupPath), "base_321")
	exists, err := backup.CheckExistence()
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestGetTarNames(t *testing.T) {
	folder := createMockStorageFolder()
	backup := internal.NewBackup(folder.GetSubFolder(internal.BaseBackupPath), "base_456")
	tarNames, err := backup.GetTarNames()
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"1", "2", "3"}, tarNames)
}

func TestIsPgControlRequired(t *testing.T) {
	folder := createMockStorageFolder()
	backup := internal.NewBackup(folder.GetSubFolder(internal.BaseBackupPath), "base_456")
	dto, err := backup.FetchSentinel()
	assert.NoError(t, err)
	assert.True(t, internal.IsPgControlRequired(backup, dto))
}

func TestIsPgControlNotRequiredForWALEBackups(t *testing.T) {
	folder := createMockStorageFolder()
	backup := internal.NewBackup(folder.GetSubFolder(internal.BaseBackupPath), "base_000000010000DD170000000C_00743784")
	assert.False(t, internal.IsPgControlRequired(backup, internal.BackupSentinelDto{}))
}

func TestFetchSentinel(t *testing.T) {
	folder := createMockStorageFolder()
	expectedSentinel := internal.BackupSentinelDto{}
	expectedSentinelJson, _ := json.Marshal(expectedSentinel)
	folder.PutObject("base_789454598_backup_stop_sentinel.json", bytes.NewReader(expectedSentinelJson))
	backup := internal.NewBackup(folder, "base_789454598")

	actualSentinel, err := backup.FetchSentinel()

	assert.NoError(t, err)
	assert.Equal(t, expectedSentinel, actualSentinel)
}

func TestFetchSentinel_ReturnError_WhenSentinelNotExist(t *testing.T) {
	folder := createMockStorageFolder()
	backup := internal.NewBackup(folder.GetSubFolder(internal.BaseBackupPath), "base_78934085033849")

	_, err := backup.FetchSentinel()

	assert.Error(t, err)
}

func TestFetchSentinel_ReturnError_WhenSentinelUnmarshallable(t *testing.T) {
	folder := createMockStorageFolder()
	backup := internal.NewBackup(folder.GetSubFolder(internal.BaseBackupPath), "base_000")
	errorMessage := "failed to unmarshal sentinel"

	_, err := backup.FetchSentinel()

	assert.Error(t, err)
	assert.Equal(t, errorMessage, err.Error()[:len(errorMessage)])
}

