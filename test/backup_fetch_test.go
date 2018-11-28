package test

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
	"testing"
)

func createMockStorageFolder() internal.StorageFolder {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	folder.PutObject("base_123_backup_stop_sentinel.json", &bytes.Buffer{})
	folder.PutObject("base_456_backup_stop_sentinel.json", &bytes.Buffer{})
	folder.PutObject("base_000_backup_stop_sentinel.json", &bytes.Buffer{}) // last put
	folder.PutObject("base_123312", &bytes.Buffer{})                        // not a sentinel
	folder.PutObject("base_321/nop", &bytes.Buffer{})
	folder.PutObject("folder123/nop", &bytes.Buffer{})
	folder.PutObject("base_456/tar_partitions/1", &bytes.Buffer{})
	folder.PutObject("base_456/tar_partitions/2", &bytes.Buffer{})
	folder.PutObject("base_456/tar_partitions/3", &bytes.Buffer{})
	return folder
}

func TestGetBackupByName_Latest(t *testing.T) {
	folder := createMockStorageFolder()
	backup, err := internal.GetBackupByName(internal.LatestString, folder)
	assert.NoError(t, err)
	assert.Equal(t, folder, backup.BaseBackupFolder)
	assert.Equal(t, "base_000", backup.Name)
}

func TestGetBackupByName_LatestNoBackups(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	folder.PutObject("folder123/nop", &bytes.Buffer{})
	_, err := internal.GetBackupByName(internal.LatestString, folder)
	assert.Error(t, err)
	assert.IsType(t, internal.NewNoBackupsFoundError(), err)
}

func TestGetBackupByName_Exists(t *testing.T) {
	folder := createMockStorageFolder()
	backup, err := internal.GetBackupByName("base_123", folder)
	assert.NoError(t, err)
	assert.Equal(t, folder, backup.BaseBackupFolder)
	assert.Equal(t, "base_123", backup.Name)
}

func TestGetBackupByName_NotExists(t *testing.T) {
	folder := createMockStorageFolder()
	_, err := internal.GetBackupByName("base_321", folder)
	assert.Error(t, err)
	assert.IsType(t, internal.NewBackupNonExistenceError(""), err)
}
