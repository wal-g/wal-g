package internal_test

import (
	"bytes"
	"encoding/json"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"

	"github.com/stretchr/testify/assert"
)

var (
	testLatestBackup = internal.GenericMetadata{
		BackupName: "stream_20231118T140000Z",
		StartTime:  time.Now(),
	}

	testOldestBackup = internal.GenericMetadata{
		BackupName: "stream_20231118T130000Z",
		StartTime:  time.Now().Add(-time.Minute),
		UserData:   map[string]interface{}{"id": "mdbb7sekqnv5lsuretgg"},
	}

	testOldestPermanentBackup = internal.GenericMetadata{
		BackupName:  "stream_20231118T120000Z",
		StartTime:   time.Now().Add(-time.Minute * 2),
		IsPermanent: true,
	}

	testRepeatedUserDataBackup = internal.GenericMetadata{
		BackupName: "stream_20231118T110000Z",
		UserData:   map[string]interface{}{"id": "mdbb7sekqnv5lsuretgg"},
	}
)

func convertMetadata(input internal.GenericMetadata) map[string]interface{} {
	metadata := map[string]interface{}{
		"start_time":   input.StartTime,
		"backup_name":  input.BackupName,
		"is_permanent": input.IsPermanent,
		"user_data":    input.UserData,
	}
	return metadata
}

func checkEmptyFolderBehaviour(t *testing.T, backupSelector internal.BackupSelector) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	_ = folder.PutObject("not_backup_path", &bytes.Buffer{})

	latestBackup, err := backupSelector.Select(folder)

	assert.Empty(t, latestBackup)
	assert.Error(t, err, internal.NoBackupsFoundError{})
}

func TestLatestBackupSelector_emptyFolder(t *testing.T) {
	backupSelector := internal.NewLatestBackupSelector()
	checkEmptyFolderBehaviour(t, backupSelector)
}

func TestLatestBackupSelector(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	b1 := path.Join(utility.BaseBackupPath, testLatestBackup.BackupName+".1"+utility.SentinelSuffix)
	b2 := path.Join(utility.BaseBackupPath, testLatestBackup.BackupName+".2"+utility.SentinelSuffix)
	_ = folder.PutObject(b1, &bytes.Buffer{})
	_ = folder.PutObject(b2, &bytes.Buffer{})

	backupSelector := internal.NewLatestBackupSelector()
	latestBackup, err := backupSelector.Select(folder)

	assert.NoError(t, err)
	assert.Equal(t, testLatestBackup.BackupName+".2", latestBackup.Name)
}

func TestLatestBackupSelector_ignoreSubFolders(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	b1 := path.Join(utility.BaseBackupPath, testLatestBackup.BackupName+".1"+utility.SentinelSuffix)
	b2 := path.Join(utility.BaseBackupPath, testLatestBackup.BackupName+".2"+utility.SentinelSuffix)
	b3 := path.Join(utility.BaseBackupPath, "subFolder", testLatestBackup.BackupName+".3"+utility.SentinelSuffix)
	_ = folder.PutObject(b1, &bytes.Buffer{})
	_ = folder.PutObject(b2, &bytes.Buffer{})
	_ = folder.PutObject(b3, &bytes.Buffer{})

	backupSelector := internal.NewLatestBackupSelector()
	latestBackup, err := backupSelector.Select(folder)

	assert.NoError(t, err)
	assert.Equal(t, testLatestBackup.BackupName+".2", latestBackup.Name)
}

func TestOldestNonPermanentSelector(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()

	b1 := path.Join(utility.BaseBackupPath, testLatestBackup.BackupName+utility.SentinelSuffix)
	meta1 := convertMetadata(testLatestBackup)
	bytesMeta1, _ := json.Marshal(&meta1)

	b2 := path.Join(utility.BaseBackupPath, testOldestBackup.BackupName+utility.SentinelSuffix)
	meta2 := convertMetadata(testOldestBackup)
	bytesMeta2, _ := json.Marshal(&meta2)

	_ = folder.PutObject(b1, strings.NewReader(string(bytesMeta1)))
	_ = folder.PutObject(b2, strings.NewReader(string(bytesMeta2)))

	backupSelector := internal.NewOldestNonPermanentSelector(greenplum.NewGenericMetaFetcher())
	latestBackup, err := backupSelector.Select(folder)

	assert.NoError(t, err)
	assert.Equal(t, testOldestBackup.BackupName, latestBackup.Name)
}

func TestOldestNonPermanentSelector_ignorePermanentBackups(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()

	b1 := path.Join(utility.BaseBackupPath, testOldestBackup.BackupName+utility.SentinelSuffix)
	meta1 := convertMetadata(testOldestBackup)
	bytesMeta1, _ := json.Marshal(&meta1)

	b2 := path.Join(utility.BaseBackupPath, testOldestPermanentBackup.BackupName+utility.SentinelSuffix)
	meta2 := convertMetadata(testOldestPermanentBackup)
	bytesMeta2, _ := json.Marshal(&meta2)

	_ = folder.PutObject(b1, strings.NewReader(string(bytesMeta1)))
	_ = folder.PutObject(b2, strings.NewReader(string(bytesMeta2)))

	backupSelector := internal.NewOldestNonPermanentSelector(greenplum.NewGenericMetaFetcher())
	latestBackup, err := backupSelector.Select(folder)

	assert.NoError(t, err)
	assert.Equal(t, testOldestBackup.BackupName, latestBackup.Name)
}

func TestOldestNonPermanentSelector_emptyFolder(t *testing.T) {
	backupSelector := internal.NewOldestNonPermanentSelector(greenplum.NewGenericMetaFetcher())
	checkEmptyFolderBehaviour(t, backupSelector)
}

func TestUserDataBackupSelector(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()

	b1 := path.Join(utility.BaseBackupPath, testOldestBackup.BackupName+utility.SentinelSuffix)
	meta1 := convertMetadata(testOldestBackup)
	bytesMeta1, _ := json.Marshal(&meta1)

	_ = folder.PutObject(b1, strings.NewReader(string(bytesMeta1)))

	byteUserData, err := json.Marshal(testOldestBackup.UserData)
	assert.NoError(t, err)
	backupSelector, err := internal.NewUserDataBackupSelector(string(byteUserData), greenplum.NewGenericMetaFetcher())
	assert.NoError(t, err)

	latestBackup, err := backupSelector.Select(folder)
	assert.NoError(t, err)
	assert.Equal(t, testOldestBackup.BackupName, latestBackup.Name)
}

func TestUserDataBackupSelector_tooManyBackupsFound(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()

	b1 := path.Join(utility.BaseBackupPath, testOldestBackup.BackupName+utility.SentinelSuffix)
	meta1 := convertMetadata(testOldestBackup)
	bytesMeta1, _ := json.Marshal(&meta1)

	b2 := path.Join(utility.BaseBackupPath, testRepeatedUserDataBackup.BackupName+utility.SentinelSuffix)
	meta2 := convertMetadata(testRepeatedUserDataBackup)
	bytesMeta2, _ := json.Marshal(&meta2)

	_ = folder.PutObject(b1, strings.NewReader(string(bytesMeta1)))
	_ = folder.PutObject(b2, strings.NewReader(string(bytesMeta2)))

	byteUserData, err := json.Marshal(testOldestBackup.UserData)
	assert.NoError(t, err)
	backupSelector, err := internal.NewUserDataBackupSelector(string(byteUserData), greenplum.NewGenericMetaFetcher())
	assert.NoError(t, err)

	latestBackup, err := backupSelector.Select(folder)

	assert.Empty(t, latestBackup)
	assert.Error(t, err)
}

func TestUserDataBackupSelector_emptyFolder(t *testing.T) {
	byteUserData, err := json.Marshal(testOldestBackup.UserData)
	assert.NoError(t, err)
	backupSelector, err := internal.NewUserDataBackupSelector(string(byteUserData), greenplum.NewGenericMetaFetcher())
	assert.NoError(t, err)
	checkEmptyFolderBehaviour(t, backupSelector)
}

func TestBackupNameSelector(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()

	b1 := path.Join(utility.BaseBackupPath, testOldestBackup.BackupName+utility.SentinelSuffix)
	_ = folder.PutObject(b1, &bytes.Buffer{})

	backupSelector, err := internal.NewBackupNameSelector(testOldestBackup.BackupName, true)
	assert.NoError(t, err)

	latestBackup, err := backupSelector.Select(folder)
	assert.NoError(t, err)
	assert.Equal(t, latestBackup.Name, testOldestBackup.BackupName)
}

func TestBackupNameSelector_backupNotFound(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()

	b1 := path.Join(utility.BaseBackupPath, testLatestBackup.BackupName+utility.SentinelSuffix)
	_ = folder.PutObject(b1, &bytes.Buffer{})

	backupSelector, err := internal.NewBackupNameSelector(testOldestBackup.BackupName, true)
	assert.NoError(t, err)

	latestBackup, err := backupSelector.Select(folder)
	assert.Empty(t, latestBackup)
	assert.Error(t, err)
}

func TestBackupNameSelector_emptyFolder(t *testing.T) {
	backupSelector, err := internal.NewBackupNameSelector(testOldestBackup.BackupName, true)
	assert.NoError(t, err)
	checkEmptyFolderBehaviour(t, backupSelector)
}

// Tests for LatestBackupSelectorWithMetaFetcher - Issue #694 fix
// These tests verify that LATEST backup selection uses creation time from metadata
// instead of storage modification time.

func TestLatestBackupSelectorWithMetaFetcher_selectsByCreationTime(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()

	// Create two backups where the "older" one (by creation time) has a newer modification time
	// This simulates the scenario where a backup is re-uploaded to storage

	// Backup 1: Created earlier (older), but will have later modification time
	olderBackup := internal.GenericMetadata{
		BackupName: "stream_20231118T100000Z",
		StartTime:  time.Date(2023, 11, 18, 10, 0, 0, 0, time.UTC),
	}

	// Backup 2: Created later (newer), but will have earlier modification time
	newerBackup := internal.GenericMetadata{
		BackupName: "stream_20231118T120000Z",
		StartTime:  time.Date(2023, 11, 18, 12, 0, 0, 0, time.UTC),
	}

	// Put backup 2 first (earlier modification time)
	b2 := path.Join(utility.BaseBackupPath, newerBackup.BackupName+utility.SentinelSuffix)
	meta2 := convertMetadata(newerBackup)
	bytesMeta2, _ := json.Marshal(&meta2)
	_ = folder.PutObject(b2, strings.NewReader(string(bytesMeta2)))

	// Then put backup 1 (later modification time)
	b1 := path.Join(utility.BaseBackupPath, olderBackup.BackupName+utility.SentinelSuffix)
	meta1 := convertMetadata(olderBackup)
	bytesMeta1, _ := json.Marshal(&meta1)
	_ = folder.PutObject(b1, strings.NewReader(string(bytesMeta1)))

	// Without metaFetcher, it would select olderBackup (has later modification time)
	// With metaFetcher, it should select newerBackup (has later creation time)
	backupSelector := internal.NewLatestBackupSelectorWithMetaFetcher(greenplum.NewGenericMetaFetcher())
	latestBackup, err := backupSelector.Select(folder)

	assert.NoError(t, err)
	assert.Equal(t, newerBackup.BackupName, latestBackup.Name)
}

func TestLatestBackupSelectorWithMetaFetcher_fallsBackToModificationTime(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()

	// Create backups without valid metadata (empty sentinel files)
	b1 := path.Join(utility.BaseBackupPath, "backup_1"+utility.SentinelSuffix)
	b2 := path.Join(utility.BaseBackupPath, "backup_2"+utility.SentinelSuffix)

	// Put backup_1 first (earlier modification time)
	_ = folder.PutObject(b1, &bytes.Buffer{})
	// Then backup_2 (later modification time)
	_ = folder.PutObject(b2, &bytes.Buffer{})

	// Even with metaFetcher, should fall back to modification time when metadata is unavailable
	backupSelector := internal.NewLatestBackupSelectorWithMetaFetcher(greenplum.NewGenericMetaFetcher())
	latestBackup, err := backupSelector.Select(folder)

	assert.NoError(t, err)
	assert.Equal(t, "backup_2", latestBackup.Name)
}

func TestLatestBackupSelectorWithMetaFetcher_emptyFolder(t *testing.T) {
	backupSelector := internal.NewLatestBackupSelectorWithMetaFetcher(greenplum.NewGenericMetaFetcher())
	checkEmptyFolderBehaviour(t, backupSelector)
}

func TestLatestBackupSelectorWithMetaFetcher_nilMetaFetcher(t *testing.T) {
	// When metaFetcher is nil, should behave like NewLatestBackupSelector()
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	b1 := path.Join(utility.BaseBackupPath, "backup_1"+utility.SentinelSuffix)
	b2 := path.Join(utility.BaseBackupPath, "backup_2"+utility.SentinelSuffix)
	_ = folder.PutObject(b1, &bytes.Buffer{})
	_ = folder.PutObject(b2, &bytes.Buffer{})

	backupSelector := internal.NewLatestBackupSelectorWithMetaFetcher(nil)
	latestBackup, err := backupSelector.Select(folder)

	assert.NoError(t, err)
	assert.Equal(t, "backup_2", latestBackup.Name)
}
