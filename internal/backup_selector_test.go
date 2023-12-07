package internal_test

import (
	"bytes"
	"path"
	"testing"
	"time"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"

	"github.com/stretchr/testify/assert"
)

var (
	testLatestBackup = internal.BackupTime{
		BackupName: "stream_20231118T120000Z",
		Time:       time.Now(),
	}
)

func TestLatestBackupSelector_emptyFolder(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	_ = folder.PutObject("not_backup_path", &bytes.Buffer{})

	backupSelector := internal.NewLatestBackupSelector()
	latestBackup, err := backupSelector.Select(folder)

	assert.Empty(t, latestBackup)
	assert.Error(t, err, internal.NoBackupsFoundError{})
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
