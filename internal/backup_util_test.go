package internal_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/storages/memory"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

var (
	testStreamBackup = internal.BackupTime{
		BackupName: "stream_20210329T125616Z",
		Time:       time.Now(),
	}
)

func TestGetBackupTimeSlices_emptyList(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	_ = folder.PutObject("base_123312", &bytes.Buffer{})
	objects, _, _ := folder.ListFolder()
	result := internal.GetBackupTimeSlices(objects)
	assert.Equalf(t, []internal.BackupTime{}, result, "GetBackupTimeSlices returned not empty list: something wrong")
}

func TestGetBackupTimeSlices_List(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	_ = folder.PutObject("base_123312", &bytes.Buffer{})
	_ = folder.PutObject(testStreamBackup.BackupName+utility.SentinelSuffix, &bytes.Buffer{})

	objects, _, _ := folder.ListFolder()

	result := internal.GetBackupTimeSlices(objects)

	assert.Equalf(t, 1, len(result), "GetBackupTimeSlices returned wrong count of backup: something wrong")
	assert.Equalf(t, testStreamBackup.BackupName, result[0].BackupName, "GetBackupTimeSlices returned strange name")
	assert.True(t, testStreamBackup.Time.Before(result[0].Time), "GetBackupTimeSlices returned bad time: storage time less then mock time")
}

func TestGetBackupTimeSlices_OrderCheck(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	_ = folder.PutObject(testStreamBackup.BackupName+".1"+utility.SentinelSuffix, &bytes.Buffer{})
	_ = folder.PutObject(testStreamBackup.BackupName+".2"+utility.SentinelSuffix, &bytes.Buffer{})

	objects, _, _ := folder.ListFolder()

	result := internal.GetBackupTimeSlices(objects)

	assert.Equalf(t, 2, len(result), "GetBackupTimeSlices returned wrong count of backup: something wrong")
	assert.True(t, result[0].BackupName == testStreamBackup.BackupName+".2", "GetBackupTimeSlices returned bad time ordering: Second file should be first, because second was added last")
	assert.True(t, result[0].Time.After(result[1].Time), "GetBackupTimeSlices returned bad time ordering: order should be Descending")
}

func TestGetGarbageFromPrefix(t *testing.T) {
	backupNames := []string{"backup", "garbage", "garbage_0"}
	folders := make([]storage.Folder, 0)
	nonGarbage := []internal.BackupTime{{"backup", time.Now(), "ZZZZZZZZZZZZZZZZZZZZZZZZ"}}

	for _, prefix := range backupNames {
		folders = append(folders, memory.NewFolder(prefix, memory.NewStorage()))
	}

	garbage := internal.GetGarbageFromPrefix(folders, nonGarbage)
	assert.Equal(t, garbage, []string{"garbage", "garbage_0"})
}

func TestGetGarbageFromPrefix_emptyNonGarbage(t *testing.T) {
	backupNames := []string{"backup", "garbage", "garbage_0"}
	folders := make([]storage.Folder, 0)
	nonGarbage := make([]internal.BackupTime, 0)

	for _, prefix := range backupNames {
		folders = append(folders, memory.NewFolder(prefix, memory.NewStorage()))
	}

	garbage := internal.GetGarbageFromPrefix(folders, nonGarbage)
	assert.Equal(t, garbage, []string{"backup", "garbage", "garbage_0"})
}

func TestGetGarbageFromPrefix_emptyFolders(t *testing.T) {
	folders := make([]storage.Folder, 0)
	nonGarbage := make([]internal.BackupTime, 0)

	garbage := internal.GetGarbageFromPrefix(folders, nonGarbage)
	assert.Equal(t, garbage, make([]string, 0))
}
