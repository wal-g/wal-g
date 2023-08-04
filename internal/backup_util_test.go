package internal_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

var (
	testStreamBackup = internal.BackupTime{
		BackupName: "stream_20210329T125616Z",
		Time:       time.Now(),
	}
)

func TestGetBackups_emptyFolder(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	_ = folder.PutObject("base_123312", &bytes.Buffer{})

	backups, err := internal.GetBackups(folder)

	assert.Empty(t, backups)
	assert.Error(t, err, internal.NoBackupsFoundError{})
}

func TestGetBackups(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	_ = folder.PutObject("base_123312", &bytes.Buffer{})
	_ = folder.PutObject(testStreamBackup.BackupName+utility.SentinelSuffix, &bytes.Buffer{})

	backups, _ := internal.GetBackups(folder)

	assert.Equal(t, 1, len(backups))
	assert.Equal(t, testStreamBackup.BackupName, backups[0].BackupName)
}

func TestGetBackupsAndGarbage_emptyList(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	_ = folder.PutObject("base_123312", &bytes.Buffer{})

	backups, garbage, _ := internal.GetBackupsAndGarbage(folder)

	assert.Empty(t, backups)
	assert.Empty(t, garbage)
}

func TestGetBackupsAndGarbage(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	_ = folder.PutObject("base_123312", &bytes.Buffer{})
	_ = folder.PutObject("base_321/nop", &bytes.Buffer{})
	_ = folder.PutObject(testStreamBackup.BackupName+utility.SentinelSuffix, &bytes.Buffer{})

	backups, garbage, _ := internal.GetBackupsAndGarbage(folder)

	assert.Equal(t, 1, len(backups))
	assert.Equal(t, 1, len(garbage))
	assert.Equal(t, testStreamBackup.BackupName, backups[0].BackupName)
	assert.Equal(t, "base_321", garbage[0])
}

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
	assert.True(t, testStreamBackup.Time.Before(result[0].Time), "GetBackupTimeSlices returned bad time: storage time less than mock time")
}

func TestGetBackupTimeSlices_OrderCheck(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	_ = folder.PutObject(testStreamBackup.BackupName+".1"+utility.SentinelSuffix, &bytes.Buffer{})
	time.Sleep(time.Second)
	_ = folder.PutObject(testStreamBackup.BackupName+".2"+utility.SentinelSuffix, &bytes.Buffer{})

	objects, _, _ := folder.ListFolder()

	result := internal.GetBackupTimeSlices(objects)
	internal.SortBackupTimeSlices(result)

	assert.Equalf(t, 2, len(result), "GetBackupTimeSlices returned wrong count of backup: something wrong")
	assert.True(t, result[0].BackupName == testStreamBackup.BackupName+".1", "GetBackupTimeSlices returned bad time ordering: "+testStreamBackup.BackupName+".1 should be first, because second was added earlier")
	assert.True(t, result[0].Time.Before(result[1].Time), "GetBackupTimeSlices returned bad time ordering: order should be Ascending")
}

func TestGetLastBackupName(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	b1 := testStreamBackup.BackupName + ".1" + utility.SentinelSuffix
	b2 := testStreamBackup.BackupName + ".2" + utility.SentinelSuffix
	_, _ = folder.PutObject(b1, &bytes.Buffer{}), folder.PutObject(b2, &bytes.Buffer{})
	lastB, _ := internal.GetLatestBackup(folder)
	assert.Equalf(t, lastB.Name+utility.SentinelSuffix, b2, "Last Backup is not b2")
}

func TestGetLatestBackupName_EmptyWhenNoBackups(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	lastB, _ := internal.GetLatestBackup(folder)
	assert.Equal(t, "", lastB.Name)
}

func TestGetGarbageFromPrefix(t *testing.T) {
	backupNames := []string{"backup", "garbage", "garbage_0"}
	folders := make([]storage.Folder, 0)
	nonGarbage := []internal.BackupTime{{BackupName: "backup", Time: time.Now(), WalFileName: "ZZZZZZZZZZZZZZZZZZZZZZZZ"}}

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

func TestDeleteGarbage_emptyFolder(t *testing.T) {

	folder := testtools.MakeDefaultInMemoryStorageFolder()

	objects, folders, _ := folder.ListFolder()
	assert.Equal(t, 0, len(objects))
	assert.Equal(t, 0, len(folders))

	err := internal.DeleteGarbage(folder, []string{"backup1", "backup2", "backup3"})
	assert.Equal(t, nil, err)

	objects, folders, _ = folder.ListFolder()
	assert.Equal(t, 0, len(objects))
	assert.Equal(t, 0, len(folders))
}

func TestDeleteGarbage_nonRecursive(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	_ = folder.PutObject("backup1/file.json", &bytes.Buffer{})
	_ = folder.PutObject("backup2/file.json", &bytes.Buffer{})

	objects, folders, _ := folder.ListFolder()
	assert.Equal(t, 0, len(objects))
	assert.Equal(t, 2, len(folders))

	err := internal.DeleteGarbage(folder, []string{"backup1"})
	assert.Equal(t, nil, err)

	objects, folders, _ = folder.ListFolder()
	assert.Equal(t, 0, len(objects))
	assert.Equal(t, 1, len(folders))
	assert.Equal(t, folders[0].GetPath(), "in_memory/backup2/")
}

func TestDeleteGarbage_recursive(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	_ = folder.PutObject("backup1/folder1/obj1.tar", &bytes.Buffer{})
	_ = folder.PutObject("backup1/folder2/obj2.zip", &bytes.Buffer{})
	_ = folder.PutObject("backup1/meta.json", &bytes.Buffer{})
	_ = folder.PutObject("backup2/meta_b2.json", &bytes.Buffer{})
	_ = folder.PutObject("backup2/folder1/obj1.tar", &bytes.Buffer{})

	objects, folders, _ := folder.ListFolder()
	assert.Equal(t, 0, len(objects))
	assert.Equal(t, 2, len(folders))

	err := internal.DeleteGarbage(folder, []string{"backup1"})
	assert.Equal(t, nil, err)

	objects, folders, _ = folder.ListFolder()
	assert.Equal(t, 0, len(objects))
	assert.Equal(t, 1, len(folders))
	backup2 := folders[0]
	assert.Equal(t, backup2.GetPath(), "in_memory/backup2/")
	objects, folders, _ = backup2.ListFolder()
	assert.Equal(t, 1, len(objects))
	assert.Equal(t, 1, len(folders))
	assert.Equal(t, objects[0].GetName(), "meta_b2.json")
	assert.Equal(t, folders[0].GetPath(), "in_memory/backup2/folder1/")

}
