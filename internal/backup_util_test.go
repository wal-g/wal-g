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
	lastB, _ := internal.GetLatestBackupName(folder)
	assert.Equalf(t, lastB+utility.SentinelSuffix, b2, "Last Backup is not b2")
}

func TestGetLatestBackupName_EmptyWhenNoBackups(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	lastB, _ := internal.GetLatestBackupName(folder)
	assert.Equal(t, "", lastB)
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

func TestSortBackupTimeWithMetadataSlices_ByCreationTimeWhenCreationTimeIsNotDefault(t *testing.T) {
	backups := []internal.BackupTimeWithMetadata{
		{
			BackupTime: internal.BackupTime{
				BackupName:  "fBackup",
				Time:        time.Date(2021, 3, 21, 0, 0, 0, 0, time.UTC),
				WalFileName: "fWalFileName",
			},
			GenericMetadata: internal.GenericMetadata{
				StartTime: time.Date(2020, 3, 21, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			BackupTime: internal.BackupTime{
				BackupName:  "sBackup",
				Time:        time.Date(2022, 3, 21, 0, 0, 0, 0, time.UTC),
				WalFileName: "sWalFileName",
			},
			GenericMetadata: internal.GenericMetadata{
				StartTime: time.Date(2016, 3, 21, 0, 0, 0, 0, time.UTC),
			},
		},
	}

	expectedBackups := []internal.BackupTimeWithMetadata{
		{
			BackupTime: internal.BackupTime{
				BackupName:  "sBackup",
				Time:        time.Date(2022, 3, 21, 0, 0, 0, 0, time.UTC),
				WalFileName: "sWalFileName",
			},
			GenericMetadata: internal.GenericMetadata{
				StartTime: time.Date(2016, 3, 21, 0, 0, 0, 0, time.UTC),
			},
		},
		{
			BackupTime: internal.BackupTime{
				BackupName:  "fBackup",
				Time:        time.Date(2021, 3, 21, 0, 0, 0, 0, time.UTC),
				WalFileName: "fWalFileName",
			},
			GenericMetadata: internal.GenericMetadata{
				StartTime: time.Date(2020, 3, 21, 0, 0, 0, 0, time.UTC),
			},
		},
	}

	internal.SortBackupTimeWithMetadataSlices(backups)
	assert.Equal(t, expectedBackups, backups)
}

func TestGetBackupsWithMetadata(t *testing.T) {
	fBackup := internal.BackupTime{
		BackupName:  "fSentinelBackup",
		WalFileName: "ZZZZZZZZZZZZZZZZZZZZZZZZ",
	}
	sBackup := internal.BackupTime{
		BackupName:  "sSentinelBackup",
		WalFileName: "ZZZZZZZZZZZZZZZZZZZZZZZZ",
	}

	metadata := map[string]internal.GenericMetadata{
		fBackup.BackupName: {StartTime: time.Date(2021, 3, 21, 0, 0, 0, 0, time.UTC)},
		sBackup.BackupName: {StartTime: time.Date(2016, 3, 21, 0, 0, 0, 0, time.UTC)},
	}

	expected := []internal.BackupTimeWithMetadata{
		{fBackup, metadata[fBackup.BackupName]},
		{sBackup, metadata[sBackup.BackupName]},
	}

	folder := testtools.MakeDefaultInMemoryStorageFolder()

	marshaller, _ := internal.NewDtoSerializer()
	fFile, _ := marshaller.Marshal(fBackup)
	sFile, _ := marshaller.Marshal(sBackup)

	_ = folder.PutObject(internal.SentinelNameFromBackup(fBackup.BackupName), fFile)
	_ = folder.PutObject(internal.SentinelNameFromBackup(sBackup.BackupName), sFile)

	actual, _ := internal.GetBackupsWithMetadata(folder, &testtools.MockGenericMetaFetcher{MockMeta: metadata})

	// ignore time difference
	for i, _ := range expected {
		expected[i].Time = actual[i].Time
	}

	assert.Equal(t, expected, actual)
}
