package greenplum_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/testtools"
)

var (
	testStreamBackup = internal.BackupTime{
		BackupName: "stream_20210329T125616Z",
		Time:       time.Now(),
	}
)

func TestGetRestorePointTimeSlices_emptyList(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	_ = folder.PutObject("restore_123312", &bytes.Buffer{})
	objects, _, _ := folder.ListFolder()
	result := greenplum.GetRestorePointsTimeSlices(objects)
	assert.Equalf(t, []internal.BackupTime{}, result, "GetRestorePointsTimeSlices returned not empty list: something wrong")
}

func TestGetRestorePointTimeSlices_List(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	_ = folder.PutObject("restore_123312", &bytes.Buffer{})
	_ = folder.PutObject(testStreamBackup.BackupName+greenplum.RestorePointSuffix, &bytes.Buffer{})

	objects, _, _ := folder.ListFolder()

	result := greenplum.GetRestorePointsTimeSlices(objects)

	assert.Equalf(t, 1, len(result), "GetRestorePointTimeSlices returned wrong count of backup: something wrong")
	assert.Equalf(t, testStreamBackup.BackupName, result[0].BackupName, "GetRestorePointTimeSlices returned strange name")
	assert.True(t, testStreamBackup.Time.Before(result[0].Time), "GetRestorePointTimeSlices returned bad time: storage time less than mock time")
}

func TestGetRestorePointTimeSlices_OrderCheck(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	_ = folder.PutObject(testStreamBackup.BackupName+".1"+greenplum.RestorePointSuffix, &bytes.Buffer{})
	time.Sleep(time.Second)
	_ = folder.PutObject(testStreamBackup.BackupName+".2"+greenplum.RestorePointSuffix, &bytes.Buffer{})

	objects, _, _ := folder.ListFolder()

	result := greenplum.GetRestorePointsTimeSlices(objects)
	internal.SortBackupTimeSlices(result)

	assert.Equalf(t, 2, len(result), "GetRestorePointTimeSlices returned wrong count of restore points: something wrong")
	assert.True(t, result[0].BackupName == testStreamBackup.BackupName+".1", "GetRestorePointTimeSlices returned bad time ordering: "+testStreamBackup.BackupName+".1 should be first, because second was added earlier")
	assert.True(t, result[0].Time.Before(result[1].Time), "GetRestorePointTimeSlices returned bad time ordering: order should be Ascending")
}
