package greenplum_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/testtools"
	"testing"
	"time"
)

func init() {
	internal.ConfigureSettings("")
	internal.InitConfig()
	internal.Configure()
}

func TestFetch(t *testing.T) {
	backupName := "test"
	data := "Data"
	hostName := "TestHost"
	compressedSize := int64(100)
	uncompressedSize := int64(10)
	restorePoint := new(string)
	*restorePoint = "restorePoint"
	var segments []greenplum.SegmentMetadata
	timeNow := time.Now()
	format := greenplum.MetadataDatetimeFormat
	version := "version"
	isPermanent := false
	systemIdentifier := new(uint64)
	*systemIdentifier = 20

	testObject := greenplum.BackupSentinelDto{
		RestorePoint:     restorePoint,
		Segments:         segments,
		UserData:         data,
		StartTime:        timeNow,
		FinishTime:       timeNow,
		DatetimeFormat:   format,
		Hostname:         hostName,
		GpVersion:        version,
		IsPermanent:      isPermanent,
		SystemIdentifier: systemIdentifier,
		UncompressedSize: uncompressedSize,
		CompressedSize:   compressedSize,
	}

	folder := testtools.MakeDefaultInMemoryStorageFolder()
	marshaller, _ := internal.NewDtoSerializer()
	file, _ := marshaller.Marshal(testObject)
	_ = folder.PutObject(internal.SentinelNameFromBackup(backupName), file)
	backup, err := greenplum.NewGenericMetaFetcher().Fetch(backupName, folder)
	assert.NoError(t, err)
	assert.Equal(t, backupName, backup.BackupName)
	assert.Equal(t, data, backup.UserData)
	assert.Equal(t, hostName, backup.Hostname)
	assert.Equal(t, isPermanent, backup.IsPermanent)
	assert.Equal(t, uncompressedSize, backup.UncompressedSize)
	assert.Equal(t, compressedSize, backup.CompressedSize)
}

func TestSetIsPermanent(t *testing.T) {
	backupName := "test"
	testObject := greenplum.BackupSentinelDto{
		RestorePoint:     nil,
		Segments:         nil,
		UserData:         nil,
		StartTime:        time.Now(),
		FinishTime:       time.Now(),
		DatetimeFormat:   greenplum.MetadataDatetimeFormat,
		Hostname:         "",
		GpVersion:        "",
		IsPermanent:      false,
		SystemIdentifier: nil,
		UncompressedSize: 0,
		CompressedSize:   0,
	}

	folder := testtools.MakeDefaultInMemoryStorageFolder()
	marshaller, _ := internal.NewDtoSerializer()
	file, _ := marshaller.Marshal(testObject)
	_ = folder.PutObject(internal.SentinelNameFromBackup(backupName), file)

	_ = greenplum.NewGenericMetaSetter().SetIsPermanent(backupName, folder, true)
	backup, err := greenplum.NewGenericMetaFetcher().Fetch(backupName, folder)

	assert.NoError(t, err)
	assert.True(t, backup.IsPermanent)
}

func TestSetUserData(t *testing.T) {
	backupName := "test"
	updatedData := "Updated Data"
	oldData := "Old Data"
	testObject := greenplum.BackupSentinelDto{
		RestorePoint:     nil,
		Segments:         nil,
		UserData:         oldData,
		StartTime:        time.Now(),
		FinishTime:       time.Now(),
		DatetimeFormat:   greenplum.MetadataDatetimeFormat,
		Hostname:         "",
		GpVersion:        "",
		IsPermanent:      false,
		SystemIdentifier: nil,
		UncompressedSize: 0,
		CompressedSize:   0,
	}

	folder := testtools.MakeDefaultInMemoryStorageFolder()
	marshaller, _ := internal.NewDtoSerializer()
	file, _ := marshaller.Marshal(testObject)
	_ = folder.PutObject(internal.SentinelNameFromBackup(backupName), file)

	_ = greenplum.NewGenericMetaSetter().SetUserData(backupName, folder, updatedData)

	backup, err := greenplum.NewGenericMetaFetcher().Fetch(backupName, folder)

	assert.NoError(t, err)
	assert.Equal(t, updatedData, backup.UserData)
}
