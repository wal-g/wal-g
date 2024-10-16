package sqlserver_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/sqlserver"
	"github.com/wal-g/wal-g/testtools"
)

func init() {
	internal.ConfigureSettings("")
	conf.InitConfig()
	conf.Configure()
}

func TestFetch(t *testing.T) {
	backupName := "test"
	data := "Data"

	date := time.Date(2022, 3, 21, 0, 0, 0, 0, time.UTC)
	isPermanent := false

	testObject := sqlserver.SentinelDto{
		StartLocalTime: date,
		StopLocalTime:  date,
		UserData:       data,
	}

	expectedResult := internal.GenericMetadata{
		BackupName:       backupName,
		StartTime:        date,
		FinishTime:       date,
		IsPermanent:      isPermanent,
		IncrementDetails: &internal.NopIncrementDetailsFetcher{},
		UserData:         data,
	}

	folder := testtools.MakeDefaultInMemoryStorageFolder()
	marshaller, err := internal.NewDtoSerializer()
	require.NoError(t, err)
	file, err := marshaller.Marshal(testObject)
	require.NoError(t, err)
	err = folder.PutObject(internal.SentinelNameFromBackup(backupName), file)
	require.NoError(t, err)
	actualResult, err := sqlserver.NewGenericMetaFetcher().Fetch(backupName, folder)
	require.NoError(t, err)

	//check equality of time separately
	isEqualTimeStart := expectedResult.StartTime.Equal(actualResult.StartTime)
	assert.True(t, isEqualTimeStart)

	isEqualTimeFinish := expectedResult.FinishTime.Equal(actualResult.FinishTime)
	assert.True(t, isEqualTimeFinish)

	// since assert.Equal doesn't compare time properly, just assign the actual to the expected time
	expectedResult.StartTime = actualResult.StartTime
	expectedResult.FinishTime = actualResult.FinishTime

	assert.Equal(t, expectedResult, actualResult)
}

func TestSetIsPermanent(t *testing.T) {
	backupName := "test"
	testObject := sqlserver.SentinelDto{
		UserData:       nil,
		StartLocalTime: time.Now(),
		IsPermanent:    false,
	}

	folder := testtools.MakeDefaultInMemoryStorageFolder()
	marshaller, err := internal.NewDtoSerializer()
	require.NoError(t, err)
	file, err := marshaller.Marshal(testObject)
	require.NoError(t, err)
	err = folder.PutObject(internal.SentinelNameFromBackup(backupName), file)
	require.NoError(t, err)

	err = sqlserver.NewGenericMetaSetter().SetIsPermanent(backupName, folder, true)
	require.NoError(t, err)
	backup, err := sqlserver.NewGenericMetaFetcher().Fetch(backupName, folder)
	require.NoError(t, err)

	assert.True(t, backup.IsPermanent)
}

func TestSetUserData(t *testing.T) {
	backupName := "test"
	updatedData := "Updated Data"
	oldData := "Old Data"
	testObject := sqlserver.SentinelDto{
		UserData:       oldData,
		StartLocalTime: time.Now(),
		IsPermanent:    false,
	}

	folder := testtools.MakeDefaultInMemoryStorageFolder()
	marshaller, err := internal.NewDtoSerializer()
	require.NoError(t, err)
	file, err := marshaller.Marshal(testObject)
	require.NoError(t, err)
	err = folder.PutObject(internal.SentinelNameFromBackup(backupName), file)
	require.NoError(t, err)

	err = sqlserver.NewGenericMetaSetter().SetUserData(backupName, folder, updatedData)
	require.NoError(t, err)

	backup, err := sqlserver.NewGenericMetaFetcher().Fetch(backupName, folder)
	require.NoError(t, err)

	assert.Equal(t, updatedData, backup.UserData)
}
