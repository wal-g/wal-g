package etcd_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/etcd"
	"github.com/wal-g/wal-g/testtools"
)

func init() {
	internal.ConfigureSettings("")
	config.InitConfig()
	config.Configure()
}

func TestFetch(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	backupName := "test"
	data := "Data"
	date := time.Date(2002, 3, 21, 0, 0, 0, 0, time.UTC)

	testObject := etcd.StreamSentinelDto{
		StartLocalTime: date,
		IsPermanent:    false,
		UserData:       data,
	}

	var expectedResult = internal.GenericMetadata{
		BackupName:  backupName,
		StartTime:   date,
		IsPermanent: false,
		UserData:    data,
	}

	marshaller, _ := internal.NewDtoSerializer()
	file, _ := marshaller.Marshal(testObject)
	_ = folder.PutObject(internal.SentinelNameFromBackup(backupName), file)

	actualResult, err := etcd.NewGenericMetaFetcher().Fetch(backupName, folder)
	assert.NoError(t, err)

	isEqualTimeStart := expectedResult.StartTime.Equal(actualResult.StartTime)
	assert.True(t, isEqualTimeStart)

	expectedResult.StartTime = actualResult.StartTime
	expectedResult.FinishTime = actualResult.FinishTime

	assert.NoError(t, err)
	assert.Equal(t, expectedResult, actualResult)
}
