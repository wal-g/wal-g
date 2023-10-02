package postgres_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
	"testing"
	"time"
)

func TestFetch(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	backupName := "test"
	data := "Data"
	hostName := "TestHost"
	compressedSize := int64(100)
	uncompressedSize := int64(10)
	date := time.Date(2002, 3, 21, 0, 0, 0, 0, time.UTC)
	isPermanent := false

	testObject := postgres.ExtendedMetadataDto{
		StartTime:        date,
		FinishTime:       date,
		Hostname:         hostName,
		IsPermanent:      isPermanent,
		UncompressedSize: uncompressedSize,
		CompressedSize:   compressedSize,
		UserData:         data,
	}

	var expectedResult = internal.GenericMetadata{
		BackupName:       backupName,
		UncompressedSize: uncompressedSize,
		CompressedSize:   compressedSize,
		Hostname:         hostName,
		StartTime:        date,
		FinishTime:       date,
		IsPermanent:      isPermanent,
		IncrementDetails: postgres.NewIncrementDetailsFetcher(postgres.Backup{
			Backup: internal.Backup{Name: backupName, Folder: folder},
		}),
		UserData: data,
	}

	marshaller, _ := internal.NewDtoSerializer()
	file, _ := marshaller.Marshal(testObject)
	_ = folder.PutObject(internal.MetadataNameFromBackup(backupName), file)

	actualResult, err := postgres.NewGenericMetaFetcher().Fetch(backupName, folder)

	assert.NoError(t, err)
	isEqualTimeStart := expectedResult.StartTime.Equal(actualResult.StartTime)
	assert.True(t, isEqualTimeStart)

	isEqualTimeFinish := expectedResult.FinishTime.Equal(actualResult.FinishTime)
	assert.True(t, isEqualTimeFinish)

	expectedResult.StartTime = actualResult.StartTime
	expectedResult.FinishTime = actualResult.FinishTime

	assert.NoError(t, err)
	assert.Equal(t, expectedResult, actualResult)
}

func TestFetchReturnErrorWhenNotFoundMetadata(t *testing.T) {
	backupName := "test"
	folder := testtools.CreateMockStorageFolder()

	_, err := postgres.NewGenericMetaFetcher().Fetch(backupName, folder)

	assert.Error(t, err)
	assert.IsType(t, storage.ObjectNotFoundError{}, err)
}
