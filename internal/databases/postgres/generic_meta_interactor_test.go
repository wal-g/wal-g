package postgres_test

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/stats"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
)

func TestFetch(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	backupName := "test"
	data := "Data"
	hostName := "TestHost"
	compressedSize := int64(100)
	uncompressedSize := int64(10)
	date := time.Date(2002, 3, 21, 0, 0, 0, 0, time.UTC)

	testObject := postgres.ExtendedMetadataDto{
		StartTime:        date,
		FinishTime:       date,
		Hostname:         hostName,
		IsPermanent:      false,
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
		IsPermanent:      false,
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

func TestSetUserData(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	backupName := "test"
	data := "Data"
	hostName := "TestHost"
	compressedSize := int64(100)
	uncompressedSize := int64(10)
	date := time.Date(2002, 3, 21, 0, 0, 0, 0, time.UTC)

	testObject := postgres.ExtendedMetadataDto{
		StartTime:        date,
		FinishTime:       date,
		Hostname:         hostName,
		IsPermanent:      false,
		UncompressedSize: uncompressedSize,
		CompressedSize:   compressedSize,
		UserData:         data,
	}

	newUserData := "NewUserData"

	marshaller, _ := internal.NewDtoSerializer()
	file, _ := marshaller.Marshal(testObject)
	_ = folder.PutObject(internal.MetadataNameFromBackup(backupName), file)

	setDataErr := postgres.NewGenericMetaSetter().SetUserData(backupName, folder, newUserData)
	fetchResult, fetchErr := postgres.NewGenericMetaFetcher().Fetch(backupName, folder)

	assert.NoError(t, setDataErr)
	assert.NoError(t, fetchErr)
	assert.Equal(t, fetchResult.UserData, newUserData)
}

func TestSetUserDataReturnErrorWhenNotFoundMetadata(t *testing.T) {
	backupName := "test"
	folder := testtools.CreateMockStorageFolder()
	testObject := postgres.ExtendedMetadataDto{}

	err := postgres.NewGenericMetaSetter().SetUserData(backupName, folder, testObject)

	assert.Error(t, err)
	assert.IsType(t, storage.ObjectNotFoundError{}, errors.Cause(err))
}

func TestSetUserDataReturnErrorWhenFolderIsMultiStorage(t *testing.T) {
	backupName := "test"

	mockCtrl := gomock.NewController(t)
	t.Cleanup(mockCtrl.Finish)
	statsCollectorMock := stats.NewMockCollector(mockCtrl)
	statsCollectorMock.EXPECT().ReportOperationResult(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	memFolders := map[string]storage.Folder{
		"s1": memory.NewFolder("s1/", memory.NewKVS()),
		"s2": memory.NewFolder("s2/", memory.NewKVS()),
	}
	folder := multistorage.NewFolder(memFolders, statsCollectorMock)
	testObject := postgres.ExtendedMetadataDto{}

	err := postgres.NewGenericMetaSetter().SetUserData(backupName, folder, testObject)

	assert.Error(t, err)
	assert.IsType(t, "failed to modify metadata", err.Error())
}

func TestSetIsPermanent(t *testing.T) {
	folder := testtools.CreateMockStorageFolder()
	backupName := "test"
	testObject := postgres.ExtendedMetadataDto{
		IsPermanent: false,
	}

	marshaller, _ := internal.NewDtoSerializer()
	file, _ := marshaller.Marshal(testObject)
	_ = folder.PutObject(internal.MetadataNameFromBackup(backupName), file)

	setErr := postgres.NewGenericMetaInteractor().SetIsPermanent(backupName, folder, true)
	actualResult, fetchErr := postgres.NewGenericMetaFetcher().Fetch(backupName, folder)

	assert.NoError(t, setErr)
	assert.NoError(t, fetchErr)
	assert.Equal(t, true, actualResult.IsPermanent)
}
