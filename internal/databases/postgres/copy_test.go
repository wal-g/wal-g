package postgres_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/testtools"
)

func TestStartCopy_WhenThereAreNoObjectsToCopy(t *testing.T) {
	var infos = make([]copy.InfoProvider, 0)
	var err = copy.Infos(infos)
	assert.NoError(t, err)
}

func TestStartCopy_WhenThereAreObjectsToCopy(t *testing.T) {
	var from = testtools.CreateMockStorageFolderWithPermanentBackups(t)
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	infos, err := postgres.WildcardInfo(from, to)
	assert.NoError(t, err)
	err = copy.Infos(infos)
	assert.NoError(t, err)

	for _, info := range infos {
		var result, err = to.Exists(info.SrcObj.GetName())
		assert.NoError(t, err)
		if !result {
			tracelog.InfoLogger.Println("Filename '" + info.SrcObj.GetName() + "' not found")
		}
		assert.True(t, result)
	}
}

func TestGetBackupCopyingInfo_WhenFolderIsEmpty(t *testing.T) {
	var from = testtools.MakeDefaultInMemoryStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var backup = postgres.NewBackup(from, "base_000000010000000000000002")
	var infos, err = postgres.BackupCopyingInfo(backup, from, to)
	assert.NoError(t, err)
	assert.Empty(t, infos)
}

func TestGetBackupCopyingInfo_WhenFolderIsNotEmpty(t *testing.T) {
	var from = testtools.CreateMockStorageFolderWithPermanentBackups(t)
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var backup = postgres.NewBackup(from, "base_000000010000000000000002")
	var infos, err = postgres.BackupCopyingInfo(backup, from, to)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(infos))
	assert.NotEmpty(t, infos)
}

func TestGetHistoryCopyingInfo_WhenFolderIsEmpty(t *testing.T) {
	var from = testtools.MakeDefaultInMemoryStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var backup = postgres.NewBackup(from, "base_000000010000000000000002")
	var infos, err = postgres.HistoryCopyingInfo(backup, from, to)
	assert.NoError(t, err)
	assert.Empty(t, infos)
}

func TestGetHistoryCopyingInfo_WhenThereIsNoHistoryObjects(t *testing.T) {
	var from = testtools.CreateMockStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var backup = postgres.NewBackup(from, "base_000000010000000000000002")
	var infos, err = postgres.HistoryCopyingInfo(backup, from, to)
	assert.NoError(t, err)
	assert.Empty(t, infos)
}

func TestGetAllCopyingInfo_WhenFromFolderIsEmpty(t *testing.T) {
	var from = testtools.MakeDefaultInMemoryStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var infos, err = postgres.WildcardInfo(from, to)
	assert.NoError(t, err)
	assert.Empty(t, infos)
}

func TestGetAllCopyingInfo_WhenFromFolderIsNotEmpty(t *testing.T) {
	var from = testtools.CreateMockStorageFolderWithPermanentBackups(t)
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var infos, err = postgres.WildcardInfo(from, to)
	assert.NoError(t, err)
	assert.NotEmpty(t, infos)

	for _, info := range infos {
		var result, err = from.Exists(info.SrcObj.GetName())
		assert.NoError(t, err)
		assert.True(t, result)
	}
}

func TestBuildCopyingInfos_WhenThereNoObjectsInFolder(t *testing.T) {
	var from = testtools.MakeDefaultInMemoryStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var infos = copy.BuildCopyingInfos(from, to, make([]storage.Object, 0), func(object storage.Object) bool { return true }, copy.NoopRenameFunc)
	assert.Empty(t, infos)
}

func TestBuildCopyingInfos_WhenConditionIsJustFalse(t *testing.T) {
	var from = testtools.CreateMockStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	objects, err := storage.ListFolderRecursively(from)
	assert.NoError(t, err)
	var infos = copy.BuildCopyingInfos(from, to, objects, func(object storage.Object) bool { return false }, copy.NoopRenameFunc)
	assert.Empty(t, infos)
}

func TestBuildCopyingInfos_WhenComplexCondition(t *testing.T) {
	var from = testtools.CreateMockStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()

	objects, err := storage.ListFolderRecursively(from)
	assert.NoError(t, err)
	var condition = func(object storage.Object) bool { return strings.HasSuffix(object.GetName(), ".json") }
	var expectedCount = 0
	for _, object := range objects {
		if condition(object) {
			expectedCount += 1
		}
	}

	assert.NotZero(t, expectedCount)

	var infos = copy.BuildCopyingInfos(from, to, objects, condition, copy.NoopRenameFunc)
	assert.Equal(t, expectedCount, len(infos))
	for _, info := range infos {
		assert.True(t, condition(info.SrcObj))
	}
}
