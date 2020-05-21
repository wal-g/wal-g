package internal_test

import (
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/testtools"
)

func TestStartCopy_WhenThereAreNoObjectsToCopy(t *testing.T) {
	var infos = make([]internal.CopyingInfo, 0)
	var isSuccess, err = internal.StartCopy(infos)
	assert.NoError(t, err)
	assert.True(t, isSuccess)
}

func TestStartCopy_WhenThereAreObjectsToCopy(t *testing.T) {
	var from = testtools.CreateMockStorageFolderWithPermanentBackups(t)
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	infos, err := internal.GetAllObjects(from, to)
	assert.NoError(t, err)
	isSuccess, err := internal.StartCopy(infos)
	assert.NoError(t, err)
	assert.True(t, isSuccess)

	for _, info := range infos {
		var filename = path.Join(from.GetPath(), info.Object.GetName())
		var result, err = to.Exists(filename)
		assert.NoError(t, err)
		if !result {
			tracelog.InfoLogger.Println("Filename '" + filename + "' not found")
		}
		assert.True(t, result)
	}
}

func TestGetBackupObjects_WhenFolderIsEmpty(t *testing.T) {
	var from = testtools.MakeDefaultInMemoryStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var backup = internal.NewBackup(from, "base_000000010000000000000002")
	var infos, err = internal.GetBackupObjects(backup, from, to)
	assert.NoError(t, err)
	assert.Empty(t, infos)
}

func TestGetBackupObjects_WhenFolderIsNotEmpty(t *testing.T) {
	var from = testtools.CreateMockStorageFolderWithPermanentBackups(t)
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var backup = internal.NewBackup(from, "base_000000010000000000000002")
	var infos, err = internal.GetBackupObjects(backup, from, to)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(infos))
	assert.NotEmpty(t, infos)
}

func TestGetHistoryObjects_WhenFolderIsEmpty(t *testing.T) {
	var from = testtools.MakeDefaultInMemoryStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var backup = internal.NewBackup(from, "base_000000010000000000000002")
	var infos, err = internal.GetHistoryObjects(backup, from, to)
	assert.NoError(t, err)
	assert.Empty(t, infos)
}

func TestGetHistoryObjects_WhenThereIsNoHistoryObjects(t *testing.T) {
	var from = testtools.CreateMockStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var backup = internal.NewBackup(from, "base_000000010000000000000002")
	var infos, err = internal.GetHistoryObjects(backup, from, to)
	assert.NoError(t, err)
	assert.Empty(t, infos)
}

func TestGetAllObjects_WhenFromFolderIsEmpty(t *testing.T) {
	var from = testtools.MakeDefaultInMemoryStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var infos, err = internal.GetAllObjects(from, to)
	assert.NoError(t, err)
	assert.Empty(t, infos)
}

func TestGetAllObjects_WhenFromFolderIsNotEmpty(t *testing.T) {
	var from = testtools.CreateMockStorageFolderWithPermanentBackups(t)
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var infos, err = internal.GetAllObjects(from, to)
	assert.NoError(t, err)
	assert.NotEmpty(t, infos)

	for _, info := range infos {
		var result, err = from.Exists(info.Object.GetName())
		assert.NoError(t, err)
		assert.True(t, result)
	}
}

func TestBuildCopyingInfos_WhenThereNoObjectsInFolder(t *testing.T) {
	var from = testtools.MakeDefaultInMemoryStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	var infos = internal.BuildCopyingInfos(from, to, make([]storage.Object, 0), func(object storage.Object) bool { return true })
	assert.Empty(t, infos)
}

func TestBuildCopyingInfos_WhenConditionIsJustFalse(t *testing.T) {
	var from = testtools.CreateMockStorageFolder()
	var to = testtools.MakeDefaultInMemoryStorageFolder()
	objects, err := storage.ListFolderRecursively(from)
	assert.NoError(t, err)
	var infos = internal.BuildCopyingInfos(from, to, objects, func(object storage.Object) bool { return false })
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

	var infos = internal.BuildCopyingInfos(from, to, objects, condition)
	assert.Equal(t, expectedCount, len(infos))
	for _, info := range infos {
		assert.True(t, condition(info.Object))
	}
}
