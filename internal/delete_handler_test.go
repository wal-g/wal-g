package internal

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func CreateMockStorageFolder() storage.Folder {
	var folder = memory.NewFolder("in_memory/", memory.NewKVS())
	subFolder := folder.GetSubFolder("basebackups_005/")
	subFolder.PutObject("base_123_backup_stop_sentinel.json", &bytes.Buffer{})
	subFolder.PutObject("base_456_backup_stop_sentinel.json", strings.NewReader("{}"))
	subFolder.PutObject("base_000_backup_stop_sentinel.json", &bytes.Buffer{}) // last put
	subFolder.PutObject("base_123312", &bytes.Buffer{})                        // not a sentinel
	subFolder.PutObject("base_321/nop", &bytes.Buffer{})
	subFolder.PutObject("folder123/nop", &bytes.Buffer{})
	subFolder.PutObject("base_456/tar_partitions/1", &bytes.Buffer{})
	subFolder.PutObject("base_456/tar_partitions/2", &bytes.Buffer{})
	subFolder.PutObject("base_456/tar_partitions/3", &bytes.Buffer{})
	subFolder.PutObject("base_456/some_folder/3", &bytes.Buffer{})
	return folder
}

func CreateMockDeleteHandler(backups []BackupObject, folder storage.Folder) *DeleteHandler {
	lessFunction := func(object1, object2 storage.Object) bool { return object1.GetName() < object2.GetName() }
	deleteHandler := NewDeleteHandler(folder, backups, lessFunction)
	return deleteHandler
}

func TestDeleteOldObjects(t *testing.T) {
	folder := CreateMockStorageFolder()
	expectedOnlyOneSavedObjectName := "basebackups_005/base_123312"
	filter := func(object storage.Object) bool {
		return object.GetName() != expectedOnlyOneSavedObjectName
	}

	folderFilter := func(path string) bool { return true }
	err := DeleteObjectsWhere(folder, true, filter, folderFilter)
	assert.NoError(t, err)
	savedObjects, err := storage.ListFolderRecursively(folder)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(savedObjects))
	assert.Equal(t, expectedOnlyOneSavedObjectName, savedObjects[0].GetName())
}

func TestDeleteOldObjectsWithFilter(t *testing.T) {
	folder := CreateMockStorageFolder()
	expectedOnlyOneSavedObjectName := "basebackups_005/base_456/some_folder/3"
	filter := func(object storage.Object) bool {
		return true
	}

	folderFilter := func(name string) bool {
		return !strings.HasPrefix(name, "basebackups_005/base_456/some_folder")
	}

	err := DeleteObjectsWhere(folder, true, filter, folderFilter)
	assert.NoError(t, err)
	savedObjects, err := storage.ListFolderRecursively(folder)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(savedObjects))
	assert.Equal(t, expectedOnlyOneSavedObjectName, savedObjects[0].GetName())
}

func TestFindTargetByName(t *testing.T) {
	mockFolder := CreateMockStorageFolder()
	objects, _, _ := mockFolder.GetSubFolder("basebackups_005/").ListFolder()
	backupObjects := []BackupObject{NewDefaultBackupObject(objects[0])}
	deleteHandler := CreateMockDeleteHandler(backupObjects, mockFolder)

	testCases := []struct {
		target   string
		expected string
	}{
		{
			"",
			backupObjects[0].GetName(),
		},
		{
			backupObjects[0].GetName(),
			backupObjects[0].GetName(),
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Case %d", i), func(t *testing.T) {
			actual, err := deleteHandler.FindTargetByName(tc.target)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, actual.GetName())
		})
	}
}

func TestFindTargetByNameNotContains(t *testing.T) {
	mockFolder := CreateMockStorageFolder()
	objects, _, _ := mockFolder.GetSubFolder("basebackups_005/").ListFolder()
	backupObjects := []BackupObject{NewDefaultBackupObject(objects[0])}
	deleteHandler := CreateMockDeleteHandler(backupObjects, mockFolder)

	notExistTarget := "base_567_backup_stop_sentinel.json"
	actual, err := deleteHandler.FindTargetByName(notExistTarget)
	assert.Error(t, err)
	assert.Equal(t, nil, actual)
}

func TestFindTargetByNameEmpty(t *testing.T) {
	mockFolder := CreateMockStorageFolder()
	deleteHandler := CreateMockDeleteHandler([]BackupObject{}, mockFolder)
	actual, err := deleteHandler.FindTargetByName("base_123312")
	assert.Error(t, err)
	assert.Equal(t, nil, actual)
}
