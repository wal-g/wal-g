package storage_test

import (
	"bytes"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestListFolderRecursively(t *testing.T) {
	var folder = memory.NewFolder("in_memory/", memory.NewStorage())
	paths := []string{
		"a",
		"subfolder1/b",
		"subfolder1/subfolder11/c",
		"subfolder2/d",
	}
	for _, relativePath := range paths {
		err := folder.PutObject(relativePath, &bytes.Buffer{})
		assert.NoError(t, err)
	}
	fullPathObjects, err := storage.ListFolderRecursively(folder)
	assert.NoError(t, err)
	for _, relativePath := range paths {
		found := false
		for _, object := range fullPathObjects {
			if object.GetName() == relativePath {
				found = true
				break
			}
		}
		assert.True(t, found)
	}
}

func CreateMockStorageFolder() storage.Folder {
	var folder = memory.NewFolder("in_memory/", memory.NewStorage())
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

func TestDeleteOldObjects(t *testing.T) {
	folder := CreateMockStorageFolder()
	expectedOnlyOneSavedObjectName := "basebackups_005/base_123312"
	filter := func(object storage.Object) bool {
		return object.GetName() != expectedOnlyOneSavedObjectName
	}

	folderFilter := func(path string) bool { return true }
	err := storage.DeleteObjectsWhere(folder, true, filter, folderFilter)
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

	err := storage.DeleteObjectsWhere(folder, true, filter, folderFilter)
	assert.NoError(t, err)
	savedObjects, err := storage.ListFolderRecursively(folder)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(savedObjects))
	assert.Equal(t, expectedOnlyOneSavedObjectName, savedObjects[0].GetName())
}

func TestListFolderRecursivelyWithFilter(t *testing.T) {
	var folder = memory.NewFolder("in_memory/", memory.NewStorage())
	subFolder := folder.GetSubFolder("basebackups_005/")
	includedObjNames := []string{
		"base_123_backup_stop_sentinel.json",
		"base_456_backup_stop_sentinel.json",
		"base_123312",
		"base_321/nop",
		"folder123/nop",
		"base_456/some_folder/2",
	}

	for _, name := range includedObjNames {
		subFolder.PutObject(name, &bytes.Buffer{})
	}

	excludedObjNames := []string{
		"base_456/tar_partitions/1",
		"base_456/tar_partitions/2",
		"base_456/tar_partitions/3",
	}

	for _, name := range excludedObjNames {
		subFolder.PutObject(name, &bytes.Buffer{})
	}

	filterFunc := func(path string) bool {
		return !strings.HasPrefix(path, "base_456/tar_partitions")
	}

	filtered, err := storage.ListFolderRecursivelyWithFilter(subFolder, filterFunc)

	filteredNames := make([]string, 0)

	for i := range filtered {
		filteredNames = append(filteredNames, filtered[i].GetName())
	}

	sort.Strings(filteredNames)
	sort.Strings(includedObjNames)

	assert.NoError(t, err)
	assert.Equal(t, filteredNames, includedObjNames)
}
