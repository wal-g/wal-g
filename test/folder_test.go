package test

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/testtools"
	"testing"
)

func TestListFolderRecursively(t *testing.T) {
	var folder = testtools.MakeDefaultInMemoryStorageFolder()
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

func TestDeleteOldObjects(t *testing.T) {
	folder := createMockStorageFolder()
	expectedOnlyOneSavedObjectName := "basebackups_005/base_123312"
	filter := func(object storage.Object) bool {
		return object.GetName() != expectedOnlyOneSavedObjectName
	}
	err := storage.DeleteObjectsWhere(folder, filter)
	assert.NoError(t, err)
	savedObjects, err := storage.ListFolderRecursively(folder)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(savedObjects))
	assert.Equal(t, expectedOnlyOneSavedObjectName, savedObjects[0].GetName())
}
