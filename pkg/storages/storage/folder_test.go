package storage_test

import (
	"bytes"
	"sort"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/cache"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
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

func TestListFolderRecursivelyWithFilter(t *testing.T) {
	var folder = memory.NewFolder("in_memory/", memory.NewStorage())
	includedObjNames := []string{
		"basebackups_005/base_123_backup_stop_sentinel.json",
		"basebackups_005/base_456_backup_stop_sentinel.json",
		"basebackups_005/base_123312",
		"basebackups_005/base_321/nop",
		"basebackups_005/folder123/nop",
		"basebackups_005/base_456/some_folder/2",
		"basebackups_005/base_456/tar_partitions",
		"basebackups_005/base_456/tar_partitions_file",
	}

	for _, name := range includedObjNames {
		_ = folder.PutObject(name, &bytes.Buffer{})
	}

	excludedObjNames := []string{
		"basebackups_005/base_456/tar_partitions/1",
		"basebackups_005/base_456/tar_partitions/2",
		"basebackups_005/base_456/tar_partitions/3",
		"basebackups_005/base_456/tar_partitions/1/1",
	}

	for _, name := range excludedObjNames {
		_ = folder.PutObject(name, &bytes.Buffer{})
	}

	filterFunc := func(path string) bool {
		return !strings.HasPrefix(path, "basebackups_005/base_456/tar_partitions")
	}

	filtered, err := storage.ListFolderRecursivelyWithFilter(folder, filterFunc)

	filteredNames := make([]string, 0)

	for i := range filtered {
		filteredNames = append(filteredNames, filtered[i].GetName())
	}

	sort.Strings(filteredNames)
	sort.Strings(includedObjNames)

	assert.NoError(t, err)
	assert.Equal(t, filteredNames, includedObjNames)
}

func TestListFolderRecursivelyWithFilter_MultiStorage(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	t.Cleanup(mockCtrl.Finish)
	cacheMock := cache.NewMockStatusCache(mockCtrl)
	memFolder := cache.NamedFolder{
		Name:   "test",
		Folder: memory.NewFolder("", memory.NewStorage()),
	}
	cacheMock.EXPECT().SpecificStorage("test").Return(&memFolder, nil)
	folder := multistorage.NewFolder(cacheMock)
	folder, err := multistorage.UseSpecificStorage("test", folder)
	require.NoError(t, err)
	folder = multistorage.SetPolicies(folder, policies.UniteAllStorages)

	includedObjNames := []string{
		"basebackups_005/base_123_backup_stop_sentinel.json",
		"basebackups_005/base_456_backup_stop_sentinel.json",
		"basebackups_005/base_123312",
		"basebackups_005/base_321/nop",
		"basebackups_005/folder123/nop",
		"basebackups_005/base_456/some_folder/2",
		"basebackups_005/base_456/tar_partitions",
		"basebackups_005/base_456/tar_partitions_file",
	}

	for _, name := range includedObjNames {
		_ = folder.PutObject(name, &bytes.Buffer{})
	}

	excludedObjNames := []string{
		"basebackups_005/base_456/tar_partitions/1",
		"basebackups_005/base_456/tar_partitions/2",
		"basebackups_005/base_456/tar_partitions/3",
		"basebackups_005/base_456/tar_partitions/1/1",
	}

	for _, name := range excludedObjNames {
		_ = folder.PutObject(name, &bytes.Buffer{})
	}

	filterFunc := func(path string) bool {
		return !strings.HasPrefix(path, "basebackups_005/base_456/tar_partitions")
	}

	filtered, err := storage.ListFolderRecursivelyWithFilter(folder, filterFunc)

	filteredNames := make([]string, 0)

	for i := range filtered {
		filteredNames = append(filteredNames, filtered[i].GetName())
	}

	sort.Strings(filteredNames)
	sort.Strings(includedObjNames)

	assert.NoError(t, err)
	assert.Equal(t, filteredNames, includedObjNames)
}

func TestListFolderRecursivelyWithPrefix(t *testing.T) {
	assertFiles := func(t *testing.T, got []storage.Object, wantNames []string) {
		var gotNames []string
		for _, g := range got {
			gotNames = append(gotNames, g.GetName())
		}
		sort.Strings(wantNames)
		sort.Strings(gotNames)
		assert.Equal(t, wantNames, gotNames)
	}

	t.Run("list single file with prefix name if exists", func(t *testing.T) {
		folder := memory.NewFolder("memory/", memory.NewStorage())
		_ = folder.PutObject("a/b/c/123", &bytes.Buffer{})
		_ = folder.PutObject("a/b/c/123/waste1", &bytes.Buffer{})
		_ = folder.PutObject("a/b/c/123/waste2/waste3", &bytes.Buffer{})
		files, err := storage.ListFolderRecursivelyWithPrefix(folder, "a/b/c/123")
		assert.NoError(t, err)
		assertFiles(t, files, []string{"a/b/c/123"})

		_ = folder.PutObject("a", &bytes.Buffer{})

		for _, prefix := range []string{"a", "/a"} {
			files, err = storage.ListFolderRecursivelyWithPrefix(folder, prefix)
			assert.NoError(t, err)
			assertFiles(t, files, []string{"a"})
		}
	})

	t.Run("list all files in dir with prefix name", func(t *testing.T) {
		folder := memory.NewFolder("memory/", memory.NewStorage())
		_ = folder.PutObject("waste1", &bytes.Buffer{})
		_ = folder.PutObject("a/111", &bytes.Buffer{})
		_ = folder.PutObject("a/b/222", &bytes.Buffer{})
		_ = folder.PutObject("a/b/c/333", &bytes.Buffer{})
		_ = folder.PutObject("b/waste2", &bytes.Buffer{})

		for _, prefix := range []string{"a", "a/", "/a", "/a/"} {
			files, err := storage.ListFolderRecursivelyWithPrefix(folder, prefix)
			assert.NoError(t, err)
			assertFiles(t, files, []string{"a/111", "a/b/222", "a/b/c/333"})
		}
	})

	t.Run("list all files for empty prefix", func(t *testing.T) {
		folder := memory.NewFolder("memory/", memory.NewStorage())
		_ = folder.PutObject("000", &bytes.Buffer{})
		_ = folder.PutObject("a/111", &bytes.Buffer{})
		_ = folder.PutObject("a/b/222", &bytes.Buffer{})
		_ = folder.PutObject("b/333", &bytes.Buffer{})

		for _, prefix := range []string{"", "/"} {
			files, err := storage.ListFolderRecursivelyWithPrefix(folder, prefix)
			assert.NoError(t, err)
			assertFiles(t, files, []string{"000", "a/111", "a/b/222", "b/333"})
		}
	})

	t.Run("dont list files and dirs with names starting with prefix", func(t *testing.T) {
		folder := memory.NewFolder("memory/", memory.NewStorage())
		_ = folder.PutObject("a_waste1", &bytes.Buffer{})
		_ = folder.PutObject("a/111", &bytes.Buffer{})
		_ = folder.PutObject("a/b/222", &bytes.Buffer{})
		_ = folder.PutObject("a_waste2/333", &bytes.Buffer{})

		files, err := storage.ListFolderRecursivelyWithPrefix(folder, "a")
		assert.NoError(t, err)
		assertFiles(t, files, []string{"a/111", "a/b/222"})
	})
}
