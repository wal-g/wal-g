package multistorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
)

func TestGetSubFolder(t *testing.T) {
	t.Run("change path in all storages regardless of policies", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2")
		folder.policies = policies.TakeFirstStorage

		subf := folder.GetSubFolder("a")
		aSubFolder, ok := subf.(Folder)
		assert.True(t, ok)
		assert.Equal(t, "a/", aSubFolder.GetPath())
		assert.Len(t, aSubFolder.storages, 2)
		assert.Equal(t, "test/a/", aSubFolder.storages[0].GetPath())
		assert.Equal(t, "test/a/", aSubFolder.storages[1].GetPath())

		subf = aSubFolder.GetSubFolder("b")
		bSubFolder, ok := subf.(Folder)
		assert.True(t, ok)
		assert.Equal(t, "a/b/", bSubFolder.GetPath())
		assert.Len(t, bSubFolder.storages, 2)
		assert.Equal(t, "test/a/b/", bSubFolder.storages[0].GetPath())
		assert.Equal(t, "test/a/b/", bSubFolder.storages[1].GetPath())
	})

	t.Run("copies cache storages and policies to subfolders", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2")
		folder.policies = policies.UniteAllStorages

		subf := folder.GetSubFolder("a")
		aSubFolder, ok := subf.(Folder)
		assert.True(t, ok)
		assert.Equal(t, policies.UniteAllStorages, aSubFolder.policies)
		assert.Len(t, aSubFolder.storages, 2)
		assert.Equal(t, "s1", aSubFolder.storages[0].Name)
		assert.Equal(t, "s2", aSubFolder.storages[1].Name)
		assert.Equal(t, folder.cache, aSubFolder.cache)
	})
}
