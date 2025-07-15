package multistorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
)

// TODO: Unit tests: check Folder.statsCollector.ReportOperationResult calls
func TestGetSubFolder(t *testing.T) {
	t.Run("change path in all storages regardless of policies", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2")
		folder.policies = policies.TakeFirstStorage

		subf := folder.GetSubFolder("a")
		aSubFolder, ok := subf.(Folder)
		assert.True(t, ok)
		assert.Equal(t, "a/", aSubFolder.GetPath())
		assert.Len(t, aSubFolder.usedFolders, 2)
		assert.Equal(t, "s1/a/", aSubFolder.usedFolders[0].GetPath())
		assert.Equal(t, "s2/a/", aSubFolder.usedFolders[1].GetPath())

		subf = aSubFolder.GetSubFolder("b")
		bSubFolder, ok := subf.(Folder)
		assert.True(t, ok)
		assert.Equal(t, "a/b/", bSubFolder.GetPath())
		assert.Len(t, bSubFolder.usedFolders, 2)
		assert.Equal(t, "s1/a/b/", bSubFolder.usedFolders[0].GetPath())
		assert.Equal(t, "s2/a/b/", bSubFolder.usedFolders[1].GetPath())
	})

	t.Run("copies stats collector storages and policies to subfolders", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2")
		folder.policies = policies.UniteAllStorages

		subf := folder.GetSubFolder("a")
		aSubFolder, ok := subf.(Folder)
		assert.True(t, ok)
		assert.Equal(t, policies.UniteAllStorages, aSubFolder.policies)
		assert.Len(t, aSubFolder.usedFolders, 2)
		assert.Equal(t, "s1", aSubFolder.usedFolders[0].StorageName)
		assert.Equal(t, "s2", aSubFolder.usedFolders[1].StorageName)
		assert.Equal(t, folder.statsCollector, aSubFolder.statsCollector)
	})
}
