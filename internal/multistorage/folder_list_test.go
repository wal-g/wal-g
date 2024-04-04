package multistorage

import (
	"bytes"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/internal/multistorage/stats"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// TODO: Unit tests: check Folder.statsCollector.ReportOperationResult calls
func TestListFolder(t *testing.T) {
	t.Run("require at least one storage for first storage policy", func(t *testing.T) {
		folder := newTestFolder(t)
		folder.policies.List = policies.ListPolicyFirst

		_, _, err := folder.ListFolder()
		assert.ErrorIs(t, err, ErrNoUsedStorages)
	})

	assertListedObjects := func(t *testing.T, got []storage.Object, want map[listedObj]bool) {
		assert.Equal(t, len(want), len(got))
		for _, obj := range got {
			multiObj, ok := obj.(multiObject)
			assert.True(t, ok)
			gotObj := listedObj{obj.GetName(), multiObj.GetStorage()}
			delete(want, gotObj)
		}
		assert.Empty(t, want)
	}

	assertListedSubFolders := func(t *testing.T, testFolder Folder, got []storage.Folder, want map[string]bool) {
		assert.Equal(t, len(want), len(got))
		for _, subf := range got {
			multiFolder, ok := subf.(Folder)
			assert.True(t, ok)
			assert.Equal(t, testFolder.policies, multiFolder.policies)
			assert.Equal(t, testFolder.statsCollector, multiFolder.statsCollector)
			assert.Equal(t, len(testFolder.usedFolders), len(multiFolder.usedFolders))
			for _, st := range multiFolder.usedFolders {
				rootf := testFolder.configuredRootFolders[st.StorageName]
				assert.Equal(t, path.Join(rootf.GetPath(), subf.GetPath())+"/", st.GetPath())
			}
			gotPath := subf.GetPath()
			delete(want, gotPath)
		}
		assert.Empty(t, want)
	}

	t.Run("list first storage", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2")
		folder.policies.List = policies.ListPolicyFirst

		_ = folder.usedFolders[0].PutObject("aaa", &bytes.Buffer{})
		_ = folder.usedFolders[0].PutObject("bbb", &bytes.Buffer{})
		_ = folder.usedFolders[0].PutObject("aaa/123", &bytes.Buffer{})
		_ = folder.usedFolders[0].PutObject("ccc/123", &bytes.Buffer{})
		_ = folder.usedFolders[1].PutObject("ddd", &bytes.Buffer{})

		objects, subFolders, err := folder.ListFolder()
		require.NoError(t, err)
		assertListedObjects(t, objects, map[listedObj]bool{
			{"aaa", "s1"}: true,
			{"bbb", "s1"}: true,
		})
		assertListedSubFolders(t, folder, subFolders, map[string]bool{
			"aaa/": true,
			"ccc/": true,
		})
	})

	t.Run("list unique files and folders from all storages", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2", "s3")
		folder.policies.List = policies.ListPolicyFoundFirst

		_ = folder.usedFolders[0].PutObject("aaa", &bytes.Buffer{})
		_ = folder.usedFolders[0].PutObject("aaa/123", &bytes.Buffer{})

		_ = folder.usedFolders[1].PutObject("aaa", &bytes.Buffer{})
		_ = folder.usedFolders[1].PutObject("bbb", &bytes.Buffer{})
		_ = folder.usedFolders[1].PutObject("aaa/123", &bytes.Buffer{})
		_ = folder.usedFolders[1].PutObject("bbb/123", &bytes.Buffer{})

		_ = folder.usedFolders[2].PutObject("aaa", &bytes.Buffer{})
		_ = folder.usedFolders[2].PutObject("bbb", &bytes.Buffer{})
		_ = folder.usedFolders[2].PutObject("ccc", &bytes.Buffer{})
		_ = folder.usedFolders[2].PutObject("aaa/123", &bytes.Buffer{})
		_ = folder.usedFolders[2].PutObject("bbb/123", &bytes.Buffer{})
		_ = folder.usedFolders[2].PutObject("ccc/123", &bytes.Buffer{})

		objects, subFolders, err := folder.ListFolder()
		require.NoError(t, err)
		assertListedObjects(t, objects, map[listedObj]bool{
			{"aaa", "s1"}: true,
			{"bbb", "s2"}: true,
			{"ccc", "s3"}: true,
		})
		assertListedSubFolders(t, folder, subFolders, map[string]bool{
			"aaa/": true,
			"bbb/": true,
			"ccc/": true,
		})
	})

	t.Run("list all files and folders from all storages", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2", "s3")
		folder.policies.List = policies.ListPolicyAll

		_ = folder.usedFolders[0].PutObject("aaa", &bytes.Buffer{})
		_ = folder.usedFolders[0].PutObject("aaa/123", &bytes.Buffer{})

		_ = folder.usedFolders[1].PutObject("aaa", &bytes.Buffer{})
		_ = folder.usedFolders[1].PutObject("bbb", &bytes.Buffer{})
		_ = folder.usedFolders[1].PutObject("aaa/123", &bytes.Buffer{})
		_ = folder.usedFolders[1].PutObject("bbb/123", &bytes.Buffer{})

		_ = folder.usedFolders[2].PutObject("aaa", &bytes.Buffer{})
		_ = folder.usedFolders[2].PutObject("bbb", &bytes.Buffer{})
		_ = folder.usedFolders[2].PutObject("ccc", &bytes.Buffer{})
		_ = folder.usedFolders[2].PutObject("aaa/123", &bytes.Buffer{})
		_ = folder.usedFolders[2].PutObject("bbb/123", &bytes.Buffer{})
		_ = folder.usedFolders[2].PutObject("ccc/123", &bytes.Buffer{})

		objects, subFolders, err := folder.ListFolder()
		require.NoError(t, err)
		assertListedObjects(t, objects, map[listedObj]bool{
			{"aaa", "s1"}: true,
			{"aaa", "s2"}: true,
			{"aaa", "s3"}: true,
			{"bbb", "s2"}: true,
			{"bbb", "s3"}: true,
			{"ccc", "s3"}: true,
		})
		assertListedSubFolders(t, folder, subFolders, map[string]bool{
			"aaa/": true,
			"bbb/": true,
			"ccc/": true,
		})
	})

	t.Run("list files with relative paths and subfolders with absolute paths", func(t *testing.T) {
		folder := newTestFolder(t, "s1")
		folder.policies.List = policies.ListPolicyFirst

		_ = folder.usedFolders[0].PutObject("sub/aaa", &bytes.Buffer{})
		_ = folder.usedFolders[0].PutObject("sub/sub2/bbb", &bytes.Buffer{})
		_ = folder.usedFolders[0].PutObject("sub/sub2/ccc", &bytes.Buffer{})

		_, subFolders, err := folder.ListFolder()
		require.NoError(t, err)
		require.Len(t, subFolders, 1)
		subFolder := subFolders[0]
		objects, subFolders, err := subFolder.ListFolder()
		assertListedObjects(t, objects, map[listedObj]bool{
			{"aaa", "s1"}: true,
		})
		assertListedSubFolders(t, folder, subFolders, map[string]bool{
			"sub/sub2/": true,
		})
	})

	getSubFolder := func(t *testing.T, subFolders []storage.Folder, name string) storage.Folder {
		for _, subf := range subFolders {
			if subf.GetPath() == name {
				return subf
			}
		}
		t.Fatalf("no %q subfolder", name)
		return nil
	}

	t.Run("list subfolders", func(t *testing.T) {
		rootFolder := newTestFolder(t, "s1", "s2", "s3")
		rootFolder.policies.List = policies.ListPolicyAll

		_ = rootFolder.usedFolders[0].PutObject("a/b/c/file", &bytes.Buffer{})
		_ = rootFolder.usedFolders[0].PutObject("a/file", &bytes.Buffer{})
		_ = rootFolder.usedFolders[0].PutObject("a1/file1", &bytes.Buffer{})

		_ = rootFolder.usedFolders[1].PutObject("a/b/c/file", &bytes.Buffer{})
		_ = rootFolder.usedFolders[1].PutObject("a/b/file", &bytes.Buffer{})
		_ = rootFolder.usedFolders[1].PutObject("a/b2/file2", &bytes.Buffer{})

		_ = rootFolder.usedFolders[2].PutObject("a/b/c/file", &bytes.Buffer{})
		_ = rootFolder.usedFolders[2].PutObject("a/b/c3/file3", &bytes.Buffer{})

		objects, subFolders, err := rootFolder.ListFolder()
		require.NoError(t, err)
		assertListedObjects(t, objects, map[listedObj]bool{})
		assertListedSubFolders(t, rootFolder, subFolders, map[string]bool{
			"a/":  true,
			"a1/": true,
		})
		aSubFolder := getSubFolder(t, subFolders, "a/")

		objects, subFolders, err = aSubFolder.ListFolder()
		require.NoError(t, err)
		assertListedObjects(t, objects, map[listedObj]bool{
			{"file", "s1"}: true,
		})
		assertListedSubFolders(t, rootFolder, subFolders, map[string]bool{
			"a/b/":  true,
			"a/b2/": true,
		})
		bSubFolder := getSubFolder(t, subFolders, "a/b/")

		objects, subFolders, err = bSubFolder.ListFolder()
		require.NoError(t, err)
		assertListedObjects(t, objects, map[listedObj]bool{
			{"file", "s2"}: true,
		})
		assertListedSubFolders(t, rootFolder, subFolders, map[string]bool{
			"a/b/c/":  true,
			"a/b/c3/": true,
		})
		cSubFolder := getSubFolder(t, subFolders, "a/b/c/")

		objects, subFolders, err = cSubFolder.ListFolder()
		require.NoError(t, err)
		assertListedObjects(t, objects, map[listedObj]bool{
			{"file", "s1"}: true,
			{"file", "s2"}: true,
			{"file", "s3"}: true,
		})
		assertListedSubFolders(t, rootFolder, subFolders, map[string]bool{})
	})

	t.Run("policies can be changed and returned back for subfolders", func(t *testing.T) {
		rootFolder := newTestFolder(t, "s1", "s2")
		rootFolder.policies.List = policies.ListPolicyAll

		_ = rootFolder.usedFolders[0].PutObject("file", &bytes.Buffer{})
		_ = rootFolder.usedFolders[0].PutObject("a/file", &bytes.Buffer{})
		_ = rootFolder.usedFolders[0].PutObject("a/b/file", &bytes.Buffer{})
		_ = rootFolder.usedFolders[1].PutObject("file", &bytes.Buffer{})
		_ = rootFolder.usedFolders[1].PutObject("a/file", &bytes.Buffer{})
		_ = rootFolder.usedFolders[1].PutObject("a/b/file", &bytes.Buffer{})

		objects, subFolders, err := rootFolder.ListFolder()
		require.NoError(t, err)
		assertListedObjects(t, objects, map[listedObj]bool{
			{"file", "s1"}: true,
			{"file", "s2"}: true,
		})
		aSubFolder := getSubFolder(t, subFolders, "a/")

		aSubFolder = SetPolicies(aSubFolder, policies.MergeAllStorages)

		objects, subFolders, err = aSubFolder.ListFolder()
		require.NoError(t, err)
		assertListedObjects(t, objects, map[listedObj]bool{
			{"file", "s1"}: true,
		})
		bSubFolder := getSubFolder(t, subFolders, "a/b/")

		bSubFolder = SetPolicies(bSubFolder, policies.UniteAllStorages)

		objects, subFolders, err = bSubFolder.ListFolder()
		require.NoError(t, err)
		assertListedObjects(t, objects, map[listedObj]bool{
			{"file", "s1"}: true,
			{"file", "s2"}: true,
		})
	})

	t.Run("storages can be changed and returned back for subfolders", func(t *testing.T) {
		rootFolder := newTestFolder(t, "s1", "s2")
		rootFolder.policies.List = policies.ListPolicyAll
		collectorMock := rootFolder.statsCollector.(*stats.MockCollector)

		_ = rootFolder.usedFolders[0].PutObject("file1", &bytes.Buffer{})
		_ = rootFolder.usedFolders[0].PutObject("a/file2", &bytes.Buffer{})
		_ = rootFolder.usedFolders[0].PutObject("a/b/file3", &bytes.Buffer{})
		_ = rootFolder.usedFolders[1].PutObject("file1", &bytes.Buffer{})
		_ = rootFolder.usedFolders[1].PutObject("a/file2", &bytes.Buffer{})
		_ = rootFolder.usedFolders[1].PutObject("a/b/file3", &bytes.Buffer{})

		objects, subFolders, err := rootFolder.ListFolder()
		require.NoError(t, err)
		assertListedObjects(t, objects, map[listedObj]bool{
			{"file1", "s1"}: true,
			{"file1", "s2"}: true,
		})
		aSubFolder := getSubFolder(t, subFolders, "a/")

		collectorMock.EXPECT().SpecificStorage("s1").Return(true, nil)
		aSubFolder, err = UseSpecificStorage("s1", aSubFolder)
		require.NoError(t, err)

		objects, subFolders, err = aSubFolder.ListFolder()
		require.NoError(t, err)
		assertListedObjects(t, objects, map[listedObj]bool{
			{"file2", "s1"}: true,
		})
		bSubFolder := getSubFolder(t, subFolders, "a/b/")

		collectorMock.EXPECT().AllAliveStorages().Return([]string{"s1", "s2", "s3"}, nil)
		bSubFolder, err = UseAllAliveStorages(bSubFolder)

		require.NoError(t, err)
		bSubFolder = SetPolicies(bSubFolder, policies.UniteAllStorages)

		objects, subFolders, err = bSubFolder.ListFolder()
		require.NoError(t, err)
		assertListedObjects(t, objects, map[listedObj]bool{
			{"file3", "s1"}: true,
			{"file3", "s2"}: true,
		})
	})
}

type listedObj struct {
	name    string
	storage string
}
