package multistorage

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/multistorage/cache"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestSetPolicies(t *testing.T) {
	t.Run("do nothing if folder is not multistorage", func(t *testing.T) {
		folder := memory.NewFolder("test/", memory.NewStorage())
		newFolder, ok := SetPolicies(folder, policies.TakeFirstStorage).(*memory.Folder)
		assert.True(t, ok)
		assert.Equal(t, folder, newFolder)
	})

	t.Run("changing policies does not affect source folder", func(t *testing.T) {
		folder := newTestFolder(t)
		folder.policies = policies.TakeFirstStorage

		newFolder := SetPolicies(folder, policies.UniteAllStorages)

		assert.Equal(t, policies.TakeFirstStorage, folder.policies)
		assert.Equal(t, policies.UniteAllStorages, newFolder.(Folder).policies)
	})
}

func TestUseDifferentStorages(t *testing.T) {
	useStorageFuncs := map[string]func(folder storage.Folder) (storage.Folder, error){
		"use_all_alive":   UseAllAliveStorages,
		"use_first_alive": UseFirstAliveStorage,
		"use_specific": func(folder storage.Folder) (storage.Folder, error) {
			return UseSpecificStorage("s3", folder)
		},
	}

	newMockFolder := func(t *testing.T, initialStorages ...string) Folder {
		folder := newTestFolder(t, initialStorages...)
		cacheMock := folder.cache.(*cache.MockStatusCache)
		s3Folder := cache.NamedFolder{
			Name:   "s3",
			Folder: memory.NewFolder("", memory.NewStorage()),
		}

		cacheMock.EXPECT().AllAliveStorages().Return([]cache.NamedFolder{s3Folder}, nil).AnyTimes()
		cacheMock.EXPECT().FirstAliveStorage().Return(&s3Folder, nil).AnyTimes()
		cacheMock.EXPECT().SpecificStorage("s3").Return(&s3Folder, nil).AnyTimes()

		return folder
	}

	newMockFolderWithNoAlive := func(t *testing.T, initialStorages ...string) Folder {
		folder := newTestFolder(t, initialStorages...)
		cacheMock := folder.cache.(*cache.MockStatusCache)

		cacheMock.EXPECT().AllAliveStorages().Return([]cache.NamedFolder{}, nil).AnyTimes()
		cacheMock.EXPECT().FirstAliveStorage().Return(nil, nil).AnyTimes()
		cacheMock.EXPECT().SpecificStorage("s3").Return(nil, nil).AnyTimes()

		return folder
	}

	for funcName, useStorageFunc := range useStorageFuncs {
		t.Run(funcName, func(t *testing.T) {
			t.Run("do nothing if folder is not multistorage", func(t *testing.T) {
				folder := memory.NewFolder("test/", memory.NewStorage())
				newFolder, err := useStorageFunc(folder)
				require.NoError(t, err)
				assert.Equal(t, folder, newFolder)
			})

			t.Run("changing storages does not affect source folder", func(t *testing.T) {
				folder := newMockFolder(t, "s1", "s2")

				newFolder, err := useStorageFunc(folder)
				require.NoError(t, err)

				assert.Len(t, folder.storages, 2)
				assert.Equal(t, "s1", folder.storages[0].Name)
				assert.Equal(t, "s2", folder.storages[1].Name)

				assert.Len(t, newFolder.(Folder).storages, 1)
			})

			t.Run("change directory in new storages", func(t *testing.T) {
				folder := newMockFolder(t, "s1", "s2").GetSubFolder("a/b/c")

				newFolder, err := useStorageFunc(folder)
				require.NoError(t, err)

				for _, st := range newFolder.(Folder).storages {
					assert.Equal(t, "a/b/c/", st.GetPath())
				}
			})

			t.Run("throw an error if no storages are alive", func(t *testing.T) {
				folder := newMockFolderWithNoAlive(t, "s1", "s2", "s3")
				_, err := useStorageFunc(folder)
				require.ErrorIs(t, err, ErrNoAliveStorages)
			})
		})
	}
}

func TestUseSpecificStorage(t *testing.T) {
	t.Run("do nothing if this storage is already used", func(t *testing.T) {
		folder := newTestFolder(t, "s2")
		cacheMock := folder.cache.(*cache.MockStatusCache)
		cacheMock.EXPECT().SpecificStorage(gomock.Any()).Times(0)

		newFolder, err := UseSpecificStorage("s2", folder)
		require.NoError(t, err, ErrNoAliveStorages)
		assert.Equal(t, folder, newFolder)
	})
}

func TestUsedStorages(t *testing.T) {
	t.Run("provide storages", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2", "s3")
		used := UsedStorages(folder)
		assert.Equal(t, []string{"s1", "s2", "s3"}, used)
	})

	t.Run("provide default name if folder is not multistorage", func(t *testing.T) {
		folder := memory.NewFolder("test/", memory.NewStorage())
		used := UsedStorages(folder)
		assert.Equal(t, []string{"default"}, used)
	})
}

func TestEnsureSingleStorageIsUsed(t *testing.T) {
	t.Run("no error if storage is single", func(t *testing.T) {
		folder := newTestFolder(t, "s1")
		err := EnsureSingleStorageIsUsed(folder)
		require.NoError(t, err)
	})

	t.Run("error if two storages are used", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2")
		err := EnsureSingleStorageIsUsed(folder)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected to use a single storage")
	})

	t.Run("no error if folder is not multistorage", func(t *testing.T) {
		folder := memory.NewFolder("test/", memory.NewStorage())
		err := EnsureSingleStorageIsUsed(folder)
		require.NoError(t, err)
	})
}

func newTestFolder(t *testing.T, storageNames ...string) Folder {
	mockCtrl := gomock.NewController(t)
	t.Cleanup(mockCtrl.Finish)
	cacheMock := cache.NewMockStatusCache(mockCtrl)

	var memStorages []cache.NamedFolder
	for _, name := range storageNames {
		memStorages = append(memStorages, cache.NamedFolder{
			Name:   name,
			Folder: memory.NewFolder("", memory.NewStorage()),
		})
	}

	folder := NewFolder(cacheMock).(Folder)
	folder.storages = memStorages

	return folder
}
