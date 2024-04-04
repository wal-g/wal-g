package multistorage

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/internal/multistorage/stats"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestSetPolicies(t *testing.T) {
	t.Run("do nothing if folder is not multistorage", func(t *testing.T) {
		folder := memory.NewFolder("test/", memory.NewKVS())
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
		collectorMock := folder.statsCollector.(*stats.MockCollector)
		newAliveStorage := "s3"
		collectorMock.EXPECT().AllAliveStorages().Return([]string{newAliveStorage}, nil).AnyTimes()
		collectorMock.EXPECT().FirstAliveStorage().Return(&newAliveStorage, nil).AnyTimes()
		collectorMock.EXPECT().SpecificStorage(newAliveStorage).Return(true, nil).AnyTimes()
		return folder
	}

	newMockFolderWithNoAlive := func(t *testing.T, initialStorages ...string) Folder {
		folder := newTestFolder(t, initialStorages...)
		collectorMock := folder.statsCollector.(*stats.MockCollector)

		collectorMock.EXPECT().AllAliveStorages().Return(nil, nil).AnyTimes()
		collectorMock.EXPECT().FirstAliveStorage().Return(nil, nil).AnyTimes()
		collectorMock.EXPECT().SpecificStorage("s3").Return(false, nil).AnyTimes()

		return folder
	}

	for funcName, useStorageFunc := range useStorageFuncs {
		t.Run(funcName, func(t *testing.T) {
			t.Run("do nothing if folder is not multistorage", func(t *testing.T) {
				folder := memory.NewFolder("test/", memory.NewKVS())
				newFolder, err := useStorageFunc(folder)
				require.NoError(t, err)
				assert.Equal(t, folder, newFolder)
			})

			t.Run("changing storages does not affect source folder", func(t *testing.T) {
				folder := newMockFolder(t, "s1", "s2")

				newFolder, err := useStorageFunc(folder)
				require.NoError(t, err)

				assert.Len(t, folder.usedFolders, 2)
				assert.Equal(t, "s1", folder.usedFolders[0].StorageName)
				assert.Equal(t, "s2", folder.usedFolders[1].StorageName)
				assert.Equal(t, folder.configuredRootFolders, newFolder.(Folder).configuredRootFolders)

				assert.Len(t, newFolder.(Folder).usedFolders, 1)
			})

			t.Run("change directory in new storages", func(t *testing.T) {
				folder := newMockFolder(t, "s1", "s2").GetSubFolder("a/b/c")

				newFolder, err := useStorageFunc(folder)
				require.NoError(t, err)

				newUsedFolders := newFolder.(Folder).usedFolders
				newConfiguredRootFolders := newFolder.(Folder).configuredRootFolders

				assert.Len(t, newUsedFolders, 1)
				assert.Equal(t, "s3/a/b/c/", newUsedFolders[0].GetPath())
				for sName, root := range newConfiguredRootFolders {
					assert.Equal(t, root.GetPath(), sName+"/")
				}
				assert.Equal(t, "a/b/c/", newFolder.GetPath())
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
		collectorMock := folder.statsCollector.(*stats.MockCollector)
		collectorMock.EXPECT().SpecificStorage(gomock.Any()).Times(0)

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
		folder := memory.NewFolder("test/", memory.NewKVS())
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
		folder := memory.NewFolder("test/", memory.NewKVS())
		err := EnsureSingleStorageIsUsed(folder)
		require.NoError(t, err)
	})
}

func newTestFolder(t *testing.T, usedStorages ...string) Folder {
	mockCtrl := gomock.NewController(t)
	t.Cleanup(mockCtrl.Finish)
	statsCollectorMock := stats.NewMockCollector(mockCtrl)
	statsCollectorMock.EXPECT().ReportOperationResult(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	memFolders := map[string]storage.Folder{
		"s1": memory.NewFolder("s1/", memory.NewKVS()),
		"s2": memory.NewFolder("s2/", memory.NewKVS()),
		"s3": memory.NewFolder("s3/", memory.NewKVS()),
	}
	folder := NewFolder(memFolders, statsCollectorMock).(Folder)
	for _, us := range usedStorages {
		folder.usedFolders = append(folder.usedFolders, NamedFolder{
			Folder:      memFolders[us],
			StorageName: us,
		})
	}

	return folder
}
