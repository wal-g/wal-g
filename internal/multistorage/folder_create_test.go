package multistorage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/fs"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestCreateObjectSelectedStorageOnly(t *testing.T) {
	primary, fallback := fs.NewFolder(t.TempDir(), ""), fs.NewFolder(t.TempDir(), "")
	folder := Folder{
		configuredRootFolders: map[string]storage.Folder{"primary": primary, "fallback": fallback},
		usedFolders:           []NamedFolder{{Folder: fallback, StorageName: "fallback"}},
	}
	// No statistics collector is needed: a name conflict must not mark a storage
	// unhealthy and must not retry on the other configured destination.
	subfolder := folder.GetSubFolder("cluster/")
	require.NoError(t, storage.CreateObject(t.Context(), subfolder, "reservation", nil))
	require.ErrorIs(t, storage.CreateObject(t.Context(), subfolder, "reservation", nil), storage.ErrObjectAlreadyExists)
	exists, err := primary.Exists(t.Context(), "cluster/reservation")
	require.NoError(t, err)
	require.False(t, exists)
	exists, err = fallback.Exists(t.Context(), "cluster/reservation")
	require.NoError(t, err)
	require.True(t, exists)
	folder.usedFolders = append(folder.usedFolders, NamedFolder{Folder: primary, StorageName: "primary"})
	require.Error(t, folder.CreateObject(t.Context(), "other", nil))
	folder.usedFolders = nil
	require.Error(t, folder.CreateObject(t.Context(), "other", nil))
}

func TestCreateObjectUnsupportedStorage(t *testing.T) {
	folder := memory.NewFolder("", memory.NewKVS())
	require.ErrorContains(t, storage.CreateObject(t.Context(), folder, "reservation", nil), "does not support atomic")
	exists, err := folder.Exists(t.Context(), "reservation")
	require.NoError(t, err)
	require.False(t, exists)
}
