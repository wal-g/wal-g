package copy_test

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/config"
	copyutil "github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
)

func TestInfosWithNoObjects(t *testing.T) {
	require.NoError(t, copyutil.Infos(t.Context(), nil))
}

func TestInfosCopiesSelectedObjects(t *testing.T) {
	previousCompressionMethod := viper.GetString(config.CompressionMethodSetting)
	t.Cleanup(func() {
		viper.Set(config.CompressionMethodSetting, previousCompressionMethod)
	})
	viper.Set(config.CompressionMethodSetting, "none")
	from := testtools.CreateMockStorageFolderWithPermanentBackups(t)
	to := testtools.MakeDefaultInMemoryStorageFolder()
	objects, err := storage.ListFolderRecursively(t.Context(), from)
	require.NoError(t, err)
	infos := copyutil.BuildCopyingInfos(
		from,
		to,
		objects,
		func(storage.Object) bool { return true },
		copyutil.NoopRenameFunc,
		copyutil.NoopSourceTransformer,
	)

	require.NoError(t, copyutil.Infos(t.Context(), infos))
	for _, info := range infos {
		exists, err := to.Exists(t.Context(), info.SrcObj.GetName())
		require.NoError(t, err)
		require.True(t, exists)
	}
}

func TestBuildCopyingInfosFiltersObjects(t *testing.T) {
	from := testtools.CreateMockStorageFolder(t.Context())
	to := testtools.MakeDefaultInMemoryStorageFolder()
	objects, err := storage.ListFolderRecursively(t.Context(), from)
	require.NoError(t, err)

	none := copyutil.BuildCopyingInfos(
		from,
		to,
		objects,
		func(storage.Object) bool { return false },
		copyutil.NoopRenameFunc,
		copyutil.NoopSourceTransformer,
	)
	require.Empty(t, none)

	jsonOnly := func(object storage.Object) bool {
		return strings.HasSuffix(object.GetName(), ".json")
	}
	expected := 0
	for _, object := range objects {
		if jsonOnly(object) {
			expected++
		}
	}
	require.NotZero(t, expected)

	infos := copyutil.BuildCopyingInfos(
		from,
		to,
		objects,
		jsonOnly,
		copyutil.NoopRenameFunc,
		copyutil.NoopSourceTransformer,
	)
	require.Len(t, infos, expected)
	for _, info := range infos {
		require.True(t, jsonOnly(info.SrcObj))
	}
}
