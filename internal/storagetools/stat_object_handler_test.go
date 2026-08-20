package storagetools

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	memmock "github.com/wal-g/wal-g/pkg/storages/memory/mock"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestStatObject(t *testing.T) {
	t.Run("print metadata of an existing object", func(t *testing.T) {
		folder := memory.NewFolder("test/", memory.NewKVS())
		require.NoError(t, folder.PutObject(t.Context(), "a/b/file", bytes.NewBufferString("12345")))

		var output bytes.Buffer
		err := statObject(t.Context(), "a/b/file", folder, &output)
		require.NoError(t, err)

		assert.Contains(t, output.String(), "a/b/file")
		assert.Contains(t, output.String(), "5")
		assert.Contains(t, output.String(), string(Object))
	})

	t.Run("return not found error for a missing object", func(t *testing.T) {
		folder := memory.NewFolder("test/", memory.NewKVS())

		var output bytes.Buffer
		err := statObject(t.Context(), "nonexistent", folder, &output)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
		assert.Empty(t, output.String())
	})

	t.Run("do not list the folder", func(t *testing.T) {
		memFolder := memory.NewFolder("test/", memory.NewKVS())
		require.NoError(t, memFolder.PutObject(t.Context(), "a/b/file", bytes.NewBufferString("12345")))

		folder := memmock.NewFolder(memFolder)
		folder.ListFolderMock = func(context.Context) ([]storage.Object, []storage.Folder, error) {
			t.Error("stat must not list the folder")
			return nil, nil, nil
		}

		var output bytes.Buffer
		err := statObject(t.Context(), "a/b/file", folder, &output)
		require.NoError(t, err)
	})
}
