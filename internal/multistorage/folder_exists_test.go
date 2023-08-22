package multistorage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/pkg/storages/memory"
)

func TestExists(t *testing.T) {
	t.Run("check folder implementation and provide default name if it is not multistorage", func(t *testing.T) {
		singleStorageFolder := memory.NewFolder("/test", memory.NewStorage())
		_ = singleStorageFolder.PutObject("a/b/c", &bytes.Buffer{})

		exists, storage, err := Exists(singleStorageFolder, "a/b/c")
		require.NoError(t, err)
		assert.Equal(t, "default", storage)
		assert.True(t, exists)

		exists, storage, err = Exists(singleStorageFolder, "1/2/3")
		require.NoError(t, err)
		assert.Equal(t, "default", storage)
		assert.False(t, exists)
	})

	t.Run("require at least one storage for first storage policy", func(t *testing.T) {
		folder := newTestFolder(t)
		folder.policies.Exists = policies.ExistsPolicyFirst

		_, _, err := Exists(folder, "kek")
		assert.ErrorIs(t, err, ErrNoUsedStorages)
	})

	t.Run("exists in first storage", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2")
		folder.policies.Exists = policies.ExistsPolicyFirst

		_ = folder.storages[0].PutObject("aaa", &bytes.Buffer{})
		_ = folder.storages[1].PutObject("aaa", &bytes.Buffer{})
		_ = folder.storages[1].PutObject("bbb", &bytes.Buffer{})

		exists, storage, err := Exists(folder, "aaa")
		require.NoError(t, err)
		assert.Equal(t, "s1", storage)
		assert.True(t, exists)

		exists, storage, err = Exists(folder, "bbb")
		require.NoError(t, err)
		assert.Equal(t, "s1", storage)
		assert.False(t, exists)
	})

	t.Run("exists in any storage", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2", "s3")
		folder.policies.Exists = policies.ExistsPolicyAny

		_ = folder.storages[0].PutObject("aaa", &bytes.Buffer{})
		_ = folder.storages[1].PutObject("bbb", &bytes.Buffer{})
		_ = folder.storages[1].PutObject("ccc", &bytes.Buffer{})
		_ = folder.storages[2].PutObject("ccc", &bytes.Buffer{})

		exists, storage, err := Exists(folder, "aaa")
		require.NoError(t, err)
		assert.Equal(t, "s1", storage)
		assert.True(t, exists)

		exists, storage, err = Exists(folder, "bbb")
		require.NoError(t, err)
		assert.Equal(t, "s2", storage)
		assert.True(t, exists)

		exists, storage, err = Exists(folder, "ccc")
		require.NoError(t, err)
		assert.Equal(t, "s2", storage)
		assert.True(t, exists)
	})

	t.Run("exists in all storages", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2", "s3")
		folder.policies.Exists = policies.ExistsPolicyAll

		_ = folder.storages[0].PutObject("aaa", &bytes.Buffer{})
		_ = folder.storages[0].PutObject("ccc", &bytes.Buffer{})

		_ = folder.storages[1].PutObject("bbb", &bytes.Buffer{})
		_ = folder.storages[1].PutObject("ccc", &bytes.Buffer{})

		_ = folder.storages[2].PutObject("bbb", &bytes.Buffer{})
		_ = folder.storages[2].PutObject("ccc", &bytes.Buffer{})

		exists, storage, err := Exists(folder, "aaa")
		require.NoError(t, err)
		assert.Equal(t, "s2", storage)
		assert.False(t, exists)

		exists, storage, err = Exists(folder, "bbb")
		require.NoError(t, err)
		assert.Equal(t, "s1", storage)
		assert.False(t, exists)

		exists, storage, err = Exists(folder, "ccc")
		require.NoError(t, err)
		assert.Equal(t, "all", storage)
		assert.True(t, exists)
	})
}
