package multistorage

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
)

func TestDeleteObjects(t *testing.T) {
	t.Run("require at least one storage for first storage policy", func(t *testing.T) {
		folder := newTestFolder(t)
		folder.policies.Delete = policies.DeletePolicyFirst

		err := folder.DeleteObjects([]string{"a/b/c/file"})
		assert.ErrorIs(t, err, ErrNoUsedStorages)
	})

	t.Run("delete objects from first storage", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2")
		folder.policies.Delete = policies.DeletePolicyFirst

		_ = folder.storages[0].PutObject("a/b/c/file1", &bytes.Buffer{})
		_ = folder.storages[0].PutObject("a/b/c/file2", &bytes.Buffer{})
		_ = folder.storages[1].PutObject("a/b/c/file1", &bytes.Buffer{})
		_ = folder.storages[1].PutObject("a/b/c/file2", &bytes.Buffer{})

		err := folder.DeleteObjects([]string{"a/b/c/file1", "a/b/c/file2"})
		require.NoError(t, err)

		exists, err := folder.storages[0].Exists("a/b/c/file1")
		assert.False(t, exists)
		exists, err = folder.storages[0].Exists("a/b/c/file2")
		assert.False(t, exists)

		exists, err = folder.storages[1].Exists("a/b/c/file1")
		assert.True(t, exists)
		exists, err = folder.storages[1].Exists("a/b/c/file2")
		assert.True(t, exists)
	})

	t.Run("delete objects from all storages", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2")
		folder.policies.Delete = policies.DeletePolicyAll

		_ = folder.storages[0].PutObject("a/b/c/file1", &bytes.Buffer{})
		_ = folder.storages[0].PutObject("a/b/c/file2", &bytes.Buffer{})
		_ = folder.storages[1].PutObject("a/b/c/file1", &bytes.Buffer{})
		_ = folder.storages[1].PutObject("a/b/c/file2", &bytes.Buffer{})

		err := folder.DeleteObjects([]string{"a/b/c/file1", "a/b/c/file2"})
		require.NoError(t, err)

		for storageIdx := 0; storageIdx < 2; storageIdx++ {
			for _, file := range []string{"a/b/c/file1", "a/b/c/file2"} {
				exists, err := folder.storages[storageIdx].Exists(file)
				require.NoError(t, err)
				assert.False(t, exists)
			}
		}
	})

	t.Run("dont throw error if there is no such objects", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2")
		folder.policies.Delete = policies.DeletePolicyAll

		_ = folder.storages[0].PutObject("a/b/c/file1", &bytes.Buffer{})

		err := folder.DeleteObjects([]string{"a/b/c/file1", "a/b/c/file2"})
		require.NoError(t, err)

		for storageIdx := 0; storageIdx < 2; storageIdx++ {
			for _, file := range []string{"a/b/c/file1", "a/b/c/file2"} {
				exists, err := folder.storages[storageIdx].Exists(file)
				require.NoError(t, err)
				assert.False(t, exists)
			}
		}
	})
}
