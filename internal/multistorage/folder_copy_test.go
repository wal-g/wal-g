package multistorage

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestCopyObject(t *testing.T) {
	t.Run("require at least one storage for first storage policy", func(t *testing.T) {
		folder := newTestFolder(t)
		folder.policies.Copy = policies.CopyPolicyFirst

		err := folder.CopyObject("a/b/c/file", "file2")
		assert.ErrorIs(t, err, ErrNoUsedStorages)
	})

	t.Run("copy in first storage", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2")
		folder.policies.Copy = policies.CopyPolicyFirst

		_ = folder.storages[0].PutObject("a/b/c/file1", bytes.NewBufferString("abc"))
		_ = folder.storages[1].PutObject("a/b/c/file1", bytes.NewBufferString("abc"))

		err := folder.CopyObject("a/b/c/file1", "file2")
		require.NoError(t, err)

		for _, file := range []string{"a/b/c/file1", "file2"} {
			reader, err := folder.storages[0].ReadObject(file)
			require.NoError(t, err)
			content, _ := io.ReadAll(reader)
			assert.Equal(t, "abc", string(content))
		}

		_, err = folder.storages[1].ReadObject("file2")
		assert.ErrorAs(t, err, &storage.ObjectNotFoundError{})
	})

	t.Run("copy in all storages", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2")
		folder.policies.Copy = policies.CopyPolicyAll

		_ = folder.storages[0].PutObject("a/b/c/file1", bytes.NewBufferString("abc"))
		_ = folder.storages[1].PutObject("a/b/c/file1", bytes.NewBufferString("abc"))

		err := folder.CopyObject("a/b/c/file1", "file2")
		require.NoError(t, err)

		for storageIdx := 0; storageIdx < 2; storageIdx++ {
			for _, file := range []string{"a/b/c/file1", "file2"} {
				reader, err := folder.storages[storageIdx].ReadObject(file)
				require.NoError(t, err)
				content, _ := io.ReadAll(reader)
				assert.Equal(t, "abc", string(content))
			}
		}
	})

	t.Run("throw error if all storages dont have the object", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2")
		folder.policies.Copy = policies.CopyPolicyAll

		err := folder.CopyObject("a/b/c/file1", "file2")
		require.ErrorAs(t, err, &storage.ObjectNotFoundError{})
	})

	t.Run("dont throw error if any of storages has the object", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2")
		folder.policies.Copy = policies.CopyPolicyAll

		_ = folder.storages[1].PutObject("a/b/c/file1", bytes.NewBufferString("abc"))

		err := folder.CopyObject("a/b/c/file1", "file2")
		require.NoError(t, err)
	})
}
