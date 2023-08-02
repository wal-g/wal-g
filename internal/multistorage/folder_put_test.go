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

func TestPutObject(t *testing.T) {
	t.Run("require at least one storage for first storage policy", func(t *testing.T) {
		folder := newTestFolder(t)
		folder.policies.Put = policies.PutPolicyFirst

		err := folder.PutObject("a/b/c/file", bytes.NewBufferString("abc"))
		assert.ErrorIs(t, err, ErrNoUsedStorages)
	})

	t.Run("put to first storage", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2")
		folder.policies.Put = policies.PutPolicyFirst

		err := folder.PutObject("a/b/c/file", bytes.NewBufferString("abc"))
		require.NoError(t, err)

		reader, err := folder.storages[0].ReadObject("a/b/c/file")
		require.NoError(t, err)
		content, _ := io.ReadAll(reader)
		assert.Equal(t, "abc", string(content))

		_, err = folder.storages[1].ReadObject("a/b/c/file")
		assert.ErrorAs(t, err, &storage.ObjectNotFoundError{})
	})

	t.Run("update first found object", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2", "s3")
		folder.policies.Put = policies.PutPolicyUpdateFirstFound

		_ = folder.storages[1].PutObject("a/b/c/file", bytes.NewBufferString("old_content"))
		_ = folder.storages[2].PutObject("a/b/c/file", bytes.NewBufferString("old_content"))

		err := folder.PutObject("a/b/c/file", bytes.NewBufferString("new_content"))
		require.NoError(t, err)

		_, err = folder.storages[0].ReadObject("a/b/c/file")
		assert.ErrorAs(t, err, &storage.ObjectNotFoundError{})

		reader, err := folder.storages[1].ReadObject("a/b/c/file")
		require.NoError(t, err)
		content, _ := io.ReadAll(reader)
		assert.Equal(t, "new_content", string(content))

		reader, err = folder.storages[2].ReadObject("a/b/c/file")
		require.NoError(t, err)
		content, _ = io.ReadAll(reader)
		assert.Equal(t, "old_content", string(content))
	})

	t.Run("put to first storage if no existing objects are found", func(t *testing.T) {
		pols := []policies.PutPolicy{
			policies.PutPolicyUpdateFirstFound,
			policies.PutPolicyUpdateAllFound,
		}
		for _, pol := range pols {
			folder := newTestFolder(t, "s1", "s2")
			folder.policies.Put = pol

			err := folder.PutObject("a/b/c/file", bytes.NewBufferString("abc"))
			require.NoError(t, err)

			reader, err := folder.storages[0].ReadObject("a/b/c/file")
			require.NoError(t, err)
			content, _ := io.ReadAll(reader)
			assert.Equal(t, "abc", string(content))

			_, err = folder.storages[1].ReadObject("a/b/c/file")
			assert.ErrorAs(t, err, &storage.ObjectNotFoundError{})
		}
	})

	t.Run("put to all storages", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2")
		folder.policies.Put = policies.PutPolicyAll

		err := folder.PutObject("a/b/c/file", bytes.NewBufferString("abc"))
		require.NoError(t, err)

		for i := 0; i < 2; i++ {
			reader, err := folder.storages[i].ReadObject("a/b/c/file")
			require.NoError(t, err)
			content, _ := io.ReadAll(reader)
			assert.Equal(t, "abc", string(content))
		}
	})

	t.Run("update all found objects", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2", "s3")
		folder.policies.Put = policies.PutPolicyUpdateAllFound

		_ = folder.storages[1].PutObject("a/b/c/file", bytes.NewBufferString("old_content"))
		_ = folder.storages[2].PutObject("a/b/c/file", bytes.NewBufferString("old_content"))

		err := folder.PutObject("a/b/c/file", bytes.NewBufferString("new_content"))
		require.NoError(t, err)

		_, err = folder.storages[0].ReadObject("a/b/c/file")
		assert.ErrorAs(t, err, &storage.ObjectNotFoundError{})

		reader, err := folder.storages[1].ReadObject("a/b/c/file")
		require.NoError(t, err)
		content, _ := io.ReadAll(reader)
		assert.Equal(t, "new_content", string(content))

		reader, err = folder.storages[2].ReadObject("a/b/c/file")
		require.NoError(t, err)
		content, _ = io.ReadAll(reader)
		assert.Equal(t, "new_content", string(content))
	})
}
