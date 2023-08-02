package multistorage

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestReadObject(t *testing.T) {
	t.Run("check folder implementation and provide default name if it is not multistorage", func(t *testing.T) {
		singleStorageFolder := memory.NewFolder("/test", memory.NewStorage())
		_ = singleStorageFolder.PutObject("a/b/c", bytes.NewBufferString("abc"))

		reader, storageName, err := ReadObject(singleStorageFolder, "a/b/c")
		require.NoError(t, err)
		assert.Equal(t, DefaultStorage, storageName)
		content, _ := io.ReadAll(reader)
		assert.Equal(t, "abc", string(content))

		reader, storageName, err = ReadObject(singleStorageFolder, "1/2/3")
		require.Error(t, err)
		assert.Equal(t, DefaultStorage, storageName)
	})

	t.Run("require at least one storage for first storage policy", func(t *testing.T) {
		folder := newTestFolder(t)
		folder.policies.Read = policies.ReadPolicyFirst

		_, _, err := ReadObject(folder, "kek")
		assert.ErrorIs(t, err, ErrNoUsedStorages)
	})

	t.Run("read from first storage", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2")
		folder.policies.Read = policies.ReadPolicyFirst

		_ = folder.storages[0].PutObject("aaa", bytes.NewBufferString("abc"))
		_ = folder.storages[1].PutObject("aaa", bytes.NewBufferString("abc"))
		_ = folder.storages[1].PutObject("bbb", bytes.NewBufferString("abc"))

		reader, storageName, err := ReadObject(folder, "aaa")
		require.NoError(t, err)
		assert.Equal(t, "s1", storageName)
		content, _ := io.ReadAll(reader)
		assert.Equal(t, "abc", string(content))

		reader, storageName, err = ReadObject(folder, "bbb")
		require.Error(t, err)
		assert.ErrorAs(t, err, &storage.ObjectNotFoundError{})
		assert.Equal(t, "s1", storageName)
	})

	t.Run("read first found object", func(t *testing.T) {
		folder := newTestFolder(t, "s1", "s2", "s3")
		folder.policies.Read = policies.ReadPolicyFoundFirst

		_ = folder.storages[0].PutObject("aaa", bytes.NewBufferString("1"))

		_ = folder.storages[1].PutObject("aaa", bytes.NewBufferString("2"))
		_ = folder.storages[1].PutObject("bbb", bytes.NewBufferString("2"))

		_ = folder.storages[2].PutObject("aaa", bytes.NewBufferString("3"))
		_ = folder.storages[2].PutObject("bbb", bytes.NewBufferString("3"))
		_ = folder.storages[2].PutObject("ccc", bytes.NewBufferString("3"))

		reader, storageName, err := ReadObject(folder, "aaa")
		require.NoError(t, err)
		assert.Equal(t, "s1", storageName)
		content, _ := io.ReadAll(reader)
		assert.Equal(t, "1", string(content))

		reader, storageName, err = ReadObject(folder, "bbb")
		require.NoError(t, err)
		assert.Equal(t, "s2", storageName)
		content, _ = io.ReadAll(reader)
		assert.Equal(t, "2", string(content))

		reader, storageName, err = ReadObject(folder, "ccc")
		require.NoError(t, err)
		assert.Equal(t, "s3", storageName)
		content, _ = io.ReadAll(reader)
		assert.Equal(t, "3", string(content))

		reader, storageName, err = ReadObject(folder, "ddd")
		require.Error(t, err)
		assert.ErrorAs(t, err, &storage.ObjectNotFoundError{})
		assert.Equal(t, "all", storageName)
	})
}
