package storagetools

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
)

func TestHandleRemove(t *testing.T) {
	t.Run("throw err when there is no files at prefix", func(t *testing.T) {
		emptyFolder := memory.NewFolder("test/", memory.NewStorage())
		err := HandleRemove("a/b/c/nonexistent", emptyFolder)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("remove single file", func(t *testing.T) {
		folder := memory.NewFolder("test/", memory.NewStorage())

		targetFile := "a/b/c/target"
		targetFolder := []string{
			"a/b/c/target/1",
			"a/b/c/target/1/2",
			"a/b/c/target/1/2/3",
		}
		for _, f := range append(targetFolder, targetFile) {
			err := folder.PutObject(f, bytes.NewBufferString("123"))
			require.NoError(t, err)
		}

		err := HandleRemove("a/b/c/target", folder)
		require.NoError(t, err)

		exists, err := folder.Exists(targetFile)
		require.NoError(t, err)
		assert.False(t, exists)

		for _, f := range targetFolder {
			exists, err = folder.Exists(f)
			require.NoError(t, err)
			assert.True(t, exists)
		}
	})

	t.Run("remove all files in folder", func(t *testing.T) {
		folder := memory.NewFolder("test/", memory.NewStorage())

		targetFile := "a/b/c/target"
		targetFolder := []string{
			"a/b/c/target/1",
			"a/b/c/target/1/2",
			"a/b/c/target/1/2/3",
		}
		for _, f := range append(targetFolder, targetFile) {
			err := folder.PutObject(f, bytes.NewBufferString("123"))
			require.NoError(t, err)
		}

		err := HandleRemove("a/b/c/target/", folder)
		require.NoError(t, err)

		exists, err := folder.Exists(targetFile)
		require.NoError(t, err)
		assert.True(t, exists)

		for _, f := range targetFolder {
			exists, err = folder.Exists(f)
			require.NoError(t, err)
			assert.False(t, exists)
		}
	})
}
