package transfer

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestRegularFileLister_ListFilesToMove(t *testing.T) {
	defaultLister := func() (lister *RegularFileLister, source, target storage.Folder) {
		lister = NewRegularFileLister("/", false, 100)
		source = memory.NewFolder("source/", memory.NewStorage())
		target = memory.NewFolder("target/", memory.NewStorage())
		return
	}

	t.Run("list files from parent dir only", func(t *testing.T) {
		l, source, target := defaultLister()
		l.Prefix = "1/"

		_ = source.PutObject("1/a", &bytes.Buffer{})
		_ = source.PutObject("1/b", &bytes.Buffer{})
		_ = source.PutObject("2/a", &bytes.Buffer{})

		groups, num, err := l.ListFilesToMove(source, target)
		assert.NoError(t, err)

		require.Len(t, groups, 2)
		assert.Equal(t, 2, num)
		sortGroups(groups)
		assert.Equal(t, FilesGroup{FileToMove{path: "1/a"}}, groups[0])
		assert.Equal(t, FilesGroup{FileToMove{path: "1/b"}}, groups[1])
	})

	t.Run("exclude already existing files", func(t *testing.T) {
		l, source, target := defaultLister()

		_ = source.PutObject("1", &bytes.Buffer{})
		_ = source.PutObject("2", &bytes.Buffer{})

		_ = target.PutObject("1", &bytes.Buffer{})

		groups, _, err := l.ListFilesToMove(source, target)
		assert.NoError(t, err)

		require.Len(t, groups, 1)
		assert.Equal(t, FilesGroup{FileToMove{path: "2"}}, groups[0])
	})

	t.Run("include existing files when overwrite allowed", func(t *testing.T) {
		l, source, target := defaultLister()
		l.Overwrite = true

		_ = source.PutObject("1", &bytes.Buffer{})
		_ = source.PutObject("2", &bytes.Buffer{})

		_ = target.PutObject("1", &bytes.Buffer{})

		groups, _, err := l.ListFilesToMove(source, target)
		assert.NoError(t, err)

		require.Len(t, groups, 2)
	})

	t.Run("dont include nonexistent files even when overwrite allowed", func(t *testing.T) {
		l, source, target := defaultLister()
		l.Overwrite = true

		_ = source.PutObject("2", &bytes.Buffer{})

		_ = target.PutObject("1", &bytes.Buffer{})

		groups, _, err := l.ListFilesToMove(source, target)
		assert.NoError(t, err)

		require.Len(t, groups, 1)
		require.Len(t, groups[0], 1)
		assert.Equal(t, "2", groups[0][0].path)
	})

	t.Run("limit number of files", func(t *testing.T) {
		l, source, target := defaultLister()
		l.MaxFiles = 1

		_ = source.PutObject("1", &bytes.Buffer{})
		_ = source.PutObject("2", &bytes.Buffer{})

		groups, _, err := l.ListFilesToMove(source, target)
		assert.NoError(t, err)

		require.Len(t, groups, 1)
	})
}
