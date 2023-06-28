package transfer

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func TestBackupFileLister_ListFilesToMove(t *testing.T) {
	defaultLister := func() (lister *BackupFileLister, source, target storage.Folder) {
		lister = NewAllBackupsFileLister(false, 100, 100)
		source = memory.NewFolder("source/", memory.NewStorage())
		target = memory.NewFolder("target/", memory.NewStorage())
		return
	}

	backupPrefix := func(i int) string {
		return fmt.Sprintf("basebackups_005/base_00%d", i+1)
	}

	t.Run("list backup files in separate groups", func(t *testing.T) {
		l, source, target := defaultLister()

		for i := 0; i < 2; i++ {
			_ = source.PutObject(backupPrefix(i)+"/a", &bytes.Buffer{})
			_ = source.PutObject(backupPrefix(i)+"/b/c", &bytes.Buffer{})
			_ = source.PutObject(backupPrefix(i)+"_backup_stop_sentinel.json", &bytes.Buffer{})
		}
		_ = source.PutObject("basebackups_005/non_backup_file", &bytes.Buffer{})

		groups, num, err := l.ListFilesToMove(source, target)
		assert.NoError(t, err)

		require.Len(t, groups, 2)
		assert.Equal(t, 6, num)
		sortGroups(groups)
		for i, group := range groups {
			sortFiles(group)
			assert.Equal(t,
				FilesGroup{
					FileToMove{
						path:        backupPrefix(i) + "/a",
						deleteAfter: []string{backupPrefix(i) + "_backup_stop_sentinel.json"},
					},
					FileToMove{
						path:        backupPrefix(i) + "/b/c",
						deleteAfter: []string{backupPrefix(i) + "_backup_stop_sentinel.json"},
					},
					FileToMove{
						path:      backupPrefix(i) + "_backup_stop_sentinel.json",
						copyAfter: []string{backupPrefix(i) + "/a", backupPrefix(i) + "/b/c"},
					},
				},
				groups[i],
			)
		}
	})

	t.Run("exclude already existing files", func(t *testing.T) {
		l, source, target := defaultLister()

		_ = source.PutObject("basebackups_005/base_001/a", &bytes.Buffer{})
		_ = source.PutObject("basebackups_005/base_001/b", &bytes.Buffer{})
		_ = source.PutObject("basebackups_005/base_001_backup_stop_sentinel.json", &bytes.Buffer{})

		_ = target.PutObject("basebackups_005/base_001/b", &bytes.Buffer{})

		groups, num, err := l.ListFilesToMove(source, target)
		require.NoError(t, err)
		assert.Equal(t, 2, num)

		require.Len(t, groups, 1)
		sortFiles(groups[0])
		assert.Equal(t,
			FilesGroup{
				FileToMove{
					path:        "basebackups_005/base_001/a",
					deleteAfter: []string{"basebackups_005/base_001_backup_stop_sentinel.json"},
				},
				FileToMove{
					path:      "basebackups_005/base_001_backup_stop_sentinel.json",
					copyAfter: []string{"basebackups_005/base_001/a"},
				},
			},
			groups[0],
		)
	})

	t.Run("include existing files when overwrite allowed", func(t *testing.T) {
		l, source, target := defaultLister()
		l.Overwrite = true

		_ = source.PutObject("basebackups_005/base_001/a", &bytes.Buffer{})
		_ = source.PutObject("basebackups_005/base_001/b", &bytes.Buffer{})
		_ = source.PutObject("basebackups_005/base_001_backup_stop_sentinel.json", &bytes.Buffer{})

		_ = target.PutObject("basebackups_005/base_001/b", &bytes.Buffer{})

		groups, _, err := l.ListFilesToMove(source, target)
		require.NoError(t, err)

		require.Len(t, groups, 1)
		require.Len(t, groups[0], 3)
	})

	t.Run("list single backup if name is specified", func(t *testing.T) {
		l, source, target := defaultLister()
		l = NewSingleBackupFileLister("base_002", l.Overwrite, l.MaxFiles)

		_ = source.PutObject("basebackups_005/base_001/a", &bytes.Buffer{})
		_ = source.PutObject("basebackups_005/base_001_backup_stop_sentinel.json", &bytes.Buffer{})

		_ = source.PutObject("basebackups_005/base_002/a", &bytes.Buffer{})
		_ = source.PutObject("basebackups_005/base_002_backup_stop_sentinel.json", &bytes.Buffer{})

		_ = source.PutObject("basebackups_005/base_003/a", &bytes.Buffer{})
		_ = source.PutObject("basebackups_005/base_003_backup_stop_sentinel.json", &bytes.Buffer{})

		groups, _, err := l.ListFilesToMove(source, target)
		require.NoError(t, err)

		require.Len(t, groups, 1)
		require.Len(t, groups[0], 2)
		require.Contains(t, groups[0][0].path, "basebackups_005/base_002")
	})

	t.Run("ignore max backups if name is specified", func(t *testing.T) {
		l, source, target := defaultLister()
		l.Name = "base_001"
		l.MaxBackups = 0

		_ = source.PutObject("basebackups_005/base_001/a", &bytes.Buffer{})
		_ = source.PutObject("basebackups_005/base_001_backup_stop_sentinel.json", &bytes.Buffer{})

		groups, _, err := l.ListFilesToMove(source, target)
		require.NoError(t, err)

		require.Len(t, groups, 1)
	})

	t.Run("skip incomplete backups", func(t *testing.T) {
		l, source, target := defaultLister()

		_ = source.PutObject("basebackups_005/base_001/a", &bytes.Buffer{})

		_ = source.PutObject("basebackups_005/base_002/a", &bytes.Buffer{})
		_ = source.PutObject("basebackups_005/base_002_backup_stop_sentinel.json", &bytes.Buffer{})

		groups, _, err := l.ListFilesToMove(source, target)
		require.NoError(t, err)

		require.Len(t, groups, 1)
		require.Len(t, groups[0], 2)
		require.Contains(t, groups[0][0].path, "basebackups_005/base_002")
	})

	t.Run("skip empty backups", func(t *testing.T) {
		l, source, target := defaultLister()

		_ = source.PutObject("basebackups_005/base_001_backup_stop_sentinel.json", &bytes.Buffer{})

		_ = source.PutObject("basebackups_005/base_002/a", &bytes.Buffer{})
		_ = source.PutObject("basebackups_005/base_002_backup_stop_sentinel.json", &bytes.Buffer{})

		groups, _, err := l.ListFilesToMove(source, target)
		require.NoError(t, err)

		require.Len(t, groups, 1)
		require.Len(t, groups[0], 2)
		require.Contains(t, groups[0][0].path, "basebackups_005/base_002")
	})

	t.Run("limit number of files", func(t *testing.T) {
		l, source, target := defaultLister()
		l.MaxFiles = 3

		_ = source.PutObject("basebackups_005/base_001/a", &bytes.Buffer{})
		_ = source.PutObject("basebackups_005/base_001_backup_stop_sentinel.json", &bytes.Buffer{})

		_ = source.PutObject("basebackups_005/base_002/a", &bytes.Buffer{})
		_ = source.PutObject("basebackups_005/base_002_backup_stop_sentinel.json", &bytes.Buffer{})

		groups, num, err := l.ListFilesToMove(source, target)
		require.NoError(t, err)
		assert.Equal(t, 2, num)

		require.Len(t, groups, 1)
		require.Len(t, groups[0], 2)
	})

	t.Run("list no backups if single backup has more than max files", func(t *testing.T) {
		l, source, target := defaultLister()
		l.MaxFiles = 1

		_ = source.PutObject("basebackups_005/base_002/a", &bytes.Buffer{})
		_ = source.PutObject("basebackups_005/base_002_backup_stop_sentinel.json", &bytes.Buffer{})

		groups, num, err := l.ListFilesToMove(source, target)
		require.NoError(t, err)
		assert.Equal(t, 0, num)

		require.Len(t, groups, 0)
	})

	t.Run("limit number of backups", func(t *testing.T) {
		l, source, target := defaultLister()
		l.MaxBackups = 1

		_ = source.PutObject("basebackups_005/base_001/a", &bytes.Buffer{})
		_ = source.PutObject("basebackups_005/base_001_backup_stop_sentinel.json", &bytes.Buffer{})

		_ = source.PutObject("basebackups_005/base_002/a", &bytes.Buffer{})
		_ = source.PutObject("basebackups_005/base_002_backup_stop_sentinel.json", &bytes.Buffer{})

		groups, num, err := l.ListFilesToMove(source, target)
		require.NoError(t, err)
		assert.Equal(t, 2, num)

		require.Len(t, groups, 1)
	})
}

func sortGroups(groups []FilesGroup) {
	sort.Slice(groups, func(i, j int) bool { return groups[i][0].path < groups[j][0].path })
}

func sortFiles(group FilesGroup) {
	sort.Slice(group, func(i, j int) bool { return group[i].path < group[j].path })
	for _, f := range group {
		sort.Strings(f.copyAfter)
		sort.Strings(f.deleteAfter)
	}
}
