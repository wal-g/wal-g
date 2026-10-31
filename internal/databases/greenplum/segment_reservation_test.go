package greenplum_test

import (
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/fs"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func TestSegmentDeletionPreservesReservations(t *testing.T) {
	const oldName = "base_000000010000000000000002"
	const newName = "base_000000010000000000000003"
	const abandonedName = "base_000000010000000000000001"
	for _, tc := range []struct {
		name   string
		kind   greenplum.SegDeleteType
		target string
	}{
		{"before", greenplum.SegDeleteBefore, newName},
		{"target", greenplum.SegDeleteTarget, oldName},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := fs.NewFolder(t.TempDir(), "")
			// A marker for an interrupted upload has no completed sentinel.
			for _, contentID := range []int{-1, 0, 1} {
				folder := root.GetSubFolder(greenplum.FormatSegmentStoragePrefix(contentID))
				for _, name := range []string{oldName, newName, abandonedName} {
					marker := path.Join("backup_reservations", utility.BaseBackupPath, name+".json")
					require.NoError(t, storage.CreateObject(t.Context(), folder, marker, []byte("{}")))
					if name != abandonedName {
						putDTO(t, folder, path.Join(utility.BaseBackupPath, name+utility.SentinelSuffix),
							postgres.BackupSentinelDto{})
					}
					require.NoError(t, folder.PutObject(t.Context(),
						path.Join(utility.BaseBackupPath, name, "tar_partitions/part_001.tar.lz4"), strings.NewReader("data")))
				}
			}

			handler, err := greenplum.NewSegDeleteHandler(t.Context(), root, 0,
				greenplum.DeleteArgs{Confirmed: true}, tc.kind)
			require.NoError(t, err)
			folder := root.GetSubFolder(greenplum.FormatSegmentStoragePrefix(0))
			target, err := greenplum.NewSegBackup(t.Context(), folder.GetSubFolder(utility.BaseBackupPath), tc.target, "default")
			require.NoError(t, err)
			require.NoError(t, handler.Delete(t.Context(), target))

			for _, contentID := range []int{-1, 0, 1} {
				folder := root.GetSubFolder(greenplum.FormatSegmentStoragePrefix(contentID))
				for _, name := range []string{oldName, newName, abandonedName} {
					marker := path.Join("backup_reservations", utility.BaseBackupPath, name+".json")
					require.ErrorIs(t, storage.CreateObject(t.Context(), folder, marker, []byte("retry")),
						storage.ErrObjectAlreadyExists)
				}
				// Data is actually removed, but only from the selected segment;
				// retained and interrupted-attempt reservations all survive.
				for _, name := range []string{oldName, newName} {
					exists, err := folder.Exists(t.Context(), path.Join(utility.BaseBackupPath, name+utility.SentinelSuffix))
					require.NoError(t, err)
					require.Equal(t, contentID != 0 || name == newName, exists)
					exists, err = folder.Exists(t.Context(), path.Join(utility.BaseBackupPath, name, "tar_partitions/part_001.tar.lz4"))
					require.NoError(t, err)
					require.Equal(t, contentID != 0 || name == newName, exists)
				}
			}
		})
	}
}
