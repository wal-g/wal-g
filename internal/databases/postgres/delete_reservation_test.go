package postgres

import (
	"context"
	"fmt"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/fs"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// Fail if cleanup even lists the reservation directory, rather than merely
// checking that it leaves the markers intact after enumerating them.
type reservationListErrorFolder struct {
	storage.Folder
	reservationPath string
}

func (f reservationListErrorFolder) GetSubFolder(name string) storage.Folder {
	return reservationListErrorFolder{f.Folder.GetSubFolder(name), f.reservationPath}
}

func (f reservationListErrorFolder) ListFolder(ctx context.Context) ([]storage.Object, []storage.Folder, error) {
	if strings.TrimRight(f.GetPath(), "/") == f.reservationPath {
		return nil, nil, fmt.Errorf("reservation directory must not be listed")
	}
	objects, folders, err := f.Folder.ListFolder(ctx)
	for i := range folders {
		folders[i] = reservationListErrorFolder{folders[i], f.reservationPath}
	}
	return objects, folders, err
}

func TestCleanupDoesNotListReservations(t *testing.T) {
	const oldName = "base_000000010000000000000001"
	const newName = "base_000000010000000000000003"
	for _, backend := range []string{"memory", "fs"} {
		for _, operation := range []string{"before", "retain", "garbage", "garbage-no-backups", "everything"} {
			t.Run(backend+"/"+operation, func(t *testing.T) {
				var folder storage.Folder = memory.NewFolder("repository/", memory.NewKVS())
				if backend == "fs" {
					// The prefix exercises filesystem-relative paths with a leading slash.
					folder = fs.NewFolder(t.TempDir(), "").GetSubFolder("segments_005/seg0")
				}
				marker := path.Join(backupReservationsPath, utility.BaseBackupPath, oldName+".json")
				require.NoError(t, folder.PutObject(t.Context(), marker, strings.NewReader("{}")))
				data := path.Join(utility.BaseBackupPath, oldName, "tar_partitions/part_001.tar.lz4")
				require.NoError(t, folder.PutObject(t.Context(), data, strings.NewReader("data")))
				if operation != "garbage-no-backups" {
					require.NoError(t, folder.PutObject(t.Context(),
						path.Join(utility.BaseBackupPath, newName+utility.SentinelSuffix), strings.NewReader("{}")))
					require.NoError(t, folder.PutObject(t.Context(),
						path.Join(utility.BaseBackupPath, newName, "metadata.json"), strings.NewReader("{}")))
				}
				wrapped := reservationListErrorFolder{folder,
					strings.TrimRight(folder.GetSubFolder(backupReservationsPath).GetPath(), "/")}
				handler, err := NewDeleteHandler(t.Context(), wrapped, nil, nil, false)
				require.NoError(t, err)
				if operation == "everything" {
					// Explicit deletion of the whole repository must still visit markers.
					// Use the real folder here, since the wrapper deliberately rejects LIST.
					handler.Folder = folder
					handler.DeleteEverything(t.Context(), true)
				} else if operation == "before" || operation == "retain" {
					var backup internal.BackupObject
					if operation == "before" {
						backup, err = handler.FindTargetByName(newName)
					} else {
						backup, err = handler.FindTargetRetain(1, internal.NoDeleteModifier)
					}
					require.NoError(t, err)
					require.NoError(t, handler.DeleteBeforeTarget(t.Context(), backup, true))
				} else {
					require.NoError(t, handler.HandleDeleteGarbage(t.Context(), nil, true, true))
				}
				exists, err := folder.Exists(t.Context(), data)
				require.NoError(t, err)
				require.False(t, exists)
				exists, err = folder.Exists(t.Context(), marker)
				require.NoError(t, err)
				require.Equal(t, operation != "everything", exists)
			})
		}
	}
}
