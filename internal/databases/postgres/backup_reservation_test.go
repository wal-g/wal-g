package postgres

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/stats"
	"github.com/wal-g/wal-g/pkg/storages/fs"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/time/rate"
)

const reservationTestName = "base_000000010000000000000002"

func TestBackupNameReservationConcurrentAndPermanent(t *testing.T) {
	root := t.TempDir()
	results := make(chan error, 16)
	var wg sync.WaitGroup
	for range cap(results) {
		wg.Go(func() {
			results <- reserveBackupName(t.Context(), fs.NewFolder(root, ""), utility.BaseBackupPath, reservationTestName)
		})
	}
	wg.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
		} else {
			require.ErrorIs(t, err, storage.ErrObjectAlreadyExists)
		}
	}
	require.Equal(t, 1, winners)
	folder := fs.NewFolder(root, "")
	// An abandoned attempt cannot be retried, even without a sentinel or data.
	require.ErrorIs(t, reserveBackupName(t.Context(), folder, utility.BaseBackupPath, reservationTestName), storage.ErrObjectAlreadyExists)
	// The final delta name is a separate namespace from its full-backup name.
	require.NoError(t, reserveBackupName(t.Context(), folder, utility.BaseBackupPath, reservationTestName+"_D_000000010000000000000001"))
	require.NoError(t, reserveBackupName(t.Context(), folder, utility.BaseBackupPath, "base_000000010000000000000003"))
}

func TestBackupNameReservationRejectsLegacyData(t *testing.T) {
	for _, object := range []string{reservationTestName + utility.SentinelSuffix, reservationTestName + "/tar_partitions/part_001.tar.lz4"} {
		t.Run(object, func(t *testing.T) {
			folder := fs.NewFolder(t.TempDir(), "")
			backups := folder.GetSubFolder(utility.BaseBackupPath)
			require.NoError(t, backups.PutObject(t.Context(), object, strings.NewReader("old data")))
			require.ErrorIs(t, reserveBackupName(t.Context(), folder, utility.BaseBackupPath, reservationTestName), storage.ErrObjectAlreadyExists)
			// Removing old data does not implicitly release the reservation.
			require.NoError(t, backups.DeleteObjects(t.Context(), []storage.Object{storage.NewLocalObject(object, time.Time{}, 0)}))
			require.ErrorIs(t, reserveBackupName(t.Context(), folder, utility.BaseBackupPath, reservationTestName), storage.ErrObjectAlreadyExists)
		})
	}
}

func TestBackupNameReservationEmptyDirectories(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		for _, data := range []string{"", "metadata.json", "tar_partitions/part_001.tar.lz4", "nested/deeper/zero-byte"} {
			t.Run(fmt.Sprintf("wrapped=%t/data=%s", wrapped, data), func(t *testing.T) {
				var folder storage.Folder = fs.NewFolder(t.TempDir(), "")
				if wrapped {
					folder = internal.NewLimitedFolder(folder, rate.NewLimiter(rate.Inf, 1))
					folder = multistorage.NewFolder(map[string]storage.Folder{"default": folder}, stats.NewNopCollector([]string{"default"}))
					var err error
					folder, err = multistorage.UseSpecificStorage(t.Context(), "default", folder)
					require.NoError(t, err)
				}
				backup := folder.GetSubFolder(utility.BaseBackupPath).GetSubFolder(reservationTestName)
				// Filesystem-backed S3 services can retain empty directories after
				// deleting all objects. A directory alone is not backup data.
				backup.GetSubFolder("tar_partitions")
				backup.GetSubFolder("nested/deeper")
				if data != "" {
					require.NoError(t, backup.PutObject(t.Context(), data, strings.NewReader("")))
				}
				err := reserveBackupName(t.Context(), folder, utility.BaseBackupPath, reservationTestName)
				if data == "" {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, storage.ErrObjectAlreadyExists)
				}
				// Even when the directory is empty, the permanent reservation
				// still prevents another writer from reusing the name.
				require.ErrorIs(t, reserveBackupName(t.Context(), folder, utility.BaseBackupPath, reservationTestName),
					storage.ErrObjectAlreadyExists)
			})
		}
	}
}

type reservationInspectionFailure struct {
	storage.Folder
	list       bool
	listSuffix string
}

func (f reservationInspectionFailure) CreateObject(ctx context.Context, name string, content []byte) error {
	return storage.CreateObject(ctx, f.Folder, name, content)
}

func (f reservationInspectionFailure) GetSubFolder(name string) storage.Folder {
	f.Folder = f.Folder.GetSubFolder(name)
	return f
}

func (f reservationInspectionFailure) Exists(ctx context.Context, name string) (bool, error) {
	if !f.list {
		return false, errors.New("HEAD unavailable")
	}
	return f.Folder.Exists(ctx, name)
}

func (f reservationInspectionFailure) ListFolder(ctx context.Context) ([]storage.Object, []storage.Folder, error) {
	if f.listSuffix != "" && !strings.HasSuffix(f.GetPath(), f.listSuffix) {
		objects, folders, err := f.Folder.ListFolder(ctx)
		for i, folder := range folders {
			folders[i] = reservationInspectionFailure{Folder: folder, list: f.list, listSuffix: f.listSuffix}
		}
		return objects, folders, err
	}
	return nil, nil, errors.New("LIST unavailable")
}

func TestBackupNameReservationNestedInspectionFailure(t *testing.T) {
	folder := fs.NewFolder(t.TempDir(), "")
	folder.GetSubFolder(path.Join(utility.BaseBackupPath, reservationTestName, "unreadable"))
	broken := reservationInspectionFailure{Folder: folder, list: true, listSuffix: "unreadable/"}
	require.ErrorContains(t, reserveBackupName(t.Context(), broken, utility.BaseBackupPath, reservationTestName), "LIST unavailable")
	require.ErrorIs(t, reserveBackupName(t.Context(), folder, utility.BaseBackupPath, reservationTestName), storage.ErrObjectAlreadyExists)
}

func TestBackupNameReservationInspectionFailure(t *testing.T) {
	for _, list := range []bool{false, true} {
		folder := fs.NewFolder(t.TempDir(), "")
		broken := reservationInspectionFailure{Folder: folder, list: list}
		require.ErrorContains(t, reserveBackupName(t.Context(), broken, utility.BaseBackupPath, reservationTestName), "unavailable")
		marker := path.Join(backupReservationsPath, utility.BaseBackupPath, reservationTestName+".json")
		exists, err := folder.Exists(t.Context(), marker)
		require.NoError(t, err)
		require.True(t, exists)
		require.ErrorIs(t, reserveBackupName(t.Context(), folder, utility.BaseBackupPath, reservationTestName), storage.ErrObjectAlreadyExists)
	}
}

func TestBackupNameReservationBackendCapability(t *testing.T) {
	for _, limited := range []bool{false, true} {
		for _, selected := range []string{"supported", "unsupported"} {
			t.Run(fmt.Sprintf("limited=%t/selected=%s", limited, selected), func(t *testing.T) {
				folders := map[string]storage.Folder{
					"supported":   fs.NewFolder(t.TempDir(), ""),
					"unsupported": memory.NewFolder("", memory.NewKVS()),
				}
				if limited {
					for name, folder := range folders {
						folders[name] = internal.NewLimitedFolder(folder, rate.NewLimiter(rate.Inf, 1))
					}
				}
				root := multistorage.NewFolder(folders, stats.NewNopCollector([]string{"supported", "unsupported"}))
				root, err := multistorage.UseSpecificStorage(t.Context(), selected, root)
				require.NoError(t, err)
				require.NoError(t, reserveBackupName(t.Context(), root, utility.BaseBackupPath, reservationTestName))
				err = reserveBackupName(t.Context(), root, utility.BaseBackupPath, reservationTestName)
				if selected == "supported" {
					require.ErrorIs(t, err, storage.ErrObjectAlreadyExists)
				} else {
					require.NoError(t, err)
				}
				marker := path.Join(backupReservationsPath, utility.BaseBackupPath, reservationTestName+".json")
				for name, folder := range folders {
					exists, err := folder.Exists(t.Context(), marker)
					require.NoError(t, err)
					require.Equal(t, selected == "supported" && name == selected, exists)
				}
			})
		}
	}
}

type reservationCreationFailure struct {
	storage.Folder
}

func (reservationCreationFailure) CreateObject(context.Context, string, []byte) error {
	return errors.New("conditional PUT not implemented by endpoint")
}

func TestBackupNameReservationDoesNotDowngradeOnError(t *testing.T) {
	// A supported backend's endpoint rejecting the operation is a fatal error,
	// not a reason to continue uploading without reservation protection.
	folder := reservationCreationFailure{Folder: fs.NewFolder(t.TempDir(), "")}
	for _, root := range []storage.Folder{folder, internal.NewLimitedFolder(folder, rate.NewLimiter(rate.Inf, 1))} {
		require.ErrorContains(t, reserveBackupName(t.Context(), root, utility.BaseBackupPath, reservationTestName),
			"conditional PUT not implemented by endpoint")
	}
}

func TestBackupNameReservationUnsupportedDoesNotAccessStorage(t *testing.T) {
	// Embedding a nil Folder panics if the unsupported path performs any I/O.
	var folder storage.Folder = struct{ storage.Folder }{}
	require.NoError(t, reserveBackupName(t.Context(), folder, utility.BaseBackupPath, reservationTestName))
	folder = internal.NewLimitedFolder(folder, rate.NewLimiter(rate.Inf, 1))
	require.NoError(t, reserveBackupName(t.Context(), folder, utility.BaseBackupPath, reservationTestName))
}

func TestBackupNameReservationIndependentSegmentPrefixes(t *testing.T) {
	root := fs.NewFolder(t.TempDir(), "")
	// Segment backups may share an LSN/name; only writers within the same
	// segment prefix should conflict, including the coordinator's segment -1.
	for _, id := range []int{-1, 0, 1} {
		folder := root.GetSubFolder(fmt.Sprintf("segments_%s/seg%d", utility.VersionStr, id))
		require.NoError(t, reserveBackupName(t.Context(), folder, utility.BaseBackupPath, reservationTestName))
		require.ErrorIs(t, reserveBackupName(t.Context(), folder, utility.BaseBackupPath, reservationTestName),
			storage.ErrObjectAlreadyExists)
	}
}

func TestBackupNameReservationSurvivesGarbageCollection(t *testing.T) {
	folder := fs.NewFolder(t.TempDir(), "")
	require.NoError(t, reserveBackupName(t.Context(), folder, utility.BaseBackupPath, reservationTestName))
	object := path.Join(utility.BaseBackupPath, reservationTestName, "tar_partitions/part_001.tar.lz4")
	require.NoError(t, folder.PutObject(t.Context(), object, strings.NewReader("incomplete backup")))
	handler, err := NewDeleteHandler(t.Context(), folder, nil, nil, false)
	require.NoError(t, err)
	require.NoError(t, handler.HandleDeleteGarbage(t.Context(), nil, true, true))
	exists, err := folder.Exists(t.Context(), object)
	require.NoError(t, err)
	require.False(t, exists)
	require.ErrorIs(t, reserveBackupName(t.Context(), folder, utility.BaseBackupPath, reservationTestName), storage.ErrObjectAlreadyExists)
}

func TestBackupNameReservationPermanentPaths(t *testing.T) {
	marker := path.Join(backupReservationsPath, utility.BaseBackupPath, reservationTestName+".json")
	require.True(t, IsPermanent(marker, "default", nil, nil))
	require.True(t, IsPermanent("/"+marker, "default", nil, nil))
	require.False(t, IsPermanent(backupReservationsPath+"_other/"+reservationTestName, "default", nil, nil))
}
