package postgres

import (
	"context"
	"errors"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/fs"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
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

type reservationInspectionFailure struct {
	storage.Folder
	list bool
}

func (f reservationInspectionFailure) CreateObject(ctx context.Context, name string, content []byte) error {
	return storage.CreateObject(ctx, f.Folder, name, content)
}

func (f reservationInspectionFailure) GetSubFolder(name string) storage.Folder {
	return reservationInspectionFailure{Folder: f.Folder.GetSubFolder(name), list: f.list}
}

func (f reservationInspectionFailure) Exists(ctx context.Context, name string) (bool, error) {
	if !f.list {
		return false, errors.New("HEAD unavailable")
	}
	return f.Folder.Exists(ctx, name)
}

func (f reservationInspectionFailure) ListFolder(context.Context) ([]storage.Object, []storage.Folder, error) {
	return nil, nil, errors.New("LIST unavailable")
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

func TestBackupNameReservationDisabled(t *testing.T) {
	// Disabled mode must not perform any storage calls.
	handler := BackupHandler{}
	require.NoError(t, handler.reserveBackupName(t.Context(), nil, "", ""))
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
