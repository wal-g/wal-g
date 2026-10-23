package postgres

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/pkg/storages/s3"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// Opt-in regression for the SeaweedFS service used by Docker CI. Start a
// disposable unauthenticated service with a reservation-test bucket and set
// WALG_TEST_RESERVATION_S3_ENDPOINT. Every run uses its own isolated prefix.
func TestBackupNameReservationS3DeletedData(t *testing.T) {
	endpoint := os.Getenv("WALG_TEST_RESERVATION_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set WALG_TEST_RESERVATION_S3_ENDPOINT for the disposable S3 integration test")
	}
	prefix := fmt.Sprintf("s3://reservation-test/regression-%d", time.Now().UnixNano())
	st, err := s3.ConfigureStorage(t.Context(), prefix, map[string]string{
		"AWS_ENDPOINT": endpoint, "AWS_REGION": "us-east-1",
		"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
		"AWS_S3_FORCE_PATH_STYLE": "true", "UPLOAD_CONCURRENCY": "1",
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	folder := st.RootFolder()
	backup := folder.GetSubFolder(utility.BaseBackupPath).GetSubFolder(reservationTestName)
	const object = "tar_partitions/part_001.tar.lz4"
	require.NoError(t, backup.PutObject(t.Context(), object, strings.NewReader("old backup")))
	require.NoError(t, backup.DeleteObjects(t.Context(), []storage.Object{
		storage.NewLocalObject(object, time.Time{}, 0),
	}))
	objects, folders, err := backup.ListFolder(t.Context())
	require.NoError(t, err)
	t.Logf("after deleting the only object: %d objects, %d subfolders", len(objects), len(folders))
	require.Empty(t, objects)
	objects, err = storage.ListFolderRecursively(t.Context(), backup)
	require.NoError(t, err)
	require.Empty(t, objects)
	require.NoError(t, reserveBackupName(t.Context(), folder, utility.BaseBackupPath, reservationTestName))
	require.ErrorIs(t, reserveBackupName(t.Context(), folder, utility.BaseBackupPath, reservationTestName),
		storage.ErrObjectAlreadyExists)
}
