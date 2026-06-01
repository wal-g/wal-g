package greenplum_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

const (
	timeline         = uint32(1)
	segmentsPerGroup = uint64(0x100)
)

func walSegFilename(segNo uint64) string {
	hi := segNo / segmentsPerGroup
	lo := segNo % segmentsPerGroup
	return fmt.Sprintf("%08X%08X%08X", timeline, hi, lo)
}

func walObjectPath(contentID int, segNo uint64) string {
	return path.Join(greenplum.FormatSegmentStoragePrefix(contentID), utility.WalPath, walSegFilename(segNo))
}

func makeTrimWalFolder(
	t *testing.T,
	backupName string,
	backupRestorePoint string,
	segments []greenplum.SegmentMetadata,
	walSegNosByContentID map[int][]uint64,
	storedRestorePoints []string,
) storage.Folder {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)

	sentinelData := map[string]interface{}{
		"segments":    segments,
		"start_time":  "2026-01-01T00:00:00Z",
		"finish_time": "2026-01-01T00:01:00Z",
		"hostname":    "testhost",
		"gp_version":  "6.0.0",
		"gp_flavor":   "greenplum",
	}
	if backupRestorePoint != "" {
		sentinelData["restore_point"] = backupRestorePoint
	}
	sentinelBytes, err := json.Marshal(sentinelData)
	require.NoError(t, err)

	sentinelFile := backupName + utility.SentinelSuffix
	err = baseBackupFolder.PutObject(sentinelFile, bytes.NewReader(sentinelBytes))
	require.NoError(t, err)

	for contentID, segNos := range walSegNosByContentID {
		segFolder := folder.GetSubFolder(greenplum.FormatSegmentStoragePrefix(contentID))
		for _, segNo := range segNos {
			walFilePath := path.Join(utility.WalPath, walSegFilename(segNo))
			err = segFolder.PutObject(walFilePath, &bytes.Buffer{})
			require.NoError(t, err)
		}
	}

	for _, restorePointName := range storedRestorePoints {
		restorePointData := map[string]interface{}{
			"name":        restorePointName,
			"start_time":  "2026-01-01T00:00:00Z",
			"finish_time": "2026-01-01T00:00:01Z",
		}
		restorePointBytes, err := json.Marshal(restorePointData)
		require.NoError(t, err)
		err = baseBackupFolder.PutObject(restorePointName+greenplum.RestorePointSuffix, bytes.NewReader(restorePointBytes))
		require.NoError(t, err)
	}

	return folder
}

func getStorageObjectsPaths(t *testing.T, folder storage.Folder) []string {
	objects, err := storage.ListFolderRecursively(folder)
	require.NoError(t, err)
	names := make([]string, len(objects))
	for i, o := range objects {
		names[i] = o.GetName()
	}
	return names
}

func TestHandleDeleteTrimWal_DeletesWalAfterCutoff(t *testing.T) {
	backupName := "backup_20260101T000000Z"
	restorePoint := "test_restore_point"
	segments := []greenplum.SegmentMetadata{
		{
			ContentID:       0,
			RestorePointLSN: "0/02000000",
		},
	}
	walSegNosByContentID := map[int][]uint64{0: {1, 2, 3, 4}}
	storedRestorePoints := []string{restorePoint}
	folder := makeTrimWalFolder(
		t, backupName, restorePoint, segments, walSegNosByContentID, storedRestorePoints)

	handler, err := greenplum.NewDeleteHandler(folder, greenplum.DeleteArgs{})
	require.NoError(t, err)

	err = handler.HandleDeleteTrimWal(backupName, true)
	require.NoError(t, err)

	objectPaths := getStorageObjectsPaths(t, folder)
	assert.Contains(t, objectPaths, walObjectPath(0, 1), "WAL at cutoff-1 must be retained")
	assert.Contains(t, objectPaths, walObjectPath(0, 2), "WAL at cutoff must be retained")
	assert.NotContains(t, objectPaths, walObjectPath(0, 3), "WAL past cutoff must be deleted")
	assert.NotContains(t, objectPaths, walObjectPath(0, 4), "WAL past cutoff must be deleted")
}

func TestHandleDeleteTrimWal_WithoutConfirm_NothingDeleted(t *testing.T) {
	backupName := "backup_20260101T000000Z"
	restorePoint := "test_restore_point"
	segments := []greenplum.SegmentMetadata{
		{
			ContentID:       0,
			RestorePointLSN: "0/02000000",
		},
	}
	walSegNos := []uint64{1, 2, 3, 4}
	walSegNosByContentID := map[int][]uint64{0: walSegNos}
	storedRestorePoints := []string{restorePoint, "old_restore_point"}

	folder := makeTrimWalFolder(
		t, backupName, restorePoint, segments, walSegNosByContentID, storedRestorePoints,
	)

	handler, err := greenplum.NewDeleteHandler(folder, greenplum.DeleteArgs{})
	require.NoError(t, err)

	err = handler.HandleDeleteTrimWal(backupName, false)
	require.NoError(t, err)

	objectPaths := getStorageObjectsPaths(t, folder)
	for _, segNo := range walSegNos {
		assert.Contains(t, objectPaths, walObjectPath(0, segNo), "dry run must not delete WAL segNo %d", segNo)
	}
	// Old restore point must still be present
	oldRestorePointFilePath := path.Join(utility.BaseBackupPath, "old_restore_point"+greenplum.RestorePointSuffix)
	assert.Contains(t, objectPaths, oldRestorePointFilePath, "dry run must not delete restore point files")
}

func TestHandleDeleteTrimWal_DeletesRestorePointsExceptTarget(t *testing.T) {
	backupName := "backup_20260101T000000Z"
	restorePoint := "test_restore_point"
	segments := []greenplum.SegmentMetadata{
		{
			ContentID:       0,
			RestorePointLSN: "0/02000000",
		},
	}
	walSegNos := []uint64{1}
	walSegNosByContentID := map[int][]uint64{0: walSegNos}
	storedRestorePoints := []string{restorePoint, "old_restore_point_1", "old_restore_point_2"}
	folder := makeTrimWalFolder(
		t, backupName, restorePoint, segments, walSegNosByContentID, storedRestorePoints,
	)

	handler, err := greenplum.NewDeleteHandler(folder, greenplum.DeleteArgs{})
	require.NoError(t, err)

	err = handler.HandleDeleteTrimWal(backupName, true)
	require.NoError(t, err)

	objectPaths := getStorageObjectsPaths(t, folder)
	restorePointFile := path.Join(utility.BaseBackupPath, restorePoint+greenplum.RestorePointSuffix)
	assert.Contains(t, objectPaths, restorePointFile, "target restore point must be kept")

	restorePointFile1 := path.Join(utility.BaseBackupPath, "old_restore_point_1"+greenplum.RestorePointSuffix)
	restorePointFile2 := path.Join(utility.BaseBackupPath, "old_restore_point_2"+greenplum.RestorePointSuffix)
	assert.NotContains(t, objectPaths, restorePointFile1, "old restore point 1 must be deleted")
	assert.NotContains(t, objectPaths, restorePointFile2, "old restore point 2 must be deleted")
}

func TestHandleDeleteTrimWal_NoRestorePoint(t *testing.T) {
	backupName := "backup_20260101T000000Z"
	restorePoint := ""
	segments := []greenplum.SegmentMetadata{
		{
			ContentID:       0,
			RestorePointLSN: "0/02000000",
		},
	}
	walSegNos := []uint64{1, 3}
	walSegNosByContentID := map[int][]uint64{0: walSegNos}
	folder := makeTrimWalFolder(
		t, backupName, restorePoint, segments, walSegNosByContentID, nil,
	)

	handler, err := greenplum.NewDeleteHandler(folder, greenplum.DeleteArgs{})
	require.NoError(t, err)

	err = handler.HandleDeleteTrimWal(backupName, true)
	require.NoError(t, err)

	objectPaths := getStorageObjectsPaths(t, folder)
	assert.Contains(t, objectPaths, walObjectPath(0, 1), "WAL at or before cutoff must be kept")
	assert.NotContains(t, objectPaths, walObjectPath(0, 3), "WAL past cutoff must be deleted")
}

func TestHandleDeleteTrimWal_MultipleSegments(t *testing.T) {
	backupName := "backup_20260101T000000Z"
	restorePoint := "test_restore_point"
	segments := []greenplum.SegmentMetadata{
		{ContentID: -1, RestorePointLSN: "0/01000000"},
		{ContentID: 0, RestorePointLSN: "0/02000000"},
	}
	walSegNosByContentID := map[int][]uint64{
		-1: {1, 2, 3},
		0:  {1, 2, 3},
	}
	storedRestorePoints := []string{restorePoint}
	folder := makeTrimWalFolder(
		t, backupName, restorePoint, segments, walSegNosByContentID, storedRestorePoints,
	)

	handler, err := greenplum.NewDeleteHandler(folder, greenplum.DeleteArgs{})
	require.NoError(t, err)

	err = handler.HandleDeleteTrimWal(backupName, true)
	require.NoError(t, err)

	objectPaths := getStorageObjectsPaths(t, folder)

	assert.Contains(t, objectPaths, walObjectPath(-1, 1))
	assert.NotContains(t, objectPaths, walObjectPath(-1, 2))
	assert.NotContains(t, objectPaths, walObjectPath(-1, 3))

	assert.Contains(t, objectPaths, walObjectPath(0, 1))
	assert.Contains(t, objectPaths, walObjectPath(0, 2))
	assert.NotContains(t, objectPaths, walObjectPath(0, 3))
}

func TestHandleDeleteTrimWal_BackupNotFound(t *testing.T) {
	folder := makeTrimWalFolder(t, "backup_20260101T000000Z", "", nil, nil, nil)

	handler, err := greenplum.NewDeleteHandler(folder, greenplum.DeleteArgs{})
	require.NoError(t, err)

	err = handler.HandleDeleteTrimWal("backup_nonexistent", true)
	assert.Error(t, err)
}
