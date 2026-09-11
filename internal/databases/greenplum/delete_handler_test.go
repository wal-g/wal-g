package greenplum_test

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

type testRestorePoint struct {
	name       string
	finishTime time.Time
	timeLine   uint32
}

var baseFinishTime = time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)

const baseWalTimeLine = uint32(1)

func walSegFilename(segNo uint64, walTimeLine uint32) string {
	return postgres.WalSegmentNo(segNo).GetFilename(walTimeLine)
}

func walObjectPath(contentID int, segNo uint64, walTimeLine uint32) string {
	return path.Join(greenplum.FormatSegmentWalPath(contentID), walSegFilename(segNo, walTimeLine))
}

func walCutoffLSN(cutoffSegNo uint64) string {
	return postgres.LSN(cutoffSegNo * postgres.WalSegmentSize).String()
}

func makeTrimWalFolder(
	t *testing.T,
	backupName string,
	backupRestorePoint string,
	segments []greenplum.SegmentMetadata,
	walSegNosByContentID map[int][]uint64,
	storedRestorePoints []testRestorePoint,
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
			walFilePath := path.Join(utility.WalPath, walSegFilename(segNo, baseWalTimeLine))
			err = segFolder.PutObject(walFilePath, &bytes.Buffer{})
			require.NoError(t, err)
		}
	}

	for _, rp := range storedRestorePoints {
		restorePointData := map[string]interface{}{
			"name":        rp.name,
			"start_time":  rp.finishTime.Add(-time.Second).Format(time.RFC3339),
			"finish_time": rp.finishTime.Format(time.RFC3339),
			"time_line":   rp.timeLine,
		}
		restorePointBytes, err := json.Marshal(restorePointData)
		require.NoError(t, err)
		err = baseBackupFolder.PutObject(rp.name+greenplum.RestorePointSuffix, bytes.NewReader(restorePointBytes))
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
		{ContentID: 0, RestorePointLSN: walCutoffLSN(2)},
	}
	walSegNosByContentID := map[int][]uint64{0: {1, 2, 3, 4}}
	storedRestorePoints := []testRestorePoint{{restorePoint, baseFinishTime, baseWalTimeLine}}
	folder := makeTrimWalFolder(
		t, backupName, restorePoint, segments, walSegNosByContentID, storedRestorePoints,
	)

	delArgs := greenplum.DeleteArgs{Confirmed: true}
	handler, err := greenplum.NewDeleteHandler(folder, delArgs)
	require.NoError(t, err)

	err = handler.HandleDeleteTrimWal(context.Background(), backupName)
	require.NoError(t, err)

	objectPaths := getStorageObjectsPaths(t, folder)
	assert.Contains(t, objectPaths, walObjectPath(0, 1, baseWalTimeLine), "WAL at cutoff-1 must be retained")
	assert.Contains(t, objectPaths, walObjectPath(0, 2, baseWalTimeLine), "WAL at cutoff must be retained")
	assert.NotContains(t, objectPaths, walObjectPath(0, 3, baseWalTimeLine), "WAL past cutoff must be deleted")
	assert.NotContains(t, objectPaths, walObjectPath(0, 4, baseWalTimeLine), "WAL past cutoff must be deleted")
}

func TestHandleDeleteTrimWal_WithoutConfirm_NothingDeleted(t *testing.T) {
	backupName := "backup_20260101T000000Z"
	restorePoint := "test_restore_point"
	laterRestorePoint := "later_restore_point"
	segments := []greenplum.SegmentMetadata{
		{ContentID: 0, RestorePointLSN: walCutoffLSN(2)},
	}
	walSegNos := []uint64{1, 2, 3, 4}
	walSegNosByContentID := map[int][]uint64{0: walSegNos}
	storedRestorePoints := []testRestorePoint{
		{restorePoint, baseFinishTime, baseWalTimeLine},
		{laterRestorePoint, baseFinishTime.Add(time.Minute), baseWalTimeLine},
	}

	folder := makeTrimWalFolder(
		t, backupName, restorePoint, segments, walSegNosByContentID, storedRestorePoints,
	)

	delArgs := greenplum.DeleteArgs{Confirmed: false}
	handler, err := greenplum.NewDeleteHandler(folder, delArgs)
	require.NoError(t, err)

	err = handler.HandleDeleteTrimWal(context.Background(), backupName)
	require.NoError(t, err)

	objectPaths := getStorageObjectsPaths(t, folder)
	for _, segNo := range walSegNos {
		assert.Contains(t, objectPaths, walObjectPath(0, segNo, baseWalTimeLine), "dry run must not delete WAL segNo %d", segNo)
	}
	// later_restore_point would be deleted in confirmed mode, but dry run must keep it
	laterRestorePointFilePath := path.Join(utility.BaseBackupPath, laterRestorePoint+greenplum.RestorePointSuffix)
	assert.Contains(t, objectPaths, laterRestorePointFilePath, "dry run must not delete restore point files")
}

func TestHandleDeleteTrimWal_DeletesRestorePointsAfterTarget(t *testing.T) {
	backupName := "backup_20260101T000000Z"
	restorePoint := "test_restore_point"
	beforeRestorePoint := "before_restore_point"
	afterRestorePoint := "after_restore_point"
	segments := []greenplum.SegmentMetadata{
		{ContentID: 0, RestorePointLSN: walCutoffLSN(2)},
	}
	walSegNos := []uint64{1}
	walSegNosByContentID := map[int][]uint64{0: walSegNos}
	storedRestorePoints := []testRestorePoint{
		{restorePoint, baseFinishTime, baseWalTimeLine},
		{beforeRestorePoint, baseFinishTime.Add(-time.Minute), baseWalTimeLine},
		{afterRestorePoint, baseFinishTime.Add(time.Minute), baseWalTimeLine},
	}
	folder := makeTrimWalFolder(
		t, backupName, restorePoint, segments, walSegNosByContentID, storedRestorePoints,
	)

	delArgs := greenplum.DeleteArgs{Confirmed: true}
	handler, err := greenplum.NewDeleteHandler(folder, delArgs)
	require.NoError(t, err)

	err = handler.HandleDeleteTrimWal(context.Background(), backupName)
	require.NoError(t, err)

	objectPaths := getStorageObjectsPaths(t, folder)
	targetFile := path.Join(utility.BaseBackupPath, restorePoint+greenplum.RestorePointSuffix)
	assert.Contains(t, objectPaths, targetFile, "target restore point must be kept")

	beforeFile := path.Join(utility.BaseBackupPath, beforeRestorePoint+greenplum.RestorePointSuffix)
	assert.Contains(t, objectPaths, beforeFile, "restore point before target must be kept")

	afterFile := path.Join(utility.BaseBackupPath, afterRestorePoint+greenplum.RestorePointSuffix)
	assert.NotContains(t, objectPaths, afterFile, "restore point after target must be deleted")
}

func TestHandleDeleteTrimWal_NoRestorePoint(t *testing.T) {
	backupName := "backup_20260101T000000Z"
	restorePoint := ""
	segments := []greenplum.SegmentMetadata{
		{ContentID: 0, RestorePointLSN: walCutoffLSN(2)},
	}
	walSegNos := []uint64{1, 3}
	walSegNosByContentID := map[int][]uint64{0: walSegNos}
	folder := makeTrimWalFolder(
		t, backupName, restorePoint, segments, walSegNosByContentID, nil,
	)

	delArgs := greenplum.DeleteArgs{Confirmed: true}
	handler, err := greenplum.NewDeleteHandler(folder, delArgs)
	require.NoError(t, err)
	err = handler.HandleDeleteTrimWal(context.Background(), backupName)
	require.NoError(t, err)

	objectPaths := getStorageObjectsPaths(t, folder)
	assert.Contains(t, objectPaths, walObjectPath(0, 1, baseWalTimeLine), "WAL at or before cutoff must be kept")
	assert.NotContains(t, objectPaths, walObjectPath(0, 3, baseWalTimeLine), "WAL past cutoff must be deleted")
}

func TestHandleDeleteTrimWal_MultipleSegments(t *testing.T) {
	backupName := "backup_20260101T000000Z"
	restorePoint := "test_restore_point"
	segments := []greenplum.SegmentMetadata{
		{ContentID: -1, RestorePointLSN: walCutoffLSN(1)},
		{ContentID: 0, RestorePointLSN: walCutoffLSN(2)},
	}
	walSegNosByContentID := map[int][]uint64{
		-1: {1, 2, 3},
		0:  {1, 2, 3},
	}
	storedRestorePoints := []testRestorePoint{
		{restorePoint, baseFinishTime, baseWalTimeLine},
	}
	folder := makeTrimWalFolder(
		t, backupName, restorePoint, segments, walSegNosByContentID, storedRestorePoints,
	)

	delArgs := greenplum.DeleteArgs{Confirmed: true}
	handler, err := greenplum.NewDeleteHandler(folder, delArgs)
	require.NoError(t, err)

	err = handler.HandleDeleteTrimWal(context.Background(), backupName)
	require.NoError(t, err)

	objectPaths := getStorageObjectsPaths(t, folder)
	assert.Contains(t, objectPaths, walObjectPath(-1, 1, baseWalTimeLine))
	assert.NotContains(t, objectPaths, walObjectPath(-1, 2, baseWalTimeLine))
	assert.NotContains(t, objectPaths, walObjectPath(-1, 3, baseWalTimeLine))

	assert.Contains(t, objectPaths, walObjectPath(0, 1, baseWalTimeLine))
	assert.Contains(t, objectPaths, walObjectPath(0, 2, baseWalTimeLine))
	assert.NotContains(t, objectPaths, walObjectPath(0, 3, baseWalTimeLine))
}

func TestHandleDeleteTrimWal_SkipsWalFromOtherTimeline(t *testing.T) {
	backupName := "backup_20260101T000000Z"
	restorePoint := "test_restore_point"
	segments := []greenplum.SegmentMetadata{
		{ContentID: 0, RestorePointLSN: walCutoffLSN(2)},
	}
	walSegNosByContentID := map[int][]uint64{0: {1, 2, 3, 4}}
	storedRestorePoints := []testRestorePoint{
		{restorePoint, baseFinishTime, baseWalTimeLine},
	}
	folder := makeTrimWalFolder(
		t, backupName, restorePoint, segments, walSegNosByContentID, storedRestorePoints,
	)

	const otherTimeline = uint32(2)
	segNosAfterCutoffOnOtherTimeline := []uint64{3, 4}
	for _, segNo := range segNosAfterCutoffOnOtherTimeline {
		err := folder.PutObject(walObjectPath(0, segNo, otherTimeline), &bytes.Buffer{})
		require.NoError(t, err)
	}

	delArgs := greenplum.DeleteArgs{Confirmed: true}
	handler, err := greenplum.NewDeleteHandler(folder, delArgs)
	require.NoError(t, err)

	err = handler.HandleDeleteTrimWal(context.Background(), backupName)
	require.NoError(t, err)

	objectPaths := getStorageObjectsPaths(t, folder)
	assert.NotContains(t, objectPaths, walObjectPath(0, 3, baseWalTimeLine), "WAL on cutoff timeline past cutoff must be deleted")
	assert.NotContains(t, objectPaths, walObjectPath(0, 4, baseWalTimeLine), "WAL on cutoff timeline past cutoff must be deleted")

	assert.Contains(t, objectPaths, walObjectPath(0, 3, otherTimeline), "WAL on other timeline must be kept")
	assert.Contains(t, objectPaths, walObjectPath(0, 4, otherTimeline), "WAL on other timeline must be kept")
}

func TestHandleDeleteTrimWal_BackupNotFound(t *testing.T) {
	folder := makeTrimWalFolder(t, "backup_20260101T000000Z", "", nil, nil, nil)

	delArgs := greenplum.DeleteArgs{Confirmed: true}
	handler, err := greenplum.NewDeleteHandler(folder, delArgs)
	require.NoError(t, err)

	err = handler.HandleDeleteTrimWal(context.Background(), "backup_nonexistent")
	assert.Error(t, err)
}
