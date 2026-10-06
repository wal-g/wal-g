package greenplum_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	copyutil "github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/databases/greenplum/ao"
	"github.com/wal-g/wal-g/internal/databases/greenplum/pax"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func TestGreenplumWithHistoryStopsAtLatestClusterRestorePoint(t *testing.T) {
	postgres.SetWalSize(64)
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	now := time.Now().UTC()
	backupName := "backup_20260721T120000Z"
	segmentBackup := "base_000000010000000000000002"
	selectedPoint := "rp_selected"
	latestPoint := "rp_latest"
	foreignPoint := "rp_foreign"
	foreignNewestPoint := "rp_foreign_newest"
	topologyPoint := "rp_other_topology"
	segmentBase := greenplum.FormatSegmentBackupPath(-1)
	systemID := uint64(101)
	foreignSystemID := uint64(202)

	topSentinel := greenplum.BackupSentinelDto{
		RestorePoint:     &selectedPoint,
		FinishTime:       now,
		SystemIdentifier: &systemID,
		Segments: []greenplum.SegmentMetadata{{
			ContentID:       -1,
			BackupName:      segmentBackup,
			RestorePointLSN: lsnIn(4),
		}},
	}
	putDTO(t, from, path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(backupName)), topSentinel)
	putDTO(t, from, path.Join(segmentBase, internal.SentinelNameFromBackup(segmentBackup)), postgres.BackupSentinelDto{})
	putDTO(t, from, path.Join(segmentBase, segmentBackup, utility.MetadataFileName), postgres.ExtendedMetadataDto{
		StartLsn:  postgres.LSN(postgres.WalSegmentSize*2 + recordOffset),
		FinishLsn: postgres.LSN(postgres.WalSegmentSize*3 + recordOffset),
	})
	putObjects(t, from, map[string]string{path.Join(segmentBase, segmentBackup, "part.tar.lz4"): "opaque-segment-backup"})

	putDTO(t, from, restorePointObject(selectedPoint), greenplum.RestorePointMetadata{
		Name: selectedPoint, FinishTime: now.Add(time.Minute), TimeLine: 1, SystemIdentifier: &systemID,
		LsnBySegment: map[int]string{-1: lsnIn(4)},
	})
	putDTO(t, from, restorePointObject(latestPoint), greenplum.RestorePointMetadata{
		Name: latestPoint, FinishTime: now.Add(2 * time.Minute), TimeLine: 1, SystemIdentifier: &systemID,
		LsnBySegment: map[int]string{-1: lsnIn(5)},
	})
	putDTO(t, from, restorePointObject(foreignPoint), greenplum.RestorePointMetadata{
		Name: foreignPoint, FinishTime: now.Add(90 * time.Second), SystemIdentifier: &foreignSystemID,
		LsnBySegment: map[int]string{-1: lsnIn(4)},
	})
	putDTO(t, from, restorePointObject(topologyPoint), greenplum.RestorePointMetadata{
		Name: topologyPoint, FinishTime: now.Add(105 * time.Second), SystemIdentifier: &systemID,
		LsnBySegment: map[int]string{-1: lsnIn(4), 0: lsnIn(4)},
	})
	putDTO(t, from, restorePointObject(foreignNewestPoint), greenplum.RestorePointMetadata{
		Name: foreignNewestPoint, FinishTime: now.Add(3 * time.Minute), SystemIdentifier: &foreignSystemID,
		LsnBySegment: map[int]string{-1: lsnIn(5)},
	})
	putWal(t, from, -1, 1, 1, 2, 3, 4, 5)

	plan, err := greenplum.BuildCopyPlan(t.Context(), from, to, backupName, true)
	require.NoError(t, err)
	entries := planEntries(plan)
	require.Contains(t, entries, walObject(-1, 1, 5))
	require.Contains(t, entries, restorePointObject(latestPoint))
	require.NotContains(t, entries, restorePointObject(foreignPoint))
	require.NotContains(t, entries, restorePointObject(foreignNewestPoint))
	require.NotContains(t, entries, restorePointObject(topologyPoint))
	topSentinelPath := path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(backupName))
	require.Greater(t, entries[topSentinelPath].Phase, entries[restorePointObject(latestPoint)].Phase)
}

func TestGreenplumPlainCopyIncludesRestorePointWal(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	backupName := "backup_20260721T130000Z"
	putGreenplumRestorePointFixture(t, from, backupName, []int{-1}, nil)
	putWal(t, from, -1, 1, 1, 2, 3, 4, 5)

	plan, err := greenplum.BuildCopyPlan(t.Context(), from, to, backupName, false)
	require.NoError(t, err)
	entries := planEntries(plan)
	require.Contains(t, entries, walObject(-1, 1, 3))
	require.Contains(t, entries, walObject(-1, 1, 4))
	require.NotContains(t, entries, walObject(-1, 1, 5))
	require.Contains(t, entries, restorePointObject(backupName))
	require.Less(t, entries[walObject(-1, 1, 4)].Phase, entries[restorePointObject(backupName)].Phase)
}

func TestGreenplumPlainCopyRequiresRestorePointWal(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	backupName := "backup_20260721T131000Z"
	putGreenplumRestorePointFixture(t, from, backupName, []int{-1}, nil)
	putWal(t, from, -1, 1, 1, 2, 3)

	_, err := greenplum.BuildCopyPlan(t.Context(), from, to, backupName, false)
	require.ErrorContains(t, err, "is not published in the source storage yet")
}

func TestGreenplumPlainCopyUsesEachSegmentsRestorePointTimeline(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	backupName := "backup_20260721T132000Z"
	putGreenplumRestorePointFixture(t, from, backupName, []int{-1, 0}, map[int]uint32{-1: 1, 0: 2})
	putWal(t, from, -1, 1, 1, 2, 3, 4)
	putWal(t, from, 0, 1, 1, 2, 3, 4)
	putWal(t, from, 0, 2, 4)

	plan, err := greenplum.BuildCopyPlan(t.Context(), from, to, backupName, false)
	require.NoError(t, err)
	entries := planEntries(plan)
	require.Contains(t, entries, walObject(-1, 1, 4))
	require.Contains(t, entries, walObject(0, 2, 4))
}

func TestGreenplumCopyPlanUsesEachSegmentsRestorePointTimeline(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	backupName := "backup_20260721T133000Z"
	putGreenplumRestorePointFixture(t, from, backupName, []int{-1, 0}, map[int]uint32{-1: 1, 0: 1})
	putRestorePoint(t, from, "rp_latest", time.Now().UTC().Add(2*time.Minute), 1, map[int]uint32{-1: 1, 0: 2},
		map[int]string{-1: lsnIn(5), 0: lsnIn(5)})
	putWal(t, from, -1, 1, 1, 2, 3, 4, 5)
	putWal(t, from, 0, 1, 1, 2, 3, 4)
	putWal(t, from, 0, 2, 4, 5)

	plan, err := greenplum.BuildCopyPlan(t.Context(), from, to, backupName, true)
	require.NoError(t, err)
	entries := planEntries(plan)
	require.Contains(t, entries, walObject(-1, 1, 5))
	require.Contains(t, entries, walObject(0, 2, 5))
	require.NotContains(t, entries, walObject(0, 1, 5))
}

func TestGreenplumPlainCopyRejectsUnresolvableRestorePointTimeline(t *testing.T) {
	for expected, timelines := range map[string]map[int]uint32{
		"cannot safely infer timeline":     nil,
		"validate Greenplum restore point": {-1: 1},
	} {
		from := testtools.MakeDefaultInMemoryStorageFolder()
		to := testtools.MakeDefaultInMemoryStorageFolder()
		backupName := "backup_20260721T133000Z"
		putGreenplumRestorePointFixture(t, from, backupName, []int{-1, 0}, timelines)
		putWal(t, from, -1, 1, 1, 2, 3, 4)
		putWal(t, from, 0, 1, 1, 2, 3, 4)
		putWal(t, from, 0, 2, 4)

		_, err := greenplum.BuildCopyPlan(t.Context(), from, to, backupName, false)
		require.ErrorContains(t, err, expected)
	}
}

func TestGreenplumCopyRestorePointAtWalSegmentBoundary(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	backupName := "backup_20260721T134000Z"
	putGreenplumRestorePointFixture(t, from, backupName, []int{-1}, nil)
	putRestorePoint(t, from, backupName, time.Time{}, 1, nil,
		map[int]string{-1: fmt.Sprintf("0/%X", postgres.WalSegmentSize*4)})
	putWal(t, from, -1, 1, 1, 2, 3)

	_, err := greenplum.BuildCopyPlan(t.Context(), from, to, backupName, false)
	require.NoError(t, err)
}

func TestGreenplumCopyRequiresStopWalBeforeRestorePointTimelineSwitch(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	backupName := "backup_20260721T135000Z"
	putGreenplumRestorePointFixture(t, from, backupName, []int{-1, 0}, map[int]uint32{-1: 1, 0: 2})
	putWal(t, from, -1, 1, 1, 2, 3, 4)
	putWal(t, from, 0, 1, 2)
	putWal(t, from, 0, 2, 4)

	_, err := greenplum.BuildCopyPlan(t.Context(), from, to, backupName, false)
	require.ErrorContains(t, err, "stop WAL \"000000010000000000000003\" of Greenplum segment 0")
	require.NotContains(t, err.Error(), "not published")
}

func TestGreenplumWithHistoryReachesSelectedRestorePointBeyondLatest(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	backupName := "backup_20260721T140000Z"
	putGreenplumRestorePointFixture(t, from, backupName, []int{-1}, nil)
	putRestorePoint(t, from, "rp_concurrent", time.Now().UTC().Add(time.Minute), 1, nil,
		map[int]string{-1: lsnIn(3)})
	putWal(t, from, -1, 1, 1, 2, 3, 4, 5)

	plan, err := greenplum.BuildCopyPlan(t.Context(), from, to, backupName, true)
	require.NoError(t, err)
	entries := planEntries(plan)
	require.Contains(t, entries, walObject(-1, 1, 4))
	require.NotContains(t, entries, walObject(-1, 1, 5))
	require.Contains(t, entries, restorePointObject(backupName))
	require.Contains(t, entries, restorePointObject("rp_concurrent"))
}

func TestGreenplumCopyInfersLegacyCoordinatorTimeline(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	backupName := "backup_20260721T141000Z"
	putGreenplumRestorePointFixture(t, from, backupName, []int{-1}, nil)
	putRestorePoint(t, from, backupName, time.Time{}, 0, nil, map[int]string{-1: lsnIn(4)})
	putWal(t, from, -1, 1, 1, 2, 3, 4)

	plan, err := greenplum.BuildCopyPlan(t.Context(), from, to, backupName, false)
	require.NoError(t, err)
	require.Contains(t, planEntries(plan), walObject(-1, 1, 4))
}

func TestGreenplumWithHistorySkipsRestorePointPastCopiedWal(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	backupName := "backup_20260721T142000Z"
	now := time.Now().UTC()
	putGreenplumRestorePointFixture(t, from, backupName, []int{-1}, nil)
	putRestorePoint(t, from, "rp_ahead", now.Add(time.Minute), 1, nil, map[int]string{-1: lsnIn(6)})
	putRestorePoint(t, from, "rp_latest", now.Add(2*time.Minute), 1, nil, map[int]string{-1: lsnIn(5)})
	putWal(t, from, -1, 1, 1, 2, 3, 4, 5, 6)

	plan, err := greenplum.BuildCopyPlan(t.Context(), from, to, backupName, true)
	require.NoError(t, err)
	entries := planEntries(plan)
	require.Contains(t, entries, walObject(-1, 1, 5))
	require.NotContains(t, entries, walObject(-1, 1, 6))
	require.Contains(t, entries, restorePointObject("rp_latest"))
	require.NotContains(t, entries, restorePointObject("rp_ahead"))
}

func TestGreenplumCopyWaitsForRestorePointWal(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	backupName := "backup_20260721T143000Z"
	putGreenplumRestorePointFixture(t, from, backupName, []int{-1}, nil)
	putWal(t, from, -1, 1, 1, 2, 3)

	_, err := greenplum.BuildCopyPlanWaitingWithSleep(t.Context(), from, to, backupName, false, 0,
		func(context.Context, time.Duration) error {
			t.Fatal("must not wait without a timeout")
			return nil
		})
	require.ErrorContains(t, err, "is not published in the source storage yet")

	delays := make([]time.Duration, 0)
	plan, err := greenplum.BuildCopyPlanWaitingWithSleep(t.Context(), from, to, backupName, false, time.Hour,
		func(_ context.Context, delay time.Duration) error {
			delays = append(delays, delay)
			if len(delays) == 2 {
				putWal(t, from, -1, 1, 4)
			}
			return nil
		})
	require.NoError(t, err)
	require.Equal(t, []time.Duration{5 * time.Second, 10 * time.Second}, delays)
	require.Contains(t, planEntries(plan), walObject(-1, 1, 4))
}

func TestGreenplumCopyDoesNotWaitForMissingStopWal(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	backupName := "backup_20260721T144000Z"
	putGreenplumRestorePointFixture(t, from, backupName, []int{-1}, nil)
	putWal(t, from, -1, 1, 1, 2, 4)

	_, err := greenplum.BuildCopyPlanWaitingWithSleep(t.Context(), from, to, backupName, false, time.Hour,
		func(context.Context, time.Duration) error {
			t.Fatal("a missing stop WAL never arrives")
			return nil
		})
	require.ErrorContains(t, err, "stop WAL")
}

func TestGreenplumCopyAllPreservesThreeLevelIncrementalCommitOrder(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	backups := []greenplumBackupFixture{
		{topName: "backup_20260721T140000Z", segmentName: "base_000000010000000000000001"},
		{topName: "backup_20260721T150000Z", segmentName: "base_000000010000000000000002_D_000000010000000000000001"},
		{topName: "backup_20260721T160000Z", segmentName: "base_000000010000000000000003_D_000000010000000000000002"},
	}
	putGreenplumCoordinatorChain(t, from, backups)

	plan, err := greenplum.BuildCopyPlan(t.Context(), from, to, "", false)
	require.NoError(t, err)
	entries := planEntries(plan)
	for i := 1; i < len(backups); i++ {
		parentTop := path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(backups[i-1].topName))
		childTop := path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(backups[i].topName))
		require.Equal(t, copyutil.FinalCommitPhase, entries[parentTop].Phase)
		require.Equal(t, copyutil.FinalCommitPhase, entries[childTop].Phase)
		require.Less(t, entries[parentTop].Sequence, entries[childTop].Sequence)
		parentSegment := path.Join(coordinatorBasePath, internal.SentinelNameFromBackup(backups[i-1].segmentName))
		childSegment := path.Join(coordinatorBasePath, internal.SentinelNameFromBackup(backups[i].segmentName))
		require.Equal(t, copyutil.BackupCommitPhase, entries[parentSegment].Phase)
		require.Equal(t, copyutil.BackupCommitPhase, entries[childSegment].Phase)
		require.Less(t, entries[parentSegment].Sequence, entries[childSegment].Sequence)
	}
	grandchildTop := path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(backups[2].topName))
	grandchildRestorePoint := restorePointObject("rp_" + backups[2].topName)
	require.Greater(t, entries[grandchildTop].Phase, entries[grandchildRestorePoint].Phase)
}

func TestGreenplumCopyIncludesReferencedSharedFiles(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	backups := []greenplumBackupFixture{
		{
			topName:     "backup_20260722T100000Z",
			segmentName: "base_000000010000000000000001",
			aoFiles: ao.BackupFiles{
				"/base/1/100": {StoragePath: "A_aoseg"},
				"/base/1/200": {StoragePath: "OLD_aoseg", IsSkipped: true},
			},
			paxFiles: pax.BackupFiles{
				"/base/1/300_pax/0":       {StoragePath: "P0_pax"},
				"/base/1/300_pax/0.toast": {StoragePath: "P0_toast_pax"},
			},
		},
		{
			topName:     "backup_20260722T110000Z",
			segmentName: "base_000000010000000000000002_D_000000010000000000000001",
			aoFiles: ao.BackupFiles{
				"/base/1/100": {StoragePath: "A_D_5_aoseg", IsIncremented: true},
				"/base/1/200": {StoragePath: "OLD_aoseg", IsSkipped: true},
			},
			paxFiles: pax.BackupFiles{
				"/base/1/300_pax/0":             {StoragePath: "P0_pax", IsSkipped: true},
				"/base/1/300_pax/0_1_2.visimap": {StoragePath: "P0_1_2_visimap_pax"},
			},
		},
	}
	putGreenplumCoordinatorChain(t, from, backups)
	referenced := map[string]string{
		path.Join(coordinatorBasePath, ao.StoragePath, "A_aoseg"):             "ao-base",
		path.Join(coordinatorBasePath, ao.StoragePath, "A_D_5_aoseg"):         "ao-delta",
		path.Join(coordinatorBasePath, ao.StoragePath, "OLD_aoseg"):           "ao-old",
		path.Join(coordinatorBasePath, pax.StoragePath, "P0_pax"):             "pax-data",
		path.Join(coordinatorBasePath, pax.StoragePath, "P0_toast_pax"):       "pax-toast",
		path.Join(coordinatorBasePath, pax.StoragePath, "P0_1_2_visimap_pax"): "pax-visimap",
	}
	orphans := map[string]string{
		path.Join(coordinatorBasePath, ao.StoragePath, "ORPHAN_aoseg"): "ao-orphan",
		path.Join(coordinatorBasePath, pax.StoragePath, "ORPHAN_pax"):  "pax-orphan",
	}
	putObjects(t, from, referenced)
	putObjects(t, from, orphans)

	plan, err := greenplum.BuildCopyPlan(t.Context(), from, to, backups[1].topName, false)
	require.NoError(t, err)
	entries := planEntries(plan)
	for name := range referenced {
		require.Contains(t, entries, name)
		require.Equal(t, copyutil.PayloadPhase, entries[name].Phase)
	}
	for _, backup := range backups {
		segmentSentinel := path.Join(coordinatorBasePath, internal.SentinelNameFromBackup(backup.segmentName))
		require.Less(t, copyutil.PayloadPhase, entries[segmentSentinel].Phase)
	}
	for name := range orphans {
		require.NotContains(t, entries, name)
	}

	require.NoError(t, copyutil.ExecuteRaw(t.Context(), plan))
	for name, content := range referenced {
		require.Equal(t, content, readObject(t, to, name))
	}

	counting := &countingFolder{Folder: to}
	plan, err = greenplum.BuildCopyPlan(t.Context(), from, counting, backups[1].topName, false)
	require.NoError(t, err)
	require.NoError(t, copyutil.ExecuteRaw(t.Context(), plan))
	require.Zero(t, counting.puts.Load())
}

func TestGreenplumCopyFailsOnMissingSharedFile(t *testing.T) {
	for _, backup := range []greenplumBackupFixture{
		{
			topName:     "backup_20260722T120000Z",
			segmentName: "base_000000010000000000000001",
			aoFiles:     ao.BackupFiles{"/base/1/100": {StoragePath: "MISSING_aoseg"}},
		},
		{
			topName:     "backup_20260722T120000Z",
			segmentName: "base_000000010000000000000001",
			paxFiles:    pax.BackupFiles{"/base/1/300_pax/0": {StoragePath: "MISSING_pax"}},
		},
	} {
		from := testtools.MakeDefaultInMemoryStorageFolder()
		to := testtools.MakeDefaultInMemoryStorageFolder()
		putGreenplumCoordinatorChain(t, from, []greenplumBackupFixture{backup})

		_, err := greenplum.BuildCopyPlan(t.Context(), from, to, backup.topName, false)
		require.ErrorContains(t, err, "MISSING_")
	}
}

func TestGreenplumCopyWithoutSharedFilesMetadata(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	backup := greenplumBackupFixture{
		topName:     "backup_20260722T130000Z",
		segmentName: "base_000000010000000000000001",
	}
	putGreenplumCoordinatorChain(t, from, []greenplumBackupFixture{backup})
	putObjects(t, from, map[string]string{
		path.Join(coordinatorBasePath, ao.StoragePath, "UNREFERENCED_aoseg"): "ao",
		path.Join(coordinatorBasePath, pax.StoragePath, "UNREFERENCED_pax"):  "pax",
	})

	plan, err := greenplum.BuildCopyPlan(t.Context(), from, to, backup.topName, false)
	require.NoError(t, err)
	for name := range planEntries(plan) {
		require.NotContains(t, name, "UNREFERENCED")
	}
}

func TestGreenplumCopySkipsClusterSharedSize(t *testing.T) {
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	backup := greenplumBackupFixture{
		topName:     "backup_20260722T140000Z",
		segmentName: "base_000000010000000000000001",
		aoFiles:     ao.BackupFiles{"/base/1/100": {StoragePath: "A_aoseg"}},
		paxFiles:    pax.BackupFiles{"/base/1/300_pax/0": {StoragePath: "P0_pax"}},
	}
	putGreenplumCoordinatorChain(t, from, []greenplumBackupFixture{backup})
	putObjects(t, from, map[string]string{
		path.Join(coordinatorBasePath, ao.StoragePath, "A_aoseg"): "ao",
		path.Join(coordinatorBasePath, pax.StoragePath, "P0_pax"): "pax",
	})
	clusterShared := []string{
		path.Join(utility.BaseBackupPath, ao.GetFilesMetadataPath(backup.topName)),
		path.Join(utility.BaseBackupPath, pax.GetFilesMetadataPath(backup.topName)),
	}
	for _, name := range clusterShared {
		putDTO(t, from, name, greenplum.SharedSizeDTO{SharedSize: 7})
	}

	plan, err := greenplum.BuildCopyPlan(t.Context(), from, to, backup.topName, false)
	require.NoError(t, err)
	entries := planEntries(plan)
	for _, name := range clusterShared {
		require.NotContains(t, entries, name)
	}
	require.Contains(t, entries, path.Join(coordinatorBasePath, ao.GetFilesMetadataPath(backup.segmentName)))
	require.Contains(t, entries, path.Join(coordinatorBasePath, pax.GetFilesMetadataPath(backup.segmentName)))
}

const recordOffset = 0x28

var coordinatorBasePath = greenplum.FormatSegmentBackupPath(-1)

func lsnIn(walSegment uint64) string {
	return fmt.Sprintf("0/%X", postgres.WalSegmentSize*walSegment+recordOffset)
}

func walObject(contentID int, timeline uint32, walSegment uint64) string {
	return path.Join(greenplum.FormatSegmentWalPath(contentID), postgres.WalSegmentNo(walSegment).GetFilename(timeline))
}

func restorePointObject(name string) string {
	return path.Join(utility.BaseBackupPath, greenplum.RestorePointMetadataFileName(name))
}

func planEntries(plan *copyutil.Plan) map[string]copyutil.Entry {
	entries := make(map[string]copyutil.Entry)
	for _, entry := range plan.Entries() {
		entries[entry.TargetPath] = entry
	}
	return entries
}

type greenplumBackupFixture struct {
	topName     string
	segmentName string
	aoFiles     ao.BackupFiles
	paxFiles    pax.BackupFiles
}

func putGreenplumCoordinatorChain(t *testing.T, from storage.Folder, backups []greenplumBackupFixture) {
	t.Helper()
	postgres.SetWalSize(16)
	now := time.Now().UTC()
	metadata := postgres.ExtendedMetadataDto{
		StartLsn:  postgres.LSN(postgres.WalSegmentSize*2 + recordOffset),
		FinishLsn: postgres.LSN(postgres.WalSegmentSize*2 + recordOffset),
	}
	for i, backup := range backups {
		restorePoint := "rp_" + backup.topName
		topSentinel := greenplum.BackupSentinelDto{
			RestorePoint: &restorePoint,
			FinishTime:   now.Add(time.Duration(i) * time.Minute),
			Segments: []greenplum.SegmentMetadata{{
				ContentID:       -1,
				BackupName:      backup.segmentName,
				RestorePointLSN: lsnIn(2),
			}},
		}
		segmentSentinel := postgres.BackupSentinelDto{}
		if i > 0 {
			topSentinel.IncrementFrom = &backups[i-1].topName
			segmentSentinel.IncrementFrom = &backups[i-1].segmentName
		}
		putDTO(t, from, path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(backup.topName)), topSentinel)
		putDTO(t, from, path.Join(coordinatorBasePath, internal.SentinelNameFromBackup(backup.segmentName)),
			segmentSentinel)
		putDTO(t, from, path.Join(coordinatorBasePath, backup.segmentName, utility.MetadataFileName), metadata)
		putObjects(t, from, map[string]string{
			path.Join(coordinatorBasePath, backup.segmentName, "part.tar.lz4"): "segment-" + backup.segmentName,
		})
		if backup.aoFiles != nil {
			putDTO(t, from, path.Join(coordinatorBasePath, ao.GetFilesMetadataPath(backup.segmentName)),
				ao.FilesMetadataDTO{Files: backup.aoFiles})
		}
		if backup.paxFiles != nil {
			putDTO(t, from, path.Join(coordinatorBasePath, pax.GetFilesMetadataPath(backup.segmentName)),
				pax.FilesMetadataDTO{Files: backup.paxFiles})
		}
		putDTO(t, from, restorePointObject(restorePoint), greenplum.RestorePointMetadata{
			Name:         restorePoint,
			FinishTime:   now.Add(time.Duration(i) * time.Minute),
			TimeLine:     1,
			LsnBySegment: map[int]string{-1: lsnIn(2)},
		})
	}
	putWal(t, from, -1, 1, 1, 2)
}

func putGreenplumRestorePointFixture(
	t *testing.T,
	from storage.Folder,
	backupName string,
	contentIDs []int,
	timelineBySegment map[int]uint32,
) {
	t.Helper()
	postgres.SetWalSize(64)
	now := time.Now().UTC()
	segmentBackup := "base_000000010000000000000002"
	segments := make([]greenplum.SegmentMetadata, 0, len(contentIDs))
	lsnBySegment := make(map[int]string, len(contentIDs))
	for _, contentID := range contentIDs {
		segments = append(segments, greenplum.SegmentMetadata{
			ContentID: contentID, BackupName: segmentBackup, RestorePointLSN: lsnIn(4),
		})
		lsnBySegment[contentID] = lsnIn(4)
		segmentBase := greenplum.FormatSegmentBackupPath(contentID)
		putDTO(t, from, path.Join(segmentBase, internal.SentinelNameFromBackup(segmentBackup)), postgres.BackupSentinelDto{})
		putDTO(t, from, path.Join(segmentBase, segmentBackup, utility.MetadataFileName), postgres.ExtendedMetadataDto{
			StartLsn:  postgres.LSN(postgres.WalSegmentSize*2 + recordOffset),
			FinishLsn: postgres.LSN(postgres.WalSegmentSize*3 + recordOffset),
		})
		putObjects(t, from, map[string]string{path.Join(segmentBase, segmentBackup, "part.tar.lz4"): "segment-backup"})
	}
	putDTO(t, from, path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(backupName)),
		greenplum.BackupSentinelDto{RestorePoint: &backupName, FinishTime: now, Segments: segments})
	putDTO(t, from, restorePointObject(backupName), greenplum.RestorePointMetadata{
		Name:              backupName,
		FinishTime:        now,
		TimeLine:          1,
		LsnBySegment:      lsnBySegment,
		TimelineBySegment: timelineBySegment,
	})
}

func putRestorePoint(
	t *testing.T,
	from storage.Folder,
	name string,
	finishTime time.Time,
	timeLine uint32,
	timelineBySegment map[int]uint32,
	lsnBySegment map[int]string,
) {
	t.Helper()
	if finishTime.IsZero() {
		previous, err := greenplum.FetchRestorePointMetadata(t.Context(), from, name)
		require.NoError(t, err)
		finishTime = previous.FinishTime
	}
	putDTO(t, from, restorePointObject(name), greenplum.RestorePointMetadata{
		Name:              name,
		FinishTime:        finishTime,
		TimeLine:          timeLine,
		LsnBySegment:      lsnBySegment,
		TimelineBySegment: timelineBySegment,
	})
}

func putWal(t *testing.T, from storage.Folder, contentID int, timeline uint32, walSegments ...uint64) {
	t.Helper()
	for _, walSegment := range walSegments {
		putObjects(t, from, map[string]string{walObject(contentID, timeline, walSegment): "wal"})
	}
}

type countingFolder struct {
	storage.Folder
	puts atomic.Int64
}

func (folder *countingFolder) PutObject(ctx context.Context, name string, content io.Reader) error {
	folder.puts.Add(1)
	return folder.Folder.PutObject(ctx, name, content)
}

func putObjects(t *testing.T, folder storage.Folder, objects map[string]string) {
	t.Helper()
	for name, content := range objects {
		require.NoError(t, folder.PutObject(t.Context(), name, bytes.NewBufferString(content)))
	}
}

func readObject(t *testing.T, folder storage.Folder, name string) string {
	t.Helper()
	reader, err := folder.ReadObject(t.Context(), name)
	require.NoError(t, err)
	defer reader.Close()
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	return string(content)
}

func putDTO(t *testing.T, folder storage.Folder, name string, value interface{}) {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	require.NoError(t, folder.PutObject(t.Context(), name, bytes.NewReader(data)))
}
