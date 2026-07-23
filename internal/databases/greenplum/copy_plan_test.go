package greenplum_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	copyutil "github.com/wal-g/wal-g/internal/copy"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
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
	segmentRoot := fmt.Sprintf("%s/seg%d", utility.SegmentsPath, -1)
	systemID := uint64(101)
	foreignSystemID := uint64(202)

	topSentinel := greenplum.BackupSentinelDto{
		RestorePoint:     &selectedPoint,
		FinishTime:       now,
		SystemIdentifier: &systemID,
		Segments: []greenplum.SegmentMetadata{{
			ContentID:       -1,
			BackupName:      segmentBackup,
			RestorePointLSN: fmt.Sprintf("0/%X", postgres.WalSegmentSize*4),
		}},
	}
	putDTO(t, from, path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(backupName)), topSentinel)
	putDTO(t, from, path.Join(segmentRoot, utility.BaseBackupPath, internal.SentinelNameFromBackup(segmentBackup)),
		postgres.BackupSentinelDto{})
	putDTO(t, from, path.Join(segmentRoot, utility.BaseBackupPath, segmentBackup, utility.MetadataFileName),
		postgres.ExtendedMetadataDto{
			StartLsn:  postgres.LSN(postgres.WalSegmentSize * 2),
			FinishLsn: postgres.LSN(postgres.WalSegmentSize * 3),
		})
	require.NoError(t, from.PutObject(t.Context(), path.Join(segmentRoot, utility.BaseBackupPath, segmentBackup, "part.tar.lz4"),
		bytes.NewBufferString("opaque-segment-backup")))

	putDTO(t, from, path.Join(utility.BaseBackupPath, greenplum.RestorePointMetadataFileName(selectedPoint)),
		greenplum.RestorePointMetadata{
			Name: selectedPoint, FinishTime: now.Add(time.Minute), TimeLine: 1, SystemIdentifier: &systemID,
			LsnBySegment: map[int]string{-1: fmt.Sprintf("0/%X", postgres.WalSegmentSize*4)},
		})
	putDTO(t, from, path.Join(utility.BaseBackupPath, greenplum.RestorePointMetadataFileName(latestPoint)),
		greenplum.RestorePointMetadata{
			Name: latestPoint, FinishTime: now.Add(2 * time.Minute), TimeLine: 1, SystemIdentifier: &systemID,
			LsnBySegment: map[int]string{-1: fmt.Sprintf("0/%X", postgres.WalSegmentSize*5)},
		})
	putDTO(t, from, path.Join(utility.BaseBackupPath, greenplum.RestorePointMetadataFileName(foreignPoint)),
		greenplum.RestorePointMetadata{
			Name: foreignPoint, FinishTime: now.Add(90 * time.Second), SystemIdentifier: &foreignSystemID,
			LsnBySegment: map[int]string{-1: fmt.Sprintf("0/%X", postgres.WalSegmentSize*4)},
		})
	putDTO(t, from, path.Join(utility.BaseBackupPath, greenplum.RestorePointMetadataFileName(topologyPoint)),
		greenplum.RestorePointMetadata{
			Name: topologyPoint, FinishTime: now.Add(105 * time.Second), SystemIdentifier: &systemID,
			LsnBySegment: map[int]string{
				-1: fmt.Sprintf("0/%X", postgres.WalSegmentSize*4),
				0:  fmt.Sprintf("0/%X", postgres.WalSegmentSize*4),
			},
		})
	putDTO(t, from, path.Join(utility.BaseBackupPath, greenplum.RestorePointMetadataFileName(foreignNewestPoint)),
		greenplum.RestorePointMetadata{
			Name: foreignNewestPoint, FinishTime: now.Add(3 * time.Minute), SystemIdentifier: &foreignSystemID,
			LsnBySegment: map[int]string{-1: fmt.Sprintf("0/%X", postgres.WalSegmentSize*5)},
		})
	for i := 1; i <= 5; i++ {
		walName := postgres.WalSegmentNo(i).GetFilename(1)
		require.NoError(t, from.PutObject(t.Context(), path.Join(segmentRoot, utility.WalPath, walName), bytes.NewBufferString("wal")))
	}

	plan, err := greenplum.BuildCopyPlan(t.Context(), from, to, backupName, true)
	require.NoError(t, err)
	targets := make(map[string]copyutil.Entry)
	for _, entry := range plan.Entries() {
		targets[entry.TargetPath] = entry
	}
	latestWal := path.Join(segmentRoot, utility.WalPath, postgres.WalSegmentNo(5).GetFilename(1))
	require.Contains(t, targets, latestWal)
	latestRestorePoint := path.Join(utility.BaseBackupPath, greenplum.RestorePointMetadataFileName(latestPoint))
	require.Contains(t, targets, latestRestorePoint)
	require.NotContains(t, targets,
		path.Join(utility.BaseBackupPath, greenplum.RestorePointMetadataFileName(foreignPoint)))
	require.NotContains(t, targets,
		path.Join(utility.BaseBackupPath, greenplum.RestorePointMetadataFileName(foreignNewestPoint)))
	require.NotContains(t, targets,
		path.Join(utility.BaseBackupPath, greenplum.RestorePointMetadataFileName(topologyPoint)))
	topSentinelPath := path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(backupName))
	require.Greater(t, targets[topSentinelPath].Phase, targets[latestRestorePoint].Phase)
}

func TestGreenplumCopyPlanSafelyInfersLegacyTimelineFromArchivedWAL(t *testing.T) {
	postgres.SetWalSize(64)
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	now := time.Now().UTC()
	backupName := "backup_20260721T130000Z"
	segmentBackup := "base_000000010000000000000002"
	restorePoint := backupName
	segmentRoot := fmt.Sprintf("%s/seg%d", utility.SegmentsPath, -1)
	restoreLSN := fmt.Sprintf("0/%X", postgres.WalSegmentSize*4)

	putDTO(t, from, path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(backupName)),
		greenplum.BackupSentinelDto{
			RestorePoint: &restorePoint,
			FinishTime:   now,
			Segments: []greenplum.SegmentMetadata{{
				ContentID:       -1,
				BackupName:      segmentBackup,
				RestorePointLSN: restoreLSN,
			}},
		})
	require.NoError(t, from.PutObject(t.Context(), path.Join(utility.BaseBackupPath, backupName, "part.tar.lz4"),
		bytes.NewBufferString("opaque-top-level-backup")))
	putDTO(t, from, path.Join(segmentRoot, utility.BaseBackupPath, internal.SentinelNameFromBackup(segmentBackup)),
		postgres.BackupSentinelDto{})
	putDTO(t, from, path.Join(segmentRoot, utility.BaseBackupPath, segmentBackup, utility.MetadataFileName),
		postgres.ExtendedMetadataDto{
			StartLsn:  postgres.LSN(postgres.WalSegmentSize * 2),
			FinishLsn: postgres.LSN(postgres.WalSegmentSize * 3),
		})
	require.NoError(t, from.PutObject(t.Context(), path.Join(segmentRoot, utility.BaseBackupPath, segmentBackup, "part.tar.lz4"),
		bytes.NewBufferString("opaque-segment-backup")))
	putDTO(t, from, path.Join(utility.BaseBackupPath, greenplum.RestorePointMetadataFileName(restorePoint)),
		greenplum.RestorePointMetadata{
			Name: restorePoint, FinishTime: now.Add(time.Minute),
			LsnBySegment: map[int]string{-1: restoreLSN},
		})
	for i := 1; i <= 4; i++ {
		walName := postgres.WalSegmentNo(i).GetFilename(1)
		require.NoError(t, from.PutObject(t.Context(), path.Join(segmentRoot, utility.WalPath, walName),
			bytes.NewBufferString("wal")))
	}

	plan, err := greenplum.BuildCopyPlan(t.Context(), from, to, backupName, false)
	require.NoError(t, err)
	targets := make(map[string]bool)
	for _, entry := range plan.Entries() {
		targets[entry.TargetPath] = true
	}
	lastWal := path.Join(segmentRoot, utility.WalPath, postgres.WalSegmentNo(4).GetFilename(1))
	require.True(t, targets[lastWal])

	secondTimelineWal := path.Join(segmentRoot, utility.WalPath, postgres.WalSegmentNo(4).GetFilename(2))
	require.NoError(t, from.PutObject(t.Context(), secondTimelineWal, bytes.NewBufferString("ambiguous-wal")))
	_, err = greenplum.BuildCopyPlan(t.Context(), from, to, backupName, false)
	require.ErrorContains(t, err, "found 2 endpoint WAL timelines")

	putDTO(t, from, path.Join(utility.BaseBackupPath, greenplum.RestorePointMetadataFileName(restorePoint)),
		greenplum.RestorePointMetadata{
			Name: restorePoint, FinishTime: now.Add(time.Minute),
			LsnBySegment:      map[int]string{-1: restoreLSN},
			TimelineBySegment: map[int]uint32{0: 1},
		})
	_, err = greenplum.BuildCopyPlan(t.Context(), from, to, backupName, false)
	require.ErrorContains(t, err, "has no timeline for segment -1")
}

func TestGreenplumCopyPlanUsesEachSegmentsRestorePointTimeline(t *testing.T) {
	postgres.SetWalSize(64)
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	now := time.Now().UTC()
	backupName := "backup_20260721T133000Z"
	selectedPoint := "rp_selected"
	latestPoint := "rp_latest"
	segments := []greenplum.SegmentMetadata{
		{ContentID: -1, BackupName: "base_000000010000000000000002"},
		{ContentID: 0, BackupName: "base_000000010000000000000002"},
	}
	putDTO(t, from, path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(backupName)),
		greenplum.BackupSentinelDto{RestorePoint: &selectedPoint, FinishTime: now, Segments: segments})

	for _, segment := range segments {
		segmentRoot := fmt.Sprintf("%s/seg%d", utility.SegmentsPath, segment.ContentID)
		putDTO(t, from, path.Join(segmentRoot, utility.BaseBackupPath,
			internal.SentinelNameFromBackup(segment.BackupName)), postgres.BackupSentinelDto{})
		putDTO(t, from, path.Join(segmentRoot, utility.BaseBackupPath, segment.BackupName, utility.MetadataFileName),
			postgres.ExtendedMetadataDto{
				StartLsn:  postgres.LSN(postgres.WalSegmentSize * 2),
				FinishLsn: postgres.LSN(postgres.WalSegmentSize * 3),
			})
		require.NoError(t, from.PutObject(t.Context(),
			path.Join(segmentRoot, utility.BaseBackupPath, segment.BackupName, "part.tar.lz4"),
			bytes.NewBufferString("segment-backup")))
	}
	putDTO(t, from, path.Join(utility.BaseBackupPath, greenplum.RestorePointMetadataFileName(selectedPoint)),
		greenplum.RestorePointMetadata{
			Name: selectedPoint, FinishTime: now.Add(time.Minute),
			LsnBySegment: map[int]string{
				-1: fmt.Sprintf("0/%X", postgres.WalSegmentSize*4),
				0:  fmt.Sprintf("0/%X", postgres.WalSegmentSize*4),
			},
			TimelineBySegment: map[int]uint32{-1: 1, 0: 1},
		})
	putDTO(t, from, path.Join(utility.BaseBackupPath, greenplum.RestorePointMetadataFileName(latestPoint)),
		greenplum.RestorePointMetadata{
			Name: latestPoint, FinishTime: now.Add(2 * time.Minute), TimeLine: 1,
			LsnBySegment: map[int]string{
				-1: fmt.Sprintf("0/%X", postgres.WalSegmentSize*5),
				0:  fmt.Sprintf("0/%X", postgres.WalSegmentSize*5),
			},
			TimelineBySegment: map[int]uint32{-1: 1, 0: 2},
		})

	coordinatorRoot := fmt.Sprintf("%s/seg%d", utility.SegmentsPath, -1)
	segmentRoot := fmt.Sprintf("%s/seg%d", utility.SegmentsPath, 0)
	for i := 1; i <= 5; i++ {
		require.NoError(t, from.PutObject(t.Context(),
			path.Join(coordinatorRoot, utility.WalPath, postgres.WalSegmentNo(i).GetFilename(1)),
			bytes.NewBufferString("coordinator-wal")))
	}
	for i := 1; i <= 4; i++ {
		require.NoError(t, from.PutObject(t.Context(),
			path.Join(segmentRoot, utility.WalPath, postgres.WalSegmentNo(i).GetFilename(1)),
			bytes.NewBufferString("segment-timeline-1")))
	}
	for i := 4; i <= 5; i++ {
		require.NoError(t, from.PutObject(t.Context(),
			path.Join(segmentRoot, utility.WalPath, postgres.WalSegmentNo(i).GetFilename(2)),
			bytes.NewBufferString("segment-timeline-2")))
	}

	plan, err := greenplum.BuildCopyPlan(t.Context(), from, to, backupName, true)
	require.NoError(t, err)
	targets := make(map[string]bool)
	for _, entry := range plan.Entries() {
		targets[entry.TargetPath] = true
	}
	require.True(t, targets[path.Join(coordinatorRoot, utility.WalPath, postgres.WalSegmentNo(5).GetFilename(1))])
	require.True(t, targets[path.Join(segmentRoot, utility.WalPath, postgres.WalSegmentNo(5).GetFilename(2))])
	require.False(t, targets[path.Join(segmentRoot, utility.WalPath, postgres.WalSegmentNo(5).GetFilename(1))])
}

func TestGreenplumCopyAllPreservesThreeLevelIncrementalCommitOrder(t *testing.T) {
	postgres.SetWalSize(16)
	from := testtools.MakeDefaultInMemoryStorageFolder()
	to := testtools.MakeDefaultInMemoryStorageFolder()
	now := time.Now().UTC()
	segmentRoot := fmt.Sprintf("%s/seg%d", utility.SegmentsPath, -1)
	topNames := []string{
		"backup_20260721T140000Z",
		"backup_20260721T150000Z",
		"backup_20260721T160000Z",
	}
	segmentNames := []string{
		"base_000000010000000000000001",
		"base_000000010000000000000002_D_000000010000000000000001",
		"base_000000010000000000000003_D_000000010000000000000002",
	}
	restorePoints := []string{"rp_full", "rp_child", "rp_grandchild"}
	restoreLSN := fmt.Sprintf("0/%X", postgres.WalSegmentSize*2)
	metadata := postgres.ExtendedMetadataDto{
		StartLsn:  postgres.LSN(postgres.WalSegmentSize * 2),
		FinishLsn: postgres.LSN(postgres.WalSegmentSize * 2),
	}

	for i := range topNames {
		topSentinel := greenplum.BackupSentinelDto{
			RestorePoint: &restorePoints[i],
			FinishTime:   now.Add(time.Duration(i) * time.Minute),
			Segments: []greenplum.SegmentMetadata{{
				ContentID:       -1,
				BackupName:      segmentNames[i],
				RestorePointLSN: restoreLSN,
			}},
		}
		segmentSentinel := postgres.BackupSentinelDto{}
		if i > 0 {
			topSentinel.IncrementFrom = &topNames[i-1]
			segmentSentinel.IncrementFrom = &segmentNames[i-1]
		}
		putDTO(t, from, path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(topNames[i])), topSentinel)
		require.NoError(t, from.PutObject(t.Context(), path.Join(utility.BaseBackupPath, topNames[i], "part.tar.lz4"),
			bytes.NewBufferString("top-"+topNames[i])))
		putDTO(t, from, path.Join(segmentRoot, utility.BaseBackupPath, internal.SentinelNameFromBackup(segmentNames[i])),
			segmentSentinel)
		putDTO(t, from, path.Join(segmentRoot, utility.BaseBackupPath, segmentNames[i], utility.MetadataFileName), metadata)
		require.NoError(t, from.PutObject(t.Context(),
			path.Join(segmentRoot, utility.BaseBackupPath, segmentNames[i], "part.tar.lz4"),
			bytes.NewBufferString("segment-"+segmentNames[i])))
		putDTO(t, from, path.Join(utility.BaseBackupPath,
			greenplum.RestorePointMetadataFileName(restorePoints[i])), greenplum.RestorePointMetadata{
			Name:         restorePoints[i],
			FinishTime:   now.Add(time.Duration(i) * time.Minute),
			TimeLine:     1,
			LsnBySegment: map[int]string{-1: restoreLSN},
		})
	}
	for i := 1; i <= 2; i++ {
		walName := postgres.WalSegmentNo(i).GetFilename(1)
		require.NoError(t, from.PutObject(t.Context(), path.Join(segmentRoot, utility.WalPath, walName),
			bytes.NewBufferString("wal")))
	}

	plan, err := greenplum.BuildCopyPlan(t.Context(), from, to, "", false)
	require.NoError(t, err)
	phases := make(map[string]uint16)
	for _, entry := range plan.Entries() {
		phases[entry.TargetPath] = uint16(entry.Phase)
	}
	for i := 1; i < len(topNames); i++ {
		parentTop := path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(topNames[i-1]))
		childTop := path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(topNames[i]))
		require.Less(t, phases[parentTop], phases[childTop])
		parentSegment := path.Join(segmentRoot, utility.BaseBackupPath,
			internal.SentinelNameFromBackup(segmentNames[i-1]))
		childSegment := path.Join(segmentRoot, utility.BaseBackupPath,
			internal.SentinelNameFromBackup(segmentNames[i]))
		require.Less(t, phases[parentSegment], phases[childSegment])
	}
	grandchildTop := path.Join(utility.BaseBackupPath, internal.SentinelNameFromBackup(topNames[2]))
	grandchildRestorePoint := path.Join(utility.BaseBackupPath,
		greenplum.RestorePointMetadataFileName(restorePoints[2]))
	require.Greater(t, phases[grandchildTop], phases[grandchildRestorePoint])
}

func putDTO(t *testing.T, folder storage.Folder, name string, value interface{}) {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	require.NoError(t, folder.PutObject(t.Context(), name, bytes.NewReader(data)))
}
