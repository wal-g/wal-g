package greenplum_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

const (
	gpBackupName     = "backup_20260721T120000Z"
	prevGpBackupName = "backup_20260721T100000Z"
)

// putSegmentJournal writes the journal a segment WAL-G instance would have left behind after
// pushing segBackupName and then measuring the WAL it archived until the next backup.
func putSegmentJournal(t *testing.T, root storage.Folder, contentID int, segBackupName string, size int64) {
	t.Helper()

	ji := internal.NewEmptyJournalInfo(segBackupName, time.Time{}, time.Now(), utility.WalPath)
	ji.SizeToNextBackup = size

	segFolder := root.GetSubFolder(greenplum.FormatSegmentStoragePrefix(contentID))
	require.NoError(t, ji.Upload(t.Context(), segFolder))
}

// putGpSentinel writes a cluster sentinel naming one segment backup per content ID.
func putGpSentinel(t *testing.T, root storage.Folder, backupName string, segBackupNames map[int]string) {
	t.Helper()

	segments := make([]greenplum.SegmentMetadata, 0, len(segBackupNames))
	for contentID, segBackupName := range segBackupNames {
		segments = append(segments, greenplum.SegmentMetadata{
			ContentID:  contentID,
			BackupName: segBackupName,
		})
	}

	putDTO(t, root, utility.BaseBackupPath+internal.SentinelNameFromBackup(backupName),
		greenplum.BackupSentinelDto{Segments: segments})
}

// prevJournal is the cluster journal the calculator is asked to fill in the size for.
func prevJournal(backupName string) internal.JournalInfo {
	return internal.NewEmptyJournalInfo(backupName, time.Time{}, time.Now(), greenplum.ClusterJournalDir)
}

func TestSegmentsSizeCalculator(t *testing.T) {
	segBackups := map[int]string{
		-1: "base_000000010000000000000001",
		0:  "base_000000010000000000000002",
		1:  "base_000000010000000000000003",
	}

	t.Run("sums the journals of every segment including the coordinator", func(t *testing.T) {
		root := testtools.MakeDefaultInMemoryStorageFolder()
		putGpSentinel(t, root, prevGpBackupName, segBackups)
		putSegmentJournal(t, root, -1, segBackups[-1], 100)
		putSegmentJournal(t, root, 0, segBackups[0], 200)
		putSegmentJournal(t, root, 1, segBackups[1], 300)

		size, ok, err := greenplum.SegmentsSizeCalculator{}.Calculate(
			t.Context(), root, internal.JournalInfo{}, prevJournal(prevGpBackupName))

		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, int64(600), size)
	})

	t.Run("reports no size when a single segment journal is missing", func(t *testing.T) {
		root := testtools.MakeDefaultInMemoryStorageFolder()
		putGpSentinel(t, root, prevGpBackupName, segBackups)
		putSegmentJournal(t, root, -1, segBackups[-1], 100)
		putSegmentJournal(t, root, 0, segBackups[0], 200)

		size, ok, err := greenplum.SegmentsSizeCalculator{}.Calculate(
			t.Context(), root, internal.JournalInfo{}, prevJournal(prevGpBackupName))

		require.NoError(t, err)
		assert.False(t, ok, "a partial sum would understate the real WAL volume")
		assert.Equal(t, int64(0), size)
	})

	t.Run("reports no size when the sentinel does not name the segment backup", func(t *testing.T) {
		root := testtools.MakeDefaultInMemoryStorageFolder()
		putGpSentinel(t, root, prevGpBackupName, map[int]string{-1: ""})

		_, ok, err := greenplum.SegmentsSizeCalculator{}.Calculate(
			t.Context(), root, internal.JournalInfo{}, prevJournal(prevGpBackupName))

		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("reports no size when the backup sentinel is gone", func(t *testing.T) {
		root := testtools.MakeDefaultInMemoryStorageFolder()

		_, ok, err := greenplum.SegmentsSizeCalculator{}.Calculate(
			t.Context(), root, internal.JournalInfo{}, prevJournal(prevGpBackupName))

		require.NoError(t, err, "a journal outliving its backup must not fail the whole backup-push")
		assert.False(t, ok)
	})
}

func TestUpdateIntervalSizeWithSegmentsCalculator(t *testing.T) {
	segBackups := map[int]string{-1: "base_000000010000000000000001", 0: "base_000000010000000000000002"}

	newClusterJournal := func(t *testing.T, root storage.Folder, backupName string, end time.Time) internal.JournalInfo {
		t.Helper()
		ji := internal.NewEmptyJournalInfo(backupName, time.Time{}, end, greenplum.ClusterJournalDir)
		require.NoError(t, ji.Upload(t.Context(), root))
		return ji
	}

	t.Run("stores the segment sum as SizeToNextBackup of the previous backup", func(t *testing.T) {
		root := testtools.MakeDefaultInMemoryStorageFolder()
		putGpSentinel(t, root, prevGpBackupName, segBackups)
		putSegmentJournal(t, root, -1, segBackups[-1], 100)
		putSegmentJournal(t, root, 0, segBackups[0], 200)

		prevJi := newClusterJournal(t, root, prevGpBackupName, time.Now().Add(-time.Hour))
		curJi := newClusterJournal(t, root, gpBackupName, time.Now())

		require.NoError(t, curJi.UpdateIntervalSize(t.Context(), root, greenplum.SegmentsSizeCalculator{}))

		require.NoError(t, prevJi.Read(t.Context(), root))
		assert.Equal(t, int64(300), prevJi.SizeToNextBackup)
		assert.Equal(t, int64(0), curJi.SizeToNextBackup, "the newest backup has nothing after it yet")
	})

	t.Run("leaves the previous size intact when the sum is unavailable", func(t *testing.T) {
		root := testtools.MakeDefaultInMemoryStorageFolder()
		putGpSentinel(t, root, prevGpBackupName, segBackups)
		putSegmentJournal(t, root, -1, segBackups[-1], 100)
		// The journal of segment 0 is missing, so the sum can not be trusted.

		prevJi := newClusterJournal(t, root, prevGpBackupName, time.Now().Add(-time.Hour))
		prevJi.SizeToNextBackup = 4096
		require.NoError(t, prevJi.Upload(t.Context(), root))

		curJi := newClusterJournal(t, root, gpBackupName, time.Now())

		require.NoError(t, curJi.UpdateIntervalSize(t.Context(), root, greenplum.SegmentsSizeCalculator{}))

		require.NoError(t, prevJi.Read(t.Context(), root))
		assert.Equal(t, int64(4096), prevJi.SizeToNextBackup, "must not be overwritten with a partial sum")
	})
}
