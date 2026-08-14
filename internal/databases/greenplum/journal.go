package greenplum

import (
	"context"
	"fmt"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum/pax"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// ClusterJournalDir is empty on purpose. Unlike a single Postgres instance, a Greenplum cluster has
// no WAL directory of its own: every segment archives into its own segments_005/seg<N>/wal_005/.
// The cluster-wide journal is summed from the per-segment journals instead of being measured
// against a directory, see SegmentsSizeCalculator.
const ClusterJournalDir = ""

// SegmentsSizeCalculator derives the WAL volume of a whole Greenplum backup by summing the
// per-segment journals, each of which is maintained by the WAL-G instance running on that segment's
// host during seg-backup-push.
//
// The sum spans the intervals the segments measured for themselves, which do not line up with a
// single wall-clock window: a segment finishes its backup-push before the coordinator creates the
// restore point, so WAL archived in between belongs to that segment's next interval. Nothing is
// lost or double counted along the chain, but the aggregate is not the same as the volume archived
// cluster-wide between two backup finish times.
type SegmentsSizeCalculator struct{}

// Calculate sums the SizeToNextBackup of the segment journals belonging to prevJi's backup.
// The rootFolder argument is the cluster root, the same folder the cluster journal itself lives in.
//
// A partial sum would silently understate the real volume and be indistinguishable from a genuinely
// small one, so a single unreadable segment journal makes the whole aggregate unavailable (ok=false)
// rather than wrong.
func (SegmentsSizeCalculator) Calculate(
	ctx context.Context,
	rootFolder storage.Folder,
	_, prevJi internal.JournalInfo,
) (int64, bool, error) {
	backupName := strings.TrimPrefix(prevJi.JournalName, internal.JournalPrefix)

	return SumSegmentWalSize(ctx, rootFolder, backupName)
}

// SumSegmentWalSize is the volume of WAL the segments archived between backupName and the backup
// following it, taken from the journal every segment keeps for itself.
//
// A partial sum would silently understate the real volume and be indistinguishable from a genuinely
// small one, so a single segment failing to report makes the whole aggregate unavailable (ok=false)
// rather than wrong.
func SumSegmentWalSize(ctx context.Context, rootFolder storage.Folder, backupName string) (int64, bool, error) {
	segments, ok, err := segmentsOfBackup(ctx, rootFolder, backupName)
	if err != nil || !ok {
		return 0, false, err
	}

	sum := int64(0)
	missing := make([]int, 0)
	for _, meta := range segments {
		if meta.BackupName == "" {
			// Written by a WAL-G old enough not to record the segment backup name in the sentinel.
			tracelog.WarningLogger.Printf("Sentinel does not name the backup of segment %d", meta.ContentID)
			missing = append(missing, meta.ContentID)
			continue
		}

		segFolder := rootFolder.GetSubFolder(FormatSegmentStoragePrefix(meta.ContentID))
		ji, err := internal.NewJournalInfo(ctx, meta.BackupName, segFolder, utility.WalPath)
		if err != nil {
			// The segment backup was pushed without journal counting, or its journal is already gone.
			tracelog.WarningLogger.Printf("Can not read the journal of segment %d backup %s: %v",
				meta.ContentID, meta.BackupName, err)
			missing = append(missing, meta.ContentID)
			continue
		}

		sum += ji.SizeToNextBackup
	}

	if len(missing) > 0 {
		tracelog.WarningLogger.Printf("Journals of backup %s are unavailable on segments %v, "+
			"the cluster-wide WAL volume can not be calculated", backupName, missing)
		return 0, false, nil
	}

	tracelog.DebugLogger.Printf("Backup %s accumulated %d bytes of WAL over %d segments",
		backupName, sum, len(segments))

	return sum, true, nil
}

// SumSegmentSharedSize is the volume backupName uploaded to the shared AO/AOCS and PAX storage,
// taken from the files metadata every segment writes next to its backup. The segments do not put it
// in their journals: only the cluster-wide journal carries the shared volume.
//
// As with the WAL volume, a single segment failing to report makes the whole aggregate unavailable.
func SumSegmentSharedSize(ctx context.Context, rootFolder storage.Folder, backupName string) (int64, bool, error) {
	segments, ok, err := segmentsOfBackup(ctx, rootFolder, backupName)
	if err != nil || !ok {
		return 0, false, err
	}

	sum := int64(0)
	missing := make([]int, 0)
	for _, meta := range segments {
		if meta.BackupName == "" {
			tracelog.WarningLogger.Printf("Sentinel does not name the backup of segment %d", meta.ContentID)
			missing = append(missing, meta.ContentID)
			continue
		}

		segFolder := rootFolder.GetSubFolder(FormatSegmentStoragePrefix(meta.ContentID))
		sharedSize, err := readUploadedSharedSize(ctx, segFolder, meta.BackupName)
		if err != nil {
			// The segment backup was pushed by a WAL-G old enough not to report the uploaded volume.
			tracelog.WarningLogger.Printf("Can not read the shared size uploaded by segment %d backup %s: %v",
				meta.ContentID, meta.BackupName, err)
			missing = append(missing, meta.ContentID)
			continue
		}

		sum += sharedSize
	}

	if len(missing) > 0 {
		tracelog.WarningLogger.Printf("Files metadata of backup %s is unavailable on segments %v, "+
			"the cluster-wide shared volume can not be calculated", backupName, missing)
		return 0, false, nil
	}

	tracelog.DebugLogger.Printf("Backup %s uploaded %d bytes to the shared storage over %d segments",
		backupName, sum, len(segments))

	return sum, true, nil
}

// segmentsOfBackup lists the segment backups a cluster backup is made of. It reports ok == false
// when the backup itself is gone: a journal can outlive its backup, since delete modes other than
// 'delete target' remove the backup without touching the journals.
func segmentsOfBackup(ctx context.Context, rootFolder storage.Folder, backupName string,
) ([]SegmentMetadata, bool, error) {
	backup, err := NewBackup(rootFolder, backupName)
	if err != nil {
		return nil, false, err
	}

	sentinel, err := backup.GetSentinel(ctx)
	if err != nil {
		tracelog.WarningLogger.Printf("Can not read the sentinel of backup %s, "+
			"the cluster-wide value can not be calculated: %v", backupName, err)
		return nil, false, nil
	}

	return sentinel.Segments, true, nil
}

// aoFilesMetadataSizeView is the part of AOFilesMetadataDTO needed to learn the uploaded volume.
type aoFilesMetadataSizeView struct {
	UploadedSharedSize int64
}

// paxFilesMetadataSizeView is the part of  pax.FilesMetadataDTO needed to learn the uploaded volume.
type paxFilesMetadataSizeView struct {
	UploadedSharedSize int64
}

// readUploadedSharedSize is the AO/AOCS plus the PAX volume the backup uploaded, as recorded in the
// two files metadata objects stored next to it.
func readUploadedSharedSize(ctx context.Context, rootFolder storage.Folder, backupName string) (int64, error) {
	baseBackupsFolder := rootFolder.GetSubFolder(utility.BaseBackupPath)

	var aoMeta aoFilesMetadataSizeView
	if err := internal.FetchDto(ctx, baseBackupsFolder, &aoMeta, getAOFilesMetadataPath(backupName)); err != nil {
		return 0, fmt.Errorf("failed to fetch the AO files metadata: %w", err)
	}

	var paxMeta paxFilesMetadataSizeView
	if err := internal.FetchDto(ctx, baseBackupsFolder, &paxMeta, pax.GetFilesMetadataPath(backupName)); err != nil {
		return 0, fmt.Errorf("failed to fetch the PAX files metadata: %w", err)
	}

	return aoMeta.UploadedSharedSize + paxMeta.UploadedSharedSize, nil
}
