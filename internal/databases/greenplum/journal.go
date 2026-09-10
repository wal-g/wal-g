package greenplum

import (
	"context"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// ClusterJournalDir is empty on purpose: a Greenplum cluster has no WAL directory of its own,
// every segment archives into its own segments_005/seg<N>/wal_005/.
const ClusterJournalDir = ""

// UpdateClusterIntervalSize stores the WAL volume the segments accumulated between the backup
// preceding ji and ji itself as that backup's SizeToNextBackup. Cluster-wide counterpart of
// JournalInfo.UpdateIntervalSize; rootFolder is the cluster root. See README.journal.md for why
// the aggregate is not a wall-clock window.
func UpdateClusterIntervalSize(ctx context.Context, rootFolder storage.Folder, ji internal.JournalInfo) error {
	prevJi, ok, err := ji.Previous(ctx, rootFolder)
	if err != nil || !ok {
		return err
	}

	backupName := strings.TrimPrefix(prevJi.JournalName, internal.JournalPrefix)
	sum, ok, err := SumSegmentWalSize(ctx, rootFolder, backupName)
	if err != nil {
		return err
	}
	if !ok {
		// Not measured rather than zero: keep the value the backup-push wrote.
		tracelog.WarningLogger.Printf("Can not determine the WAL volume of backup %s, "+
			"leaving its SizeToNextBackup intact", backupName)
		return nil
	}

	prevJi.SizeToNextBackup = sum

	return prevJi.Upload(ctx, rootFolder)
}

// DeleteClusterJournalInfo removes the cluster-wide journal of backupName and re-links its
// neighbors. Cluster-wide counterpart of internal.DeleteJournalInfo, differing only in how the
// merged interval is recalculated. A backup pushed without journal counting has none, which is
// not an error.
func DeleteClusterJournalInfo(ctx context.Context, rootFolder storage.Folder, backupName string, confirmed bool) {
	journalInfo, err := internal.NewJournalInfo(ctx, backupName, rootFolder, ClusterJournalDir)
	if err != nil {
		tracelog.WarningLogger.Printf("Can't find the journal info: %s", err.Error())
		return
	}

	if !confirmed {
		tracelog.InfoLogger.Printf("Journal info to delete: %+v", journalInfo)
		return
	}

	newerJi, ok, err := journalInfo.Unlink(ctx, rootFolder)
	if err != nil {
		tracelog.ErrorLogger.Print(err)
		return
	}

	if ok {
		if err := UpdateClusterIntervalSize(ctx, rootFolder, newerJi); err != nil {
			tracelog.ErrorLogger.Print(err)
			return
		}
	}

	tracelog.InfoLogger.Printf("Deleted journal info: %+v", journalInfo)
}

// SumSegmentWalSize is the volume of WAL the segments archived between backupName and the backup
// following it, taken from the journal every segment keeps for itself. ok == false when any
// segment fails to report: a partial sum would understate the volume rather than be missing.
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

// segmentsOfBackup lists the segment backups a cluster backup is made of, ok == false when the
// backup itself is gone: a journal can outlive its backup.
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
