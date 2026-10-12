package greenplum

import (
	"context"
	"fmt"
	"slices"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum/ao"
	"github.com/wal-g/wal-g/internal/databases/greenplum/pax"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type referencedFilesFetcher func(
	ctx context.Context,
	baseBackupsFolder storage.Folder,
	backupTime internal.BackupTime,
) (map[string]int64, error)

// ReassignSharedSizes refreshes the cluster-level AO/AOCS and PAX sizes of backups whose
// preceding survivor changed. It leaves the original segment files metadata unchanged.
func ReassignSharedSizes(ctx context.Context, rootFolder storage.Folder, backupNames []string, confirmed bool) error {
	if !confirmed {
		return nil
	}

	for _, backupName := range backupNames {
		if err := reassignSharedStorage(ctx, rootFolder, backupName); err != nil {
			return fmt.Errorf("failed to recalculate backup %s shared sizes: %w", backupName, err)
		}
	}

	return nil
}

// reassignSharedStorage recalculates the shared-storage ownership of a surviving cluster backup
// and uploads only its small cluster-level SharedSizeDTO objects. Segment files metadata remains
// unchanged: its UploadedSharedSize describes the initial upload.
func reassignSharedStorage(ctx context.Context, rootFolder storage.Folder, backupName string) error {
	// AO shared storage
	if err := uploadSharedSize(ctx, rootFolder, backupName, ao.GetFilesMetadataPath, readReassignedAOSize); err != nil {
		return fmt.Errorf("failed to reassign the AO shared size: %w", err)
	}
	// PAX shared storage
	if err := uploadSharedSize(ctx, rootFolder, backupName, pax.GetFilesMetadataPath, readReassignedPaxSize); err != nil {
		return fmt.Errorf("failed to reassign the PAX shared size: %w", err)
	}
	return nil
}

func readReassignedAOSize(
	ctx context.Context,
	baseBackupsFolder storage.Folder,
	backupName string,
) (int64, error) {
	return reassignedSharedSize(ctx, baseBackupsFolder, backupName, ao.FetchReferencedFiles)
}

func readReassignedPaxSize(
	ctx context.Context,
	baseBackupsFolder storage.Folder,
	backupName string,
) (int64, error) {
	return reassignedSharedSize(ctx, baseBackupsFolder, backupName, pax.FetchReferencedFiles)
}

func reassignedSharedSize(
	ctx context.Context,
	baseBackupsFolder storage.Folder,
	backupName string,
	fetchReferencedFiles referencedFilesFetcher,
) (int64, error) {
	// Stage 1: get all surviving backups in chronological order. Only their sentinels are read here.
	backupObjects, _, err := baseBackupsFolder.ListFolder(ctx)
	if err != nil {
		return 0, err
	}

	backupTimes := internal.GetBackupTimeSlices(backupObjects)
	internal.SortBackupTimeSlices(backupTimes)
	backupIndex := slices.IndexFunc(backupTimes, func(backup internal.BackupTime) bool {
		return backup.BackupName == backupName
	})
	if backupIndex == -1 {
		return 0, fmt.Errorf("backup %s not found", backupName)
	}

	// Stage 2: load referenced paths and sizes only for the affected backup and, if present, its
	// immediately preceding surviving backup.
	files, err := fetchReferencedFiles(ctx, baseBackupsFolder, backupTimes[backupIndex])
	if err != nil {
		return 0, err
	}

	var previousReferences map[string]int64
	if backupIndex > 0 {
		previousReferences, err = fetchReferencedFiles(ctx, baseBackupsFolder, backupTimes[backupIndex-1])
		if err != nil {
			return 0, fmt.Errorf("the preceding backup files metadata is unavailable: %w", err)
		}
	}

	// Stage 3: the affected backup owns every object absent from its current predecessor.
	ownedSize := int64(0)
	for storagePath, size := range files {
		if _, wasReferenced := previousReferences[storagePath]; !wasReferenced {
			ownedSize += size
		}
	}
	return ownedSize, nil
}

func listBackupsInOrder(ctx context.Context, baseBackupsFolder storage.Folder) ([]internal.BackupTime, error) {
	backupObjects, _, err := baseBackupsFolder.ListFolder(ctx)
	if err != nil {
		return nil, err
	}

	backups := internal.GetBackupTimeSlices(backupObjects)
	internal.SortBackupTimeSlices(backups)
	return backups, nil
}

// backupsWithChangedPredecessor returns surviving backups whose immediately preceding survivor
// changed between the two ordered snapshots. These are the only backups whose shared-storage
// ownership changes after deletion.
func backupsWithChangedPredecessor(before, after []internal.BackupTime) []string {
	previousBefore := make(map[string]string, len(before))
	previous := ""
	for _, backup := range before {
		previousBefore[backup.BackupName] = previous
		previous = backup.BackupName
	}

	affected := make([]string, 0)
	previous = ""
	for _, backup := range after {
		oldPrevious, existedBefore := previousBefore[backup.BackupName]
		if existedBefore && oldPrevious != previous {
			affected = append(affected, backup.BackupName)
		}
		previous = backup.BackupName
	}
	return affected
}
