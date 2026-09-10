package greenplum

import (
	"context"
	"fmt"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum/ao"
	"github.com/wal-g/wal-g/internal/databases/greenplum/pax"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// SharedSizeDTO is the cluster-level counterpart of the per-segment files metadata. A cluster
// uploads nothing of its own and has no shared file to list, so it records neither the file list
// nor the uploaded volume, only the total its segments are accountable for.
//
// It is stored under the same names the segments use, ao_files_metadata.json and
// pax_files_metadata.json, one folder level up. The name is shared but the shape is not: the two
// are told apart by the folder they are read from, and nothing reads the cluster-level one as
// ao.FilesMetadataDTO or vice versa (ao.LoadStorageAOFiles and pax.LoadStoragePaxFiles are only ever
// given a segment folder).
type SharedSizeDTO struct {
	// SharedSize is the volume, in bytes, the backup added to one of the shared storages. It is the
	// sum of what the segments uploaded when the backup is created, and is meant to be recalculated
	// later, when a neighboring backup is deleted and this one inherits objects still in use.
	SharedSize int64 `json:"SharedSize"`
}

type segmentSharedSizeReader func(
	ctx context.Context,
	baseBackupsFolder storage.Folder,
	backupName string,
) (int64, error)

// UploadSharedSizes writes the cluster-wide shared size of backupName, one object per shared
// storage, each summed from the segment metadata named in the backup sentinel.
//
// Unlike the journal this is recorded on every backup-push, with no flag and for permanent backups
// too: a permanent backup occupies the shared storage like any other, and leaving it out would
// break the property that the sizes of all backups add up to the size of the storage.
func UploadSharedSizes(ctx context.Context, rootFolder storage.Folder, backupName string) error {
	if err := uploadSharedSize(ctx, rootFolder, backupName, ao.GetFilesMetadataPath, readUploadedAOSize); err != nil {
		return fmt.Errorf("failed to upload the AO/AOCS shared size: %w", err)
	}
	if err := uploadSharedSize(ctx, rootFolder, backupName, pax.GetFilesMetadataPath, readUploadedPaxSize); err != nil {
		return fmt.Errorf("failed to upload the PAX shared size: %w", err)
	}
	return nil
}

func uploadSharedSize(
	ctx context.Context,
	rootFolder storage.Folder,
	backupName string,
	metadataPath func(backupName string) string,
	readSegmentSize segmentSharedSizeReader,
) error {
	segments, ok, err := segmentsOfBackup(ctx, rootFolder, backupName)
	if err != nil {
		return fmt.Errorf("failed to get backup segments: %w", err)
	}
	if !ok {
		return fmt.Errorf("can not read the sentinel of backup %s", backupName)
	}

	baseBackupsFolder := rootFolder.GetSubFolder(utility.BaseBackupPath)
	sum, ok := sumOverSegments(ctx, rootFolder, segments, readSegmentSize)
	if !ok {
		// A partial sum would understate the real volume and be indistinguishable from a
		// genuinely small one, so the object is left unwritten rather than written wrong. An
		// absent object means "never determined", which a zero could not express.
		tracelog.WarningLogger.Printf("The files metadata of backup %s is unavailable, "+
			"the cluster-wide shared volume can not be calculated", backupName)
		return nil
	}
	tracelog.DebugLogger.Printf("Backup %s added %d bytes to shared storage over %d segments",
		backupName, sum, len(segments))

	dto := SharedSizeDTO{SharedSize: sum}
	if err := internal.UploadDto(ctx, baseBackupsFolder, dto, metadataPath(backupName)); err != nil {
		return fmt.Errorf("failed to upload the cluster-wide shared size: %w", err)
	}
	return nil
}

// sumOverSegments adds up the volume for which every segment backup is accountable in one shared
// storage.
//
// A partial sum would silently understate the real volume, so a single segment failing to report
// makes the whole aggregate unavailable (ok == false) rather than wrong.
func sumOverSegments(ctx context.Context, rootFolder storage.Folder,
	segments []SegmentMetadata, readSegmentSize segmentSharedSizeReader) (int64, bool) {
	sum := int64(0)
	missing := make([]int, 0)
	for _, meta := range segments {
		if meta.BackupName == "" {
			// Written by a WAL-G old enough not to record the segment backup name in the sentinel.
			tracelog.WarningLogger.Printf("Sentinel does not name the backup of segment %d", meta.ContentID)
			missing = append(missing, meta.ContentID)
			continue
		}

		segFolder := rootFolder.GetSubFolder(FormatSegmentStoragePrefix(meta.ContentID)).
			GetSubFolder(utility.BaseBackupPath)
		size, err := readSegmentSize(ctx, segFolder, meta.BackupName)
		if err != nil {
			tracelog.WarningLogger.Printf("Can not calculate the shared size of segment %d backup %s: %v",
				meta.ContentID, meta.BackupName, err)
			missing = append(missing, meta.ContentID)
			continue
		}

		sum += size
	}

	if len(missing) > 0 {
		tracelog.WarningLogger.Printf("Shared files metadata is unavailable on segments %v", missing)
		return 0, false
	}

	return sum, true
}

func readUploadedAOSize(ctx context.Context, baseBackupsFolder storage.Folder, backupName string) (int64, error) {
	// aoUploadedSizeView is the parts of ao.FilesMetadataDTO needed to learn the uploaded volume
	type aoUploadedSizeView struct {
		UploadedSharedSize int64
	}

	var meta aoUploadedSizeView
	if err := internal.FetchDto(ctx, baseBackupsFolder, &meta, ao.GetFilesMetadataPath(backupName)); err != nil {
		return 0, err
	}
	return meta.UploadedSharedSize, nil
}

func readUploadedPaxSize(ctx context.Context, baseBackupsFolder storage.Folder, backupName string) (int64, error) {
	// paxUploadedSizeView is the parts of the pax.FilesMetadataDTO needed to learn the uploaded volume.
	type paxUploadedSizeView struct {
		UploadedSharedSize int64
	}

	var meta paxUploadedSizeView
	if err := internal.FetchDto(ctx, baseBackupsFolder, &meta, pax.GetFilesMetadataPath(backupName)); err != nil {
		return 0, err
	}
	return meta.UploadedSharedSize, nil
}

// FetchAOSharedSize is the volume backupName added to the shared AO/AOCS storage cluster-wide.
// A missing object is an error rather than a zero: it means the volume was never determined.
//
// rootFolder must be the cluster root. Given a segment folder this would read that segment's files
// metadata, which has no SharedSize, and report a zero.
func FetchAOSharedSize(ctx context.Context, rootFolder storage.Folder, backupName string) (int64, error) {
	return fetchSharedSize(ctx, rootFolder, ao.GetFilesMetadataPath(backupName))
}

// FetchPaxSharedSize is the volume backupName added to the shared PAX storage cluster-wide.
func FetchPaxSharedSize(ctx context.Context, rootFolder storage.Folder, backupName string) (int64, error) {
	return fetchSharedSize(ctx, rootFolder, pax.GetFilesMetadataPath(backupName))
}

func fetchSharedSize(ctx context.Context, rootFolder storage.Folder, path string) (int64, error) {
	var dto SharedSizeDTO
	if err := internal.FetchDto(ctx, rootFolder.GetSubFolder(utility.BaseBackupPath), &dto, path); err != nil {
		return 0, err
	}
	return dto.SharedSize, nil
}
