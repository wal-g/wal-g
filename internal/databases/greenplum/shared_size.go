package greenplum

import (
	"context"
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
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
// AOFilesMetadataDTO or vice versa (LoadStorageAoFiles and pax.LoadStoragePaxFiles are only ever
// given a segment folder).
type SharedSizeDTO struct {
	// SharedSize is the volume, in bytes, the backup added to one of the shared storages. It is the
	// sum of what the segments uploaded when the backup is created, and is meant to be recalculated
	// later, when a neighbouring backup is deleted and this one inherits objects still in use.
	SharedSize int64 `json:"SharedSize"`
}

// sharedStorageKind is one of the two storages shared between backups, described by everything the
// cluster-wide bookkeeping needs to know about it: where a segment reports what it uploaded, and
// where the cluster total is kept.
//
// Both live under basebackups_005/<backup>/, which is what makes the cluster-level objects
// disappear together with the backup: the path reduces to the backup name exactly like the sentinel
// does, so every delete mode picks them up without any code of its own (see
// utility.StripLeftmostBackupName).
type sharedStorageKind struct {
	name            string
	path            func(backupName string) string
	readSegmentSize func(ctx context.Context, baseBackupsFolder storage.Folder, backupName string) (int64, error)
}

func sharedStorageKinds() []sharedStorageKind {
	return []sharedStorageKind{
		{name: "AO/AOCS", path: getAOFilesMetadataPath, readSegmentSize: readUploadedAOSize},
		{name: "PAX", path: pax.GetFilesMetadataPath, readSegmentSize: readUploadedPaxSize},
	}
}

// UploadSharedSizes writes the cluster-wide shared size of backupName, one object per shared
// storage, each summed from what the segments named in the backup sentinel reported uploading.
//
// Unlike the journal this is recorded on every backup-push, with no flag and for permanent backups
// too: a permanent backup occupies the shared storage like any other, and leaving it out would
// break the property that the sizes of all backups add up to the size of the storage.
func UploadSharedSizes(ctx context.Context, rootFolder storage.Folder, backupName string) error {
	segments, ok, err := segmentsOfBackup(ctx, rootFolder, backupName)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("can not read the sentinel of backup %s", backupName)
	}

	baseBackupsFolder := rootFolder.GetSubFolder(utility.BaseBackupPath)
	for _, kind := range sharedStorageKinds() {
		sum, ok := sumOverSegments(ctx, rootFolder, backupName, segments, kind)
		if !ok {
			// A partial sum would understate the real volume and be indistinguishable from a
			// genuinely small one, so the object is left unwritten rather than written wrong. An
			// absent object means "never determined", which a zero could not express.
			continue
		}

		dto := SharedSizeDTO{SharedSize: sum}
		if err := internal.UploadDto(ctx, baseBackupsFolder, dto, kind.path(backupName)); err != nil {
			return fmt.Errorf("failed to upload the cluster-wide %s shared size: %w", kind.name, err)
		}
	}

	return nil
}

// sumOverSegments adds up the volume every segment of the backup uploaded to one shared storage.
//
// A partial sum would silently understate the real volume, so a single segment failing to report
// makes the whole aggregate unavailable (ok == false) rather than wrong.
func sumOverSegments(ctx context.Context, rootFolder storage.Folder, backupName string,
	segments []SegmentMetadata, kind sharedStorageKind) (int64, bool) {
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
		size, err := kind.readSegmentSize(ctx, segFolder, meta.BackupName)
		if err != nil {
			// The segment backup was pushed by a WAL-G old enough not to report the uploaded volume.
			tracelog.WarningLogger.Printf("Can not read the %s volume uploaded by segment %d backup %s: %v",
				kind.name, meta.ContentID, meta.BackupName, err)
			missing = append(missing, meta.ContentID)
			continue
		}

		sum += size
	}

	if len(missing) > 0 {
		tracelog.WarningLogger.Printf("The %s files metadata of backup %s is unavailable on segments %v, "+
			"the cluster-wide shared volume can not be calculated", kind.name, backupName, missing)
		return 0, false
	}

	tracelog.DebugLogger.Printf("Backup %s added %d bytes to the shared %s storage over %d segments",
		backupName, sum, kind.name, len(segments))

	return sum, true
}

// aoUploadedSizeView and paxUploadedSizeView are the parts of the segment files metadata needed to
// learn the uploaded volume. The file lists those objects also carry are skipped: only the segments
// have any use for them.
type aoUploadedSizeView struct {
	UploadedSharedSize int64
}

type paxUploadedSizeView struct {
	UploadedSharedSize int64
}

func readUploadedAOSize(ctx context.Context, baseBackupsFolder storage.Folder, backupName string) (int64, error) {
	var meta aoUploadedSizeView
	if err := internal.FetchDto(ctx, baseBackupsFolder, &meta, getAOFilesMetadataPath(backupName)); err != nil {
		return 0, err
	}
	return meta.UploadedSharedSize, nil
}

func readUploadedPaxSize(ctx context.Context, baseBackupsFolder storage.Folder, backupName string) (int64, error) {
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
	return fetchSharedSize(ctx, rootFolder, getAOFilesMetadataPath(backupName))
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
