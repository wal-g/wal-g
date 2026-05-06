package pax

import (
	"fmt"
	"strings"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const (
	StoragePath = "paxfiles"
	KeySuffix   = "_pax"
)

// MakeFileStorageKey builds the object name (without StoragePath prefix) for a single
// PAX file. The key embeds the relation identity, the leaf filename inside `_pax/`,
// and a per-backup uniquifier so subsequent backups never overwrite an earlier one's
// upload of the same file.
//
// Dots in the filename (`<id>.toast`, `<id>_<gen>_<xid>.visimap`) are replaced with
// underscores so that path.Ext on the resulting storage path returns "" — otherwise
// the extract path treats e.g. `.visimap_<ts>_pax` as a file extension and rejects it
// as an unsupported format.
func MakeFileStorageKey(relNameMd5 string, key FileKey, paxFilesID string) string {
	return fmt.Sprintf("%d_%d_%s_%d_%s_%s%s",
		key.SpcNode, key.DBNode,
		relNameMd5,
		key.RelFileNode,
		strings.ReplaceAll(key.Filename, ".", "_"),
		paxFilesID,
		KeySuffix)
}

// LoadStoragePaxFiles loads the set of PAX file storage keys referenced by every
// existing backup in baseBackupsFolder. The returned set is what the uploader uses
// to decide whether a file may be skipped (already in storage) or must be uploaded.
func LoadStoragePaxFiles(baseBackupsFolder storage.Folder) (map[string]struct{}, error) {
	known := make(map[string]struct{})
	err := IterateStoragePaxFilesWithFunc(baseBackupsFolder, func(_ string, desc BackupFileDesc) {
		known[desc.StoragePath] = struct{}{}
	})
	if err != nil {
		return nil, err
	}
	return known, nil
}

// IterateStoragePaxFilesWithFunc visits every PAX file referenced by any backup that
// has a `pax_files_metadata.json` next to its sentinel. Backups without the metadata
// (older format, or non-PAX clusters) are silently skipped.
func IterateStoragePaxFilesWithFunc(baseBackupsFolder storage.Folder, fn func(string, BackupFileDesc)) error {
	backupObjects, _, err := baseBackupsFolder.ListFolder()
	if err != nil {
		return err
	}
	for _, b := range internal.GetBackupTimeSlices(backupObjects) {
		var meta FilesMetadataDTO
		err := internal.FetchDto(baseBackupsFolder, &meta, GetFilesMetadataPath(b.BackupName))
		if err != nil {
			if _, ok := err.(storage.ObjectNotFoundError); ok {
				tracelog.DebugLogger.Printf("No PAX files metadata for backup %s, skipping", b.BackupName)
				continue
			}
			return err
		}
		for localPath, fileDesc := range meta.Files {
			fn(localPath, fileDesc)
		}
	}
	return nil
}
