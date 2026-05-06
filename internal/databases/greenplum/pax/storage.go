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

// MakeFileStorageKey builds the object name for a single PAX file.
// The key embeds the relation identity, the leaf filename inside `_pax/`,
// and a per-backup uniquifier so subsequent backups never overwrite an earlier one's
// upload of the same file.
func MakeFileStorageKey(relNameMd5 string, key FileKey, paxFilesID string) string {
	//Storage object names are built from `<spc>_<db>_<md5>_<rel>_<filename>_<id>_pax`.
	//* `<spc>` - Tablespace OID. 1663 for pg_default, 1664 for pg_global, otherwise the OID of a user-defined tablespace.
	//			Comes from the standard PostgreSQL relfile path base/<dbOid>/<relfilenode> or pg_tblspc/<spc>/<...>/<dbOid>/<relfilenode>.
	//* `<db>`  - Database OID - the <dbOid> segment of the relfile path. Disambiguates files that happen to share the same <rel> value across databases.
	//* `<md5>` - MD5 of the fully-qualified relation name (<schema>.<table>). Stable across VACUUM FULL / CLUSTER
	//			even though those rotate the relfilenode, so a re-clustered table can still match its own previously-uploaded
	//			files for dedup purposes.
	//* `<rel>` - Relfilenode - the directory name <relfilenode> in base/<dbOid>/<relfilenode>_pax/. The on-disk identifier of the
	//			current physical relation file set.
	//* `<filename>` - sanitized file name
	//* `<id>`  - Per-backup uniquifier - the nanosecond-precision timestamp captured when the backup started (newPaxFilesID).
	//			Two backups uploading the same file produce two different keys, so the older object survives until its owning backup is deleted.
	//            When dedup kicks in, the new backup reuses the old key - the uniquifier is stable per object, not regenerated.
	//* `_pax`  - Fixed suffix, mirrors _aoseg for AO/AOCS objects. Marks the object as belonging to the PAX shared storage and
	//			protects against accidental overlap with any other key shape under paxfiles/.

	return fmt.Sprintf("%d_%d_%s_%d_%s_%s%s",
		key.SpcNode,
		key.DBNode,
		relNameMd5,
		key.RelFileNode,
		// sanitize dots in the filename (`<id>.toast`, `<id>_<gen>_<xid>.visimap`)
		strings.ReplaceAll(key.Filename, ".", "_"),
		paxFilesID,
		KeySuffix)
}

// LoadStoragePaxFiles loads the set of PAX file storage keys referenced by every
// existing backup in baseBackupsFolder. The returned set is what the uploader uses
// to decide whether a file may be skipped (already in storage) or must be uploaded.
func LoadStoragePaxFiles(baseBackupsFolder storage.Folder) (map[string]struct{}, error) {
	known := make(map[string]struct{})
	err := iterateStoragePaxFilesWithFunc(baseBackupsFolder, func(_ string, desc BackupFileDesc) {
		known[desc.StoragePath] = struct{}{}
	})
	if err != nil {
		return nil, err
	}
	return known, nil
}

// iterateStoragePaxFilesWithFunc visits every PAX file referenced by any backup that
// has a `pax_files_metadata.json` next to its sentinel. Backups without the metadata
// (older format, or non-PAX clusters) are silently skipped.
func iterateStoragePaxFilesWithFunc(baseBackupsFolder storage.Folder, fn func(string, BackupFileDesc)) error {
	backupObjects, _, err := baseBackupsFolder.ListFolder()
	if err != nil {
		return err
	}
	for _, b := range internal.GetBackupTimeSlices(backupObjects) {
		var meta FilesMetadataDTO
		err := internal.FetchDto(baseBackupsFolder, &meta, GetFilesMetadataPath(b.BackupName))
		if err != nil {
			if _, ok := err.(storage.ObjectNotFoundError); ok {
				tracelog.DebugLogger.Printf("No PAX files metadata for backup %s in folder %s, skipping",
					b.BackupName, baseBackupsFolder.GetPath())
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
