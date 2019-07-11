package internal

import (
	"bytes"
	"encoding/json"

	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// GetImpermanentBackupsBefore marks all previous related backups permanent,
// including itself, any previous delta backups and initial full backup
// TODO: unit tests
func GetImpermanentBackupsBefore(baseBackupFolder storage.Folder, backupName string, toUpload *[]UploadObject) error {
	backup := NewBackup(baseBackupFolder, backupName)
	sentinel, err := backup.GetSentinel()
	if err != nil {
		return err
	}
	meta, err := backup.FetchMeta()
	if err != nil {
		return err
	}

	// only upload currently impermanent backups
	if !meta.IsPermanent {
		meta.IsPermanent = true
		uploadObject, err := getMetadataUploadObject(backup.Name, meta)
		if err != nil {
			return err
		}
		*toUpload = append(*toUpload, uploadObject)
	}

	// return when no longer incremental
	if !sentinel.IsIncremental() {
		return nil
	}

	return GetImpermanentBackupsBefore(baseBackupFolder, *sentinel.IncrementFrom, toUpload)
}

func getMetadataUploadObject(backupName string, meta ExtendedMetadataDto) (UploadObject, error) {
	metaFilePath := storage.JoinPath(backupName, utility.MetadataFileName)
	dtoBody, err := json.Marshal(meta)
	if err != nil {
		return UploadObject{}, err
	}
	return UploadObject{metaFilePath, bytes.NewReader(dtoBody)}, nil
}
