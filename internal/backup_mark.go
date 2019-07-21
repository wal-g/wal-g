package internal

import (
	"bytes"
	"encoding/json"

	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// GetImpermanentBackupMetadataBefore gets all previous impermanent backup
// metas, including itself, any previous delta backups and initial full backup,
// in increasing order beginning from full backup
func GetImpermanentBackupMetadataBefore(baseBackupFolder storage.Folder, backupName string) ([]UploadObject, error) {
	backupMetadata := []UploadObject{}

	// retrieve current backup sentinel and meta
	backup := NewBackup(baseBackupFolder, backupName)
	sentinel, err := backup.GetSentinel()
	if err != nil {
		return nil, err
	}
	meta, err := backup.FetchMeta()
	if err != nil {
		return nil, err
	}

	// only return currently impermanent backups
	if !meta.IsPermanent {
		meta.IsPermanent = true
		metadataUploadObject, err := getMetadataUploadObject(backup.Name, meta)
		if err != nil {
			return nil, err
		}
		backupMetadata = append(backupMetadata, metadataUploadObject)
	}

	// return when no longer incremental
	if !sentinel.IsIncremental() {
		return backupMetadata, nil
	}

	previousImpermanentBackupMetadata, err := GetImpermanentBackupMetadataBefore(baseBackupFolder, *sentinel.IncrementFrom)
	if err != nil {
		return nil, err
	}

	previousImpermanentBackupMetadata = append(previousImpermanentBackupMetadata, backupMetadata...)
	return previousImpermanentBackupMetadata, nil
}

func getMetadataUploadObject(backupName string, meta ExtendedMetadataDto) (UploadObject, error) {
	metaFilePath := storage.JoinPath(backupName, utility.MetadataFileName)
	dtoBody, err := json.Marshal(meta)
	if err != nil {
		return UploadObject{}, err
	}
	return UploadObject{metaFilePath, bytes.NewReader(dtoBody)}, nil
}
