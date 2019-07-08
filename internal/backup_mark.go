package internal

import (
	"bytes"
	"encoding/json"

	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
)

// MarkSelfAndPreviousBackupsPermanent marks all previous related backups
// permanent, including itself, any previous delta backups and initial full
// backup
// TODO: unit tests
func MarkSelfAndPreviousBackupsPermanent(uploader *Uploader, baseBackupFolder storage.Folder, backupName string) error {
	toUpload := []UploadObject{}

	// mark self if impermanent
	backup := NewBackup(baseBackupFolder, backupName)
	currentSentinel, err := backup.FetchSentinel()
	if err != nil {
		return err
	}
	currentMeta, err := backup.FetchMeta()
	if err != nil {
		return err
	}
	if !currentMeta.IsPermanent {
		currentMeta.IsPermanent = true
		uploadObject, err := getMetadataUploadObject(backup.Name, currentMeta)
		if err != nil {
			return err
		}
		toUpload = append(toUpload, uploadObject)
	}

	// mark previous backups if impermanent
	if currentSentinel.IsIncremental() {
		backupName = *currentSentinel.IncrementFrom
		for i := 0; i < *currentSentinel.IncrementCount; i++ {
			previousBackup := NewBackup(baseBackupFolder, backupName)

			// fetch sentinel to get previous backup name
			previousSentinel, err := previousBackup.FetchSentinel()
			if err != nil {
				return err
			}
			if previousSentinel.IncrementFrom != nil {
				backupName = *previousSentinel.IncrementFrom
			}

			// skip backups that are already permanent
			previousMeta, err := previousBackup.FetchMeta()
			if err != nil {
				return err
			}
			if previousMeta.IsPermanent {
				continue
			}
			previousMeta.IsPermanent = true
			uploadObject, err := getMetadataUploadObject(previousBackup.Name, previousMeta)
			if err != nil {
				return err
			}
			toUpload = append(toUpload, uploadObject)
		}
	}

	tracelog.InfoLogger.Printf("Marking permanent backups: %v\n", toUpload)
	return uploader.UploadMultiple(toUpload)
}

func getMetadataUploadObject(backupName string, meta ExtendedMetadataDto) (UploadObject, error) {
	metaFilePath := storage.JoinPath(backupName, utility.MetadataFileName)
	dtoBody, err := json.Marshal(meta)
	if err != nil {
		return UploadObject{}, err
	}
	return UploadObject{metaFilePath, bytes.NewReader(dtoBody)}, nil
}
