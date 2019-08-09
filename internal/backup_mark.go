package internal

import (
	"bytes"
	"encoding/json"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/pkg/errors"

	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

// MarkBackup marks a backup as permanent or impermanent
func MarkBackup(uploader *Uploader, baseBackupFolder storage.Folder, backupName string, toPermanent bool) {
	tracelog.InfoLogger.Printf("Retrieving previous related backups to be marked: toPermanent=%t", toPermanent)
	metadataToUpload, err := GetMarkedBackupMetadataToUpload(baseBackupFolder, backupName, toPermanent)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("Failed to get previous backups: %v", err)
	} else {
		tracelog.InfoLogger.Printf("Retrieved backups to be marked, marking: %v", metadataToUpload)
		err = uploader.UploadMultiple(metadataToUpload)
		if err != nil {
			tracelog.ErrorLogger.Fatalf("Failed to mark previous backups: %v", err)
		}
	}
}

// GetMarkedBackupMetadataToUpload retrieves all previous permanent or
// impermanent backup metas, including itself, any previous delta backups and
// initial full backup, in increasing order beginning from full backup,
// returning modified metadata ready to be uploaded
//
// For example, when marking backups from impermanent to permanent, we retrieve
// all currently impermanent backup metadata, set them to permanent, and return
// the modified metadata as a slice of uploadable objects
func GetMarkedBackupMetadataToUpload(
	baseBackupFolder storage.Folder,
	backupName string,
	toPermanent bool) ([]UploadObject, error) {
	if toPermanent {
		return getMarkedPermanentBackupMetadata(baseBackupFolder, backupName)
	} else {
		//
		return nil, nil
	}
}

func getMarkedPermanentBackupMetadata(baseBackupFolder storage.Folder, backupName string) ([]UploadObject, error){
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

	// only return backups that we want to update
	if !sentinel.IsIncremental() || meta.IsPermanent {
		return backupMetadata, nil
	}

	if !meta.IsPermanent {
		meta.IsPermanent = true
		metadataUploadObject, err := getMetadataUploadObject(backup.Name, meta)
		if err != nil {
			return nil, err
		}
		backupMetadata = append(backupMetadata, metadataUploadObject)
	}
	// return when no longer incremental

	previousImpermanentBackupMetadata, err := getMarkedPermanentBackupMetadata(
		baseBackupFolder,
		*sentinel.IncrementFrom)
	if err != nil {
		return nil, err
	}

	previousImpermanentBackupMetadata = append(previousImpermanentBackupMetadata, backupMetadata...)
	return previousImpermanentBackupMetadata, nil
}

func getMarkedImpermanentBackupMetadata(baseBackupFolder storage.Folder, backupName string) ([]UploadObject, error){
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

	backups, err := getBackups(baseBackupFolder)
	if err != nil {
		return nil, err
	}
	backupDetails, _ := getBackupDetails(baseBackupFolder, backups)
	reverseLinks := make(map[string][]string)
	permanentBackups := make(map[string]bool)
	for i := len(backupDetails) - 1; i >= 0; i-- {
		b := backupDetails[i]
		incrementFrom, isIncrement, err := getMetadataFromBackup(baseBackupFolder, b.BackupName)
		if err != nil {
			return nil, err
		}
		permanentBackups[b] = is
		if isIncrement{
			if _, ok := reverseLinks[incrementFrom]; !ok{
				reverseLinks[incrementFrom] = make([]string, len(backups))
			}
			reverseLinks[incrementFrom] = append(reverseLinks[incrementFrom], b.BackupName)
		}
	}
	if backupHasPermanentInFuture(reverseLinks, backupName){
		return nil, NewHasPermanentBackupInFutureError(backupName)
	}
	meta.IsPermanent = false
	metadataUploadObject, err := getMetadataUploadObject(backup.Name, meta)
	if err != nil {
		return nil, err
	}
	backupMetadata = append(backupMetadata, metadataUploadObject)

	return backupMetadata, nil

}

func backupHasPermanentInFuture(reverseLinks map[string][]string, backupName string, permanentBackups []string) (bool) {
	if _, ok := reverseLinks[backupName]; !ok {
		return false
	}

	for _, b := range reverseLinks[backupName]{
		if backupHasPermanentInFuture(reverseLinks, b) {
			return true
		}
	}


	return false
}

func getMetadataFromBackup(baseBackupFolder storage.Folder, backupName string) (incrementFrom string, isIncrement bool, isPermanent bool, err error){
	backup := NewBackup(baseBackupFolder, backupName)
	sentinel, err := backup.GetSentinel()
	if err != nil {
		return "", false, false, err
	}

	meta, err := backup.FetchMeta()
	if err != nil {
		return "", false, false, err
	}

	return *sentinel.IncrementFrom, sentinel.IsIncremental(), meta.IsPermanent, nil
}

func getMetadataUploadObject(backupName string, meta ExtendedMetadataDto) (UploadObject, error) {
	metaFilePath := storage.JoinPath(backupName, utility.MetadataFileName)
	dtoBody, err := json.Marshal(meta)
	if err != nil {
		return UploadObject{}, err
	}
	return UploadObject{metaFilePath, bytes.NewReader(dtoBody)}, nil
}

type HasPermanentBackupInFutureError struct {
	error
}

func NewHasPermanentBackupInFutureError(backupName string) HasPermanentBackupInFutureError {
	return HasPermanentBackupInFutureError{errors.Errorf("Can't mark backup '%s' as impermanent. There is permanent increment backup.", backupName)}
}

