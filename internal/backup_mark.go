package internal

import (
	"bytes"
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

// markBackup marks a backup as permanent or impermanent
func markBackup(uploader *Uploader, folder storage.Folder, backupName string, toPermanent bool) {
	tracelog.InfoLogger.Printf("Retrieving previous related backups to be marked: toPermanent=%t", toPermanent)
	metadataToUpload, err := GetMarkedBackupMetadataToUpload(folder, backupName, toPermanent)

	tracelog.ErrorLogger.FatalfOnError("Failed to get previous backups: %v", err)
	tracelog.InfoLogger.Printf("Retrieved backups to be marked, marking: %v", metadataToUpload)

	err = uploader.UploadMultiple(metadataToUpload)
	tracelog.ErrorLogger.FatalfOnError("Failed to mark previous backups: %v", err)
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
	folder storage.Folder,
	backupName string,
	toPermanent bool) ([]UploadObject, error) {
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)

	backup := NewBackup(baseBackupFolder, backupName)
	meta, err := backup.FetchMeta()
	if err != nil {
		return nil, err
	}

	//raise error when backup already has that type
	if toPermanent == meta.IsPermanent {
		permanentType := "permanent"
		if !meta.IsPermanent {
			permanentType = "impermanent"
		}
		tracelog.WarningLogger.Printf("Backup %s is already marked as %s, ignoring...", backupName, permanentType)
	}

	if toPermanent {
		return getMarkedPermanentBackupMetadata(baseBackupFolder, backupName)
	}
	return getMarkedImpermanentBackupMetadata(folder, backupName)
}

func getMarkedPermanentBackupMetadata(baseBackupFolder storage.Folder, backupName string) ([]UploadObject, error) {
	var backupMetadata []UploadObject

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
	if !meta.IsPermanent {
		meta.IsPermanent = true
		metadataUploadObject, err := GetMetadataUploadObject(backup.Name, &meta)
		if err != nil {
			return nil, err
		}
		backupMetadata = append(backupMetadata, metadataUploadObject)
	}

	// return when no longer incremental
	if !sentinel.IsIncremental() {
		return backupMetadata, nil
	}

	// mark previous backup
	previousImpermanentBackupMetadata, err := getMarkedPermanentBackupMetadata(
		baseBackupFolder,
		*sentinel.IncrementFrom)
	if err != nil {
		return nil, err
	}

	previousImpermanentBackupMetadata = append(previousImpermanentBackupMetadata, backupMetadata...)
	return previousImpermanentBackupMetadata, nil
}

func getMarkedImpermanentBackupMetadata(folder storage.Folder, backupName string) ([]UploadObject, error) {
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)

	// retrieve current backup meta
	backup := NewBackup(baseBackupFolder, backupName)

	meta, err := backup.FetchMeta()
	if err != nil {
		return nil, err
	}

	permanentBackups, _ := GetPermanentObjects(folder)
	//  del current backup from
	delete(permanentBackups, getBackupNumber(backupName))

	reverseLinks, err := getGraphFromBaseToIncrement(folder)
	if err != nil {
		return nil, err
	}

	if backupHasPermanentInFuture(&reverseLinks, backupName, &permanentBackups) {
		return nil, newBackupHasPermanentBackupInFutureError(backupName)
	}

	metadataToUpload := make([]UploadObject, 0)
	if meta.IsPermanent {
		meta.IsPermanent = false
		metadataUploadObject, err := GetMetadataUploadObject(backup.Name, &meta)
		if err != nil {
			return nil, err
		}
		metadataToUpload = append(metadataToUpload, metadataUploadObject)
	}

	return metadataToUpload, nil
}

func getBackupNumber(backupName string) string {
	return backupName[len(utility.BackupNamePrefix) : len(utility.BackupNamePrefix)+24]
}

//backup has permanent in future only when one of the next backups is permanent
func backupHasPermanentInFuture(reverseLinks *map[string][]string,
	backupName string,
	permanentBackups *map[string]bool) bool {
	//if there is no next backups
	if _, ok := (*reverseLinks)[backupName]; !ok {
		return false
	}

	//if one of the next backups is permanent
	for _, b := range (*reverseLinks)[backupName] {
		if _, ok := (*permanentBackups)[getBackupNumber(b)]; ok {
			return true
		}
	}

	return false
}

//return graph where nodes - backup names, edges - links from base backups to increment backups
func getGraphFromBaseToIncrement(folder storage.Folder) (map[string][]string, error) {
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)

	backups, err := GetBackups(folder)
	if err != nil {
		return nil, err
	}

	reverseLinks := make(map[string][]string)
	for _, b := range backups {
		incrementFrom, isIncrement, err := GetMetadataFromBackup(baseBackupFolder, b.BackupName)
		if err != nil {
			return nil, err
		}

		if isIncrement {
			reverseLinks[incrementFrom] = append(reverseLinks[incrementFrom], b.BackupName)
		}
	}

	return reverseLinks, nil
}

func GetMetadataFromBackup(baseBackupFolder storage.Folder,
	backupName string) (incrementFrom string, isIncrement bool, err error) {
	backup := NewBackup(baseBackupFolder, backupName)
	sentinel, err := backup.GetSentinel()
	if err != nil {
		return "", false, err
	}
	if !sentinel.IsIncremental() {
		return "", false, nil
	}
	return *sentinel.IncrementFrom, true, nil
}

func GetMetadataUploadObject(backupName string, meta *ExtendedMetadataDto) (UploadObject, error) {
	metaFilePath := storage.JoinPath(backupName, utility.MetadataFileName)
	dtoBody, err := json.Marshal(meta)
	if err != nil {
		return UploadObject{}, err
	}
	return UploadObject{metaFilePath, bytes.NewReader(dtoBody)}, nil
}


type BackupHasPermanentBackupInFutureError struct {
	error
}

func newBackupHasPermanentBackupInFutureError(backupName string) BackupHasPermanentBackupInFutureError {
	return BackupHasPermanentBackupInFutureError{
		errors.Errorf("Can't mark backup '%s' as impermanent. There is permanent increment backup.",
			backupName)}
}

func GetPermanentObjects(folder storage.Folder) (map[string]bool, map[string]bool) {
	tracelog.InfoLogger.Println("retrieving permanent objects")
	backupTimes, err := GetBackups(folder)
	if err != nil {
		return map[string]bool{}, map[string]bool{}
	}

	permanentBackups := map[string]bool{}
	permanentWals := map[string]bool{}
	for _, backupTime := range backupTimes {
		backup, err := GetBackupByName(backupTime.BackupName, utility.BaseBackupPath, folder)
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to get backup by name with error %s, ignoring...", err.Error())
			continue
		}
		meta, err := backup.FetchMeta()
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to fetch backup meta for backup %s with error %s, ignoring...",
				backupTime.BackupName, err.Error())
			continue
		}
		if meta.IsPermanent {
			timelineID, err := ParseTimelineFromBackupName(backup.Name)
			if err != nil {
				tracelog.ErrorLogger.Printf("failed to parse backup timeline for backup %s with error %s, ignoring...",
					backupTime.BackupName, err.Error())
				continue
			}

			startWalSegmentNo := newWalSegmentNo(meta.StartLsn - 1)
			endWalSegmentNo := newWalSegmentNo(meta.FinishLsn - 1)
			for walSegmentNo := startWalSegmentNo; walSegmentNo <= endWalSegmentNo; walSegmentNo = walSegmentNo.next() {
				permanentWals[walSegmentNo.getFilename(timelineID)] = true
			}
			permanentBackups[backupTime.BackupName[len(utility.BackupNamePrefix):len(utility.BackupNamePrefix)+24]] = true
		}
	}
	return permanentBackups, permanentWals
}

func IsPermanent(objectName string, permanentBackups, permanentWals map[string]bool) bool {
	if objectName[:len(utility.WalPath)] == utility.WalPath {
		wal := objectName[len(utility.WalPath) : len(utility.WalPath)+24]
		return permanentWals[wal]
	}
	if objectName[:len(utility.BaseBackupPath)] == utility.BaseBackupPath {
		var startIndex = len(utility.BaseBackupPath) + len(utility.BackupNamePrefix)
		var endIndex = len(utility.BaseBackupPath) + len(utility.BackupNamePrefix) + 24
		backup := objectName[startIndex:endIndex]
		return permanentBackups[backup]
	}
	// should not reach here, default to false
	return false
}
