package internal

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/utility"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const LatestString = "LATEST"

// BackupSelector returns the backup chosen according to the internal rules.
// Returns NoBackupsFoundError in case there are no backups matching the criteria.
type BackupSelector interface {
	Select(folder storage.Folder) (Backup, error)
}

// LatestBackupSelector selects the latest backup from storage
type LatestBackupSelector struct {
}

func NewLatestBackupSelector() LatestBackupSelector {
	return LatestBackupSelector{}
}

// TODO: unit tests
func (s LatestBackupSelector) Select(folder storage.Folder) (Backup, error) {
	return GetLatestBackup(folder.GetSubFolder(utility.BaseBackupPath))
}

// UserDataBackupSelector selects a backup which has the provided user data
type UserDataBackupSelector struct {
	userData    interface{}
	metaFetcher GenericMetaFetcher
}

func NewUserDataBackupSelector(userDataRaw string, metaFetcher GenericMetaFetcher) (UserDataBackupSelector, error) {
	userData, err := UnmarshalSentinelUserData(userDataRaw)
	if err != nil {
		return UserDataBackupSelector{}, err
	}
	return UserDataBackupSelector{
		userData:    userData,
		metaFetcher: metaFetcher,
	}, nil
}

// TODO: unit tests
func (s UserDataBackupSelector) Select(folder storage.Folder) (Backup, error) {
	return s.findBackupByUserData(s.userData, folder)
}

// Exists backup with UserData exactly matching the provided one
func (s UserDataBackupSelector) findBackupByUserData(userData interface{}, folder storage.Folder) (Backup, error) {
	matchUserData := func(d GenericMetadata) bool {
		return reflect.DeepEqual(userData, d.UserData)
	}
	foundMetas, err := searchInMetadata(matchUserData, folder, s.metaFetcher)
	if err != nil {
		return Backup{}, errors.Wrapf(err, "UserData search failed")
	}

	if len(foundMetas) == 0 {
		return Backup{}, NewNoBackupsFoundError()
	}

	var foundBackupName string
	var foundStorage string
	uniqueNames := map[string]bool{}
	for storageName := range foundMetas {
		foundBackupName = foundMetas[storageName].BackupName
		foundStorage = storageName
		uniqueNames[foundBackupName] = true
	}

	if len(uniqueNames) > 1 {
		var backupNames []string
		for st := range foundMetas {
			backupNames = append(backupNames, foundMetas[st].BackupName)
		}
		return Backup{}, fmt.Errorf("too many backups (%d) found with specified user data: %s",
			len(uniqueNames), strings.Join(backupNames, " "))
	}

	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	return NewBackupInStorage(baseBackupFolder, foundBackupName, foundStorage)
}

// Search backups in storage using specified criteria
func searchInMetadata(
	criteria func(GenericMetadata) bool,
	folder storage.Folder,
	metaFetcher GenericMetaFetcher,
) (metaByStorage map[string]GenericMetadata, err error) {
	sentinels, err := GetBackupSentinelObjects(folder)
	if err != nil {
		return nil, err
	}

	backupTimes := GetBackupTimeSlices(sentinels)
	foundMeta := make(map[string]GenericMetadata, 0)

	for _, backupTime := range backupTimes {
		specificFolder, err := multistorage.UseSpecificStorage(backupTime.StorageName, folder)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to select source storage for backup %s to fetch its meta: %s\n",
				backupTime.BackupName, err.Error())
			continue
		}
		meta, err := metaFetcher.Fetch(backupTime.BackupName, specificFolder.GetSubFolder(utility.BaseBackupPath))
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to get metadata of backup %s, error: %s\n",
				backupTime.BackupName, err.Error())
			continue
		}
		if criteria(meta) {
			foundMeta[backupTime.StorageName] = meta
		}
	}
	return foundMeta, nil
}

// Select backup by provided backup name
type BackupNameSelector struct {
	backupName     string
	checkExistence bool
}

func NewBackupNameSelector(backupName string, checkExistence bool) (BackupNameSelector, error) {
	return BackupNameSelector{backupName: backupName, checkExistence: checkExistence}, nil
}

// TODO: unit tests
func (s BackupNameSelector) Select(folder storage.Folder) (Backup, error) {
	if !s.checkExistence {
		return NewBackupInStorage(folder, s.backupName, multistorage.DefaultStorage)
	}
	return GetBackupByName(s.backupName, utility.BaseBackupPath, folder)
}

func NewTargetBackupSelector(targetUserData, targetName string, metaFetcher GenericMetaFetcher) (BackupSelector, error) {
	var err error
	switch {
	case targetName != "" && targetUserData != "":
		err = errors.New("incorrect arguments. Specify target backup name OR target userdata, not both")

	case targetName == LatestString:
		tracelog.InfoLogger.Printf("Selecting the latest backup...\n")
		return NewLatestBackupSelector(), nil

	case targetName != "":
		tracelog.InfoLogger.Printf("Selecting the backup with name %s...\n", targetName)
		return NewBackupNameSelector(targetName, true)

	case targetUserData != "":
		tracelog.InfoLogger.Println("Selecting the backup with the specified user data...")
		return NewUserDataBackupSelector(targetUserData, metaFetcher)

	default:
		err = errors.New("insufficient arguments")
	}
	return nil, err
}

// NewDeltaBaseSelector returns the BackupSelector for delta backup base according to the provided flags
func NewDeltaBaseSelector(
	targetBackupName, targetUserData string, metaFetcher GenericMetaFetcher) (BackupSelector, error) {
	switch {
	case targetUserData != "" && targetBackupName != "":
		return nil, errors.New("only one delta target should be specified")

	case targetBackupName != "":
		tracelog.InfoLogger.Printf("Selecting the backup with name %s as the base for the current delta backup...\n",
			targetBackupName)
		return NewBackupNameSelector(targetBackupName, true)

	case targetUserData != "":
		tracelog.InfoLogger.Println(
			"Selecting the backup with specified user data as the base for the current delta backup...")
		return NewUserDataBackupSelector(targetUserData, metaFetcher)

	default:
		return NewLatestBackupSelector(), nil
	}
}

// OldestNonPermanentSelector finds oldest non-permanent backup available in storage.
type OldestNonPermanentSelector struct {
	metaFetcher GenericMetaFetcher
}

func NewOldestNonPermanentSelector(metaFetcher GenericMetaFetcher) *OldestNonPermanentSelector {
	return &OldestNonPermanentSelector{metaFetcher: metaFetcher}
}

// TODO: unit tests
func (s *OldestNonPermanentSelector) Select(folder storage.Folder) (Backup, error) {
	searchFn := func(d GenericMetadata) bool {
		if !d.IsPermanent {
			return true
		}

		tracelog.InfoLogger.Printf(
			"Backup %s is permanent, it is not eligible to be selected "+
				"as the oldest backup\n", d.BackupName)
		return false
	}

	foundMetas, err := searchInMetadata(searchFn, folder, s.metaFetcher)
	if err != nil {
		return Backup{}, errors.Wrapf(err, "backups lookup failed")
	}

	if len(foundMetas) == 0 {
		return Backup{}, NewNoBackupsFoundError()
	}

	var oldestMeta GenericMetadata
	var oldestStorage string
	for storageName := range foundMetas {
		if foundMetas[storageName].StartTime.Before(oldestMeta.StartTime) {
			oldestMeta = foundMetas[storageName]
			oldestStorage = storageName
		}
	}

	return NewBackupInStorage(folder, oldestMeta.BackupName, oldestStorage)
}
