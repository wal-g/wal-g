package internal

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/wal-g/wal-g/utility"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const LatestString = "LATEST"

// BackupSelector returns the name of storage backup chosen according to the internal rules.
// Returns NoBackupsFoundError in case there are no backups matching the criteria.
type BackupSelector interface {
	Select(folder storage.Folder) (string, error)
}

// Select the latest backup from storage
type LatestBackupSelector struct {
}

func NewLatestBackupSelector() LatestBackupSelector {
	return LatestBackupSelector{}
}

// TODO: unit tests
func (s LatestBackupSelector) Select(folder storage.Folder) (string, error) {
	backupName, err := GetLatestBackupName(folder.GetSubFolder(utility.BaseBackupPath))
	if err == nil {
		tracelog.InfoLogger.Printf("LATEST backup is: '%s'\n", backupName)
	}
	return backupName, err
}

// Select backup which has the provided user data
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
func (s UserDataBackupSelector) Select(folder storage.Folder) (string, error) {
	backupName, err := s.findBackupByUserData(s.userData, folder)
	if err != nil {
		return "", err
	}
	return backupName, nil
}

// Find backup with UserData exactly matching the provided one
func (s UserDataBackupSelector) findBackupByUserData(userData interface{}, folder storage.Folder) (string, error) {
	foundBackups, err := searchInMetadata(
		func(d GenericMetadata) bool {
			return reflect.DeepEqual(userData, d.UserData)
		}, folder, s.metaFetcher)
	if err != nil {
		return "", errors.Wrapf(err, "UserData search failed")
	}

	if len(foundBackups) == 0 {
		return "", NewNoBackupsFoundError()
	}

	if len(foundBackups) > 1 {
		var backupNames []string
		for idx := range foundBackups {
			backupNames = append(backupNames, foundBackups[idx].BackupName)
		}
		return "", fmt.Errorf("too many backups (%d) found with specified user data: %s",
			len(backupNames), strings.Join(backupNames, " "))
	}

	return foundBackups[0].BackupName, nil
}

// Search backups in storage using specified criteria
func searchInMetadata(
	criteria func(GenericMetadata) bool,
	folder storage.Folder,
	metaFetcher GenericMetaFetcher,
) ([]GenericMetadata, error) {
	backups, err := GetBackupSentinelObjects(folder)
	if err != nil {
		return nil, err
	}

	backupTimes := GetBackupTimeSlices(backups)
	foundMeta := make([]GenericMetadata, 0)

	for _, backupTime := range backupTimes {
		meta, err := metaFetcher.Fetch(backupTime.BackupName, folder.GetSubFolder(utility.BaseBackupPath))
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to get metadata of backup %s, error: %s\n",
				backupTime.BackupName, err.Error())
		} else if criteria(meta) {
			foundMeta = append(foundMeta, meta)
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
func (s BackupNameSelector) Select(folder storage.Folder) (string, error) {
	if !s.checkExistence {
		return s.backupName, nil
	}

	_, err := GetBackupByName(s.backupName, utility.BaseBackupPath, folder)
	if err != nil {
		return "", err
	}
	return s.backupName, nil
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
func (s *OldestNonPermanentSelector) Select(folder storage.Folder) (string, error) {
	searchFn := func(d GenericMetadata) bool {
		if !d.IsPermanent {
			return true
		}

		tracelog.InfoLogger.Printf(
			"Backup %s is permanent, it is not eligible to be selected "+
				"as the oldest backup\n", d.BackupName)
		return false
	}

	foundBackups, err := searchInMetadata(searchFn, folder, s.metaFetcher)
	if err != nil {
		return "", errors.Wrapf(err, "backups lookup failed")
	}

	if len(foundBackups) == 0 {
		return "", NewNoBackupsFoundError()
	}

	oldest := foundBackups[0]
	for i := range foundBackups {
		if foundBackups[i].StartTime.Before(oldest.StartTime) {
			oldest = foundBackups[i]
		}
	}

	return oldest.BackupName, nil
}
