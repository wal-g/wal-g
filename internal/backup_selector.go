package internal

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/wal-g/wal-g/utility"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/storages/storage"
)

const LatestString = "LATEST"

// Select the name of storage backup chosen according to the internal rules
type BackupSelector interface {
	Select(folder storage.Folder) (string, error)
}

// Select the latest backup from storage
type LatestBackupSelector struct {
}

func NewLatestBackupSelector() LatestBackupSelector {
	return LatestBackupSelector{}
}

func (s LatestBackupSelector) Select(folder storage.Folder) (string, error) {
	return GetLatestBackupName(folder.GetSubFolder(utility.BaseBackupPath))
}

// Select backup which has the provided user data
type UserDataBackupSelector struct {
	userData    interface{}
	metaFetcher GenericMetaFetcher
}

func NewUserDataBackupSelector(userDataRaw string, metaFetcher GenericMetaFetcher) UserDataBackupSelector {
	return UserDataBackupSelector{
		userData:    UnmarshalSentinelUserData(userDataRaw),
		metaFetcher: metaFetcher,
	}
}

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
		return "", errors.New("no backups found with specified user data")
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
	backupName string
}

func NewBackupNameSelector(backupName string) (BackupNameSelector, error) {
	return BackupNameSelector{backupName: backupName}, nil
}

func (s BackupNameSelector) Select(folder storage.Folder) (string, error) {
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
		return NewBackupNameSelector(targetName)

	case targetUserData != "":
		tracelog.InfoLogger.Println("Selecting the backup with the specified user data...")
		return NewUserDataBackupSelector(targetUserData, metaFetcher), nil

	default:
		err = errors.New("insufficient arguments")
	}
	return nil, err
}
