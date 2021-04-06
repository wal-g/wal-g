package internal

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

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
	return getLatestBackupName(folder)
}

// Select backup which has the provided user data
type UserDataBackupSelector struct {
	userData interface{}
}

func NewUserDataBackupSelector(userDataRaw string) UserDataBackupSelector {
	return UserDataBackupSelector{userData: UnmarshalSentinelUserData(userDataRaw)}
}

func (s UserDataBackupSelector) Select(folder storage.Folder) (string, error) {
	backupDetails, err := findBackupByUserData(s.userData, folder)
	if err != nil {
		return "", err
	}
	return backupDetails.BackupName, nil
}

// Find backup with UserData exactly matching the provided one
func findBackupByUserData(userData interface{}, folder storage.Folder) (BackupDetail, error) {
	foundBackups, err := searchBackupDetails(
		func(d BackupDetail) bool {
			return reflect.DeepEqual(userData, d.UserData)
		}, folder)
	if err != nil {
		return BackupDetail{}, errors.Wrapf(err, "UserData search failed")
	}

	if len(foundBackups) == 0 {
		return BackupDetail{}, errors.New("no backups found with specified user data")
	}

	if len(foundBackups) > 1 {
		var backupNames []string
		for idx := range foundBackups {
			backupNames = append(backupNames, foundBackups[idx].BackupName)
		}
		return BackupDetail{}, fmt.Errorf("too many backups (%d) found with specified user data: %s",
			len(backupNames), strings.Join(backupNames, " "))
	}

	return foundBackups[0], nil
}

// Search backups in storage using specified criteria
func searchBackupDetails(criteria func(BackupDetail) bool, folder storage.Folder) ([]BackupDetail, error) {
	backups, err := GetBackupSentinelObjects(folder)
	if err != nil {
		return nil, err
	}

	backupTimes := GetBackupTimeSlices(backups)
	foundBackups := make([]BackupDetail, 0)

	for _, backupTime := range backupTimes {
		backupDetails, err := GetBackupDetails(folder, backupTime)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to get metadata of backup %s, error: %s\n",
				backupTime.BackupName, err.Error())
		} else if criteria(backupDetails) {
			foundBackups = append(foundBackups, backupDetails)
		}
	}
	return foundBackups, nil
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

func NewTargetBackupSelector(targetUserData, targetName string) (BackupSelector, error) {
	var err error
	switch {
	case targetName != "" && targetUserData != "":
		err = errors.New("Incorrect arguments. Specify target backup name OR target userdata, not both.")

	case targetName == LatestString:
		tracelog.InfoLogger.Printf("Selecting the latest backup...\n")
		return NewLatestBackupSelector(), nil

	case targetName != "":
		tracelog.InfoLogger.Printf("Selecting the backup with name %s...\n", targetName)
		return NewBackupNameSelector(targetName)

	case targetUserData != "":
		tracelog.InfoLogger.Println("Selecting the backup with the specified user data...")
		return NewUserDataBackupSelector(targetUserData), nil

	default:
		err = errors.New("Insufficient arguments.")
	}
	return nil, err
}
