package internal

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
)

const (
	PgControlPath = "/global/pg_control"
	LatestString  = "LATEST"
)

var UnwrapAll map[string]bool = nil

var UtilityFilePaths = map[string]bool{
	PgControlPath:         true,
	BackupLabelFilename:   true,
	TablespaceMapFilename: true,
}

type BackupNonExistenceError struct {
	error
}

func NewBackupNonExistenceError(backupName string) BackupNonExistenceError {
	return BackupNonExistenceError{errors.Errorf("Backup '%s' does not exist.", backupName)}
}

func (err BackupNonExistenceError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type NonEmptyDbDataDirectoryError struct {
	error
}

func NewNonEmptyDbDataDirectoryError(dbDataDirectory string) NonEmptyDbDataDirectoryError {
	return NonEmptyDbDataDirectoryError{errors.Errorf("Directory %v for delta base must be empty", dbDataDirectory)}
}

func (err NonEmptyDbDataDirectoryError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type PgControlNotFoundError struct {
	error
}

func NewPgControlNotFoundError() PgControlNotFoundError {
	return PgControlNotFoundError{errors.Errorf("Expect pg_control archive, but not found")}
}

func (err PgControlNotFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// TODO : unit tests
// HandleBackupFetch is invoked to perform wal-g backup-fetch
func HandleBackupFetch(folder storage.Folder, dbDataDirectory string, backupName string) {
	tracelog.DebugLogger.Printf("HandleBackupFetch(%s, folder, %s)\n", backupName, dbDataDirectory)
	dbDataDirectory = ResolveSymlink(dbDataDirectory)
	err := deltaFetchRecursion(backupName, folder, dbDataDirectory, nil)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("Failed to fetch backup: %v\n", err)
	}
}

func GetBackupByName(backupName string, folder storage.Folder) (*Backup, error) {
	baseBackupFolder := folder.GetSubFolder(BaseBackupPath)

	var backup *Backup
	if backupName == LatestString {
		latest, err := GetLatestBackupName(folder)
		if err != nil {
			return nil, err
		}
		tracelog.InfoLogger.Printf("LATEST backup is: '%s'\n", latest)

		backup = NewBackup(baseBackupFolder, latest)

	} else {
		backup = NewBackup(baseBackupFolder, backupName)

		exists, err := backup.CheckExistence()
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, NewBackupNonExistenceError(backupName)
		}
	}
	return backup, nil
}

// TODO : unit tests
// deltaFetchRecursion function composes Backup object and recursively searches for necessary base backup
func deltaFetchRecursion(backupName string, folder storage.Folder, dbDataDirectory string, filesToUnwrap map[string]bool) error {
	backup, err := GetBackupByName(backupName, folder)
	if err != nil {
		return err
	}
	sentinelDto, err := backup.FetchSentinel()
	if err != nil {
		return err
	}

	if filesToUnwrap == nil { // it is the exact backup we want to fetch, so we want to include all files here
		filesToUnwrap = GetRestoredBackupFilesToUnwrap(sentinelDto)
	}

	if sentinelDto.IsIncremental() {
		tracelog.InfoLogger.Printf("Delta from %v at LSN %x \n", *(sentinelDto.IncrementFrom), *(sentinelDto.IncrementFromLSN))
		baseFilesToUnwrap, err := GetBaseFilesToUnwrap(sentinelDto.Files, filesToUnwrap)
		if err != nil {
			return err
		}
		err = deltaFetchRecursion(*sentinelDto.IncrementFrom, folder, dbDataDirectory, baseFilesToUnwrap)
		if err != nil {
			return err
		}
		tracelog.InfoLogger.Printf("%v fetched. Upgrading from LSN %x to LSN %x \n", *(sentinelDto.IncrementFrom), *(sentinelDto.IncrementFromLSN), *(sentinelDto.BackupStartLSN))
	}

	return backup.unwrap(dbDataDirectory, sentinelDto, filesToUnwrap)
}

func GetRestoredBackupFilesToUnwrap(sentinelDto BackupSentinelDto) map[string]bool {
	if sentinelDto.Files == nil { // in case of WAL-E of old WAL-G backup
		return UnwrapAll
	}
	filesToUnwrap := make(map[string]bool)
	for file := range sentinelDto.Files {
		filesToUnwrap[file] = true
	}
	for utilityFilePath := range UtilityFilePaths {
		filesToUnwrap[utilityFilePath] = true
	}
	return filesToUnwrap
}

func GetBaseFilesToUnwrap(backupFileStates BackupFileList, currentFilesToUnwrap map[string]bool) (map[string]bool, error) {
	baseFilesToUnwrap := make(map[string]bool)
	for file := range currentFilesToUnwrap {
		fileDescription, hasDescription := backupFileStates[file]
		if !hasDescription {
			if _, ok := UtilityFilePaths[file]; !ok {
				tracelog.ErrorLogger.Panicf("Wanted to fetch increment for file: '%s', but didn't find one in base", file)
			}
			continue
		}
		if fileDescription.IsSkipped || fileDescription.IsIncremented {
			baseFilesToUnwrap[file] = true
		}
	}
	return baseFilesToUnwrap, nil
}
