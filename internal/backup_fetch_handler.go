package internal

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/tinsane/storages/storage"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/utility"
	"io/ioutil"
)

const LatestString = "LATEST"

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

func ReadRestoreSpec(path string, spec *TablespaceSpec) (err error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("Unable to read file: %v\n", err)
	}
	err = json.Unmarshal(data, spec)
	if err != nil {
		return fmt.Errorf("Unable to unmarshal json: %v\n Full json data:\n %s", err, data)
	}

	return nil
}

// TODO : unit tests
// HandleBackupFetch is invoked to perform wal-g backup-fetch
func HandleBackupFetch(folder storage.Folder, dbDataDirectory string, backupName string, fileMask string, restoreSpecPath string) {
	tracelog.DebugLogger.Printf("HandleBackupFetch(%s, folder, %s)\n", backupName, dbDataDirectory)
	var spec *TablespaceSpec
	if restoreSpecPath != "" {
		spec = &TablespaceSpec{}
		err := ReadRestoreSpec(restoreSpecPath, spec)
		errMessege := fmt.Sprintf("Invalid restore specification path %s\n", restoreSpecPath)
		tracelog.ErrorLogger.FatalfOnError(errMessege, err)
	}
	backup, err := GetBackupByName(backupName, folder)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
	filesToUnwrap, err := backup.GetFilesToUnwrap(fileMask)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
	err = deltaFetchRecursion(backupName, folder, utility.ResolveSymlink(dbDataDirectory), spec, filesToUnwrap)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
}

func GetBackupByName(backupName string, folder storage.Folder) (*Backup, error) {
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)

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

// If specified - choose specified, else choose from latest sentinelDto
func chooseTablespaceSpecification(sentinelDto BackupSentinelDto, spec *TablespaceSpec) {
	if spec != nil {
		sentinelDto.TablespaceSpec = spec
	} else {
		if sentinelDto.TablespaceSpec == nil {
			sentinelDto.TablespaceSpec = &TablespaceSpec{}
		}
		spec = sentinelDto.TablespaceSpec
	}
}

// TODO : unit tests
// deltaFetchRecursion function composes Backup object and recursively searches for necessary base backup
func deltaFetchRecursion(backupName string, folder storage.Folder, dbDataDirectory string,
	tablespaceSpec *TablespaceSpec, filesToUnwrap map[string]bool) error {
	backup, err := GetBackupByName(backupName, folder)
	if err != nil {
		return err
	}
	sentinelDto, err := backup.GetSentinel()
	if err != nil {
		return err
	}
	chooseTablespaceSpecification(sentinelDto, tablespaceSpec)

	if sentinelDto.IsIncremental() {
		tracelog.InfoLogger.Printf("Delta from %v at LSN %x \n", *(sentinelDto.IncrementFrom), *(sentinelDto.IncrementFromLSN))
		baseFilesToUnwrap, err := GetBaseFilesToUnwrap(sentinelDto.Files, filesToUnwrap)
		if err != nil {
			return err
		}
		err = deltaFetchRecursion(*sentinelDto.IncrementFrom, folder, dbDataDirectory, tablespaceSpec, baseFilesToUnwrap)
		if err != nil {
			return err
		}
		tracelog.InfoLogger.Printf("%v fetched. Upgrading from LSN %x to LSN %x \n", *(sentinelDto.IncrementFrom), *(sentinelDto.IncrementFromLSN), *(sentinelDto.BackupStartLSN))
	}

	return backup.unwrap(dbDataDirectory, sentinelDto, filesToUnwrap)
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
