package walg

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/tracelog"
	"os"
	"runtime/pprof"
)

const PgControlPath = "/global/pg_control"

var UtilityFilePaths = map[string]bool {
	PgControlPath: true,
	BackupLabelFilename: true,
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

type NoDescriptionError struct {
	error
}

func NewNoDescriptionError(fileName string) NoDescriptionError {
	return NoDescriptionError{errors.Errorf("Wanted to fetch increment for file: '%s', but didn't found one in base", fileName)}
}

func (err NoDescriptionError) Error() string {
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
func HandleBackupFetch(backupName string, folder *S3Folder, dbDataDirectory string, mem bool) {
	dbDataDirectory = ResolveSymlink(dbDataDirectory)
	err := deltaFetchRecursion(backupName, folder, dbDataDirectory, nil)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("Failed to fetch backup: %v\n", err)
	}

	if mem {
		memProfileLog, err := os.Create("mem.prof")
		if err != nil {
			tracelog.ErrorLogger.FatalError(err)
		}

		pprof.WriteHeapProfile(memProfileLog)
		defer memProfileLog.Close()
	}
	return
}

// TODO : unit tests
func getBackupByName(backupName string, folder *S3Folder) (*Backup, error) {
	var backup *Backup
	if backupName == "LATEST" {
		latest, err := GetLatestBackupKey(folder)
		if err != nil {
			return nil, err
		}

		backup = NewBackup(folder, latest)

	} else {
		backup = NewBackup(folder, backupName)

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
func deltaFetchRecursion(backupName string, folder *S3Folder, dbDataDirectory string, filesToUnwrap map[string]bool) error {
	backup, err := getBackupByName(backupName, folder)
	if err != nil {
		return err
	}
	sentinelDto, err := backup.fetchSentinel()
	if err != nil {
		return err
	}

	if filesToUnwrap == nil { // it is the exact backup we want to fetch, so we want to include all files here
		filesToUnwrap = getRestoredBackupFilesToUnwrap(sentinelDto)
	}

	if sentinelDto.isIncremental() {
		tracelog.InfoLogger.Printf("Delta from %v at LSN %x \n", *(sentinelDto.IncrementFrom), *(sentinelDto.IncrementFromLSN))
		baseFilesToUnwrap, err := getBaseFilesToUnwrap(sentinelDto.Files, filesToUnwrap)
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

// TODO : unit tests
func getRestoredBackupFilesToUnwrap(sentinelDto S3TarBallSentinelDto) map[string]bool {
	filesToUnwrap := make(map[string]bool)
	for file := range sentinelDto.Files {
		filesToUnwrap[file] = true
	}
	for utilityFilePath := range UtilityFilePaths {
		filesToUnwrap[utilityFilePath] = true
	}
	return filesToUnwrap
}

// TODO : unit tests
func getBaseFilesToUnwrap(backupFileStates BackupFileList, currentFilesToUnwrap map[string]bool) (map[string]bool, error) {
	baseFilesToUnwrap := make(map[string]bool)
	for file := range currentFilesToUnwrap {
		fileDescription, hasDescription := backupFileStates[file]
		if !hasDescription {
			if _, ok := UtilityFilePaths[file]; !ok {
				return nil, NewNoDescriptionError(file)
			}
		}
		if fileDescription.IsSkipped || fileDescription.IsIncremented {
			baseFilesToUnwrap[file] = true
		}
	}
	return baseFilesToUnwrap, nil
}
