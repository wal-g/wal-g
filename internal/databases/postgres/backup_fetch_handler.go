package postgres

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

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

func newPgControlNotFoundError() PgControlNotFoundError {
	return PgControlNotFoundError{errors.Errorf("Expect pg_control archive, but not found")}
}

func (err PgControlNotFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

func readRestoreSpec(path string, spec *TablespaceSpec) (err error) {
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

// If specified - choose specified, else choose from latest sentinelDto
func chooseTablespaceSpecification(sentinelDtoSpec, spec *TablespaceSpec) *TablespaceSpec {
	// spec is preferred over sentinelDtoSpec.TablespaceSpec if it is non-nil
	if spec != nil {
		return spec
	} else if sentinelDtoSpec == nil {
		return &TablespaceSpec{}
	}
	return sentinelDtoSpec
}

// TODO : unit tests
// deltaFetchRecursion function composes Backup object and recursively searches for necessary base backup
func deltaFetchRecursionOld(backupName string, folder storage.Folder, dbDataDirectory string,
	tablespaceSpec *TablespaceSpec, filesToUnwrap map[string]bool) error {
	backup := NewBackup(folder.GetSubFolder(utility.BaseBackupPath), backupName)
	sentinelDto, err := backup.GetSentinel()
	if err != nil {
		return err
	}
	tablespaceSpec = chooseTablespaceSpecification(sentinelDto.TablespaceSpec, tablespaceSpec)
	sentinelDto.TablespaceSpec = tablespaceSpec

	if sentinelDto.IsIncremental() {
		tracelog.InfoLogger.Printf("Delta from %v at LSN %x \n", *(sentinelDto.IncrementFrom), *(sentinelDto.IncrementFromLSN))
		baseFilesToUnwrap, err := GetBaseFilesToUnwrap(sentinelDto.Files, filesToUnwrap)
		if err != nil {
			return err
		}
		err = deltaFetchRecursionOld(*sentinelDto.IncrementFrom, folder, dbDataDirectory, tablespaceSpec, baseFilesToUnwrap)
		if err != nil {
			return err
		}
		tracelog.InfoLogger.Printf("%v fetched. Upgrading from LSN %x to LSN %x \n", *(sentinelDto.IncrementFrom), *(sentinelDto.IncrementFromLSN), *(sentinelDto.BackupStartLSN))
	}

	return backup.unwrapToEmptyDirectory(dbDataDirectory, sentinelDto, filesToUnwrap, false)
}

func GetPgFetcherOld(dbDataDirectory, fileMask, restoreSpecPath string) func(rootFolder storage.Folder, backup internal.Backup) {
	return func(rootFolder storage.Folder, backup internal.Backup) {
		pgBackup := ToPgBackup(backup)
		filesToUnwrap, err := pgBackup.GetFilesToUnwrap(fileMask)
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)

		var spec *TablespaceSpec
		if restoreSpecPath != "" {
			spec = &TablespaceSpec{}
			err := readRestoreSpec(restoreSpecPath, spec)
			errMessege := fmt.Sprintf("Invalid restore specification path %s\n", restoreSpecPath)
			tracelog.ErrorLogger.FatalfOnError(errMessege, err)
		}
		err = deltaFetchRecursionOld(backup.Name, rootFolder, utility.ResolveSymlink(dbDataDirectory), spec, filesToUnwrap)
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
	}
}

func GetBaseFilesToUnwrap(backupFileStates internal.BackupFileList, currentFilesToUnwrap map[string]bool) (map[string]bool, error) {
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
