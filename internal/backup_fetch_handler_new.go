package internal

import (
	"fmt"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type FetchConfig struct {
	filesToUnwrap   map[string]bool
	// missingBlocks stores count of blocks missing for file path
	missingBlocks   map[string]int64
	tablespaceSpec  *TablespaceSpec
	backupName      string
	folder          storage.Folder
	dbDataDirectory string
}

func (fc *FetchConfig) applyUnwrapResult(unwrapResult *UnwrapResult) {
	for filePath, missingBlockCount := range unwrapResult.createdPageFiles {
		_, ok := fc.filesToUnwrap[filePath]
		if !ok {
			// file is already excluded, skip it
			continue
		}
		if missingBlockCount == 0 {
			unwrapResult.completedFiles = append(unwrapResult.completedFiles, filePath)
		} else {
			fc.missingBlocks[filePath] = missingBlockCount
		}
	}
	for filePath, restoredBlockCount := range unwrapResult.writtenIncrementFiles {
		_, ok := fc.filesToUnwrap[filePath]
		if !ok {
			// file is already excluded, skip it
			continue
		}
		missingBlockCount, ok := fc.missingBlocks[filePath]
		if !ok {
			// file is not in file blocks to restore, skip it
			tracelog.WarningLogger.Printf("New written increment blocks, " +
				"but file doesn't exist in missingBlocks: '%s'", filePath)
			continue
		}
		missingBlockCount -= restoredBlockCount
		if missingBlockCount <= 0 {
			unwrapResult.completedFiles = append(unwrapResult.completedFiles, filePath)
		} else {
			fc.missingBlocks[filePath] = missingBlockCount
		}
	}
	for _, filePath := range unwrapResult.completedFiles {
		delete(fc.filesToUnwrap, filePath)
		fmt.Println("Excluded file " + filePath)
	}
}

func newFetchConfig(backupName,dbDataDirectory string, folder storage.Folder, spec *TablespaceSpec,
	filesToUnwrap map[string]bool) *FetchConfig {
	return &FetchConfig{filesToUnwrap,make(map[string]int64), spec,
		backupName, folder, dbDataDirectory}
}

func GetPgFetcherNew(dbDataDirectory, fileMask, restoreSpecPath string) func(folder storage.Folder, backup Backup) {
	return func(folder storage.Folder, backup Backup) {
		filesToUnwrap, err := backup.GetFilesToUnwrap(fileMask)
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)

		var spec *TablespaceSpec
		if restoreSpecPath != "" {
			spec = &TablespaceSpec{}
			err := readRestoreSpec(restoreSpecPath, spec)
			errMessage := fmt.Sprintf("Invalid restore specification path %s\n", restoreSpecPath)
			tracelog.ErrorLogger.FatalfOnError(errMessage, err)
		}

		// directory must be empty before starting a deltaFetch
		isEmpty, err := isDirectoryEmpty(dbDataDirectory)
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)

		if !isEmpty {
			tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n",
				newNonEmptyDbDataDirectoryError(dbDataDirectory))
		}
		config := newFetchConfig(backup.Name, utility.ResolveSymlink(dbDataDirectory), folder, spec, filesToUnwrap)
		err = deltaFetchRecursionNew(config)
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
	}
}

// TODO : unit tests
// deltaFetchRecursion function composes Backup object and recursively searches for necessary base backup
func deltaFetchRecursionNew(cfg *FetchConfig) error {
	backup, err := GetBackupByName(cfg.backupName, utility.BaseBackupPath, cfg.folder)
	if err != nil {
		return err
	}
	sentinelDto, err := backup.GetSentinel()
	if err != nil {
		return err
	}
	chooseTablespaceSpecification(sentinelDto, cfg.tablespaceSpec)

	if sentinelDto.IsIncremental() {
		tracelog.InfoLogger.Printf("Delta %v at LSN %x \n", cfg.backupName, *(sentinelDto.BackupStartLSN))
		baseFilesToUnwrap, err := GetBaseFilesToUnwrap(sentinelDto.Files, cfg.filesToUnwrap)
		if err != nil {
			return err
		}
		unwrapResult, err := backup.unwrapNew(cfg.dbDataDirectory, sentinelDto, cfg.filesToUnwrap, false)
		if err != nil {
			return err
		}
		cfg.filesToUnwrap = baseFilesToUnwrap
		cfg.backupName = *sentinelDto.IncrementFrom
		cfg.applyUnwrapResult(unwrapResult)
		tracelog.InfoLogger.Printf("%v fetched. Downgrading from LSN %x to LSN %x \n",
			cfg.backupName, *(sentinelDto.BackupStartLSN), *(sentinelDto.IncrementFromLSN))
		err = deltaFetchRecursionNew(cfg)
		if err != nil {
			return err
		}

		return nil
	}

	tracelog.InfoLogger.Printf("%x reached. Applying base backup... \n", *(sentinelDto.BackupStartLSN))
	_, err = backup.unwrapNew(cfg.dbDataDirectory, sentinelDto, cfg.filesToUnwrap, false)
	return err
}
