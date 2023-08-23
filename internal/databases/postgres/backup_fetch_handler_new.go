package postgres

import (
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func GetFetcherNew(dbDataDirectory, fileMask, restoreSpecPath string, skipRedundantTars bool,
	extractProv ExtractProvider,
) internal.Fetcher {
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

		// directory must be empty before starting a deltaFetch
		isEmpty, err := utility.IsDirectoryEmpty(dbDataDirectory)
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)

		if !isEmpty {
			tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n",
				NewNonEmptyDBDataDirectoryError(dbDataDirectory))
		}
		config := NewFetchConfig(
			utility.ResolveSymlink(dbDataDirectory),
			pgBackup,
			rootFolder,
			spec,
			filesToUnwrap,
			skipRedundantTars,
			extractProv,
		)
		err = deltaFetchRecursionNew(config)
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
	}
}

// TODO : unit tests
// deltaFetchRecursion function composes Backup object and recursively searches for necessary base backup
func deltaFetchRecursionNew(cfg *FetchConfig) error {
	backup, err := NewBackupInStorage(
		cfg.rootFolder.GetSubFolder(utility.BaseBackupPath),
		cfg.backup.Name,
		cfg.backup.GetStorageName(),
	)
	if err != nil {
		return err
	}
	sentinelDto, filesMetaDto, err := backup.GetSentinelAndFilesMetadata()
	if err != nil {
		return err
	}
	cfg.tablespaceSpec = chooseTablespaceSpecification(sentinelDto.TablespaceSpec, cfg.tablespaceSpec)
	sentinelDto.TablespaceSpec = cfg.tablespaceSpec

	if sentinelDto.IsIncremental() {
		tracelog.InfoLogger.Printf("Delta %v at LSN %s \n",
			cfg.backup.Name,
			*(sentinelDto.BackupStartLSN))
		baseFilesToUnwrap, err := GetBaseFilesToUnwrap(filesMetaDto.Files, cfg.filesToUnwrap)
		if err != nil {
			return err
		}
		unwrapResult, err := backup.unwrapNew(cfg.dbDataDirectory, cfg.filesToUnwrap,
			false, cfg.skipRedundantTars, cfg.extractProv)
		if err != nil {
			return err
		}
		cfg.filesToUnwrap = baseFilesToUnwrap
		cfg.backup.Name = *sentinelDto.IncrementFrom
		if cfg.skipRedundantTars {
			// if we skip redundant tars we should exclude files that
			// no longer need any additional information (completed ones)
			cfg.SkipRedundantFiles(unwrapResult)
		}
		tracelog.InfoLogger.Printf("%v fetched. Downgrading from LSN %s to LSN %s \n",
			cfg.backup.Name,
			*(sentinelDto.BackupStartLSN),
			*(sentinelDto.IncrementFromLSN))
		err = deltaFetchRecursionNew(cfg)
		if err != nil {
			return err
		}

		return nil
	}

	tracelog.InfoLogger.Printf("%s reached. Applying base backup... \n",
		*(sentinelDto.BackupStartLSN))
	_, err = backup.unwrapNew(cfg.dbDataDirectory, cfg.filesToUnwrap,
		false, cfg.skipRedundantTars, cfg.extractProv)
	return err
}
