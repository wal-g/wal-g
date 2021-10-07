package postgres

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/wal-g/wal-g/internal"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

// temporary flag is used in tar interpreter to determine if it should use new unwrap logic
var useNewUnwrapImplementation = false

// UnwrapResult stores information about
// the result of single backup unwrap operation
type UnwrapResult struct {
	// completely restored files
	completedFiles      []string
	completedFilesMutex sync.Mutex
	// for each created page file
	// store count of blocks left to restore
	createdPageFiles      map[string]int64
	createdPageFilesMutex sync.Mutex
	// for those page files to which the increment was applied
	// store count of written increment blocks
	writtenIncrementFiles      map[string]int64
	writtenIncrementFilesMutex sync.Mutex
}

func newUnwrapResult() *UnwrapResult {
	return &UnwrapResult{make([]string, 0), sync.Mutex{},
		make(map[string]int64), sync.Mutex{},
		make(map[string]int64), sync.Mutex{}}
}

func checkDBDirectoryForUnwrapNew(dbDataDirectory string, sentinelDto BackupSentinelDto, filesMetaDto FilesMetadataDto) error {
	tracelog.DebugLogger.Println("DB data directory before applying backup:")
	_ = filepath.Walk(dbDataDirectory,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				tracelog.DebugLogger.Println(path)
			}
			return nil
		})

	for fileName, fileDescription := range filesMetaDto.Files {
		if fileDescription.IsSkipped {
			tracelog.DebugLogger.Printf("Skipped file %v\n", fileName)
		}
	}

	if sentinelDto.TablespaceSpec != nil && !sentinelDto.TablespaceSpec.empty() {
		err := setTablespacePaths(*sentinelDto.TablespaceSpec)
		if err != nil {
			return err
		}
	}

	return nil
}

// TODO : unit tests
// Do the job of unpacking Backup object
func (backup *Backup) unwrapNew(
	dbDataDirectory string, sentinelDto BackupSentinelDto, filesMetaDto FilesMetadataDto, filesToUnwrap map[string]bool,
	createIncrementalFiles, skipRedundantTars bool) (*UnwrapResult, error) {
	useNewUnwrapImplementation = true
	err := checkDBDirectoryForUnwrapNew(dbDataDirectory, sentinelDto, filesMetaDto)
	if err != nil {
		return nil, err
	}

	tarInterpreter := NewFileTarInterpreter(dbDataDirectory, sentinelDto, filesMetaDto, filesToUnwrap, createIncrementalFiles)
	tarsToExtract, pgControlKey, err := backup.getTarsToExtract(filesMetaDto, filesToUnwrap, skipRedundantTars)
	if err != nil {
		return nil, err
	}

	// Check name for backwards compatibility. Will check for `pg_control` if WALG version of backup.
	needPgControl := IsPgControlRequired(*backup, sentinelDto)

	if pgControlKey == "" && needPgControl {
		return nil, newPgControlNotFoundError()
	}

	err = internal.ExtractAll(tarInterpreter, tarsToExtract)
	if _, ok := err.(internal.NoFilesToExtractError); ok {
		// in case of no tars to extract, just ignore this backup and proceed to the next
		tracelog.InfoLogger.Println("Skipping backup: no useful files found.")
		return tarInterpreter.UnwrapResult, nil
	}
	if err != nil {
		return nil, err
	}

	if needPgControl {
		readerMakers := []internal.ReaderMaker{internal.NewStorageReaderMaker(backup.getTarPartitionFolder(), pgControlKey)}
		err = internal.ExtractAll(tarInterpreter, readerMakers)
		if err != nil {
			return nil, errors.Wrap(err, "failed to extract pg_control")
		}
	}

	tracelog.InfoLogger.Print("\nBackup extraction complete.\n")
	return tarInterpreter.UnwrapResult, nil
}
