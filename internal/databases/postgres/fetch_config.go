package postgres

import (
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/tracelog"
)

func NewFetchConfig(backupName, dbDataDirectory string, folder storage.Folder, spec *TablespaceSpec,
	filesToUnwrap map[string]bool, skipRedundantTars bool) *FetchConfig {
	fetchConfig := &FetchConfig{
		filesToUnwrap:     filesToUnwrap,
		missingBlocks:     make(map[string]int64),
		tablespaceSpec:    spec,
		backupName:        backupName,
		folder:            folder,
		dbDataDirectory:   dbDataDirectory,
		skipRedundantTars: skipRedundantTars,
	}
	return fetchConfig
}

type FetchConfig struct {
	filesToUnwrap map[string]bool
	// missingBlocks stores count of blocks missing for file path
	missingBlocks     map[string]int64
	tablespaceSpec    *TablespaceSpec
	backupName        string
	folder            storage.Folder
	dbDataDirectory   string
	skipRedundantTars bool
}

func (fc *FetchConfig) SkipRedundantFiles(unwrapResult *UnwrapResult) {
	fc.processCreatedPageFiles(unwrapResult.createdPageFiles)
	fc.processWrittenIncrementFiles(unwrapResult.writtenIncrementFiles)
	fc.excludeCompletedFiles(unwrapResult.completedFiles)
}

func (fc *FetchConfig) excludeCompletedFile(filePath string) {
	delete(fc.filesToUnwrap, filePath)
	tracelog.DebugLogger.Printf("Excluded file %s\n", filePath)
}

func (fc *FetchConfig) processCreatedPageFiles(createdPageFiles map[string]int64) {
	for filePath, missingBlockCount := range createdPageFiles {
		_, ok := fc.filesToUnwrap[filePath]
		if !ok {
			// file is already excluded, skip it
			continue
		}
		if missingBlockCount == 0 {
			fc.excludeCompletedFile(filePath)
		} else {
			fc.missingBlocks[filePath] = missingBlockCount
		}
	}
}

func (fc *FetchConfig) processWrittenIncrementFiles(writtenIncrementFiles map[string]int64) {
	for filePath, restoredBlockCount := range writtenIncrementFiles {
		_, ok := fc.filesToUnwrap[filePath]
		if !ok {
			// file is already excluded, skip it
			continue
		}
		missingBlockCount, ok := fc.missingBlocks[filePath]
		if !ok {
			// file is not in file blocks to restore, skip it
			tracelog.WarningLogger.Printf("New written increment blocks, "+
				"but file doesn't exist in missingBlocks: '%s'", filePath)
			continue
		}
		missingBlockCount -= restoredBlockCount
		if missingBlockCount <= 0 {
			fc.excludeCompletedFile(filePath)
		} else {
			fc.missingBlocks[filePath] = missingBlockCount
		}
	}
}

func (fc *FetchConfig) excludeCompletedFiles(completedFiles []string) {
	for _, filePath := range completedFiles {
		fc.excludeCompletedFile(filePath)
	}
}
