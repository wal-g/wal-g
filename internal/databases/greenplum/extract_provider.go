package greenplum

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

type ExtractProviderImpl struct {
	FilesToExtractProviderImpl
}

func (t ExtractProviderImpl) Get(
	backup postgres.Backup,
	filesToUnwrap map[string]bool,
	skipRedundantTars bool,
	dbDataDir string,
	createNewIncrementalFiles bool,
) (postgres.IncrementalTarInterpreter, []internal.ReaderMaker, string, error) {
	segBackup := ToGpSegBackup(backup)

	interpreter, err := t.getTarInterpreter(dbDataDir, segBackup, filesToUnwrap, createNewIncrementalFiles)
	if err != nil {
		return nil, nil, "", err
	}

	tarsToExtract, pgControlKey, err := t.FilesToExtractProviderImpl.Get(segBackup, filesToUnwrap, skipRedundantTars)
	return interpreter, tarsToExtract, pgControlKey, err
}

func (t ExtractProviderImpl) getTarInterpreter(dbDataDir string, backup SegBackup,
	filesToUnwrap map[string]bool, createNewIncrementalFiles bool) (*IncrementalTarInterpreter, error) {
	_, err := backup.LoadAoFilesMetadata()
	if err != nil {
		return nil, err
	}

	_, _, err = backup.Backup.GetSentinelAndFilesMetadata()
	if err != nil {
		return nil, err
	}

	return NewIncrementalTarInterpreter(dbDataDir, *backup.SentinelDto, *backup.FilesMetadataDto, *backup.AoFilesMetadataDto,
		filesToUnwrap, createNewIncrementalFiles), nil
}
