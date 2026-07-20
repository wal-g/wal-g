package greenplum

import (
	"context"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

type ExtractProviderImpl struct {
	FilesToExtractProviderImpl
}

func (t ExtractProviderImpl) Get(
	ctx context.Context,
	backup postgres.Backup,
	filesToUnwrap map[string]bool,
	skipRedundantTars bool,
	dbDataDir string,
	createNewIncrementalFiles bool,
) (postgres.IncrementalTarInterpreter, []internal.ReaderMaker, []internal.ReaderMaker, error) {
	segBackup := ToGpSegBackup(backup)

	interpreter, err := t.getTarInterpreter(ctx, dbDataDir, segBackup, filesToUnwrap, createNewIncrementalFiles)
	if err != nil {
		return nil, nil, nil, err
	}

	concurrentTarsToExtract, sequentialTarsToExtract, err := t.FilesToExtractProviderImpl.Get(ctx, segBackup, filesToUnwrap, skipRedundantTars)
	return interpreter, concurrentTarsToExtract, sequentialTarsToExtract, err
}

func (t ExtractProviderImpl) getTarInterpreter(ctx context.Context, dbDataDir string, backup SegBackup,
	filesToUnwrap map[string]bool, createNewIncrementalFiles bool) (*IncrementalTarInterpreter, error) {
	_, err := backup.LoadAoFilesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	_, _, err = backup.GetSentinelAndFilesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	return NewIncrementalTarInterpreter(dbDataDir, *backup.SentinelDto, *backup.FilesMetadataDto, *backup.AoFilesMetadataDto,
		filesToUnwrap, createNewIncrementalFiles), nil
}
