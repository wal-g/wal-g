package greenplum

import (
	"context"
	"fmt"
	"path"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/greenplum/pax"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type FilesToExtractProviderImpl struct {
	postgres.FilesToExtractProviderImpl
}

func (t FilesToExtractProviderImpl) Get(ctx context.Context, backup SegBackup, filesToUnwrap map[string]bool, skipRedundantTars bool) (
	concurrentTarsToExtract []internal.ReaderMaker, sequentialTarsToExtract []internal.ReaderMaker, err error) {
	concurrentTarsToExtract, sequentialTarsToExtract, err = t.FilesToExtractProviderImpl.Get(ctx, backup.Backup,
		filesToUnwrap, skipRedundantTars)
	if err != nil {
		return nil, nil, err
	}

	// AO files metadata exists only in a Greenplum segment backups.
	aoMeta, err := backup.LoadAoFilesMetadata(ctx)
	if err != nil {
		if _, ok := err.(storage.ObjectNotFoundError); !ok {
			return nil, nil,
				fmt.Errorf("failed to fetch AO files metadata for backup %s: %w", backup.Name, err)
		}
		tracelog.WarningLogger.Printf("AO files metadata was not found. Skipping the AO segments unpacking.")
	} else {
		tracelog.InfoLogger.Printf("AO files metadata found. Will perform the AO segments unpacking.")
		for extractPath, meta := range aoMeta.Files {
			if filesToUnwrap != nil && !filesToUnwrap[extractPath] {
				tracelog.InfoLogger.Printf("Don't need to unwrap the %s AO segment file, skipping it...", extractPath)
				continue
			}
			objPath := path.Join(AoStoragePath, meta.StoragePath)
			readerMaker := internal.NewRegularFileStorageReaderMarker(backup.Folder, objPath, extractPath, meta.FileMode)
			concurrentTarsToExtract = append(concurrentTarsToExtract, readerMaker)
		}
	}

	// PAX files metadata only exists for Cloudberry backups that included PAX relations.
	paxMeta, err := backup.LoadPaxFilesMetadata(ctx)
	if err != nil {
		if _, ok := err.(storage.ObjectNotFoundError); !ok {
			return nil, nil,
				fmt.Errorf("failed to fetch PAX files metadata for backup %s: %w", backup.Name, err)
		}
		tracelog.DebugLogger.Printf("PAX files metadata was not found. Skipping PAX file unpacking.")
	} else {
		tracelog.InfoLogger.Printf("PAX files metadata found. Will perform PAX file unpacking.")
		for extractPath, meta := range paxMeta.Files {
			if filesToUnwrap != nil && !filesToUnwrap[extractPath] {
				tracelog.InfoLogger.Printf("Don't need to unwrap the %s PAX file, skipping it...", extractPath)
				continue
			}
			objPath := path.Join(pax.StoragePath, meta.StoragePath)
			readerMaker := internal.NewRegularFileStorageReaderMarker(backup.Folder, objPath, extractPath, meta.FileMode)
			concurrentTarsToExtract = append(concurrentTarsToExtract, readerMaker)
		}
	}

	return concurrentTarsToExtract, sequentialTarsToExtract, nil
}
