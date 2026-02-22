package greenplum

import (
	"fmt"
	"log/slog"
	"path"

	"github.com/wal-g/wal-g/pkg/storages/storage"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

type FilesToExtractProviderImpl struct {
	postgres.FilesToExtractProviderImpl
}

func (t FilesToExtractProviderImpl) Get(backup SegBackup, filesToUnwrap map[string]bool, skipRedundantTars bool) (
	concurrentTarsToExtract []internal.ReaderMaker, sequentialTarsToExtract []internal.ReaderMaker, err error) {
	concurrentTarsToExtract, sequentialTarsToExtract, err = t.FilesToExtractProviderImpl.Get(backup.Backup, filesToUnwrap, skipRedundantTars)
	if err != nil {
		return nil, nil, err
	}

	// AO files metadata exists only in a Greenplum segment backups.
	aoMeta, err := backup.LoadAoFilesMetadata()
	if err != nil {
		if _, ok := err.(storage.ObjectNotFoundError); !ok {
			return nil, nil,
				fmt.Errorf("failed to fetch AO files metadata for backup %s: %w", backup.Name, err)
		}
		slog.Warn(fmt.Sprintf("AO files metadata was not found. Skipping the AO segments unpacking."))
	} else {
		slog.Info(fmt.Sprintf("AO files metadata found. Will perform the AO segments unpacking."))
		for extractPath, meta := range aoMeta.Files {
			if filesToUnwrap != nil && !filesToUnwrap[extractPath] {
				slog.Info(fmt.Sprintf("Don't need to unwrap the %s AO segment file, skipping it...", extractPath))
				continue
			}
			objPath := path.Join(AoStoragePath, meta.StoragePath)
			readerMaker := internal.NewRegularFileStorageReaderMarker(backup.Folder, objPath, extractPath, meta.FileMode)
			concurrentTarsToExtract = append(concurrentTarsToExtract, readerMaker)
		}
	}

	return concurrentTarsToExtract, sequentialTarsToExtract, nil
}
