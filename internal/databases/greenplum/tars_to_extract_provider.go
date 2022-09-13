package greenplum

import (
	"fmt"
	"path"

	"github.com/wal-g/wal-g/pkg/storages/storage"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

type FilesToExtractProviderImpl struct {
	postgres.FilesToExtractProviderImpl
}

func (t FilesToExtractProviderImpl) Get(backup SegBackup, filesToUnwrap map[string]bool, skipRedundantTars bool) (
	tarsToExtract []internal.ReaderMaker, pgControlKey string, err error) {
	tarsToExtract, pgControlKey, err = t.FilesToExtractProviderImpl.Get(backup.Backup, filesToUnwrap, skipRedundantTars)
	if err != nil {
		return nil, "", err
	}

	// AO files metadata exists only in a Greenplum segment backups.
	aoMeta, err := backup.LoadAoFilesMetadata()
	if err != nil {
		if _, ok := err.(storage.ObjectNotFoundError); !ok {
			return nil, "",
				fmt.Errorf("failed to fetch AO files metadata for backup %s: %w", backup.Name, err)
		}
		tracelog.DebugLogger.Printf("AO files metadata was not found. Skipping the AO segments unpacking.")
	} else {
		tracelog.InfoLogger.Printf("AO files metadata found. Will perform the AO segments unpacking.")
		for extractPath, meta := range aoMeta.Files {
			if !filesToUnwrap[extractPath] {
				tracelog.InfoLogger.Printf("Don't need to unwrap the %s AO segment file, skipping it...", extractPath)
				continue
			}
			objPath := path.Join(AoStoragePath, meta.StoragePath)
			readerMaker := internal.NewRegularFileStorageReaderMarker(backup.Folder, objPath, extractPath, meta.FileMode)
			tarsToExtract = append(tarsToExtract, readerMaker)
		}
	}

	return tarsToExtract, pgControlKey, nil
}
