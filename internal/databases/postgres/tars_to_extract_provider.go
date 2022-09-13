package postgres

import (
	"regexp"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

type FilesToExtractProvider interface {
	Get(backup Backup, filesToUnwrap map[string]bool, skipRedundantTars bool) (
		tarsToExtract []internal.ReaderMaker, pgControlKey string, err error)
}

type FilesToExtractProviderImpl struct {
}

func (t FilesToExtractProviderImpl) Get(backup Backup, filesToUnwrap map[string]bool, skipRedundantTars bool) (
	tarsToExtract []internal.ReaderMaker, pgControlKey string, err error) {
	_, filesMeta, err := backup.GetSentinelAndFilesMetadata()
	if err != nil {
		return nil, "", err
	}

	tarNames, err := backup.GetTarNames()
	if err != nil {
		return nil, "", err
	}
	tracelog.DebugLogger.Printf("Tars to extract: '%+v'\n", tarNames)
	tarsToExtract = make([]internal.ReaderMaker, 0, len(tarNames))

	pgControlRe := regexp.MustCompile(`^.*?pg_control\.tar(\..+$|$)`)
	for _, tarName := range tarNames {
		// Separate the pg_control tarName from the others to
		// extract it at the end, as to prevent server startup
		// with incomplete backup restoration.  But only if it
		// exists: it won't in the case of WAL-E backup
		// backwards compatibility.
		if pgControlRe.MatchString(tarName) {
			if pgControlKey != "" {
				panic("expect only one pg_control tar name match")
			}
			pgControlKey = tarName
			continue
		}

		if skipRedundantTars && !shouldUnwrapTar(tarName, filesMeta, filesToUnwrap) {
			continue
		}

		tarToExtract := internal.NewStorageReaderMaker(backup.getTarPartitionFolder(), tarName)
		tarsToExtract = append(tarsToExtract, tarToExtract)
	}
	return tarsToExtract, pgControlKey, nil
}
