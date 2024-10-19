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
	concurrentTarsToExtract []internal.ReaderMaker, sequentialTarsToExtract []internal.ReaderMaker, err error) {
	_, filesMeta, err := backup.GetSentinelAndFilesMetadata()
	if err != nil {
		return nil, nil, err
	}

	tarNames, err := backup.GetTarNames()
	if err != nil {
		return nil, nil, err
	}
	tracelog.DebugLogger.Printf("Tars to extract: '%+v'\n", tarNames)
	concurrentTarsToExtract = make([]internal.ReaderMaker, 0, len(tarNames))
	sequentialTarsToExtract = make([]internal.ReaderMaker, 0, 2)

	pgControlRe := regexp.MustCompile(`^.*?pg_control\.tar(\..+$|$)`)
	backupLabelRe := regexp.MustCompile(`^.*?backup_label\.tar(\..+$|$)`)
	for _, tarName := range tarNames {
		// Separate the pg_control tarName from the others to
		// extract it at the end, as to prevent server startup
		// with incomplete backup restoration.  But only if it
		// exists: it won't in the case of WAL-E backup
		// backwards compatibility.
		if pgControlRe.MatchString(tarName) {
			tarToExtract := internal.NewStorageReaderMaker(backup.getTarPartitionFolder(), tarName)
			sequentialTarsToExtract = append(sequentialTarsToExtract, tarToExtract)
			continue
		}

		// wal-g creates fictional `backup_label.tar` at the end of backup.
		// It contains values from `pg_stop_backup`. Postgres datadir may have other `backup_label` file
		// from some exclusive backup (likely not ours).
		// We should override it in order to reach correct end of backup point.
		// so, we should extract our `backup_label` after extracting regular tars.
		if backupLabelRe.MatchString(tarName) {
			tarToExtract := internal.NewStorageReaderMaker(backup.getTarPartitionFolder(), tarName)
			sequentialTarsToExtract = append(sequentialTarsToExtract, tarToExtract)
			continue
		}

		if skipRedundantTars && !shouldUnwrapTar(tarName, filesMeta, filesToUnwrap) {
			continue
		}

		tarToExtract := internal.NewStorageReaderMaker(backup.getTarPartitionFolder(), tarName)
		concurrentTarsToExtract = append(concurrentTarsToExtract, tarToExtract)
	}
	return concurrentTarsToExtract, sequentialTarsToExtract, nil
}
