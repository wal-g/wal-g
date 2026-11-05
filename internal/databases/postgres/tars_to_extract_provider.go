package postgres

import (
	"context"
	"math"
	"regexp"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/progress"
)

type FilesToExtractProvider interface {
	Get(ctx context.Context, backup Backup, filesToUnwrap map[string]bool, skipRedundantTars bool) (
		tarsToExtract []internal.ReaderMaker, pgControlKey string, err error)
}

type FilesToExtractProviderImpl struct {
}

func (t FilesToExtractProviderImpl) Get(ctx context.Context, backup Backup, filesToUnwrap map[string]bool, skipRedundantTars bool) (
	concurrentTarsToExtract []internal.ReaderMaker, sequentialTarsToExtract []internal.ReaderMaker, err error) {
	_, filesMeta, err := backup.GetSentinelAndFilesMetadata(ctx)
	if err != nil {
		return nil, nil, err
	}

	objects, err := backup.getTarObjects(ctx)
	if err != nil {
		return nil, nil, err
	}
	tracelog.DebugLogger.Printf("Found %d tar archives to consider for extraction", len(objects))
	concurrentTarsToExtract = make([]internal.ReaderMaker, 0, len(objects))
	sequentialTarsToExtract = make([]internal.ReaderMaker, 0, 2)

	pgControlRe := regexp.MustCompile(`^.*?pg_control\.tar(\..+$|$)`)
	backupLabelRe := regexp.MustCompile(`^.*?backup_label\.tar(\..+$|$)`)
	var total int64
	knownSize := true
	addSize := func(size int64, willRead bool) {
		if !willRead {
			return
		}
		if size <= 0 || total > math.MaxInt64-size {
			knownSize = false
		} else {
			total += size
		}
	}
	// Legacy WAL-E restores may skip the sequential archives entirely.
	needPgControl := IsPgControlRequired(backup)
	for _, object := range objects {
		tarName := object.GetName()
		// Separate the pg_control tarName from the others to
		// extract it at the end, as to prevent server startup
		// with incomplete backup restoration.  But only if it
		// exists: it won't in the case of WAL-E backup
		// backwards compatibility.
		if pgControlRe.MatchString(tarName) {
			tarToExtract := internal.NewStorageReaderMaker(backup.GetTarPartitionFolder(), tarName)
			sequentialTarsToExtract = append(sequentialTarsToExtract, tarToExtract)
			addSize(object.GetSize(), needPgControl)
			continue
		}

		// wal-g creates fictional `backup_label.tar` at the end of backup.
		// It contains values from `pg_stop_backup`. Postgres datadir may have other `backup_label` file
		// from some exclusive backup (likely not ours).
		// We should override it in order to reach correct end of backup point.
		// so, we should extract our `backup_label` after extracting regular tars.
		if backupLabelRe.MatchString(tarName) {
			tarToExtract := internal.NewStorageReaderMaker(backup.GetTarPartitionFolder(), tarName)
			sequentialTarsToExtract = append(sequentialTarsToExtract, tarToExtract)
			addSize(object.GetSize(), needPgControl)
			continue
		}

		if skipRedundantTars && !shouldUnwrapTar(tarName, filesMeta, filesToUnwrap) {
			continue
		}

		tarToExtract := internal.NewStorageReaderMaker(backup.GetTarPartitionFolder(), tarName)
		concurrentTarsToExtract = append(concurrentTarsToExtract, tarToExtract)
		addSize(object.GetSize(), true)
	}
	// Reverse unpack skips a backup with no useful concurrent archives before
	// reading its control/label files. Unknown sizes must remain indeterminate.
	if !knownSize || len(concurrentTarsToExtract) == 0 {
		total = 0
	}
	progress.BeginDownload(ctx, total)
	return concurrentTarsToExtract, sequentialTarsToExtract, nil
}
