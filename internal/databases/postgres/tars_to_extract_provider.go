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
	needPgControl := IsPgControlRequired(backup)
	for _, object := range objects {
		tarName := object.GetName()
		// Extract pg_control and the generated backup_label last: they mark a
		// consistent backup and must override any older label from the data directory.
		sequential := pgControlRe.MatchString(tarName) || backupLabelRe.MatchString(tarName)
		if !sequential && skipRedundantTars && !shouldUnwrapTar(tarName, filesMeta, filesToUnwrap) {
			continue
		}
		tarToExtract := internal.NewStorageReaderMaker(backup.GetTarPartitionFolder(), tarName)
		if sequential {
			sequentialTarsToExtract = append(sequentialTarsToExtract, tarToExtract)
		} else {
			concurrentTarsToExtract = append(concurrentTarsToExtract, tarToExtract)
		}
		// Legacy WAL-E restores may skip the sequential archives entirely.
		if !sequential || needPgControl {
			size := object.GetSize()
			if size <= 0 || total > math.MaxInt64-size {
				knownSize = false
			} else {
				total += size
			}
		}
	}
	// Reverse unpack skips a backup with no useful concurrent archives before
	// reading its control/label files. Unknown sizes must remain indeterminate.
	if !knownSize || len(concurrentTarsToExtract) == 0 {
		total = 0
	}
	progress.BeginDownload(ctx, total)
	return concurrentTarsToExtract, sequentialTarsToExtract, nil
}
