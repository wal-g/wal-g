package postgres

import (
	"errors"

	"github.com/wal-g/tracelog"

	"github.com/jackc/pgx"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/parallel"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type TarBallComposerType int

const (
	RegularComposer TarBallComposerType = iota + 1
	RatingComposer
	CopyComposer
)

// TarBallComposerMaker is used to make an instance of TarBallComposer
type TarBallComposerMaker interface {
	Make(bundle *Bundle) (parallel.TarBallComposer, error)
}

func NewTarBallComposerMaker(composerType TarBallComposerType, conn *pgx.Conn, folder storage.Folder,
	newBackupName string, filePackOptions TarBallFilePackerOptions,
	withoutFilesMetadata bool) (TarBallComposerMaker, error) {
	switch composerType {
	case RegularComposer:
		if withoutFilesMetadata {
			return NewRegularTarBallComposerMaker(filePackOptions, &parallel.NopBundleFiles{}, parallel.NewNopTarFileSets()), nil
		}
		return NewRegularTarBallComposerMaker(filePackOptions, &parallel.RegularBundleFiles{}, parallel.NewRegularTarFileSets()), nil
	case RatingComposer:
		relFileStats, err := newRelFileStatistics(conn)
		if err != nil {
			return nil, err
		}
		return NewRatingTarBallComposerMaker(relFileStats, filePackOptions)
	case CopyComposer:
		previousBackupName, err := internal.GetLatestBackupName(folder)
		if err != nil {
			tracelog.InfoLogger.Printf(
				"Failed to init the CopyComposer, will use the RegularComposer instead:"+
					" couldn't get the previous backup name: %v", err)
			return NewRegularTarBallComposerMaker(filePackOptions, &parallel.RegularBundleFiles{}, parallel.NewRegularTarFileSets()), nil
		}
		previousBackup := NewBackup(folder, previousBackupName)
		prevBackupSentinelDto, _, err := previousBackup.GetSentinelAndFilesMetadata()
		if err != nil {
			return nil, err
		}
		if prevBackupSentinelDto.IncrementFullName != nil {
			previousBackupName = *prevBackupSentinelDto.IncrementFullName
			previousBackup = NewBackup(folder, previousBackupName)
			_, _, err = previousBackup.GetSentinelAndFilesMetadata()
			if err != nil {
				return nil, err
			}
		}
		return NewCopyTarBallComposerMaker(previousBackup, newBackupName, filePackOptions), nil
	default:
		return nil, errors.New("NewTarBallComposerMaker: Unknown TarBallComposerType")
	}
}
