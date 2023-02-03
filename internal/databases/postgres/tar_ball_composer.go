package postgres

import (
	"errors"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal"
)

type TarBallComposerType int

const (
	RegularComposer TarBallComposerType = iota + 1
	RatingComposer
	CopyComposer
	DatabaseComposer
)

// TarBallComposerMaker is used to make an instance of TarBallComposer
type TarBallComposerMaker interface {
	Make(bundle *Bundle) (internal.TarBallComposer, error)
}

func NewTarBallComposerMaker(composerType TarBallComposerType, queryRunner *PgQueryRunner, uploader *internal.Uploader,
	newBackupName string, filePackOptions TarBallFilePackerOptions,
	withoutFilesMetadata bool) (TarBallComposerMaker, error) {
	folder := uploader.UploadingFolder

	if withoutFilesMetadata {
		if composerType != RegularComposer {
			tracelog.InfoLogger.Printf("No files metadata mode is enabled. Choosing the regular tar ball composer.")
		}
		return NewRegularTarBallComposerMaker(filePackOptions, &internal.NopBundleFiles{}, internal.NewNopTarFileSets()), nil
	}

	switch composerType {
	case RegularComposer:
		return NewRegularTarBallComposerMaker(filePackOptions, &internal.RegularBundleFiles{}, internal.NewRegularTarFileSets()), nil
	case RatingComposer:
		relFileStats, err := newRelFileStatistics(queryRunner)
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
			return NewRegularTarBallComposerMaker(filePackOptions, &internal.RegularBundleFiles{}, internal.NewRegularTarFileSets()), nil
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
	case DatabaseComposer:
		return NewDirDatabaseTarBallComposerMaker(&internal.RegularBundleFiles{}, filePackOptions, internal.NewRegularTarFileSets()), nil
	default:
		return nil, errors.New("NewTarBallComposerMaker: Unknown TarBallComposerType")
	}
}
