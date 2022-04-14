package postgres

import (
	"archive/tar"
	"errors"
	"os"

	"github.com/wal-g/tracelog"

	"github.com/jackc/pgx"
	"github.com/wal-g/wal-g/internal"
)

// TarBallComposer is used to compose files into tarballs.
type TarBallComposer interface {
	AddFile(info *ComposeFileInfo)
	AddHeader(header *tar.Header, fileInfo os.FileInfo) error
	SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo)
	FinishComposing() (TarFileSets, error)
	GetFiles() BundleFiles
}

// ComposeFileInfo holds data which is required to pack a file to some tarball
type ComposeFileInfo struct {
	path          string
	fileInfo      os.FileInfo
	wasInBase     bool
	header        *tar.Header
	isIncremented bool
}

func NewComposeFileInfo(path string, fileInfo os.FileInfo, wasInBase, isIncremented bool,
	header *tar.Header) *ComposeFileInfo {
	return &ComposeFileInfo{path: path, fileInfo: fileInfo,
		wasInBase: wasInBase, header: header, isIncremented: isIncremented}
}

type TarBallComposerType int

const (
	RegularComposer TarBallComposerType = iota + 1
	RatingComposer
	CopyComposer
	GreenplumComposer
)

// TarBallComposerMaker is used to make an instance of TarBallComposer
type TarBallComposerMaker interface {
	Make(bundle *Bundle) (TarBallComposer, error)
}

func NewTarBallComposerMaker(composerType TarBallComposerType, conn *pgx.Conn, uploader *internal.Uploader,
	newBackupName string, filePackOptions TarBallFilePackerOptions,
	withoutFilesMetadata bool) (TarBallComposerMaker, error) {
	folder := uploader.UploadingFolder
	switch composerType {
	case RegularComposer:
		if withoutFilesMetadata {
			return NewRegularTarBallComposerMaker(filePackOptions, &NopBundleFiles{}, NewNopTarFileSets()), nil
		}
		return NewRegularTarBallComposerMaker(filePackOptions, &RegularBundleFiles{}, NewRegularTarFileSets()), nil
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
			return NewRegularTarBallComposerMaker(filePackOptions, &RegularBundleFiles{}, NewRegularTarFileSets()), nil
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
	case GreenplumComposer:
		relStorageMap, err := newAoRelFileStorageMap(conn)
		if err != nil {
			return nil, err
		}

		return NewGpTarBallComposerMaker(relStorageMap, uploader, newBackupName)
	default:
		return nil, errors.New("NewTarBallComposerMaker: Unknown TarBallComposerType")
	}
}
