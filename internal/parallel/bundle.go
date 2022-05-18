package parallel

import (
	"archive/tar"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/utility"

	"github.com/wal-g/wal-g/internal"
)

type TarSizeError struct {
	error
}

func newTarSizeError(packedFileSize, expectedSize int64) TarSizeError {
	return TarSizeError{errors.Errorf("packed wrong numbers of bytes %d instead of %d", packedFileSize, expectedSize)}
}

type Bundle struct {
	Directory string
	Sentinel  *internal.Sentinel

	TarBallComposer TarBallComposer
	TarBallQueue    *internal.TarBallQueue

	Crypter crypto.Crypter

	TarSizeThreshold int64

	ExcludedFilenames map[string]utility.Empty

	FilesFilter FilesFilter
}

func NewBundle(
	directory string, crypter crypto.Crypter,
	tarSizeThreshold int64, excludedFilenames map[string]utility.Empty) *Bundle {
	return &Bundle{
		Directory:         directory,
		Crypter:           crypter,
		TarSizeThreshold:  tarSizeThreshold,
		ExcludedFilenames: excludedFilenames,
		FilesFilter:       &CommonFilesFilter{},
	}
}

func (bundle *Bundle) StartQueue(tarBallMaker internal.TarBallMaker) error {
	bundle.TarBallQueue = internal.NewTarBallQueue(bundle.TarSizeThreshold, tarBallMaker)
	return bundle.TarBallQueue.StartQueue()
}

func (bundle *Bundle) SetupComposer(composerMaker TarBallComposerMaker) (err error) {
	tarBallComposer, err := composerMaker.Make(bundle)
	if err != nil {
		return err
	}
	bundle.TarBallComposer = tarBallComposer
	return nil
}

func (bundle *Bundle) FinishQueue() error {
	return bundle.TarBallQueue.FinishQueue()
}

func (bundle *Bundle) AddToBundle(path string, info os.FileInfo, err error) error {
	if err != nil {
		if os.IsNotExist(err) {
			tracelog.WarningLogger.Println(path, " deleted during filepath walk")
			return nil
		}
		return errors.Wrap(err, "HandleWalkedFSObject: walk failed")
	}

	fileName := info.Name()
	_, excluded := bundle.ExcludedFilenames[fileName]
	isDir := info.IsDir()

	if excluded && !isDir {
		return nil
	}
	fileInfoHeader, err := bundle.createTarFileInfoHeader(path, info)
	if err != nil {
		return err
	}

	tracelog.DebugLogger.Println(fileInfoHeader.Name)

	if bundle.FilesFilter.ShouldUploadFile(path) && info.Mode().IsRegular() {
		bundle.TarBallComposer.AddFile(NewComposeFileInfo(path, info, false, false, fileInfoHeader))
	} else {
		err := bundle.TarBallComposer.AddHeader(fileInfoHeader, info)
		if err != nil {
			return err
		}
		if excluded && isDir {
			return filepath.SkipDir
		}
	}

	return nil
}

func (bundle *Bundle) FinishComposing() (TarFileSets, error) {
	return bundle.TarBallComposer.FinishComposing()
}

func (bundle *Bundle) getFileRelPath(fileAbsPath string) string {
	return utility.GetSubdirectoryRelativePath(fileAbsPath, bundle.Directory)
}

func (bundle *Bundle) createTarFileInfoHeader(path string, info os.FileInfo) (header *tar.Header, err error) {
	var symLinkTarget string
	if info.Mode()&os.ModeSymlink == os.ModeSymlink {
		symLinkTarget, err = os.Readlink(path)
		if err != nil {
			return
		}
	}

	header, err = tar.FileInfoHeader(info, symLinkTarget)
	if err != nil {
		return nil, errors.Wrap(err, "addToBundle: could not grab header info")
	}

	header.Name = bundle.getFileRelPath(path)
	return
}
