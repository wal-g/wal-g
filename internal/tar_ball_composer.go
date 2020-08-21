package internal

import (
	"archive/tar"
	"errors"
	"os"
)

// TarBallComposer is used to compose files into tarballs.
type TarBallComposer interface {
	AddFile(info *ComposeFileInfo)
	AddHeader(header *tar.Header, fileInfo os.FileInfo) error
	SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo)
	PackTarballs() (TarFileSets, error)
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

type TarFileSets map[string][]string

func NewComposeFileInfo(path string, fileInfo os.FileInfo, wasInBase, isIncremented bool,
	header *tar.Header) *ComposeFileInfo {
	return &ComposeFileInfo{path: path, fileInfo: fileInfo,
		wasInBase: wasInBase, header: header, isIncremented: isIncremented}
}

type TarBallComposerType int

const (
	RegularComposer TarBallComposerType = iota + 1
)

func NewTarBallComposer(composerType TarBallComposerType, bundle *Bundle, verifyPageChecksums bool) (TarBallComposer, error) {
	switch composerType {
	case RegularComposer:
		bundleFiles := &RegularBundleFiles{}
		tarBallFilePacker := newTarBallFilePacker(bundle.DeltaMap, bundle.IncrementFromLsn, bundleFiles, verifyPageChecksums)
		return NewRegularTarBallComposer(bundle.TarBallQueue, tarBallFilePacker, bundleFiles, bundle.Crypter), nil
	default:
		return nil, errors.New("NewTarBallComposer: Unknown TarBallComposerType")
	}
}
