package internal

import (
	"archive/tar"
	"os"
)

// TarBallComposer is used to compose files into tarballs.
type TarBallComposer interface {
	AddFile(info *ComposeFileInfo)
	AddHeader(header *tar.Header, fileInfo os.FileInfo) error
	SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo)
	PackTarballs() (TarFileSets, error)
	GetFiles() BundleFileList
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

func NewTarBallComposer(composerType TarBallComposerType, bundle *Bundle) (TarBallComposer, error) {
	switch composerType {
	case RegularComposer:
		fileList := &RegularBundleFileList{}
		tarBallFilePacker := newTarBallFilePacker(bundle.DeltaMap, bundle.IncrementFromLsn, fileList)
		return NewRegularTarBallComposer(bundle.TarBallQueue, tarBallFilePacker, fileList, bundle.Crypter), nil
	default:
		fileList := &RegularBundleFileList{}
		tarBallFilePacker := newTarBallFilePacker(bundle.DeltaMap, bundle.IncrementFromLsn, fileList)
		return NewRegularTarBallComposer(bundle.TarBallQueue, tarBallFilePacker, fileList, bundle.Crypter), nil
	}
}
