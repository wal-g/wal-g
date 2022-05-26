package internal

import (
	"archive/tar"
	"os"
)

type TarBallComposer interface {
	AddFile(info *ComposeFileInfo)
	AddHeader(header *tar.Header, fileInfo os.FileInfo) error
	SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo)
	FinishComposing() (TarFileSets, error)
	GetFiles() BundleFiles
}

// TarBallComposerMaker is used to make an instance of TarBallComposer
type TarBallComposerMaker interface {
	Make(bundle *Bundle) (TarBallComposer, error)
}

type ComposeFileInfo struct {
	Path          string
	FileInfo      os.FileInfo
	WasInBase     bool
	Header        *tar.Header
	IsIncremented bool
}

func NewComposeFileInfo(path string, fileInfo os.FileInfo, wasInBase, isIncremented bool,
	header *tar.Header) *ComposeFileInfo {
	return &ComposeFileInfo{Path: path, FileInfo: fileInfo,
		WasInBase: wasInBase, Header: header, IsIncremented: isIncremented}
}
