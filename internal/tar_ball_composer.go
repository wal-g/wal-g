package internal

import (
	"archive/tar"
	"os"
	"sort"
)

// TarBallComposer receives all files and tar headers
// that are going to be written to the backup,
// calculates update rating and returns tar headers and files,
// ordered by the update rating
// It also should compose tarballs in the future,
// but atm tarballs composing logic is in the bundle.go
type TarBallComposer struct {
	IncrementFromFiles BackupFileList
	headersToCompose []*tar.Header
	files            []*ComposeFileInfo
}

type ComposeFileInfo struct {
	path string
	fileInfo os.FileInfo
	wasInBase bool
	updateRating uint64
	header *tar.Header
	updatesCount uint64
}

func NewTarBallComposer(incrementFromFiles BackupFileList) *TarBallComposer {
	return &TarBallComposer{headersToCompose: make([]*tar.Header,0), files: make([]*ComposeFileInfo,0),
		IncrementFromFiles: incrementFromFiles}
}

func (c *TarBallComposer) AddHeader(fileInfoHeader *tar.Header) {
	c.headersToCompose = append(c.headersToCompose, fileInfoHeader)
}

func (c *TarBallComposer) AddFile(path string, fileInfo os.FileInfo, wasInBase bool, header *tar.Header, updatesCount uint64) {
	updateRating := c.calcUpdateRating(path, updatesCount, wasInBase)
	newFile := &ComposeFileInfo{path: path, fileInfo: fileInfo, wasInBase: wasInBase,
		updateRating: updateRating, header: header, updatesCount: updatesCount}
	c.files = append(c.files, newFile)
}

func (c *TarBallComposer) calcUpdateRating(path string, updatesCount uint64, wasInBase bool) uint64 {
	if !wasInBase {
		return updatesCount
	}
	prevUpdateCount := c.IncrementFromFiles[path].UpdatesCount
	if prevUpdateCount == 0 {
		return updatesCount
	}
	return (updatesCount * 100) / prevUpdateCount
}

func (c *TarBallComposer) sortFiles() {
	sort.Slice(c.files, func (i,j int) bool {
		return c.files[i].updateRating < c.files[j].updateRating
	})
}

func (c *TarBallComposer) Compose() ([]*tar.Header, []*ComposeFileInfo) {
	c.sortFiles()
	return c.headersToCompose, c.files
}