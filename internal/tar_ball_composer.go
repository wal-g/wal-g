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
	headersToCompose       []*tar.Header
	files                  []*ComposeFileInfo
	tarSizeThreshold       uint64
	composeRatingEvaluator ComposeRatingEvaluator
}

func NewTarBallComposer(tarSizeThreshold uint64, updateRatingEvaluator ComposeRatingEvaluator) *TarBallComposer {
	return &TarBallComposer{headersToCompose: make([]*tar.Header,0), files: make([]*ComposeFileInfo,0),
		tarSizeThreshold: tarSizeThreshold, composeRatingEvaluator: updateRatingEvaluator}
}

type ComposeFileInfo struct {
	path string
	fileInfo os.FileInfo
	wasInBase bool
	updateRating uint64
	header *tar.Header
	updatesCount uint64
	// for regular files this value should match their size on the disk
	// for increments this value is the estimated size of the increment that is going to be created
	expectedSize uint64
}

func newComposeFileInfo(path string, fileInfo os.FileInfo, wasInBase bool,
	header *tar.Header, updatesCount, updateRating, expectedFileSize uint64) *ComposeFileInfo {
	return &ComposeFileInfo{path: path, fileInfo: fileInfo, wasInBase: wasInBase,
		updateRating: updateRating, header: header, updatesCount: updatesCount, expectedSize: expectedFileSize}
}

type ComposeRatingEvaluator interface {
	Evaluate(path string, updatesCount uint64, wasInBase bool) uint64
}

type DefaultComposeRatingEvaluator struct {
	incrementFromFiles BackupFileList
}

func NewDefaultComposeRatingEvaluator(incrementFromFiles BackupFileList) *DefaultComposeRatingEvaluator {
	return &DefaultComposeRatingEvaluator{incrementFromFiles: incrementFromFiles}
}

type TarFilesCollection struct {
	files        []*ComposeFileInfo
	expectedSize uint64
}

func newTarFilesCollection() *TarFilesCollection {
	return &TarFilesCollection{files: make([]*ComposeFileInfo,0), expectedSize: 0}
}

func (collection *TarFilesCollection) AddFile(file *ComposeFileInfo) {
	collection.files = append(collection.files, file)
	collection.expectedSize += file.expectedSize
}

func (c *TarBallComposer) AddHeader(fileInfoHeader *tar.Header) {
	c.headersToCompose = append(c.headersToCompose, fileInfoHeader)
}

func (c *TarBallComposer) AddFile(path string, fileInfo os.FileInfo, wasInBase bool,
	header *tar.Header, updatesCount uint64, expectedFileSize uint64) {
	updateRating := c.composeRatingEvaluator.Evaluate(path, updatesCount, wasInBase)
	newFile := newComposeFileInfo(path, fileInfo, wasInBase, header, updatesCount, updateRating, expectedFileSize)
	c.files = append(c.files, newFile)
}

func (c *TarBallComposer) sortFiles() {
	sort.Slice(c.files, func (i,j int) bool {
		return c.files[i].updateRating < c.files[j].updateRating
	})
}

func (c *TarBallComposer) Compose() ([]*tar.Header, []*TarFilesCollection) {
	c.sortFiles()
	tarFilesCollections := make([]*TarFilesCollection,0)
	currentFilesCollection := newTarFilesCollection()
	prevUpdateRating := uint64(0)

	for _, file := range c.files {
		// if the estimated size of the current collection exceeds the threshold,
		// or if the updateRating just went to non-zero from zero,
		// start packing to the new tar files collection
		if currentFilesCollection.expectedSize > 0 &&
			currentFilesCollection.expectedSize + file.expectedSize > c.tarSizeThreshold ||
			prevUpdateRating == 0 && file.updateRating > 0 {
			tarFilesCollections = append(tarFilesCollections, currentFilesCollection)
			currentFilesCollection = newTarFilesCollection()
		}
		currentFilesCollection.AddFile(file)
		prevUpdateRating = file.updateRating
	}

	tarFilesCollections = append(tarFilesCollections, currentFilesCollection)
	return c.headersToCompose, tarFilesCollections
}

func (evaluator *DefaultComposeRatingEvaluator) Evaluate(path string, updatesCount uint64, wasInBase bool) uint64 {
	if !wasInBase {
		return updatesCount
	}
	prevUpdateCount := evaluator.incrementFromFiles[path].UpdatesCount
	if prevUpdateCount == 0 {
		return updatesCount
	}
	return (updatesCount * 100) / prevUpdateCount
}
