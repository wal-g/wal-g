package internal

import (
	"archive/tar"
	"os"
	"sync"
)

// BundleFiles represents the files in the backup that is going to be created
type BundleFiles interface {
	AddSkippedFile(tarHeader *tar.Header, fileInfo os.FileInfo)
	AddFile(tarHeader *tar.Header, fileInfo os.FileInfo, isIncremented bool)
	AddFileWithCorruptBlocks(tarHeader *tar.Header, fileInfo os.FileInfo, isIncremented bool, corruptedBlocks []uint32)
	GetUnderlyingMap() *sync.Map
}

type RegularBundleFiles struct {
	sync.Map
}

func (files *RegularBundleFiles) AddSkippedFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	files.Store(tarHeader.Name,
		BackupFileDescription{IsSkipped: true, IsIncremented: false, MTime: fileInfo.ModTime()})
}

func (files *RegularBundleFiles) AddFile(tarHeader *tar.Header, fileInfo os.FileInfo, isIncremented bool) {
	files.Store(tarHeader.Name,
		BackupFileDescription{IsSkipped: false, IsIncremented: isIncremented, MTime: fileInfo.ModTime()})
}

func (files *RegularBundleFiles) AddFileWithCorruptBlocks(tarHeader *tar.Header, fileInfo os.FileInfo,
	isIncremented bool, corruptedBlocks []uint32) {
	fileDescription := BackupFileDescription{IsSkipped: false, IsIncremented: isIncremented, MTime: fileInfo.ModTime()}
	fileDescription.SetCorruptBlocks(corruptedBlocks)
	files.Store(tarHeader.Name, fileDescription)
}

func (files *RegularBundleFiles) GetUnderlyingMap() *sync.Map {
	return &files.Map
}
