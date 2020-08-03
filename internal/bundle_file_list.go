package internal

import (
	"archive/tar"
	"os"
	"sync"
)

// BundleFileList represents the files in the backup that is going to be created
type BundleFileList interface {
	AddSkippedFile(tarHeader *tar.Header, fileInfo os.FileInfo)
	AddFile(tarHeader *tar.Header, fileInfo os.FileInfo, isIncremented bool)
	GetUnderlyingMap() *sync.Map
}

type RegularBundleFileList struct {
	sync.Map
}

func (list *RegularBundleFileList) AddSkippedFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	list.Store(tarHeader.Name,
		BackupFileDescription{IsSkipped: true, IsIncremented: false, MTime: fileInfo.ModTime()})
}

func (list *RegularBundleFileList) AddFile(tarHeader *tar.Header, fileInfo os.FileInfo, isIncremented bool) {
	list.Store(tarHeader.Name,
		BackupFileDescription{IsSkipped: false, IsIncremented: isIncremented, MTime: fileInfo.ModTime()})
}

func (list *RegularBundleFileList) GetUnderlyingMap() *sync.Map {
	return &list.Map
}