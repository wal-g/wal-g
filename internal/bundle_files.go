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
	AddFileDescription(name string, backupFileDescription BackupFileDescription)
	AddFileWithCorruptBlocks(tarHeader *tar.Header, fileInfo os.FileInfo, isIncremented bool,
		corruptedBlocks []uint32, storeAllBlocks bool)
	GetUnderlyingMap() *sync.Map
}

type RegularBundleFiles struct {
	sync.Map
}

func (files *RegularBundleFiles) AddSkippedFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	files.AddFileDescription(tarHeader.Name,
		BackupFileDescription{IsSkipped: true, IsIncremented: false, MTime: fileInfo.ModTime()})
}

func (files *RegularBundleFiles) AddFile(tarHeader *tar.Header, fileInfo os.FileInfo, isIncremented bool) {
	files.AddFileDescription(tarHeader.Name,
		BackupFileDescription{IsSkipped: false, IsIncremented: isIncremented, MTime: fileInfo.ModTime()})
}

func (files *RegularBundleFiles) AddFileDescription(name string, backupFileDescription BackupFileDescription) {
	files.Store(name, backupFileDescription)
}

func (files *RegularBundleFiles) AddFileWithCorruptBlocks(tarHeader *tar.Header, fileInfo os.FileInfo,
	isIncremented bool, corruptedBlocks []uint32, storeAllBlocks bool) {
	fileDescription := BackupFileDescription{IsSkipped: false, IsIncremented: isIncremented, MTime: fileInfo.ModTime()}
	fileDescription.SetCorruptBlocks(corruptedBlocks, storeAllBlocks)
	files.AddFileDescription(tarHeader.Name, fileDescription)
}

func (files *RegularBundleFiles) GetUnderlyingMap() *sync.Map {
	return &files.Map
}

type NopBundleFiles struct {
}

func (files *NopBundleFiles) AddSkippedFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
}

func (files *NopBundleFiles) AddFile(tarHeader *tar.Header, fileInfo os.FileInfo, isIncremented bool) {
}

func (files *NopBundleFiles) AddFileDescription(name string, backupFileDescription BackupFileDescription) {
}

func (files *NopBundleFiles) AddFileWithCorruptBlocks(tarHeader *tar.Header, fileInfo os.FileInfo,
	isIncremented bool, corruptedBlocks []uint32, storeAllBlocks bool) {
}

func (files *NopBundleFiles) GetUnderlyingMap() *sync.Map {
	return &sync.Map{}
}
