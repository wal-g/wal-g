package archive

import (
	"io/fs"
	"os"
)

type FolderInfo struct {
	Path     string
	fileMode fs.FileMode
}

func CreateFolderInfo(path string, fileMode fs.FileMode) *FolderInfo {
	return &FolderInfo{
		Path:     path,
		fileMode: fileMode,
	}
}

func (f *FolderInfo) Clean() error {
	err := os.RemoveAll(f.Path)
	if err != nil {
		return err
	}

	return os.MkdirAll(f.Path, f.fileMode)
}
