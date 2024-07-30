package archive

import (
	"io/fs"
	"os"
	"path/filepath"
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

func (f *FolderInfo) CleanParent() error {
	parent := filepath.Dir(f.Path)
	err := os.RemoveAll(parent)
	if err != nil {
		return err
	}

	return os.MkdirAll(f.Path, f.fileMode)
}
