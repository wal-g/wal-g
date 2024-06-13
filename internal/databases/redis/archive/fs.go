package archive

import (
	"io/fs"
	"os"
)

type Folder struct {
	Path string
	fileMode fs.FileMode
}

func CreateFolder(path string, fileMode fs.FileMode) *Folder {
	return &Folder{
		Path: path,
		fileMode: fileMode,
	}
}

func (f *Folder) Clean() error {
	err := os.RemoveAll(f.Path)
	if err != nil {
		return err
	}

	return os.MkdirAll(f.Path, f.fileMode)
}
