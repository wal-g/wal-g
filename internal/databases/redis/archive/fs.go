package archive

import (
	"os"
	"path/filepath"
)

type FolderInfo struct {
	Path string
}

func CreateFolderInfo(path string) *FolderInfo {
	return &FolderInfo{
		Path: path,
	}
}

func (f *FolderInfo) CleanParent() (err error) {
	parent := filepath.Dir(f.Path)
	starred := filepath.Join(parent, "*")
	contents, err := filepath.Glob(starred)
	if err != nil {
		return
	}

	for _, item := range contents {
		err = os.RemoveAll(item)
		if err != nil {
			return
		}
	}
	return
}
