package archive

import (
	"fmt"
	"os"
	"path/filepath"
)

type AofFolderInfo struct {
	Path string
}

func CreateAofFolderInfo(path string) *AofFolderInfo {
	return &AofFolderInfo{
		Path: path,
	}
}

func (f *AofFolderInfo) CleanData() error {
	path := filepath.Clean(f.Path)
	err := os.RemoveAll(path)
	if err != nil {
		return fmt.Errorf("failed to remove AOF folder: %v", err)
	}

	parent := filepath.Dir(path)
	starred := filepath.Join(parent, "*.rdb")
	contents, err := filepath.Glob(starred)
	if err != nil {
		return fmt.Errorf("failed to create glob for rdb files: %v", err)
	}

	for _, item := range contents {
		err = os.RemoveAll(item)
		if err != nil {
			return fmt.Errorf("failed to remove rdb file: %v", err)
		}
	}

	return nil
}
