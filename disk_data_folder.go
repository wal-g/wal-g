package walg

import (
	"io"
	"os"
	"path/filepath"
)

type DiskDataFolder struct {
	path string
}

func NewDiskDataFolder(folderPath string) (*DiskDataFolder, error) {
	_, err := os.Stat(folderPath)
	if os.IsNotExist(err) {
		err = os.MkdirAll(folderPath, os.ModePerm)
	}
	if err != nil {
		return nil, err
	}
	return &DiskDataFolder{folderPath}, nil
}

func (folder *DiskDataFolder) OpenReadonlyFile(filename string) (io.ReadCloser, error) {
	filePath := filepath.Join(folder.path, filename)
	file, err := os.Open(filePath)
	if err != nil && os.IsNotExist(err) {
		return file, NewNoSuchFileError(filename)
	}
	return file, err
}

func (folder *DiskDataFolder) OpenWriteOnlyFile(filename string) (io.WriteCloser, error) {
	filePath := filepath.Join(folder.path, filename)
	return os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
}

func (folder *DiskDataFolder) CleanFolder() error {
	cleaner := FileSystemCleaner{}
	files, err := cleaner.GetFiles(folder.path)
	if err != nil {
		return err
	}
	for _, file := range files {
		cleaner.Remove(file)
	}
	return nil
}
