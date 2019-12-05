package internal

import (
	"io"
	"os"
	"path/filepath"
)

type DiskDataFolder struct {
	path string
}

func NewDiskDataFolder(folderPath string) (*DiskDataFolder, error) {
	err := os.MkdirAll(folderPath, os.ModePerm)
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

func (folder *DiskDataFolder) cleanFolder() error {
	cleaner := FileSystemCleaner{}
	files, err := cleaner.getFiles(folder.path)
	if err != nil {
		return err
	}
	for _, file := range files {
		cleaner.remove(filepath.Join(folder.path, file))
	}
	return nil
}

func (folder *DiskDataFolder) fileExists(filename string) bool {
	filePath := filepath.Join(folder.path, filename)
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}

func (folder *DiskDataFolder) createFile(filename string) error {
	filePath := filepath.Join(folder.path, filename)
	_, err := os.Create(filePath)

	return err
}

func (folder *DiskDataFolder) deleteFile(filename string) error {
	filePath := filepath.Join(folder.path, filename)
	return os.Remove(filePath)
}
