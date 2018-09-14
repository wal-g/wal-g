package walg

import (
	"io"
	"os"
	"path/filepath"
)

type TemporaryDataFolder struct {
	path string
}

func NewTemporaryDataFolder(folderPath string) (*TemporaryDataFolder, error) {
	_, err := os.Stat(folderPath)
	if os.IsNotExist(err) {
		err = os.MkdirAll(folderPath, os.ModePerm)
	}
	if err != nil {
		return nil, err
	}
	return &TemporaryDataFolder{folderPath}, nil
}

func (folder *TemporaryDataFolder) openReadonlyFile(filename string) (io.ReadCloser, error) {
	filePath := filepath.Join(folder.path, filename)
	return os.Open(filePath)
}

func (folder *TemporaryDataFolder) openWriteOnlyFile(filename string) (io.WriteCloser, error) {
	filePath := filepath.Join(folder.path, filename)
	return os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
}
