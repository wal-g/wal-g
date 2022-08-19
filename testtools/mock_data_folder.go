package testtools

import (
	"bytes"
	"io"

	"github.com/wal-g/wal-g/internal/fsutil"
)

type MockDataFolder map[string]*bytes.Buffer

func (folder *MockDataFolder) FileExists(filename string) bool {
	_, ok := (*folder)[filename]
	return ok
}

func (folder *MockDataFolder) DeleteFile(filename string) error {
	delete(*folder, filename)
	return nil
}

func (folder *MockDataFolder) CreateFile(filename string) error {
	(*folder)[filename] = bytes.NewBuffer(nil)
	return nil
}

func (folder *MockDataFolder) CleanFolder() error {
	return nil
}

func NewMockDataFolder() *MockDataFolder {
	dataFolder := MockDataFolder(make(map[string]*bytes.Buffer))
	return &dataFolder
}

func (folder *MockDataFolder) IsEmpty() bool {
	return len(*folder) == 0
}

func (folder *MockDataFolder) OpenReadonlyFile(filename string) (io.ReadCloser, error) {
	_, ok := (*folder)[filename]
	if ok {
		return io.NopCloser(bytes.NewReader((*folder)[filename].Bytes())), nil
	}
	return nil, fsutil.NewNoSuchFileError(filename)
}

func (folder *MockDataFolder) OpenWriteOnlyFile(filename string) (io.WriteCloser, error) {
	file := bytes.NewBuffer(nil)
	(*folder)[filename] = file
	return &ReadWriteNopCloser{file}, nil
}

func (folder *MockDataFolder) RenameFile(oldFilename string, newFilename string) error {
	return nil
}
