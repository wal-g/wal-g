package testtools

import (
	"bytes"
	"io"
	"io/ioutil"

	"github.com/wal-g/wal-g/internal"
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
	if _, ok := (*folder)[filename]; ok {
		return ioutil.NopCloser(bytes.NewReader((*folder)[filename].Bytes())), nil
	} else {
		return nil, internal.NewNoSuchFileError(filename)
	}
}

func (folder *MockDataFolder) OpenWriteOnlyFile(filename string) (io.WriteCloser, error) {
	file := bytes.NewBuffer(nil)
	(*folder)[filename] = file
	return &ReadWriteNopCloser{file}, nil
}
