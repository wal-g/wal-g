package testtools

import (
	"bytes"
	"github.com/wal-g/wal-g"
	"io"
	"io/ioutil"
)

type MockDataFolder map[string]*bytes.Buffer

func NewMockDataFolder(files map[string]*bytes.Buffer) *MockDataFolder {
	dataFolder := MockDataFolder(files)
	return &dataFolder
}

func (folder *MockDataFolder) IsEmpty() bool {
	return len(*folder) == 0
}

func (folder *MockDataFolder) OpenReadonlyFile(filename string) (io.ReadCloser, error) {
	if _, ok := (*folder)[filename]; ok {
		return ioutil.NopCloser((*folder)[filename]), nil
	} else {
		return nil, walg.NewNoSuchFileError(filename)
	}
}

func (folder *MockDataFolder) OpenWriteOnlyFile(filename string) (io.WriteCloser, error) {
	file := bytes.NewBuffer(nil)
	(*folder)[filename] = file
	return &ReadWriteNopCloser{file}, nil
}
