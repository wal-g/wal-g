package ioextensions

import (
	"io"
	"sync"

	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func NewUnexpectedEOFLimitReader(readCloser io.ReadCloser, maxRead int64) io.ReadCloser {
	return &UnexpectedEOFLimitReader{
		source: readCloser,
		left:   maxRead,
	}
}

type UnexpectedEOFLimitReader struct {
	source io.ReadCloser
	left   int64
}

func (u *UnexpectedEOFLimitReader) Read(p []byte) (n int, err error) {
	if u.left <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	unexpectedEOF := false
	if int64(len(p)) > u.left {
		unexpectedEOF = true
		p = p[:u.left]
	}
	n, err = u.source.Read(p)
	u.left -= int64(n)
	if unexpectedEOF && err == nil {
		err = io.ErrUnexpectedEOF
	}
	return
}

func (u *UnexpectedEOFLimitReader) Close() error {
	return u.source.Close()
}

func NewNetworkErrorFolder(sourceFolder storage.Folder, networkErrorAfterByteSize int) storage.Folder {
	return &TestFolder{
		Folder:       sourceFolder,
		maxReadSize:  int64(networkErrorAfterByteSize),
		readFromFile: make(map[string]*int64),
	}
}

type TestFolder struct {
	storage.Folder
	maxReadSize  int64
	readFromFile map[string]*int64
	mutex        sync.Mutex
}

func (tf *TestFolder) ReadObject(path string) (io.ReadCloser, error) {
	reader, err := tf.Folder.ReadObject(path)
	if tf.maxReadSize != 0 {
		tf.mutex.Lock()
		defer tf.mutex.Unlock()
		if _, ok := tf.readFromFile[path]; !ok {
			tf.readFromFile[path] = new(int64)
		}
		reader = utility.NewWithSizeReadCloser(reader, tf.readFromFile[path])
		reader = NewUnexpectedEOFLimitReader(reader, *tf.readFromFile[path]+tf.maxReadSize)
	}
	return reader, err
}
