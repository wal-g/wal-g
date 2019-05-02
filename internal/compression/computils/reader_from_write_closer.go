package computils

import (
	"github.com/wal-g/wal-g/utility"
	"io"
)

type ReaderFromWriteCloser interface {
	io.ReaderFrom
	io.WriteCloser
}

type ReaderFromWriteCloserImpl struct {
	io.WriteCloser
}

func NewReaderFromWriteCloserImpl(writeCloser io.WriteCloser) ReaderFromWriteCloser {
	return &ReaderFromWriteCloserImpl{writeCloser}
}

func (writer *ReaderFromWriteCloserImpl) ReadFrom(reader io.Reader) (n int64, err error) {
	n, err = utility.FastCopy(writer, reader)
	return
}
