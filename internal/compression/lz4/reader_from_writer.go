package lz4

import (
	"github.com/pierrec/lz4"
	"github.com/wal-g/wal-g/utility"
	"io"
)

type ReaderFromWriter struct {
	lz4.Writer
}

func NewReaderFromWriter(dst io.Writer) *ReaderFromWriter {
	lzWriter := lz4.NewWriter(dst)
	return &ReaderFromWriter{*lzWriter}
}

func (writer *ReaderFromWriter) ReadFrom(reader io.Reader) (n int64, err error) {
	n, err = utility.FastCopy(writer, reader)
	return
}
