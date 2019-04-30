package internal

import (
	"github.com/pierrec/lz4"
	"github.com/wal-g/wal-g/utility"
	"io"
)

type Lz4ReaderFromWriter struct {
	lz4.Writer
}

func NewLz4ReaderFromWriter(dst io.Writer) *Lz4ReaderFromWriter {
	lzWriter := lz4.NewWriter(dst)
	return &Lz4ReaderFromWriter{*lzWriter}
}

func (writer *Lz4ReaderFromWriter) ReadFrom(reader io.Reader) (n int64, err error) {
	n, err = utility.FastCopy(writer, reader)
	return
}
