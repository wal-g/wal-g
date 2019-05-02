package brotli

import (
	"github.com/google/brotli/go/cbrotli"
	"github.com/wal-g/wal-g/utility"
	"io"
)

type ReaderFromWriter struct {
	cbrotli.Writer
}

func NewReaderFromWriter(dst io.Writer) *ReaderFromWriter {
	brotliWriter := cbrotli.NewWriter(dst, cbrotli.WriterOptions{Quality: 3})
	return &ReaderFromWriter{Writer: *brotliWriter}
}

func (writer *ReaderFromWriter) ReadFrom(reader io.Reader) (n int64, err error) {
	n, err = utility.FastCopy(writer, reader)
	return
}
