package internal

import (
	"github.com/google/brotli/go/cbrotli"
	"github.com/wal-g/wal-g/utility"
	"io"
)

type BrotliReaderFromWriter struct {
	cbrotli.Writer
}

func NewBrotliReaderFromWriter(dst io.Writer) *BrotliReaderFromWriter {
	brotliWriter := cbrotli.NewWriter(dst, cbrotli.WriterOptions{Quality: 3})
	return &BrotliReaderFromWriter{Writer: *brotliWriter}
}

func (writer *BrotliReaderFromWriter) ReadFrom(reader io.Reader) (n int64, err error) {
	n, err = utility.FastCopy(writer, reader)
	return
}
