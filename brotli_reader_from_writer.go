package walg

import (
	"github.com/google/brotli/go/cbrotli"
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
	n, err = FastCopy(writer, reader)
	return
}
