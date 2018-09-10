package walg

import (
	"github.com/google/brotli/go/cbrotli"
	"io"
)

type BrotliReaderFromWriter struct {
	cbrotli.Writer
}

func NewBrotliReaderFromWriter(dst io.Writer) *BrotliReaderFromWriter {
	options := cbrotli.WriterOptions{Quality: 2}
	return &BrotliReaderFromWriter{Writer: *cbrotli.NewWriter(dst, options)}
}

func (writer *BrotliReaderFromWriter) ReadFrom(reader io.Reader) (n int64, err error) {
	n, err = FastCopy(writer, reader)
	return
}
