package walg

import (
	"io"
	"gopkg.in/kothar/brotli-go.v0/enc"
)

type BrotliReaderFromWriter struct {
	enc.BrotliWriter
}

func NewBrotliReaderFromWriter(dst io.Writer) *BrotliReaderFromWriter {
	params := enc.NewBrotliParams()
	params.SetQuality(1)
	return &BrotliReaderFromWriter{BrotliWriter: *enc.NewBrotliWriter(params, dst)}
}

func (writer *BrotliReaderFromWriter) ReadFrom(reader io.Reader) (n int64, err error) {
	n, err = FastCopy(writer, reader)
	return
}
