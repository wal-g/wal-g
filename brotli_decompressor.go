package walg

import (
	"io"
	"gopkg.in/kothar/brotli-go.v0/dec"
)

type BrotliDecompressor struct{}

func (decompressor BrotliDecompressor) Decompress(dst io.Writer, src io.Reader) error {
	brotliReader := dec.NewBrotliReader(NewUntilEofReader(src))
	defer brotliReader.Close()
	_, err := fastCopy(dst, brotliReader)
	return err
}

func (decompressor BrotliDecompressor) FileExtension() string {
	return BrotliFileExtension
}
