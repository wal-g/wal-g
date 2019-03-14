package internal

import (
	"io"

	"github.com/google/brotli/go/cbrotli"
)

type BrotliDecompressor struct{}

func (decompressor BrotliDecompressor) Decompress(dst io.Writer, src io.Reader) error {
	brotliReader := cbrotli.NewReader(NewUntilEofReader(src))
	defer brotliReader.Close()
	_, err := FastCopy(dst, brotliReader)
	return err
}

func (decompressor BrotliDecompressor) FileExtension() string {
	return BrotliFileExtension
}
