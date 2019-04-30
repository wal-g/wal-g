package internal

import (
	"github.com/google/brotli/go/cbrotli"
	"github.com/wal-g/wal-g/utility"
	"io"
)

type BrotliDecompressor struct{}

func (decompressor BrotliDecompressor) Decompress(dst io.Writer, src io.Reader) error {
	brotliReader := cbrotli.NewReader(NewUntilEofReader(src))
	defer brotliReader.Close()
	_, err := utility.FastCopy(dst, brotliReader)
	return err
}

func (decompressor BrotliDecompressor) FileExtension() string {
	return BrotliFileExtension
}
