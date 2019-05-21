package brotli

import (
	"github.com/google/brotli/go/cbrotli"
	"github.com/wal-g/wal-g/internal/compression/computils"
	"github.com/wal-g/wal-g/utility"
	"io"
)

type Decompressor struct{}

func (decompressor Decompressor) Decompress(dst io.Writer, src io.Reader) error {
	brotliReader := cbrotli.NewReader(computils.NewUntilEofReader(src))
	defer brotliReader.Close()
	_, err := utility.FastCopy(dst, brotliReader)
	return err
}

func (decompressor Decompressor) FileExtension() string {
	return FileExtension
}
