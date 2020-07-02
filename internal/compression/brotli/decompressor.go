// +build brotli

package brotli

import (
	"io"

	"github.com/google/brotli/go/cbrotli"
	"github.com/wal-g/wal-g/internal/compression/computils"
	"github.com/wal-g/wal-g/utility"
)

type Decompressor struct{}

func (decompressor Decompressor) Decompress(dst io.Writer, src io.Reader) error {
	brotliReader := cbrotli.NewReader(computils.NewUntilEofReader(src))
	defer utility.LoggedClose(brotliReader, "")
	_, err := utility.FastCopy(dst, brotliReader)
	return err
}

func (decompressor Decompressor) FileExtension() string {
	return FileExtension
}
