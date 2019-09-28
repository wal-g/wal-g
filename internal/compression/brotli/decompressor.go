// +build brotli

package brotli

import (
	"github.com/itchio/go-brotli/dec"
	"github.com/wal-g/wal-g/internal/compression/computils"
	"github.com/wal-g/wal-g/utility"
	"io"
)

type Decompressor struct{}

func (decompressor Decompressor) Decompress(dst io.Writer, src io.Reader) error {
	brotliReader := dec.NewBrotliReader(computils.NewUntilEofReader(src))
	defer utility.LoggedClose(brotliReader, "")
	_, err := utility.FastCopy(dst, brotliReader)
	return err
}

func (decompressor Decompressor) FileExtension() string {
	return FileExtension
}
