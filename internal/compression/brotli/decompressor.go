//go:build brotli
// +build brotli

package brotli

import (
	"io"

	"github.com/google/brotli/go/cbrotli"
	"github.com/wal-g/wal-g/internal/compression/computils"
)

type Decompressor struct{}

func (decompressor Decompressor) Decompress(src io.Reader) (io.ReadCloser, error) {
	return cbrotli.NewReader(computils.NewUntilEOFReader(src)), nil
}

func (decompressor Decompressor) FileExtension() string {
	return FileExtension
}
