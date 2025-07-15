//go:build brotli
// +build brotli

package brotli

import (
	"io"

	"github.com/wal-g/wal-g/internal/ioextensions"

	"github.com/google/brotli/go/cbrotli"
)

const (
	AlgorithmName = "brotli"
	FileExtension = "br"
)

type Compressor struct{}

func (compressor Compressor) NewWriter(writer io.Writer) ioextensions.WriteFlushCloser {
	return cbrotli.NewWriter(writer, cbrotli.WriterOptions{Quality: 3})
}

func (compressor Compressor) FileExtension() string {
	return FileExtension
}
