// +build brotli

package brotli

import (
	"github.com/google/brotli/go/cbrotli"
	"io"
)

const (
	AlgorithmName = "brotli"
	FileExtension = "br"
)

type Compressor struct{}

func (compressor Compressor) NewWriter(writer io.Writer) io.WriteCloser {
	return cbrotli.NewWriter(writer, cbrotli.WriterOptions{Quality: 3})
}

func (compressor Compressor) FileExtension() string {
	return FileExtension
}
