// +build brotli

package brotli

import (
	"github.com/itchio/go-brotli/enc"
	"io"
)

const (
	AlgorithmName = "brotli"
	FileExtension = "br"
)

type Compressor struct{}

func (compressor Compressor) NewWriter(writer io.Writer) io.WriteCloser {
	return enc.NewBrotliWriter(writer, &enc.BrotliWriterOptions{Quality: 3})
}

func (compressor Compressor) FileExtension() string {
	return FileExtension
}
