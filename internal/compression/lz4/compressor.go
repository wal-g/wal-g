package lz4

import (
	"io"

	"github.com/pierrec/lz4"
)

const (
	AlgorithmName = "lz4"
	FileExtension = "lz4"
)

type Compressor struct{}

func (compressor Compressor) NewWriter(writer io.Writer) io.WriteCloser {
	return lz4.NewWriter(writer)
}

func (compressor Compressor) FileExtension() string {
	return FileExtension
}
