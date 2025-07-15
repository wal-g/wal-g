package lz4

import (
	"io"

	"github.com/wal-g/wal-g/internal/ioextensions"

	"github.com/pierrec/lz4/v4"
)

const (
	AlgorithmName = "lz4"
	FileExtension = "lz4"
)

type Compressor struct{}

func (compressor Compressor) NewWriter(writer io.Writer) ioextensions.WriteFlushCloser {
	return lz4.NewWriter(writer)
}

func (compressor Compressor) FileExtension() string {
	return FileExtension
}
