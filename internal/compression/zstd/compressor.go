package zstd

import (
	"io"

	"github.com/DataDog/zstd"
)

const (
	AlgorithmName = "zstd"
	FileExtension = "zst"
)

type Compressor struct{}

func (compressor Compressor) NewWriter(writer io.Writer) io.WriteCloser {
	return zstd.NewWriterLevel(writer, 3)
}

func (compressor Compressor) FileExtension() string {
	return FileExtension
}
