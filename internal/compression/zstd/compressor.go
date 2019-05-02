package zstd

import (
	"github.com/DataDog/zstd"
	"github.com/wal-g/wal-g/internal/compression/computils"
	"io"
)

const (
	AlgorithmName = "zstd"
	FileExtension = "zst"
)

type Compressor struct{}

func (compressor Compressor) NewWriter(writer io.Writer) computils.ReaderFromWriteCloser {
	return computils.NewReaderFromWriteCloserImpl(zstd.NewWriterLevel(writer, 3))
}

func (compressor Compressor) FileExtension() string {
	return FileExtension
}
