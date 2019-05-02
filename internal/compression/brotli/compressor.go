package brotli

import (
	"github.com/wal-g/wal-g/internal/compression/computils"
	"io"
)

const (
	AlgorithmName = "brotli"
	FileExtension = "br"
)

type Compressor struct{}

func (compressor Compressor) NewWriter(writer io.Writer) computils.ReaderFromWriteCloser {
	return NewReaderFromWriter(writer)
}

func (compressor Compressor) FileExtension() string {
	return FileExtension
}
