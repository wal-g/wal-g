package brotli

import (
	"github.com/google/brotli/go/cbrotli"
	"github.com/wal-g/wal-g/internal/compression/computils"
	"io"
)

const (
	AlgorithmName = "brotli"
	FileExtension = "br"
)

type Compressor struct{}

func (compressor Compressor) NewWriter(writer io.Writer) computils.ReaderFromWriteCloser {
	return computils.NewReaderFromWriteCloserImpl(cbrotli.NewWriter(writer, cbrotli.WriterOptions{Quality: 3}))
}

func (compressor Compressor) FileExtension() string {
	return FileExtension
}
