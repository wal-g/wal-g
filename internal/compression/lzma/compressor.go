package lzma

import (
	"github.com/wal-g/wal-g/internal/compression/computils"
	"io"
)

const (
	AlgorithmName = "lzma"
	FileExtension = "lzma"
)

type Compressor struct{}

func (compressor Compressor) NewWriter(writer io.Writer) computils.ReaderFromWriteCloser {
	lzmaWriter, err := NewReaderFromWriter(writer)
	if err != nil {
		panic(err.Error())
	}
	return lzmaWriter
}

func (compressor Compressor) FileExtension() string {
	return FileExtension
}
