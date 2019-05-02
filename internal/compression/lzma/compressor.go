package lzma

import (
	"github.com/ulikunitz/xz/lzma"
	"github.com/wal-g/wal-g/internal/compression/computils"
	"io"
)

const (
	AlgorithmName = "lzma"
	FileExtension = "lzma"
)

type Compressor struct{}

func (compressor Compressor) NewWriter(writer io.Writer) computils.ReaderFromWriteCloser {
	lzmaWriter, err := lzma.NewWriter(writer)
	if err != nil {
		panic(err)
	}
	return computils.NewReaderFromWriteCloserImpl(lzmaWriter)
}

func (compressor Compressor) FileExtension() string {
	return FileExtension
}
